#!/usr/bin/python
# -*- coding: utf-8 -*-
#
# Copyright 2015 clowwindy
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import, division, print_function, with_statement

import binascii
import errno
import logging
import platform
import random
import socket
import struct
import threading
import time
import traceback

from collections import Counter
from shadowsocks import common, encrypt, eventloop, obfs, shell
from shadowsocks.common import (
    IPNetwork,
    PortRange,
    parse_header,
    pre_parse_header,
)

# we clear at most TIMEOUTS_CLEAN_SIZE timeouts each time
TIMEOUTS_CLEAN_SIZE = 512

MSG_FASTOPEN = 0x20000000

# SOCKS command definition
CMD_CONNECT = 1
CMD_BIND = 2
CMD_UDP_ASSOCIATE = 3

# for each opening port, we have a TCP Relay

# for each connection, we have a TCP Relay Handler to handle the connection

# for each handler, we have 2 sockets:
#    local:   connected to the client
#    remote:  connected to remote server

# for each handler, it could be at one of several stages:

# as sslocal:
# stage 0 SOCKS hello received from local, send hello to local
# stage 1 addr received from local, query DNS for remote
# stage 2 UDP assoc
# stage 3 DNS resolved, connect to remote
# stage 4 still connecting, more data from local received
# stage 5 remote connected, piping local and remote

# as ssserver:
# stage 0 just jump to stage 1
# stage 1 addr received from local, query DNS for remote
# stage 3 DNS resolved, connect to remote
# stage 4 still connecting, more data from local received
# stage 5 remote connected, piping local and remote

STAGE_INIT = 0
STAGE_ADDR = 1
STAGE_UDP_ASSOC = 2
STAGE_DNS = 3
STAGE_CONNECTING = 4
STAGE_STREAM = 5
STAGE_DESTROYED = -1

# for each handler, we have 2 stream directions:
#    upstream:    from client to server direction
#                 read local and write to remote
#    downstream:  from server to client direction
#                 read remote and write to local

STREAM_UP = 0
STREAM_DOWN = 1

# for each stream, it's waiting for reading, or writing, or both
WAIT_STATUS_INIT = 0
WAIT_STATUS_READING = 1
WAIT_STATUS_WRITING = 2
WAIT_STATUS_READWRITING = WAIT_STATUS_READING | WAIT_STATUS_WRITING

NETWORK_MTU = 1500
TCP_MSS = NETWORK_MTU - 40
BUF_SIZE = 32 * 1024
UDP_MAX_BUF_SIZE = 65536


class SpeedTester(object):

    def __init__(self, max_speed=0):
        self.max_speed = max_speed * 1024
        self.last_time = time.time()
        self.sum_len = 0

    def add(self, data_len):
        if self.max_speed > 0:
            cut_t = time.time()
            self.sum_len -= (cut_t - self.last_time) * self.max_speed
            if self.sum_len < 0:
                self.sum_len = 0
            self.last_time = cut_t
            self.sum_len += data_len

    def isExceed(self):
        if self.max_speed > 0:
            cut_t = time.time()
            self.sum_len -= (cut_t - self.last_time) * self.max_speed
            if self.sum_len < 0:
                self.sum_len = 0
            self.last_time = cut_t
            return self.sum_len >= self.max_speed
        return False

class DataStorage(object):
    def __init__(self):
        self.saved_obfs_param = []

    def add_data(self, obfs_decode):
        if obfs_decode[3] not in self.saved_obfs_param:
            self.saved_obfs_param.append(obfs_decode[3])

    def get_data(self):
        rets = self.saved_obfs_param
        self.saved_obfs_param = []
        return rets
        
# 在 TCPRelayHandler 类外部创建 DataStorage 实例
data_storage = DataStorage()

class TCPRelayHandler(object):

    def __init__(self, server, fd_to_handlers, loop, local_sock, config,
                 dns_resolver, is_local):
        self._server = server
        self._fd_to_handlers = fd_to_handlers
        self._loop = loop
        self._local_sock = local_sock
        self._remote_sock = None
        self._remote_sock_v6 = None
        self._local_sock_fd = None
        self._remote_sock_fd = None
        self._remotev6_sock_fd = None
        self._remote_udp = False
        self._config = config
        self._dns_resolver = dns_resolver
        self._current_user_id = 0
        self._add_ref = 0
        self._real_addr = ""
        if not self._create_encryptor(config):
            return

        self._client_address = local_sock.getpeername()[:2]
        self._accept_address = local_sock.getsockname()[:2]
        self._user = None
        self._update_tcp_mss(local_sock)

        # TCP Relay works as either sslocal or ssserver
        # if is_local, this is sslocal
        self._is_local = is_local
        self._encrypt_correct = True
        self._obfs = obfs.obfs(config['obfs'])
        self._protocol = obfs.obfs(config['protocol'])
        self._overhead = self._obfs.get_overhead(self._is_local) + self._protocol.get_overhead(self._is_local)
        self._recv_buffer_size = BUF_SIZE - self._overhead

        server_info = obfs.server_info(server.obfs_data)
        server_info.host = config['server']
        server_info.port = server._listen_port
        #server_info.users = server.server_users
        #server_info.update_user_func = self._update_user
        server_info.client = self._client_address[0]
        server_info.client_port = self._client_address[1]
        server_info.protocol_param = ''
        server_info.obfs_param = config['obfs_param']

        server_info.iv = self._encryptor.cipher_iv
        server_info.recv_iv = b''
        server_info.key_str = common.to_bytes(config['password'])
        server_info.key = self._encryptor.key
        server_info.head_len = 30
        server_info.tcp_mss = self._tcp_mss
        server_info.buffer_size = self._recv_buffer_size
        server_info.overhead = self._overhead
        self._obfs.set_server_info(server_info)

        server_info = obfs.server_info(server.protocol_data)
        server_info.host = config['server']
        server_info.port = server._listen_port
        if 'users_table' in config:
            server_info.users = server.multi_user_table
        else:
            server_info.users = {}
        server_info.update_user_func = self._update_user
        server_info.is_multi_user = config["is_multi_user"]
        server_info.client = self._client_address[0]
        server_info.client_port = self._client_address[1]
        server_info.protocol_param = config['protocol_param']
        server_info.obfs_param = ''
        server_info.iv = self._encryptor.cipher_iv
        server_info.recv_iv = b''
        server_info.key_str = common.to_bytes(config['password'])
        server_info.key = self._encryptor.key
        server_info.head_len = 30
        server_info.tcp_mss = self._tcp_mss
        server_info.buffer_size = self._recv_buffer_size
        server_info.overhead = self._overhead
        self._protocol.set_server_info(server_info)

        self._redir_list = config.get('redirect', ["*#0.0.0.0:0"])
        self._is_redirect = False
        self._bind = config.get('out_bind', '')
        self._bindv6 = config.get('out_bindv6', '')
        self._ignore_bind_list = config.get('ignore_bind', [])

        self._fastopen_connected = False
        self._data_to_write_to_local = []
        self._data_to_write_to_remote = []
        self._udp_data_send_buffer = b''
        self._upstream_status = WAIT_STATUS_READING
        self._downstream_status = WAIT_STATUS_INIT
        self._remote_address = None

        self._header_buf = []
        if is_local:
            self._chosen_server = self._get_a_server()

        self.last_activity = 0
        self._update_activity()
        self._server.add_connection(1)
        self._server.stat_add(self._client_address[0], 1)

        self._add_ref = 1

        self._recv_u_max_size = BUF_SIZE
        self._recv_d_max_size = BUF_SIZE
        self._recv_pack_id = 0
        self._udp_send_pack_id = 0
        self._udpv6_send_pack_id = 0

        local_sock.setblocking(False)
        local_sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
        self._local_sock_fd = local_sock.fileno()
        fd_to_handlers[self._local_sock_fd] = self
        loop.add(local_sock, eventloop.POLL_IN | eventloop.POLL_ERR, self._server)
        self._stage = STAGE_INIT
       
        self.data_storage = data_storage

    def __hash__(self):
        # default __hash__ is id / 16
        # we want to eliminate collisions
        return id(self)

    @property
    def remote_address(self):
        return self._remote_address

    def _get_a_server(self):
        server = self._config['server']
        server_port = self._config['server_port']
        if isinstance(server_port, list):
            server_port = random.choice(server_port)
        if isinstance(server, list):
            server = random.choice(server)
        logging.debug('chosen server: %s:%d', server, server_port)
        return server, server_port

    def _update_tcp_mss(self, local_sock):
        self._tcp_mss = TCP_MSS
        try:
            tcp_mss = local_sock.getsockopt(socket.SOL_TCP, socket.TCP_MAXSEG)
            if tcp_mss > 500 and tcp_mss <= 1500:
                self._tcp_mss = tcp_mss
            logging.debug("TCP MSS = %d" % (self._tcp_mss,))
        except:
            pass

    def _create_encryptor(self, config):
        try:
            self._encryptor = encrypt.Encryptor(config['password'],
                                                config['method'])
            return True
        except Exception:
            self._stage = STAGE_DESTROYED
            logging.error('create encryptor fail at port %d', self._server._listen_port)
            traceback.print_exc()

    def _update_user(self, user):
        if self._current_user_id == 0:
            self._current_user_id = int(user)
            self.mu_reset_time = self._server.mu_reset_time[self._current_user_id]
            if self._current_user_id not in self._server.mu_server_transfer_ul:
                self._server.mu_server_transfer_ul[self._current_user_id] = 0
            if self._current_user_id not in self._server.mu_server_transfer_dl:
                self._server.mu_server_transfer_dl[self._current_user_id] = 0
            if self._current_user_id not in self._server.mu_connected_iplist:
                self._server.mu_connected_iplist[self._current_user_id] = []
            if self._current_user_id not in self._server.mu_detect_log_list:
                self._server.mu_detect_log_list[self._current_user_id] = []

    def _update_activity(self, data_len=0):
        # tell the TCP Relay we have activities recently
        # else it will think we are inactive and timed out
        self._server.update_activity(self, data_len)

    def _update_stream(self, stream, status):
        # update a stream to a new waiting status

        # check if status is changed
        # only update if dirty
        dirty = False
        if stream == STREAM_DOWN:
            if self._downstream_status != status:
                self._downstream_status = status
                dirty = True
        elif stream == STREAM_UP:
            if self._upstream_status != status:
                self._upstream_status = status
                dirty = True
        if dirty:
            if self._local_sock:
                event = eventloop.POLL_ERR
                if self._downstream_status & WAIT_STATUS_WRITING:
                    event |= eventloop.POLL_OUT
                if self._upstream_status & WAIT_STATUS_READING:
                    event |= eventloop.POLL_IN
                self._loop.modify(self._local_sock, event)
            if self._remote_sock:
                event = eventloop.POLL_ERR
                if self._downstream_status & WAIT_STATUS_READING:
                    event |= eventloop.POLL_IN
                if self._upstream_status & WAIT_STATUS_WRITING:
                    event |= eventloop.POLL_OUT
                self._loop.modify(self._remote_sock, event)
                if self._remote_sock_v6:
                    self._loop.modify(self._remote_sock_v6, event)

    def _write_to_sock(self, data, sock):
        # write data to sock
        # if only some of the data are written, put remaining in the buffer
        # and update the stream to wait for writing

        if self._config['is_multi_user'] != 0 and self._current_user_id != 0:
            if self._current_user_id not in self._server.multi_user_table:
                self.destroy()
                return False
            if self._server.mu_reset_time[self._current_user_id] > self.mu_reset_time:
                self.destroy()
                return False

        if not sock:
            return False
        uncomplete = False
        if self._remote_udp and sock == self._remote_sock:
            try:
                self._udp_data_send_buffer += data
                #logging.info('UDP over TCP sendto %d %s' % (len(data), binascii.hexlify(data)))
                while len(self._udp_data_send_buffer) > 6:
                    length = struct.unpack(
                        '>H', self._udp_data_send_buffer[:2])[0]

                    if length > len(self._udp_data_send_buffer):
                        break

                    data = self._udp_data_send_buffer[:length]

                    self._udp_data_send_buffer = self._udp_data_send_buffer[
                        length:]

                    frag = common.ord(data[2])
                    if frag != 0:
                        logging.warn(
                            'drop a message since frag is %d' %
                            (frag,))
                        continue
                    else:
                        data = data[3:]
                    header_result = parse_header(data)
                    if header_result is None:
                        continue

                    connecttype, addrtype, dest_addr, dest_port, header_length = header_result
                    if (addrtype & 7) == 3:
                        af = common.is_ip(dest_addr)
                        if af == False:
                            handler = common.UDPAsyncDNSHandler(data[header_length:])
                            handler.resolve(self._dns_resolver, (dest_addr, dest_port), self._handle_server_dns_resolved)
                        else:
                            return self._handle_server_dns_resolved("", (dest_addr, dest_port), dest_addr, data[header_length:])
                    else:
                        return self._handle_server_dns_resolved("", (dest_addr, dest_port), dest_addr, data[header_length:])

            except Exception as e:
                #trace = traceback.format_exc()
                # logging.error(trace)
                error_no = eventloop.errno_from_exception(e)
                if error_no in (errno.EAGAIN, errno.EINPROGRESS,
                                errno.EWOULDBLOCK):
                    uncomplete = True
                else:
                    shell.print_exception(e)
                    logging.error(
                        "exception from %s:%d" %
                        (self._client_address[0], self._client_address[1]))
                    self.destroy()
                    return False
            return True
        else:
            try:
                if self._encrypt_correct:
                    if sock == self._remote_sock:
                        self._server.add_transfer_u(
                            self._current_user_id, len(data))
                self._update_activity(len(data))
                if data:
                    l = len(data)
                    s = sock.send(data)
                    if s < l:
                        data = data[s:]
                        uncomplete = True
                else:
                    return
            except (OSError, IOError) as e:
                error_no = eventloop.errno_from_exception(e)
                if error_no in (errno.EAGAIN, errno.EINPROGRESS,
                                errno.EWOULDBLOCK):
                    uncomplete = True
                else:
                    # traceback.print_exc()
                    shell.print_exception(e)
                    logging.error(
                        "exception from %s:%d" %
                        (self._client_address[0], self._client_address[1]))
                    self.destroy()
                    return False
            except Exception as e:
                shell.print_exception(e)
                logging.error(
                    "exception from %s:%d" %
                    (self._client_address[0],
                     self._client_address[1]))
                self.destroy()
                return False
        if uncomplete:
            if sock == self._local_sock:
                self._data_to_write_to_local.append(data)
                self._update_stream(STREAM_DOWN, WAIT_STATUS_WRITING)
            elif sock == self._remote_sock:
                self._data_to_write_to_remote.append(data)
                self._update_stream(STREAM_UP, WAIT_STATUS_WRITING)
            else:
                logging.error(
                    'write_all_to_sock:unknown socket from %s:%d' %
                    (self._client_address[0], self._client_address[1]))
        else:
            if sock == self._local_sock:
                self._update_stream(STREAM_DOWN, WAIT_STATUS_READING)
            elif sock == self._remote_sock:
                self._update_stream(STREAM_UP, WAIT_STATUS_READING)
            else:
                logging.error(
                    'write_all_to_sock:unknown socket from %s:%d' %
                    (self._client_address[0], self._client_address[1]))
        return True

    def _handle_server_dns_resolved(self, error, remote_addr, server_addr, data):
        if error:
            return
        try:
            addrs = socket.getaddrinfo(server_addr, remote_addr[1], 0, socket.SOCK_DGRAM, socket.SOL_UDP)
            if not addrs: # drop
                return
            af, socktype, proto, canonname, sa = addrs[0]
            if af == socket.AF_INET6:
                self._remote_sock_v6.sendto(data, (server_addr, remote_addr[1]))
                if self._udpv6_send_pack_id == 0:
                    addr, port = self._remote_sock_v6.getsockname()[:2]
                    common.connect_log('UDPv6 sendto %s(%s):%d from %s:%d by user %d' %
                        (common.to_str(remote_addr[0]), common.to_str(server_addr), remote_addr[1], addr, port, self._current_user_id))
                self._udpv6_send_pack_id += 1
            else:
                self._remote_sock.sendto(data, (server_addr, remote_addr[1]))
                if self._udp_send_pack_id == 0:
                    addr, port = self._remote_sock.getsockname()[:2]
                    common.connect_log('UDP sendto %s(%s):%d from %s:%d by user %d' %
                        (common.to_str(remote_addr[0]), common.to_str(server_addr), remote_addr[1], addr, port, self._current_user_id))
                self._udp_send_pack_id += 1
            return True
        except Exception as e:
            shell.print_exception(e)
            logging.error("exception from %s:%d" % (self._client_address[0], self._client_address[1]))

    def _get_redirect_host(self, client_address, ogn_data):
        host_list = self._redir_list or ["*#0.0.0.0:0"]

        if not isinstance(host_list, list):
            host_list = [host_list]

        items_sum = common.to_str(host_list[0]).rsplit('#', 1)
        if len(items_sum) < 2:
            hash_code = binascii.crc32(ogn_data)
            addrs = socket.getaddrinfo(
                client_address[0],
                client_address[1],
                0,
                socket.SOCK_STREAM,
                socket.SOL_TCP)
            af, socktype, proto, canonname, sa = addrs[0]
            address_bytes = common.inet_pton(af, sa[0])
            if af == socket.AF_INET6:
                addr = struct.unpack('>Q', address_bytes[8:])[0]
            elif af == socket.AF_INET:
                addr = struct.unpack('>I', address_bytes)[0]
            else:
                addr = 0

            host_port = []
            match_port = False
            for host in host_list:
                items = common.to_str(host).rsplit(':', 1)
                if len(items) > 1:
                    try:
                        port = int(items[1])
                        if port == self._server._listen_port:
                            match_port = True
                        host_port.append((items[0], port))
                    except:
                        pass
                else:
                    host_port.append((host, 80))

            if match_port:
                last_host_port = host_port
                host_port = []
                for host in last_host_port:
                    if host[1] == self._server._listen_port:
                        host_port.append(host)

            return host_port[
                ((hash_code & 0xffffffff) + addr) %
                len(host_port)]

        else:
            host_port = []
            for host in host_list:
                items_sum = common.to_str(host).rsplit('#', 1)
                items_match = common.to_str(items_sum[0]).rsplit(':', 1)
                items = common.to_str(items_sum[1]).rsplit(':', 1)
                if len(items_match) > 1:
                    if items_match[1] != "*":
                        try:
                            if self._server._listen_port != int(items_match[1]) and int(items_match[1]) != 0:
                                continue
                        except:
                            pass

                if items_match[0] != "*" and common.match_regex(
                        items_match[0], ogn_data) == False:
                    continue
                if len(items) > 1:
                    try:
                        port = int(items[1])
                        return (items[0], port)
                    except:
                        pass
                else:
                    return (items[0], 80)

            return ("0.0.0.0", 0)

    def _handel_protocol_error(self, client_address, ogn_data):
        if self._config['redirect_verbose']:
            logging.warn(
                "Protocol ERROR, TCP ogn data %s from %s:%d via port %d" %
                (binascii.hexlify(ogn_data),
                 client_address[0],
                 client_address[1],
                 self._server._listen_port))
        if client_address[0] not in self._server.wrong_iplist and client_address[
                0] != 0 and self._server.is_cleaning_wrong_iplist == False:
            self._server.wrong_iplist[client_address[0]] = time.time()
        self._encrypt_correct = False
        # create redirect or disconnect by hash code
        host, port = self._get_redirect_host(client_address, ogn_data)
        if port == 0:
            raise Exception('can not parse header')
        data = b"\x03" + common.to_bytes(common.chr(len(host))) + \
            common.to_bytes(host) + struct.pack('>H', port)
        if self._config['redirect_verbose']:
            logging.warn(
                "TCP data redir %s:%d %s" %
                (host, port, binascii.hexlify(data)))
        self._is_redirect = True
        return data + ogn_data

    def _handel_mu_protocol_error(self, client_address, ogn_data):
        if self._config['redirect_verbose']:
            logging.warn(
                "Protocol ERROR, TCP ogn data %s from %s:%d via port %d" %
                (binascii.hexlify(ogn_data),
                 client_address[0],
                 client_address[1],
                 self._server._listen_port))
        if client_address[0] not in self._server.wrong_iplist and client_address[
                0] != 0 and self._server.is_cleaning_wrong_iplist == False:
            self._server.wrong_iplist[client_address[0]] = time.time()
        self._encrypt_correct = False
        # create redirect or disconnect by hash code
        host, port = self._get_redirect_host(client_address, ogn_data)
        if port == 0:
            raise Exception('can not parse header')
        data = b"\x03" + common.to_bytes(common.chr(len(host))) + \
            common.to_bytes(host) + struct.pack('>H', port)
        if self._config['redirect_verbose']:
            logging.warn(
                "TCP data mu redir %s:%d %s" %
                (host, port, binascii.hexlify(data)))
        self._is_redirect = True
        return data + ogn_data

    def _handle_stage_connecting(self, data):
        if self._is_local:
            if self._encryptor is not None:
                data = self._protocol.client_pre_encrypt(data)
                data = self._encryptor.encrypt(data)
                data = self._obfs.client_encode(data)
        if data:
            self._data_to_write_to_remote.append(data)
        if self._is_local and not self._fastopen_connected and \
                self._config['fast_open']:
            # for sslocal and fastopen, we basically wait for data and use
            # sendto to connect
            try:
                # only connect once
                self._fastopen_connected = True
                remote_sock = \
                    self._create_remote_socket(self._chosen_server[0],
                                               self._chosen_server[1])
                self._loop.add(remote_sock, eventloop.POLL_ERR, self._server)
                data = b''.join(self._data_to_write_to_remote)
                l = len(data)
                s = remote_sock.sendto(data, MSG_FASTOPEN, self._chosen_server)
                if s < l:
                    data = data[s:]
                    self._data_to_write_to_remote = [data]
                else:
                    self._data_to_write_to_remote = []
                self._update_stream(STREAM_UP, WAIT_STATUS_READWRITING)
            except (OSError, IOError) as e:
                if eventloop.errno_from_exception(e) == errno.EINPROGRESS:
                    # in this case data is not sent at all
                    self._update_stream(STREAM_UP, WAIT_STATUS_READWRITING)
                elif eventloop.errno_from_exception(e) == errno.ENOTCONN:
                    logging.error('fast open not supported on this OS')
                    self._config['fast_open'] = False
                    self.destroy()
                else:
                    shell.print_exception(e)
                    if self._config['verbose']:
                        traceback.print_exc()
                    logging.error(
                        "exception from %s:%d" %
                        (self._client_address[0], self._client_address[1]))
                    self.destroy()

    def _get_head_size(self, buf, def_value):
        if len(buf) < 2:
            return def_value
        head_type = common.ord(buf[0]) & 0xF
        if head_type == 1:
            return 7
        if head_type == 4:
            return 19
        if head_type == 3:
            return 4 + common.ord(buf[1])
        return def_value

    def _handle_stage_addr(self, ogn_data, data):
        is_error = False
        try:
            if self._is_local:
                cmd = common.ord(data[1])
                if cmd == CMD_UDP_ASSOCIATE:
                    logging.debug('UDP associate')
                    if self._local_sock.family == socket.AF_INET6:
                        header = b'\x05\x00\x00\x04'
                    else:
                        header = b'\x05\x00\x00\x01'
                    addr, port = self._local_sock.getsockname()[:2]
                    addr_to_send = socket.inet_pton(self._local_sock.family,
                                                    addr)
                    port_to_send = struct.pack('>H', port)
                    self._write_to_sock(header + addr_to_send + port_to_send,
                                        self._local_sock)
                    self._stage = STAGE_UDP_ASSOC
                    # just wait for the client to disconnect
                    return
                elif cmd == CMD_CONNECT:
                    # just trim VER CMD RSV
                    data = data[3:]
                else:
                    logging.error('invalid command %d', cmd)
                    self.destroy()
                    return

            before_parse_data = data
            if self._is_local:
                header_result = parse_header(data)
            else:
                data = pre_parse_header(data)
                if data is None:
                    is_error = True
                    data = self._handel_protocol_error(
                        self._client_address, ogn_data)
                header_result = parse_header(data)
                if header_result is not None:
                    try:
                        common.to_str(header_result[2])
                    except Exception as e:
                        header_result = None
                if header_result is None:
                    is_error = True
                    data = self._handel_protocol_error(
                        self._client_address, ogn_data)
                    header_result = parse_header(data)
                self._overhead = self._obfs.get_overhead(self._is_local) + self._protocol.get_overhead(self._is_local)
                self._recv_buffer_size = BUF_SIZE - self._overhead
                server_info = self._obfs.get_server_info()
                server_info.buffer_size = self._recv_buffer_size
                server_info = self._protocol.get_server_info()
                server_info.buffer_size = self._recv_buffer_size
            connecttype, addrtype, remote_addr, remote_port, header_length = header_result
            if not self._server._connect_hex_data:
                common.connect_log(
                    '%s connecting %s:%d from %s:%d via port %d' %
                    ((connecttype == 0) and 'TCP' or 'UDP',
                     common.to_str(remote_addr),
                        remote_port,
                        self._client_address[0],
                        self._client_address[1],
                        self._server._listen_port))
            if connecttype != 0:
                pass
                #common.connect_log('UDP over TCP by user %d' %
                #        (self._user_id, ))
            else:
                common.connect_log(
                    '%s connecting %s:%d from %s:%d via port %d,hex data : %s' %
                    ((connecttype == 0) and 'TCP' or 'UDP',
                     common.to_str(remote_addr),
                        remote_port,
                        self._client_address[0],
                        self._client_address[1],
                        self._server._listen_port,
                        binascii.hexlify(data)))
            if not is_error:
                if not self._server.is_pushing_detect_text_list:
                    for id in self._server.detect_text_list:
                        if common.match_regex(
                                self._server.detect_text_list[id]['regex'], str(data)):
                            if self._config[
                                    'is_multi_user'] != 0 and self._current_user_id != 0:
                                if self._server.is_cleaning_mu_detect_log_list == False and id not in self._server.mu_detect_log_list[
                                        self._current_user_id]:
                                    self._server.mu_detect_log_list[
                                        self._current_user_id].append(id)
                            else:
                                if self._server.is_cleaning_detect_log == False and id not in self._server.detect_log_list:
                                    self._server.detect_log_list.append(id)
                            self._handle_detect_rule_match(remote_port)
                            raise Exception(
                                'This connection match the regex: id:%d was reject,regex: %s ,%s connecting %s:%d from %s:%d via port %d' %
                                (self._server.detect_text_list[id]['id'],
                                 self._server.detect_text_list[id]['regex'],
                                 (connecttype == 0) and 'TCP' or 'UDP',
                                    common.to_str(remote_addr),
                                    remote_port,
                                    self._client_address[0],
                                    self._client_address[1],
                                    self._server._listen_port))
                if not self._server.is_pushing_detect_hex_list:
                    for id in self._server.detect_hex_list:
                        if common.match_regex(
                                self._server.detect_hex_list[id]['regex'],
                                binascii.hexlify(data)):
                            if self._config[
                                    'is_multi_user'] != 0 and self._current_user_id != 0:
                                if self._server.is_cleaning_mu_detect_log_list == False and id not in self._server.mu_detect_log_list[
                                        self._current_user_id]:
                                    self._server.mu_detect_log_list[
                                        self._current_user_id].append(id)
                            else:
                                if self._server.is_cleaning_detect_log == False and id not in self._server.detect_log_list:
                                    self._server.detect_log_list.append(id)
                            self._handle_detect_rule_match(remote_port)
                            raise Exception(
                                'This connection match the regex: id:%d was reject,regex: %s ,connecting %s:%d from %s:%d via port %d' %
                                (self._server.detect_hex_list[id]['id'],
                                 self._server.detect_hex_list[id]['regex'],
                                 common.to_str(remote_addr),
                                    remote_port,
                                    self._client_address[0],
                                    self._client_address[1],
                                    self._server._listen_port))

                ip = self._real_addr or common.getRealIp(self._client_address[0])
                if self._config['is_multi_user'] == 0 and ip not in self._server.connected_iplist and self._client_address[0] != 0 and self._server.is_cleaning_connected_iplist == False:
                    self._server.connected_iplist.append(ip)

                if self._config['is_multi_user'] != 0 and self._current_user_id != 0:
                    if ip not in self._server.mu_connected_iplist[self._current_user_id] and self._client_address[0] != 0:
                        self._server.mu_connected_iplist[self._current_user_id].append(ip)

                # TODO: ???
                if self._client_address[0] in self._server.wrong_iplist and self._client_address[
                        0] != 0 and self._server.is_cleaning_wrong_iplist == False:
                    del self._server.wrong_iplist[self._client_address[0]]

            self._remote_address = (common.to_str(remote_addr), remote_port)
            self._remote_udp = (connecttype != 0)
            # pause reading
            self._update_stream(STREAM_UP, WAIT_STATUS_WRITING)
            self._stage = STAGE_DNS
            if self._is_local:
                # forward address to remote
                self._write_to_sock((b'\x05\x00\x00\x01'
                                     b'\x00\x00\x00\x00\x10\x10'),
                                    self._local_sock)
                head_len = self._get_head_size(data, 30)
                self._obfs.obfs.server_info.head_len = head_len
                self._protocol.obfs.server_info.head_len = head_len
                if self._encryptor is not None:
                    data = self._protocol.client_pre_encrypt(data)
                    data_to_send = self._encryptor.encrypt(data)
                    data_to_send = self._obfs.client_encode(data_to_send)
                if data_to_send:
                    self._data_to_write_to_remote.append(data_to_send)
                # notice here may go into _handle_dns_resolved directly
                self._dns_resolver.resolve(self._chosen_server[0],
                                           self._handle_dns_resolved)
            else:
                if len(data) > header_length:
                    self._data_to_write_to_remote.append(data[header_length:])
                # notice here may go into _handle_dns_resolved directly
                self._dns_resolver.resolve(remote_addr,
                                           self._handle_dns_resolved)
        except Exception as e:
            self._log_error(e)
            if self._config['verbose']:
                traceback.print_exc()
            self.destroy()

    def _socket_bind_addr(self, sock, af):
        bind_addr = ''
        if self._bind and af == socket.AF_INET:
            bind_addr = self._bind
        elif self._bindv6 and af == socket.AF_INET6:
            bind_addr = self._bindv6
        else:
            bind_addr = self._accept_address[0]
            bind_addr = bind_addr.replace("::ffff:", "")

        #if bind_addr in self._ignore_bind_list:
         #   bind_addr = None

        if bind_addr:
            local_addrs = socket.getaddrinfo(
                bind_addr, 0, 0, socket.SOCK_STREAM, socket.SOL_TCP)
            if local_addrs[0][0] == af:
                logging.debug("bind %s" % (bind_addr,))
                sock.bind((bind_addr, 0))

    def _create_remote_socket(self, ip, port):
        if self._remote_udp:
            addrs_v6 = socket.getaddrinfo(
                "::", 0, 0, socket.SOCK_DGRAM, socket.SOL_UDP)
            addrs = socket.getaddrinfo(
                "0.0.0.0", 0, 0, socket.SOCK_DGRAM, socket.SOL_UDP)
        else:
            addrs = socket.getaddrinfo(
                ip, port, 0, socket.SOCK_STREAM, socket.SOL_TCP)
        if len(addrs) == 0:
            raise Exception("getaddrinfo failed for %s:%d" % (ip, port))
        af, socktype, proto, canonname, sa = addrs[0]

        if not self._remote_udp and self._is_redirect == False:
            if self._server._config[
                    "is_multi_user"] != 0 and self._current_user_id != 0:
                if self._server.multi_user_table[
                        self._current_user_id]['_forbidden_iplist']:
                    if common.to_str(sa[0]) in self._server.multi_user_table[
                            self._current_user_id]['_forbidden_iplist']:
                        if self._remote_address:
                            raise Exception(
                                'IP %s is in forbidden list, when connect to %s:%d via port %d' %
                                (common.to_str(
                                    sa[0]),
                                    self._remote_address[0],
                                    self._remote_address[1],
                                    self._server.multi_user_table[
                                    self._current_user_id]['port']))
                        raise Exception('IP %s is in forbidden list, reject' %
                                        common.to_str(sa[0]))
                if self._server.multi_user_table[
                        self._current_user_id]['_forbidden_portset']:
                    if sa[1] in self._server.multi_user_table[
                            self._current_user_id]['_forbidden_portset']:
                        if self._remote_address:
                            raise Exception(
                                'Port %d is in forbidden list, when connect to %s:%d via port %d' %
                                (sa[1],
                                 self._remote_address[0],
                                    self._remote_address[1],
                                    self._server.multi_user_table[
                                    self._current_user_id]['port']))
                        raise Exception(
                            'Port %d is in forbidden list, reject' %
                            sa[1])

            else:
                if self._server._forbidden_iplist:
                    if common.to_str(sa[0]) in self._server._forbidden_iplist:
                        if self._remote_address:
                            raise Exception(
                                'IP %s is in forbidden list, when connect to %s:%d via port %d' %
                                (self._client_address[0],
                                    self._remote_address[0],
                                    self._remote_address[1],
                                    self._server._listen_port))
                        raise Exception('IP %s is in forbidden list, reject' %
                                        common.to_str(sa[0]))
                if self._server._forbidden_portset:
                    if sa[1] in self._server._forbidden_portset:
                        if self._remote_address:
                            raise Exception(
                                'Port %d is in forbidden list, when connect to %s:%d via port %d' %
                                (sa[1],
                                 self._remote_address[0],
                                    self._remote_address[1],
                                    self._server._listen_port))
                        raise Exception(
                            'Port %d is in forbidden list, reject' %
                            sa[1])
        remote_sock = socket.socket(af, socktype, proto)
        self._remote_sock = remote_sock
        self._remote_sock_fd = remote_sock.fileno()
        self._fd_to_handlers[self._remote_sock_fd] = self

        if self._remote_udp:
            af, socktype, proto, canonname, sa = addrs_v6[0]
            remote_sock_v6 = socket.socket(af, socktype, proto)
            self._remote_sock_v6 = remote_sock_v6
            self._remotev6_sock_fd = remote_sock_v6.fileno()
            self._fd_to_handlers[self._remotev6_sock_fd] = self

        remote_sock.setblocking(False)
        if self._remote_udp:
            remote_sock_v6.setblocking(False)

            if not self._is_local:
                self._socket_bind_addr(remote_sock, af)
                self._socket_bind_addr(remote_sock_v6, af)
        else:
            remote_sock.setsockopt(socket.SOL_TCP, socket.TCP_NODELAY, 1)
            if not self._is_local:
                self._socket_bind_addr(remote_sock, af)
        return remote_sock

    def _handle_dns_resolved(self, result, error):
        if error:
            self._log_error(error)
            self.destroy()
            return
        if result:
            ip = result[1]
            if ip:

                try:
                    self._stage = STAGE_CONNECTING
                    remote_addr = ip
                    if self._is_local:
                        remote_port = self._chosen_server[1]
                    else:
                        remote_port = self._remote_address[1]

                    if self._is_local and self._config['fast_open']:
                        # for fastopen:
                        # wait for more data to arrive and send them in one SYN
                        self._stage = STAGE_CONNECTING
                        # we don't have to wait for remote since it's not
                        # created
                        self._update_stream(STREAM_UP, WAIT_STATUS_READING)
                        # TODO when there is already data in this packet
                    else:
                        # else do connect
                        remote_sock = self._create_remote_socket(remote_addr,
                                                                 remote_port)
                        if self._remote_udp:
                            self._loop.add(remote_sock,
                                           eventloop.POLL_IN,
                                           self._server)
                            if self._remote_sock_v6:
                                self._loop.add(self._remote_sock_v6,
                                               eventloop.POLL_IN,
                                               self._server)
                        else:
                            try:
                                remote_sock.connect(
                                    (remote_addr, remote_port))
                            except (OSError, IOError) as e:
                                if eventloop.errno_from_exception(e) in (
                                        errno.EINPROGRESS, errno.EWOULDBLOCK):
                                    pass  # always goto here
                                else:
                                    raise e

                            addr, port = self._remote_sock.getsockname()[:2]
                            common.connect_log('TCP connecting %s(%s):%d from %s:%d by user %d' %
                                (common.to_str(self._remote_address[0]), common.to_str(remote_addr), remote_port, addr, port, self._current_user_id))

                            self._loop.add(remote_sock,
                                       eventloop.POLL_ERR | eventloop.POLL_OUT,
                                       self._server)
                        self._stage = STAGE_CONNECTING
                        self._update_stream(STREAM_UP, WAIT_STATUS_READWRITING)
                        self._update_stream(STREAM_DOWN, WAIT_STATUS_READING)
                        if self._remote_udp:
                            while self._data_to_write_to_remote:
                                data = self._data_to_write_to_remote[0]
                                del self._data_to_write_to_remote[0]
                                self._write_to_sock(data, self._remote_sock)
                    return
                except Exception as e:
                    shell.print_exception(e)
                    if self._config['verbose']:
                        traceback.print_exc()
                    logging.error(
                        "exception from %s:%d" %
                        (self._client_address[0], self._client_address[1]))
        self.destroy()

    def is_match_relay_rule_mu(self):
        host, port = self._get_mu_relay_host('')
        if host is None:
            return False
        else:
            return True

    def _get_read_size(self, sock, recv_buffer_size, up):
        if self._overhead == 0:
            return recv_buffer_size
        buffer_size = len(sock.recv(recv_buffer_size, socket.MSG_PEEK))
        frame_size = self._tcp_mss - self._overhead
        if up:
            buffer_size = min(buffer_size, self._recv_u_max_size)
            self._recv_u_max_size = min(self._recv_u_max_size + frame_size, BUF_SIZE)
        else:
            buffer_size = min(buffer_size, self._recv_d_max_size)
            self._recv_d_max_size = min(self._recv_d_max_size + frame_size, BUF_SIZE)
        if buffer_size == recv_buffer_size:
            return buffer_size
        if buffer_size > frame_size:
            buffer_size = int(buffer_size / frame_size) * frame_size
        return buffer_size

    def _handle_detect_rule_match(self, port):
        if port == 80 and self._config['friendly_detect']:
            backdata = b'HTTP/1.0 200 OK\r\nConnection: close\r\nContent-Type: text/html; charset=utf-8\r\n\r\n' + self._config['detect_block_html']
            backdata = self._protocol.server_pre_encrypt(backdata)
            backdata = self._encryptor.encrypt(backdata)
            backdata = self._obfs.server_encode(backdata)
            self._write_to_sock(backdata, self._local_sock)
    
    def save_data(self, obfs_decode):
        self.data_storage.add_data(obfs_decode)
        
    def _on_local_read(self):
        # handle all local read events and dispatch them to methods for
        # each stage
        try:
            if not self._local_sock:
                return
            is_local = self._is_local
            if is_local:
                recv_buffer_size = self._get_read_size(self._local_sock, self._recv_buffer_size, True)
            else:
                recv_buffer_size = BUF_SIZE
            is_Failed = False
            data = None
            try:
                data = self._local_sock.recv(recv_buffer_size)
            except (OSError, IOError) as e:
                if eventloop.errno_from_exception(e) in \
                        (errno.ETIMEDOUT, errno.EAGAIN, errno.EWOULDBLOCK):
                    return
            if not data:
                self.destroy()
                return

            self._server.speed_tester_u.add(len(data))
            if self._current_user_id != 0 and self._server._config[
                    "is_multi_user"] != 0:
                self._server.mu_speed_tester_u[
                    self._current_user_id].add(len(data))

            ogn_data = data

            if self._stage == STAGE_INIT and data[0:12] == b'\x0D\x0A\x0D\x0A\x00\x0D\x0A\x51\x55\x49\x54\x0A':
                addr_len = int.from_bytes(data[14:16], byteorder='big')
                if addr_len == 12:
                    addr = data[16:20]
                    self._real_addr = ".".join([str(x) for x in addr])
                data = data[16 + addr_len:]
                if len(data) == 0:
                    return

            if self._encryptor is not None:
                if self._encrypt_correct:
                    host = ''
                    try:
                        obfs_decode = self._obfs.server_decode(data)
                        if self._stage == STAGE_INIT:
                            self._overhead = self._obfs.get_overhead(self._is_local) + self._protocol.get_overhead(self._is_local)
                            server_info = self._protocol.get_server_info()
                            server_info.overhead = self._overhead
                    except Exception as e:
                        shell.print_exception(e)
                        logging.error(
                            "exception from %s:%d" %
                            (self._client_address[0], self._client_address[1]))
                        self.destroy()
                        return
                    need_sendback = False
                    if obfs_decode[2]:
                        host_name = ''
                        if self._server._config[
                                "is_multi_user"] == 1 and self._current_user_id == 0:
                            if self._server._config[
                                    "obfs"] == b"tls1.2_ticket_auth" or self._server._config[
                                            "obfs"] == b"tls1.2_ticket_fastauth":
                                if(len(obfs_decode) > 3):
                                    host = obfs_decode[3] + ":" + str(self._server._listen_port)
                        need_sendback = True
                    if obfs_decode[1]:
                        if self._server._config[
                                "is_multi_user"] == 1 and self._current_user_id == 0:
                            if self._server._config["obfs"] in [b"http_simple", b"http_post", b"simple_obfs_tls", b"simple_obfs_http"]:
                                if(len(obfs_decode) > 3):
                                    host = obfs_decode[3]
                        if not self._protocol.obfs.server_info.recv_iv:
                            iv_len = len(
                                self._protocol.obfs.server_info.iv)
                            self._protocol.obfs.server_info.recv_iv = obfs_decode[0][:iv_len]
                        try:
                            data = self._encryptor.decrypt(obfs_decode[0])
                        except Exception as e:
                            logging.error(
                                "decrypt data failed, exception from %s:%d" %
                                (self._client_address[0], self._client_address[1]))
                            data = [0]
                    else:
                        self.save_data(obfs_decode)
                        data = obfs_decode[0]

                    if self._server._config[
                                "is_multi_user"] == 1 and self._current_user_id == 0:
                        try:
                            host_list = host.split(":", 2)
                            host_name = host_list[0]
                            if host_name in self._server.multi_user_host_table:
                                self._update_user(
                                    self._server.multi_user_host_table[host_name])
                            else:
                                logging.error(
                                    'The host:%s md5 is mismatch,so The connection has been rejected, when connect from %s:%d via port %d' %
                                    (host_name, self._client_address[0], self._client_address[1], self._server._listen_port))
                                is_Failed = True
                        except Exception as e:
                            logging.error(
                                'The mu hostname is error,so The connection has been rejected, when connect from %s:%d via port %d' %
                                (self._client_address[0], self._client_address[1], self._server._listen_port))
                            is_Failed = True

                    try:
                        data, sendback = self._protocol.server_post_decrypt(data)

                        if self._server._config[
                                "is_multi_user"] == 2 and self._current_user_id == 0 and data:
                            logging.error(
                                'The port is multi user in single port only , but the key remote provided is error or empty, so The connection has been rejected, when connect from %s:%d via port %d' %
                                (self._client_address[0], self._client_address[1], self._server._listen_port))
                            is_Failed = True

                        if self._server._config[
                                "is_multi_user"] == 2 and self._current_user_id == 0 and ogn_data:
                            self._header_buf = ogn_data[:]

                        if need_sendback:
                            data_sendback = self._obfs.server_encode(b'')
                            try:
                                self._write_to_sock(
                                    data_sendback, self._local_sock)
                            except Exception as e:
                                shell.print_exception(e)
                                if self._config['verbose']:
                                    traceback.print_exc()
                                logging.error(
                                    "exception from %s:%d" %
                                    (self._client_address[0], self._client_address[1]))
                                self.destroy()
                                return

                        if sendback:
                            backdata = self._protocol.server_pre_encrypt(b'')
                            backdata = self._encryptor.encrypt(backdata)
                            backdata = self._obfs.server_encode(backdata)
                            try:
                                self._write_to_sock(
                                    backdata, self._local_sock)
                            except Exception as e:
                                shell.print_exception(e)
                                if self._config['verbose']:
                                    traceback.print_exc()
                                logging.error(
                                    "exception from %s:%d" %
                                    (self._client_address[0], self._client_address[1]))
                                self.destroy()
                                return
                    except Exception as e:
                        shell.print_exception(e)
                        logging.error(
                            "exception from %s:%d" %
                            (self._client_address[0], self._client_address[1]))
                        self.destroy()

                    if is_Failed:
                        data = self._handel_mu_protocol_error(
                            self._client_address, ogn_data)

            else:
                return
            if not data:
                return
        except Exception as e:
            self._log_error(e)
            if self._config['verbose']:
                traceback.print_exc()
            self.destroy()
        if self._stage == STAGE_STREAM:
            if self._is_local:
                if self._encryptor is not None:
                    data = self._protocol.client_pre_encrypt(data)
                    data = self._encryptor.encrypt(data)
                    data = self._obfs.client_encode(data)
            self._write_to_sock(data, self._remote_sock)
        elif is_local and self._stage == STAGE_INIT:
            # TODO check auth method
            self._write_to_sock(b'\x05\00', self._local_sock)
            self._stage = STAGE_ADDR
        elif self._stage == STAGE_CONNECTING:
            self._handle_stage_connecting(data)
        elif (is_local and self._stage == STAGE_ADDR) or \
                (not is_local and self._stage == STAGE_INIT):
            self._handle_stage_addr(ogn_data, data)

    def _on_remote_read(self, is_remote_sock):
        if self._config['is_multi_user'] != 0 and self._current_user_id != 0:
            if self._current_user_id not in self._server.multi_user_table:
                self.destroy()
                return
            if self._server.mu_reset_time[self._current_user_id] > self.mu_reset_time:
                self.destroy()
                return

        # handle all remote read events
        data = None
        try:
            if self._remote_udp:
                if is_remote_sock:
                    data, addr = self._remote_sock.recvfrom(UDP_MAX_BUF_SIZE)
                else:
                    data, addr = self._remote_sock_v6.recvfrom(
                        UDP_MAX_BUF_SIZE)
                port = struct.pack('>H', addr[1])
                try:
                    ip = socket.inet_aton(addr[0])
                    data = b'\x00\x01' + ip + port + data
                except Exception as e:
                    ip = socket.inet_pton(socket.AF_INET6, addr[0])
                    data = b'\x00\x04' + ip + port + data
                size = len(data) + 2

                data = struct.pack('>H', size) + data
                #logging.info('UDP over TCP recvfrom %s:%d %d bytes to %s:%d' % (addr[0], addr[1], len(data), self._client_address[0], self._client_address[1]))
            else:
                if self._is_local:
                    recv_buffer_size = BUF_SIZE
                else:
                    recv_buffer_size = self._get_read_size(self._remote_sock, self._recv_buffer_size, False)
                data = self._remote_sock.recv(recv_buffer_size)
                self._recv_pack_id += 1
        except (OSError, IOError) as e:
            if eventloop.errno_from_exception(e) in (
                    errno.ETIMEDOUT,
                    errno.EAGAIN,
                    errno.EWOULDBLOCK,
                    10035):  # errno.WSAEWOULDBLOCK
                return
        if not data:
            self.destroy()
            return

        self._server.speed_tester_d.add(len(data))
        if self._current_user_id != 0 and self._server._config[
                "is_multi_user"] != 0:
            self._server.mu_speed_tester_d[
                self._current_user_id].add(len(data))

        if self._encryptor is not None:
            if self._is_local:
                try:
                    obfs_decode = self._obfs.client_decode(data)
                except Exception as e:
                    shell.print_exception(e)
                    logging.error(
                        "exception from %s:%d" %
                        (self._client_address[0], self._client_address[1]))
                    self.destroy()
                    return
                if obfs_decode[1]:
                    send_back = self._obfs.client_encode(b'')
                    self._write_to_sock(send_back, self._remote_sock)
                if not self._protocol.obfs.server_info.recv_iv:
                    iv_len = len(self._protocol.obfs.server_info.iv)
                    self._protocol.obfs.server_info.recv_iv = obfs_decode[
                        0][:iv_len]
                try:
                    data = self._encryptor.decrypt(obfs_decode[0])
                except Exception as e:
                    logging.error(
                        "decrypt data failed, exception from %s:%d" %
                        (self._client_address[0], self._client_address[1]))
                    self.destroy()
                    return
                try:
                    data = self._protocol.client_post_decrypt(data)
                    if self._recv_pack_id == 1:
                        self._tcp_mss = self._protocol.get_server_info().tcp_mss
                except Exception as e:
                    shell.print_exception(e)
                    logging.error(
                        "exception from %s:%d" %
                        (self._client_address[0], self._client_address[1]))
                    self.destroy()
                    return
            else:
                if self._encrypt_correct:
                    data = self._protocol.server_pre_encrypt(data)
                    data = self._encryptor.encrypt(data)
                    data = self._obfs.server_encode(data)
            if self._encrypt_correct:
                self._server.add_transfer_d(self._current_user_id, len(data))
            self._update_activity(len(data))
        else:
            return
        try:
            self._write_to_sock(data, self._local_sock)
        except Exception as e:
            shell.print_exception(e)
            if self._config['verbose']:
                traceback.print_exc()
            logging.error(
                "exception from %s:%d" %
                (self._client_address[0],
                 self._client_address[1]))
            self.destroy()

    def _on_local_write(self):
        # handle local writable event
        if self._data_to_write_to_local:
            data = b''.join(self._data_to_write_to_local)
            self._data_to_write_to_local = []
            self._write_to_sock(data, self._local_sock)
        else:
            self._update_stream(STREAM_DOWN, WAIT_STATUS_READING)

    def _on_remote_write(self):
        # handle remote writable event

        self._stage = STAGE_STREAM
        if self._data_to_write_to_remote:
            data = b''.join(self._data_to_write_to_remote)
            self._data_to_write_to_remote = []
            self._write_to_sock(data, self._remote_sock)
        else:
            self._update_stream(STREAM_UP, WAIT_STATUS_READING)

    def _on_local_error(self):
        logging.debug('got local error')
        if self._local_sock:
            logging.error(eventloop.get_sock_error(self._local_sock))
            logging.error(
                "exception from %s:%d" %
                (self._client_address[0],
                 self._client_address[1]))
        self.destroy()

    def _on_remote_error(self):
        logging.debug('got remote error')
        if self._remote_sock:
            logging.error(eventloop.get_sock_error(self._remote_sock))
            if self._remote_address:
                logging.error(
                    "when connect to %s:%d from %s:%d via port %d" %
                    (self._remote_address[0],
                     self._remote_address[1],
                     self._client_address[0],
                     self._client_address[1],
                     self._server._listen_port))
            else:
                logging.error(
                    "exception from %s:%d" %
                    (self._client_address[0],
                     self._client_address[1]))
        self.destroy()

    def handle_event(self, sock, fd, event):
        # handle all events in this handler and dispatch them to methods
        handle = False
        if self._stage == STAGE_DESTROYED:
            logging.debug('ignore handle_event: destroyed')
            return True
        if fd == self._remote_sock_fd or fd == self._remotev6_sock_fd:
            if event & eventloop.POLL_ERR:
                handle = True
                self._on_remote_error()
            elif event & (eventloop.POLL_IN | eventloop.POLL_HUP):
                is_exceed = False
                if self._server.speed_tester_d.isExceed():
                    is_exceed = True
                if self._current_user_id != 0 and self._server._config[
                        "is_multi_user"] != 0:
                    if self._server.mu_speed_tester_d[
                            self._current_user_id].isExceed():
                        is_exceed = True
                if not is_exceed:
                    handle = True
                    self._on_remote_read(sock == self._remote_sock)
                else:
                    self._recv_d_max_size = self._tcp_mss - self._overhead
            elif event & eventloop.POLL_OUT:
                handle = True
                self._on_remote_write()
        elif fd == self._local_sock_fd:
            if event & eventloop.POLL_ERR:
                handle = True
                self._on_local_error()
                if self._stage == STAGE_DESTROYED:
                    return True
            elif event & (eventloop.POLL_IN | eventloop.POLL_HUP):
                is_exceed = False
                if self._server.speed_tester_u.isExceed():
                    is_exceed = True
                if self._current_user_id != 0 and self._server._config[
                        "is_multi_user"] != 0:
                    if self._server.mu_speed_tester_u[
                            self._current_user_id].isExceed():
                        is_exceed = True
                if not is_exceed:
                    handle = True
                    self._on_local_read()
                else:
                    self._recv_u_max_size = self._tcp_mss - self._overhead
            elif event & eventloop.POLL_OUT:
                handle = True
                self._on_local_write()
        else:
            logging.warn('unknown socket from %s:%d' %
                         (self._client_address[0], self._client_address[1]))
            try:
                self._loop.removefd(fd)
            except Exception as e:
                shell.print_exception(e)
            try:
                del self._fd_to_handlers[fd]
            except Exception as e:
                shell.print_exception(e)
            sock.close()

        return handle

    def _log_error(self, e):
        logging.error('%s when handling connection from %s:%d' %
                      (e, self._client_address[0], self._client_address[1]))

    def stage(self):
        return self._stage

    def destroy(self):
        # destroy the handler and release any resources
        # promises:
        # 1. destroy won't make another destroy() call inside
        # 2. destroy releases resources so it prevents future call to destroy
        # 3. destroy won't raise any exceptions
        # if any of the promises are broken, it indicates a bug has been
        # introduced! mostly likely memory leaks, etc
        if self._stage == STAGE_DESTROYED:
            # this couldn't happen
            logging.debug('already destroyed')
            return
        self._stage = STAGE_DESTROYED
        if self._remote_address:
            logging.debug('destroy: %s:%d' %
                          self._remote_address)
        else:
            logging.debug('destroy')
        if self._remote_sock:
            logging.debug('destroying remote')
            try:
                self._loop.removefd(self._remote_sock_fd)
            except Exception as e:
                shell.print_exception(e)
            try:
                if self._remote_sock_fd is not None:
                    del self._fd_to_handlers[self._remote_sock_fd]
            except Exception as e:
                shell.print_exception(e)
            self._remote_sock.close()
            self._remote_sock = None
        if self._remote_sock_v6:
            logging.debug('destroying remote_v6')
            try:
                self._loop.removefd(self._remotev6_sock_fd)
            except Exception as e:
                shell.print_exception(e)
            try:
                if self._remotev6_sock_fd is not None:
                    del self._fd_to_handlers[self._remotev6_sock_fd]
            except Exception as e:
                shell.print_exception(e)
            self._remote_sock_v6.close()
            self._remote_sock_v6 = None
        if self._local_sock:
            logging.debug('destroying local')
            try:
                self._loop.removefd(self._local_sock_fd)
            except Exception as e:
                shell.print_exception(e)
            try:
                if self._local_sock_fd is not None:
                    del self._fd_to_handlers[self._local_sock_fd]
            except Exception as e:
                shell.print_exception(e)
            self._local_sock.close()
            self._local_sock = None
        if self._obfs:
            self._obfs.dispose()
            self._obfs = None
        if self._protocol:
            self._protocol.dispose()
            self._protocol = None
        self._encryptor = None
        self._dns_resolver.remove_callback(self._handle_dns_resolved)
        self._server.remove_handler(self)
        if self._add_ref > 0:
            self._server.add_connection(-1)
            self._server.stat_add(self._client_address[0], -1)


class TCPRelay(object):

    def __init__(
            self,
            config,
            dns_resolver,
            is_local,
            stat_callback=None,
            stat_counter=None):
        self._config = config
        self._is_local = is_local
        self._dns_resolver = dns_resolver
        self._closed = False
        self._eventloop = None
        self._fd_to_handlers = {}
        self.server_transfer_ul = 0
        self.server_transfer_dl = 0
        self.mu_server_transfer_ul = {}
        self.mu_server_transfer_dl = {}
        self.server_connections = 0
        self.connected_iplist = []
        self.mu_connected_iplist = {}
        self.is_cleaning_connected_iplist = False
        self.is_cleaning_mu_connected_iplist = False
        self.wrong_iplist = {}
        self.is_cleaning_wrong_iplist = False
        self.detect_log_list = []
        self.mu_detect_log_list = {}

        self.mu_speed_tester_u = {}
        self.mu_speed_tester_d = {}

        if 'users_table' in self._config:
            self.multi_user_host_table = {}
            self.multi_user_table = self._config['users_table']
            if 'node_speedlimit' not in config:
                self.mu_bandwidth = 0
            else:
                self.mu_bandwidth = float(config['node_speedlimit']) * 128

            for id in self.multi_user_table:
                self.multi_user_host_table[common.get_mu_host(
                    id, self.multi_user_table[id]['md5'])] = id

                if 'node_speedlimit' not in self.multi_user_table[id]:
                    bandwidth = max(
                        float(self.mu_bandwidth), float(0.00))
                else:
                    if float(
                            self.mu_bandwidth) > 0.0 or float(
                            self.multi_user_table[id]['node_speedlimit']) * 128 > 0.0:
                        bandwidth = max(
                            float(
                                self.mu_bandwidth), float(
                                self.multi_user_table[id]['node_speedlimit']) * 128)
                    else:
                        bandwidth = 0

                self.mu_speed_tester_u[id] = SpeedTester(bandwidth)
                self.mu_speed_tester_d[id] = SpeedTester(bandwidth)

        self.is_cleaning_detect_log = False
        self.is_cleaning_mu_detect_log_list = False

        self.is_pushing_detect_hex_list = False
        self.is_pushing_detect_text_list = False
        self.detect_hex_list = self._config['detect_hex_list'].copy()
        self.detect_text_list = self._config['detect_text_list'].copy()

        if 'forbidden_ip' in config:
            self._forbidden_iplist = IPNetwork(config['forbidden_ip'])
        else:
            self._forbidden_iplist = None
        if 'forbidden_port' in config:
            self._forbidden_portset = PortRange(config['forbidden_port'])
        else:
            self._forbidden_portset = None

        if config["is_multi_user"] != 0:
            self.mu_reset_time = {}
            for id in self.multi_user_table:
                self.mu_reset_time[id] = time.time()

                if self.multi_user_table[id]['forbidden_ip'] is not None:
                    self.multi_user_table[id]['_forbidden_iplist'] = IPNetwork(
                        str(self.multi_user_table[id]['forbidden_ip']))
                else:
                    self.multi_user_table[id][
                        '_forbidden_iplist'] = IPNetwork(str(""))

                if self.multi_user_table[id]['forbidden_port'] is not None:
                    self.multi_user_table[id]['_forbidden_portset'] = PortRange(
                        str(self.multi_user_table[id]['forbidden_port']))
                else:
                    self.multi_user_table[id][
                        '_forbidden_portset'] = PortRange(str(""))

        if 'node_speedlimit' not in config or 'users_table' in self._config:
            self.bandwidth = 0
        else:
            self.bandwidth = float(config['node_speedlimit']) * 128

        self.speed_tester_u = SpeedTester(self.bandwidth)
        self.speed_tester_d = SpeedTester(self.bandwidth)

        self.protocol_data = obfs.obfs(config['protocol']).init_data()
        self.obfs_data = obfs.obfs(config['obfs']).init_data()

        if config.get('connect_verbose_info', 0) > 0:
            common.connect_log = logging.info

        if config.get('connect_hex_data', 0) > 0:
            self._connect_hex_data = True
        else:
            self._connect_hex_data = False

        self._timeout = config['timeout']
        self._timeouts = []  # a list for all the handlers
        # we trim the timeouts once a while
        self._timeout_offset = 0   # last checked position for timeout
        self._handler_to_timeouts = {}  # key: handler value: index in timeouts

        if is_local:
            listen_addr = config['local_address']
            listen_port = config['local_port']
        else:
            listen_addr = config['server']
            listen_port = config['server_port']
        self._listen_port = listen_port

        addrs = socket.getaddrinfo(listen_addr, listen_port, 0,
                                   socket.SOCK_STREAM, socket.SOL_TCP)
        if len(addrs) == 0:
            raise Exception("can't get addrinfo for %s:%d" %
                            (listen_addr, listen_port))
        af, socktype, proto, canonname, sa = addrs[0]
        server_socket = socket.socket(af, socktype, proto)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(sa)
        server_socket.setblocking(False)
        if config['fast_open']:
            try:
                server_socket.setsockopt(socket.SOL_TCP, 23, 5)
            except socket.error:
                logging.error('warning: fast open is not available')
                self._config['fast_open'] = False
        server_socket.listen(socket.SOMAXCONN)
        self._server_socket = server_socket
        self._server_socket_fd = server_socket.fileno()
        self._stat_counter = stat_counter
        self._stat_callback = stat_callback

    def add_to_loop(self, loop):
        if self._eventloop:
            raise Exception('already add to loop')
        if self._closed:
            raise Exception('already closed')
        self._eventloop = loop
        self._eventloop.add(self._server_socket,
                            eventloop.POLL_IN | eventloop.POLL_ERR, self)
        self._eventloop.add_periodic(self.handle_periodic)

    def remove_handler(self, handler):
        index = self._handler_to_timeouts.get(hash(handler), -1)
        if index >= 0:
            # delete is O(n), so we just set it to None
            self._timeouts[index] = None
            del self._handler_to_timeouts[hash(handler)]

    def add_connection(self, val):
        self.server_connections += val
        logging.debug(
            'server port %5d connections = %d' %
            (self._listen_port, self.server_connections,))

    def add_transfer_u(self, user, transfer):
        if ((user is None or user == 0) and self._config["is_multi_user"] != 0) or self._config["is_multi_user"] == 0:
            self.server_transfer_ul += transfer
        else:
            if user not in self.mu_server_transfer_ul:
                self.mu_server_transfer_ul[user] = 0
            self.mu_server_transfer_ul[
                user] += transfer + self.server_transfer_ul
            self.server_transfer_ul = 0

    def add_transfer_d(self, user, transfer):
        if ((user is None or user == 0) and self._config["is_multi_user"] != 0) or self._config["is_multi_user"] == 0:
            self.server_transfer_dl += transfer
        else:
            if user not in self.mu_server_transfer_dl:
                self.mu_server_transfer_dl[user] = 0
            self.mu_server_transfer_dl[
                user] += transfer + self.server_transfer_dl
            self.server_transfer_dl = 0

    def update_stat(self, port, stat_dict, val):
        newval = stat_dict.get(0, 0) + val
        stat_dict[0] = newval
        logging.debug('port %d connections %d' % (port, newval))
        connections_step = 25
        if newval >= stat_dict.get(-1, 0) + connections_step:
            logging.info('port %d connections up to %d' % (port, newval))
            stat_dict[-1] = stat_dict.get(-1, 0) + connections_step
        elif newval <= stat_dict.get(-1, 0) - connections_step:
            logging.info('port %d connections down to %d' % (port, newval))
            stat_dict[-1] = stat_dict.get(-1, 0) - connections_step

    def stat_add(self, local_addr, val):
        if self._stat_counter is not None:
            if self._listen_port not in self._stat_counter:
                self._stat_counter[self._listen_port] = {}
            newval = self._stat_counter[
                self._listen_port].get(
                local_addr, 0) + val
            logging.debug(
                'port %d addr %s connections %d' %
                (self._listen_port, local_addr, newval))
            self._stat_counter[self._listen_port][local_addr] = newval
            self.update_stat(
                self._listen_port, self._stat_counter[
                    self._listen_port], val)
            if newval <= 0:
                if local_addr in self._stat_counter[self._listen_port]:
                    del self._stat_counter[self._listen_port][local_addr]

            newval = self._stat_counter.get(0, 0) + val
            self._stat_counter[0] = newval
            logging.debug('Total connections %d' % newval)

            connections_step = 50
            if newval >= self._stat_counter.get(-1, 0) + connections_step:
                logging.info('Total connections up to %d' % newval)
                self._stat_counter[
                    -1] = self._stat_counter.get(-1, 0) + connections_step
            elif newval <= self._stat_counter.get(-1, 0) - connections_step:
                logging.info('Total connections down to %d' % newval)
                self._stat_counter[
                    -1] = self._stat_counter.get(-1, 0) - connections_step

    def update_activity(self, handler, data_len):
        if data_len and self._stat_callback:
            self._stat_callback(self._listen_port, data_len)

        # set handler to active
        now = int(time.time())
        if now - handler.last_activity < eventloop.TIMEOUT_PRECISION:
            # thus we can lower timeout modification frequency
            return
        handler.last_activity = now
        index = self._handler_to_timeouts.get(hash(handler), -1)
        if index >= 0:
            # delete is O(n), so we just set it to None
            self._timeouts[index] = None
        length = len(self._timeouts)
        self._timeouts.append(handler)
        self._handler_to_timeouts[hash(handler)] = length

    def _sweep_timeout(self):
        # tornado's timeout memory management is more flexible than we need
        # we just need a sorted last_activity queue and it's faster than heapq
        # in fact we can do O(1) insertion/remove so we invent our own
        if self._timeouts:
            logging.log(shell.VERBOSE_LEVEL, 'sweeping timeouts')
            now = time.time()
            length = len(self._timeouts)
            pos = self._timeout_offset
            while pos < length:
                handler = self._timeouts[pos]
                if handler:
                    if now - handler.last_activity < self._timeout:
                        break
                    else:
                        if handler.remote_address:
                            logging.debug('timed out: %s:%d' %
                                          handler.remote_address)
                        else:
                            logging.debug('timed out')
                        handler.destroy()
                        self._timeouts[pos] = None  # free memory
                        pos += 1
                else:
                    pos += 1
            if pos > TIMEOUTS_CLEAN_SIZE and pos > length >> 1:
                # clean up the timeout queue when it gets larger than half
                # of the queue
                self._timeouts = self._timeouts[pos:]
                for key in self._handler_to_timeouts:
                    self._handler_to_timeouts[key] -= pos
                pos = 0
            self._timeout_offset = pos

    def handle_event(self, sock, fd, event):
        # handle events and dispatch to handlers
        handle = False
        if sock:
            logging.log(shell.VERBOSE_LEVEL, 'fd %d %s', fd,
                        eventloop.EVENT_NAMES.get(event, event))
        if sock == self._server_socket:
            if event & eventloop.POLL_ERR:
                # TODO
                raise Exception('server_socket error')
            handler = None
            handle = True
            try:
                logging.debug('accept')
                conn = self._server_socket.accept()
                handler = TCPRelayHandler(self, self._fd_to_handlers,
                                          self._eventloop, conn[0], self._config,
                                          self._dns_resolver, self._is_local)
                if handler.stage() == STAGE_DESTROYED:
                    conn[0].close()
            except (OSError, IOError) as e:
                error_no = eventloop.errno_from_exception(e)
                if error_no in (errno.EAGAIN, errno.EINPROGRESS,
                                errno.EWOULDBLOCK):
                    return
                else:
                    shell.print_exception(e)
                    if self._config['verbose']:
                        traceback.print_exc()
                    if handler:
                        handler.destroy()
        else:
            if sock:
                handler = self._fd_to_handlers.get(fd, None)
                if handler:
                    handle = handler.handle_event(sock, fd, event)
                else:
                    logging.warn('unknown fd')
                    handle = True
                    try:
                        self._eventloop.removefd(fd)
                    except Exception as e:
                        shell.print_exception(e)
                    sock.close()
            else:
                logging.warn('poll removed fd')
                handle = True
                if fd in self._fd_to_handlers:
                    try:
                        del self._fd_to_handlers[fd]
                    except Exception as e:
                        shell.print_exception(e)
        return handle

    def handle_periodic(self):
        if self._closed:
            if self._server_socket:
                if self._server_socket_fd:
                    self._eventloop.removefd(self._server_socket_fd)
                    self._server_socket_fd = 0
                self._server_socket.close()
                self._server_socket = None
                logging.info('closed TCP port %d', self._listen_port)
            for handler in list(self._fd_to_handlers.values()):
                handler.destroy()
        self._sweep_timeout()

    def connected_iplist_clean(self):
        self.is_cleaning_connected_iplist = True
        del self.connected_iplist[:]
        self.is_cleaning_connected_iplist = False

    def mu_connected_iplist_clean(self):
        self.is_cleaning_mu_connected_iplist = True
        for id in self.mu_connected_iplist:
            del self.mu_connected_iplist[id][:]
        self.is_cleaning_mu_connected_iplist = False

    def wrong_iplist_clean(self):
        self.is_cleaning_wrong_iplist = True

        temp_new_list = {}
        for key in self.wrong_iplist:
            if self.wrong_iplist[key] > time.time() - 60:
                temp_new_list[key] = self.wrong_iplist[key]

        self.wrong_iplist = temp_new_list.copy()

        self.is_cleaning_wrong_iplist = False

    def detect_log_list_clean(self):
        self.is_cleaning_detect_log = True
        del self.detect_log_list[:]
        self.is_cleaning_detect_log = False

    def mu_detect_log_list_clean(self):
        self.is_cleaning_mu_detect_log_list = True
        for id in self.mu_detect_log_list:
            del self.mu_detect_log_list[id][:]
        self.is_cleaning_mu_detect_log_list = False

    def reset_single_multi_user_traffic(self, user_id):
        self.mu_reset_time[user_id] = time.time()
        if user_id in self.mu_server_transfer_ul:
            self.mu_server_transfer_ul[user_id] = 0
        if user_id in self.mu_server_transfer_dl:
            self.mu_server_transfer_dl[user_id] = 0

    def modify_multi_user_table(self, new_table):
        self.multi_user_table = new_table.copy()
        self.multi_user_host_table = {}

        for id in self.multi_user_table:
            if id not in self.mu_reset_time:
                self.mu_reset_time[id] = time.time()

            self.multi_user_host_table[common.get_mu_host(
                id, self.multi_user_table[id]['md5'])] = id
            if self.multi_user_table[id]['forbidden_ip'] is not None:
                self.multi_user_table[id]['_forbidden_iplist'] = IPNetwork(
                    str(self.multi_user_table[id]['forbidden_ip']))
            else:
                self.multi_user_table[id][
                    '_forbidden_iplist'] = IPNetwork(str(""))
            if self.multi_user_table[id]['forbidden_port'] is not None:
                self.multi_user_table[id]['_forbidden_portset'] = PortRange(
                    str(self.multi_user_table[id]['forbidden_port']))
            else:
                self.multi_user_table[id][
                    '_forbidden_portset'] = PortRange(str(""))

            if 'node_speedlimit' not in self.multi_user_table[id]:
                bandwidth = max(
                    float(self.mu_bandwidth), float(0.00))
            else:
                if float(
                        self.mu_bandwidth) > 0.0 or float(
                        self.multi_user_table[id]['node_speedlimit']) * 128 > 0.0:
                    bandwidth = max(
                        float(
                            self.mu_bandwidth), float(
                            self.multi_user_table[id]['node_speedlimit']) * 128)
                else:
                    bandwidth = 0

            self.mu_speed_tester_u[id] = SpeedTester(bandwidth)
            self.mu_speed_tester_d[id] = SpeedTester(bandwidth)

    def modify_detect_text_list(self, new_list):
        self.is_pushing_detect_text_list = True
        self.detect_text_list = new_list.copy()
        self.is_pushing_detect_text_list = False

    def modify_detect_hex_list(self, new_list):
        self.is_pushing_detect_hex_list = True
        self.detect_hex_list = new_list.copy()
        self.is_pushing_detect_hex_list = False

    def close(self, next_tick=False):
        logging.debug('TCP close')
        self._closed = True
        if not next_tick:
            if self._eventloop:
                self._eventloop.remove_periodic(self.handle_periodic)
                if self._server_socket_fd:
                    self._eventloop.removefd(self._server_socket_fd)
                    self._server_socket_fd = 0
            self._server_socket.close()
            for handler in list(self._fd_to_handlers.values()):
                handler.destroy()
