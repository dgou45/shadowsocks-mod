#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
import copy
import requests
from configloader import get_config

class WebApi(object):
    def __init__(self):
        self.session_pool = requests.Session()
        self.etags = {}  # 用来存储每个资源的 ETag 值
        self.tempData = {}  # 用来存储从服务器获取的数据

    def getApi(self, uri, params={}):
        r""" Send a ``GET`` request to API server. Return response["data"] or []

        :param uri: URI to request
        :param params: Optional arguments ``request`` takes
        :rtype: list
        """
        params["key"] = get_config().WEBAPI_TOKEN
        
        # 获取之前缓存的 ETag 值（如果存在）
        etag = self.etags.get(uri)  # 假设我们按照 URI 来存储 ETag
        logging.info("%s：SSR后端储存的ETag为：%s" % (uri, etag))

        headers = {}
        if etag:
            headers["If-None-Match"] = etag  # 如果存在 ETag，将其加到请求头中
        
        # 发送请求
        response = self.session_pool.get(
            "%s/mod_mu/%s" % (get_config().WEBAPI_URL, uri),
            params=params,
            headers=headers,
            timeout=10,
        )

        # 如果响应头中包含新的 ETag，则存储它
        if "ETag" in response.headers:
            self.etags[uri] = response.headers["ETag"]
            logging.info("%s：本次获取到的新的ETag为：%s" % (uri, response.headers["ETag"]))

        if response.status_code == 304:
            logging.info("%s：该条数据没有新的更新, 使用SSR后端储存的旧数据" % uri)
            # 获取之前缓存的资源 必须使用deepcopy 否则数据后续将被其他方法修改
            json_data = copy.deepcopy(self.tempData.get(uri))
            # if uri == "users":
            #     logging.info("%s：旧数据：%s" % (uri, json_data))
        else:
            if response.status_code != 200:
                logging.error("Server error with status code: %i" %
                              response.status_code)
                raise Exception('Server Error!')

            try:
                json_data = response.json()
            except:
                logging.error("Wrong data: %s" % response.text)
                raise Exception('Server Error!')

            if len(json_data) != 2:
                logging.error("Wrong data: %s" % response.text)
                raise Exception('Server Error!')
            if json_data["ret"] == 0:
                logging.error("Wrong data: %s" % json_data["data"])
                raise Exception('Server Error!')

            # 必须使用deepcopy 否则数据后续将被其他方法修改
            self.tempData[uri] = copy.deepcopy(json_data)
            logging.info("%s：从服务器获取到了新数据，数据已经储存..." % uri)
            # if uri == "users":
            #     logging.info("%s：新数据：%s" % (uri, self.tempData[uri]))

        return json_data["data"]

    def postApi(self, uri, params={}, json={}):
        r""" Send a ``POST`` request to API server. Return response["data"] or []

        :param uri: URI to request
        :param params: Optional arguments ``request`` takes
        :param json: Optional arguments ``json`` that ``request`` takes
        :rtype: list
        """
        params["key"] = get_config().WEBAPI_TOKEN
        response = self.session_pool.post(
            "%s/mod_mu/%s" % (get_config().WEBAPI_URL, uri),
            params=params,
            json=json,
            timeout=10,
        )
        if response.status_code != 200:
            logging.error("Server error with status code: %i" %
                          response.status_code)
            raise Exception('Server Error!')

        try:
            json_data = response.json()
        except:
            logging.error("Wrong data: %s" % response.text)
            raise Exception('Server Error!')

        if len(json_data) != 2:
            logging.error("Wrong data: %s" % response.text)
            raise Exception('Server Error!')
        if json_data["ret"] == 0:
            logging.error("Wrong data: %s" % json_data["data"])
            raise Exception('Server Error!')

        return json_data["data"]
