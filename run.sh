#!/bin/bash
set -e
cd "$(dirname "$0")"

# 查找旧进程并杀掉（不报错）
pkill -f "python3 server.py" || true

# 设置最大打开文件数
ulimit -n 512000

# 以前台方式运行 SSR（关键）
exec python3 server.py

