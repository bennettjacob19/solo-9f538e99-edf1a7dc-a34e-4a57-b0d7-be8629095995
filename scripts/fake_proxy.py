#!/usr/bin/env python3
"""验证用假代理：通配绑定 UDP 端口，原样回显收到的报文。

由于绑定的是 0.0.0.0，回包的源地址由内核按路由选择（发往 127.0.0.1 的
回包源地址就是 127.0.0.1），用来制造“回包源地址与入口不一致”的场景。
用法：python3 fake_proxy.py <监听端口>
"""

import socket
import sys

port = int(sys.argv[1])

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
sock.bind(("0.0.0.0", port))
while True:
    data, addr = sock.recvfrom(65535)
    sock.sendto(data, addr)
