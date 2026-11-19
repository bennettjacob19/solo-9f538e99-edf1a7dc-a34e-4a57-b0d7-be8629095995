#!/usr/bin/env python3
"""本地遥测接收器：原样回显报文，可延迟回包。

延迟来源（优先级从高到低）：
1. 报文以 "DELAY:<秒>:" 开头时，按该秒数延迟（回包仍包含完整原始报文）；
2. 否则用环境变量 REPLY_DELAY（秒，默认 0）。

多地址监听（默认开启）：同一端口监听本机的所有 IPv4 地址。延迟回包时用
IP_PKTINFO 从“实际收到该报文”的 IP 和端口发出，因此连到哪个地址、哪个端口
的代理只会收到自己发去的回包。无法从原地址发出时丢弃该回包并记日志。
设置 MULTI_ADDR=0 则只监听 LISTEN_HOST 一个地址，行为与原来一致。

环境变量：
  RECEIVER_NAME  显示名（默认 receiver）
  LISTEN_PORT    监听端口（默认 9001）
  REPLY_DELAY    默认回包延迟秒数（默认 0）
  MAX_DELAY      单次延迟上限秒数（默认 60）
  MULTI_ADDR     1=监听本机所有 IPv4 地址（默认），0=单地址监听
  LISTEN_ADDRS   指定要监听的地址列表（逗号分隔，留空=所有地址）
  LISTEN_HOST    单地址监听的地址（默认 0.0.0.0）
"""

import asyncio
import os
import re
import socket
import struct

NAME = os.environ.get("RECEIVER_NAME", "receiver")
PORT = int(os.environ.get("LISTEN_PORT", "9001"))
DEFAULT_DELAY = float(os.environ.get("REPLY_DELAY", "0"))
MAX_DELAY = float(os.environ.get("MAX_DELAY", "60"))
MULTI_ADDR = os.environ.get("MULTI_ADDR", "1").lower() not in ("0", "false", "no", "off")
LISTEN_HOST = os.environ.get("LISTEN_HOST", "0.0.0.0")
LISTEN_ADDRS = [x.strip() for x in os.environ.get("LISTEN_ADDRS", "").split(",") if x.strip()]

DELAY_RE = re.compile(rb"^DELAY:([0-9]+(?:\.[0-9]+)?):")

# Linux 的 IP_PKTINFO；Python 的 socket 模块在部分版本未导出该常量。
IP_PKTINFO = getattr(socket, "IP_PKTINFO", None)
if IP_PKTINFO is None and os.name == "posix":
    IP_PKTINFO = 8
_PKTINFO_LEN = 12  # struct in_pktinfo: unsigned int + 2 * struct in_addr


def _packet_dst(ancdata):
    """从 IP_PKTINFO 辅助数据中取出实际收包的目的 IPv4 地址。"""
    for level, ctype, cdata in ancdata:
        if (level == socket.IPPROTO_IP and ctype == IP_PKTINFO
                and len(cdata) >= _PKTINFO_LEN):
            _ifindex, _spec_dst, dst = struct.unpack("I4s4s", cdata[:_PKTINFO_LEN])
            return socket.inet_ntoa(dst)
    return None


class Listener:
    """一条监听套接字。多地址模式下用 IP_PKTINFO 记录并复用收包地址。"""

    def __init__(self, loop, sock, bound_ip, pktinfo):
        self.loop = loop
        self.sock = sock
        self.bound_ip = bound_ip
        self.pktinfo = pktinfo
        self.closed = False
        loop.add_reader(sock.fileno(), self._on_readable)

    def _on_readable(self):
        while not self.closed:
            try:
                data, ancdata, _flags, addr = self.sock.recvmsg(
                    65535, socket.CMSG_SPACE(_PKTINFO_LEN)
                )
            except BlockingIOError:
                return
            except OSError as exc:
                if not self.closed:
                    print(f"[{NAME}] 收包失败（{self.bound_ip}:{PORT}）: {exc}", flush=True)
                return
            dst_ip = _packet_dst(ancdata) or self.bound_ip
            delay = DEFAULT_DELAY
            m = DELAY_RE.match(data)
            if m:
                delay = min(float(m.group(1)), MAX_DELAY)
            print(f"[{NAME}] 收到 {len(data)}B 来自 {addr[0]}:{addr[1]}"
                  f"（入口 {dst_ip}:{PORT}），{delay}s 后回包", flush=True)
            self.loop.call_later(delay, self._reply, data, addr, dst_ip)

    def _reply(self, data, addr, src_ip):
        try:
            if self.pktinfo and src_ip and src_ip != "0.0.0.0":
                # 必须从实际收包的 IP 与端口发出，否则连到该地址的代理
                # （已 connect 到它）会因源地址不符而收不到回包。
                pktinfo = struct.pack(
                    "I4s4s", 0, socket.inet_aton(src_ip), socket.inet_aton(src_ip)
                )
                self.sock.sendmsg(
                    [data], [(socket.IPPROTO_IP, IP_PKTINFO, pktinfo)], 0, addr
                )
                print(f"[{NAME}] 已回包 {len(data)}B 到 {addr[0]}:{addr[1]}"
                      f"（从 {src_ip}:{PORT}）", flush=True)
            else:
                self.sock.sendto(data, addr)  # 单地址监听照常
                print(f"[{NAME}] 已回包 {len(data)}B 到 {addr[0]}:{addr[1]}", flush=True)
        except OSError as exc:
            print(f"[{NAME}] 无法从 {src_ip}:{PORT} 回包到 {addr[0]}:{addr[1]}，"
                  f"已丢弃: {exc}", flush=True)

    def close(self):
        if self.closed:
            return
        self.closed = True
        try:
            self.loop.remove_reader(self.sock.fileno())
        except (OSError, ValueError):
            pass
        self.sock.close()


def _make_socket(bind_ip, want_pktinfo):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    pktinfo = False
    if want_pktinfo and IP_PKTINFO is not None:
        try:
            sock.setsockopt(socket.IPPROTO_IP, IP_PKTINFO, 1)
            pktinfo = True
        except OSError:
            pktinfo = False
    sock.bind((bind_ip, PORT))
    sock.setblocking(False)
    return sock, pktinfo


async def main():
    loop = asyncio.get_running_loop()
    listeners = []

    if MULTI_ADDR:
        # 默认用通配地址监听所有本机 IPv4 地址，靠 IP_PKTINFO 区分实际入口；
        # 也支持 LISTEN_ADDRS 明确列出要监听的多个地址（每个地址一条套接字）。
        targets = LISTEN_ADDRS or ["0.0.0.0"]
        for ip in targets:
            try:
                sock, pktinfo = _make_socket(ip, want_pktinfo=True)
            except OSError as exc:
                print(f"[{NAME}] 跳过 {ip}:{PORT}：{exc}", flush=True)
                continue
            listeners.append(Listener(loop, sock, ip, pktinfo))
            shown = "本机所有 IPv4 地址" if ip == "0.0.0.0" else ip
            print(f"[{NAME}] UDP 监听 {shown}:{PORT}"
                  f"（IP_PKTINFO {'启用' if pktinfo else '不可用'}）", flush=True)
        if not listeners:
            print(f"[{NAME}] 多地址监听无可用地址，回退到单地址 {LISTEN_HOST}:{PORT}",
                  flush=True)

    if not listeners:
        sock, _ = _make_socket(LISTEN_HOST, want_pktinfo=False)
        listeners.append(Listener(loop, sock, LISTEN_HOST, False))
        print(f"[{NAME}] UDP 监听 {LISTEN_HOST}:{PORT}（单地址）", flush=True)

    print(f"[{NAME}] 默认延迟 {DEFAULT_DELAY}s", flush=True)
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
