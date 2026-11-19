#!/usr/bin/env python3
"""遥测 UDP 代理 + 操作网页。

功能要点：
- 两个入口（同一 UDP 端口上的多个本机 IPv4 地址，如 127.0.0.1 与 127.0.0.2）
  共用一条监听套接字；会话按“入口 IP + 客户端 IP:端口”建立，
  因此同一终端用同一源 IP:端口访问不同入口时互不串会话、各自独立过期；
- 回包使用 IP_PKTINFO 从终端访问的那个入口 IP 与同一端口发出；
- 建会话时把该会话固定到当前目标接收器（A/B），会话期间不随切换改变；
- 每个会话使用一条独立的、已 connect 的上游 UDP 套接字（独立本地端口），
  只把该会话对应接收器的回包送回该客户端；
- 会话过期即关闭其上游端口，且端口进入冷却期不再分配；
- 报文与回包原样透传，不做任何修改；无法从原入口地址发出时丢弃并记日志；
- 当前目标保存在 SQLite，重启后保留；会话全在内存，重启即清空；
- 切换目标时先落库再生效，保存失败则保留原目标；
- Flask 网页可切换目标、从指定入口发送试传报文、查看会话与报文日志。
"""

import asyncio
import os
import socket
import sqlite3
import struct
import threading
import time

from flask import Flask, flash, jsonify, redirect, render_template, request, url_for

# ---------------- 配置 ----------------
UDP_LISTEN_HOST = os.environ.get("UDP_LISTEN_HOST", "0.0.0.0")
UDP_LISTEN_PORT = int(os.environ.get("UDP_LISTEN_PORT", "9000"))
HTTP_HOST = os.environ.get("HTTP_HOST", "0.0.0.0")
HTTP_PORT = int(os.environ.get("HTTP_PORT", "8080"))
DB_PATH = os.environ.get(
    "DB_PATH", os.path.join(os.path.dirname(os.path.abspath(__file__)), "proxy.db")
)
SESSION_IDLE_TIMEOUT = float(os.environ.get("SESSION_IDLE_TIMEOUT", "5"))
UPSTREAM_PORT_MIN = int(os.environ.get("UPSTREAM_PORT_MIN", "20000"))
UPSTREAM_PORT_MAX = int(os.environ.get("UPSTREAM_PORT_MAX", "29999"))
# 上游端口关闭后的冷却时间：冷却期内不再分配该端口，
# 保证旧会话迟到的回包不会被复用同端口的新会话收到。应大于接收器最大回包延迟。
UPSTREAM_PORT_COOLDOWN = float(os.environ.get("UPSTREAM_PORT_COOLDOWN", "30"))
LOG_KEEP = int(os.environ.get("LOG_KEEP", "1000"))

# 网页试传可选的入口（本机 IPv4 地址），同一源地址/端口分别访问。
TEST_ENTRANCES = [
    x.strip()
    for x in os.environ.get("TEST_ENTRANCES", "127.0.0.1,127.0.0.2").split(",")
    if x.strip()
]
# 试传客户端绑定的源地址。固定客户端端口时，访问不同入口仍用同一源 IP:端口，
# 用来验证“同源 IP:端口访问不同入口会各自建会话”。
TEST_SOURCE_HOST = os.environ.get("TEST_SOURCE_HOST", "127.0.0.1")

RECEIVERS = {
    "A": (
        os.environ.get("RECEIVER_A_HOST", "127.0.0.1"),
        int(os.environ.get("RECEIVER_A_PORT", "9001")),
    ),
    "B": (
        os.environ.get("RECEIVER_B_HOST", "127.0.0.1"),
        int(os.environ.get("RECEIVER_B_PORT", "9002")),
    ),
}
DEFAULT_TARGET = os.environ.get("DEFAULT_TARGET", "A")

# Linux 的 IP_PKTINFO；Python 的 socket 模块在部分版本未导出该常量。
IP_PKTINFO = getattr(socket, "IP_PKTINFO", None)
if IP_PKTINFO is None and os.name == "posix":
    IP_PKTINFO = 8
_PKTINFO_LEN = 12  # struct in_pktinfo: unsigned int + 2 * struct in_addr


def fmt_addr(addr):
    return f"{addr[0]}:{addr[1]}"


def _is_wildcard(host):
    return host in ("", "0.0.0.0", "*")


def _enable_pktinfo(sock):
    """让 recvmsg 带回数据包的目的地址（即终端访问的入口地址）。"""
    if IP_PKTINFO is None:
        return False
    try:
        sock.setsockopt(socket.IPPROTO_IP, IP_PKTINFO, 1)
        return True
    except OSError:
        return False


def _packet_dst(ancdata):
    """从 IP_PKTINFO 辅助数据中取出目的地址与实际入接口。"""
    for level, ctype, cdata in ancdata:
        if (level == socket.IPPROTO_IP and ctype == IP_PKTINFO
                and len(cdata) >= _PKTINFO_LEN):
            ifindex, _spec_dst, dst = struct.unpack("I4s4s", cdata[:_PKTINFO_LEN])
            return socket.inet_ntoa(dst), ifindex
    return None, 0


def _send_from(sock, data, dest, src_ip):
    """从指定源 IP 发出 UDP 报文（端口由已绑定的套接字决定）。

    无法按指定源地址发出时抛 OSError，由调用方丢弃并记日志；绝不退回
    sendto()，否则回包会带错误的源地址。
    """
    pktinfo = struct.pack(
        "I4s4s", 0, socket.inet_aton(src_ip), socket.inet_aton(src_ip)
    )
    sock.sendmsg([data], [(socket.IPPROTO_IP, IP_PKTINFO, pktinfo)], 0, dest)


def preview(data: bytes) -> str:
    """报文内容展示：可打印文本原样显示，否则用 hex。"""
    try:
        text = data.decode("utf-8")
        if all(ch.isprintable() or ch in "\r\n\t" for ch in text):
            return text if len(text) <= 200 else text[:200] + "…"
    except UnicodeDecodeError:
        pass
    hexed = data.hex()
    return "hex:" + (hexed if len(hexed) <= 200 else hexed[:200] + "…")


def fmt_ts(ts: float) -> str:
    return time.strftime("%H:%M:%S", time.localtime(ts)) + f".{int((ts % 1) * 1000):03d}"


# ---------------- SQLite：目标持久化 + 报文日志 ----------------
class Store:
    """线程安全的 SQLite 存取。目标(target)落库，重启后保留。"""

    _COLUMNS = ("id", "ts", "client", "entrance", "session_id", "direction",
                "receiver", "payload", "note")

    def __init__(self, path):
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute(
            "CREATE TABLE IF NOT EXISTS settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
        )
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS message_log (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   ts REAL NOT NULL,
                   client TEXT NOT NULL DEFAULT '',
                   entrance TEXT NOT NULL DEFAULT '',
                   session_id INTEGER NOT NULL DEFAULT 0,
                   direction TEXT NOT NULL,
                   receiver TEXT NOT NULL DEFAULT '',
                   payload TEXT NOT NULL DEFAULT '',
                   note TEXT NOT NULL DEFAULT '')"""
        )
        # 兼容旧库：没有 entrance 列时补上。
        cols = {row[1] for row in self._conn.execute("PRAGMA table_info(message_log)")}
        if "entrance" not in cols:
            self._conn.execute(
                "ALTER TABLE message_log ADD COLUMN entrance TEXT NOT NULL DEFAULT ''"
            )
        self._conn.commit()
        row = self._conn.execute("SELECT value FROM settings WHERE key='target'").fetchone()
        if row and row[0] in RECEIVERS:
            self._target = row[0]
        else:
            self._target = DEFAULT_TARGET if DEFAULT_TARGET in RECEIVERS else sorted(RECEIVERS)[0]
            self._conn.execute(
                "INSERT OR REPLACE INTO settings(key,value) VALUES('target',?)", (self._target,)
            )
            self._conn.commit()
        self._inserts = 0

    def get_target(self):
        with self._lock:
            return self._target

    def set_target(self, target):
        """先持久化，成功后才切换内存中的当前目标；失败则抛异常并保留原目标。"""
        if target not in RECEIVERS:
            raise ValueError(f"未知接收器: {target!r}")
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO settings(key,value) VALUES('target',?)", (target,)
            )
            self._conn.commit()
            self._target = target

    def log(self, direction, client="", session_id=0, receiver="", payload="",
            note="", entrance=""):
        try:
            with self._lock:
                self._conn.execute(
                    "INSERT INTO message_log"
                    "(ts,client,entrance,session_id,direction,receiver,payload,note)"
                    " VALUES(?,?,?,?,?,?,?,?)",
                    (time.time(), client, entrance, session_id, direction, receiver,
                     payload, note),
                )
                self._conn.commit()
                self._inserts += 1
                if self._inserts % 200 == 0:
                    self._conn.execute(
                        "DELETE FROM message_log WHERE id < "
                        "(SELECT COALESCE(MIN(id),0) FROM "
                        "(SELECT id FROM message_log ORDER BY id DESC LIMIT ?))",
                        (LOG_KEEP,),
                    )
                    self._conn.commit()
        except sqlite3.Error:
            pass  # 日志失败绝不影响转发

    def recent_messages(self, limit=100):
        with self._lock:
            rows = self._conn.execute(
                "SELECT id,ts,client,entrance,session_id,direction,receiver,payload,note "
                "FROM message_log ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(zip(self._COLUMNS, r)) for r in rows]

    def latest_receiver_for_client(self, client, entrance=None):
        sql = ("SELECT receiver FROM message_log WHERE client=? AND direction='c2r'")
        args = [client]
        if entrance is not None:
            sql += " AND entrance=?"
            args.append(entrance)
        sql += " ORDER BY id DESC LIMIT 1"
        with self._lock:
            row = self._conn.execute(sql, args).fetchone()
        return row[0] if row else None


# ---------------- 上游端口分配（带冷却期） ----------------
class PortAllocator:
    """显式管理上游本地端口：关闭后冷却期内不再分配，
    确保旧会话迟到的回包不会被复用同端口的新会话收到。"""

    def __init__(self, low, high, cooldown):
        self.low, self.high, self.cooldown = low, high, cooldown
        self._next = low
        self._in_use = set()
        self._closed = {}  # port -> 关闭时刻(monotonic)
        self._lock = threading.Lock()

    def allocate(self):
        with self._lock:
            now = time.monotonic()
            for _ in range(self.high - self.low + 1):
                port = self._next
                self._next += 1
                if self._next > self.high:
                    self._next = self.low
                if port in self._in_use:
                    continue
                closed_at = self._closed.get(port)
                if closed_at is not None and now - closed_at < self.cooldown:
                    continue
                self._in_use.add(port)
                self._closed.pop(port, None)
                return port
        raise RuntimeError("上游 UDP 端口耗尽")

    def release(self, port):
        with self._lock:
            self._in_use.discard(port)
            self._closed[port] = time.monotonic()

    def discard(self, port):
        """分配后绑定失败：不进入冷却，直接可用。"""
        with self._lock:
            self._in_use.discard(port)


# ---------------- 会话与转发 ----------------
class Session:
    __slots__ = (
        "id", "key", "client_addr", "entrance", "receiver_name", "receiver_addr",
        "upstream_port", "upstream", "state", "closed",
        "created", "last_activity", "pending",
    )

    def __init__(self, sid, client_addr, entrance):
        self.id = sid
        self.key = (entrance, client_addr[0], client_addr[1])
        self.client_addr = client_addr
        self.entrance = entrance  # 终端访问的代理本机 IPv4 地址
        self.receiver_name = ""
        self.receiver_addr = None
        self.upstream_port = None
        self.upstream = None
        self.state = "opening"  # opening -> open -> closed
        self.closed = False
        self.created = time.monotonic()
        self.last_activity = self.created
        self.pending = []


class UpstreamEndpoint:
    """每个会话一条、已 connect 到其接收器的上游 UDP 套接字。
    只有该会话对应接收器的回包会到达这里。"""

    def __init__(self, state, session, sock):
        self.state = state
        self.session = session
        self.sock = sock
        self.closed = False
        state.loop.add_reader(sock.fileno(), self._on_readable)

    def _on_readable(self):
        while not self.closed:
            try:
                data, addr = self.sock.recvfrom(65535)
            except BlockingIOError:
                return
            except OSError as exc:
                # 如对端 ICMP 不可达；connect 的套接字会报错，忽略即可。
                return
            self.state.on_upstream_datagram(self.session, data, addr)

    def close(self):
        if self.closed:
            return
        self.closed = True
        try:
            self.state.loop.remove_reader(self.sock.fileno())
        except (OSError, ValueError):
            pass
        try:
            self.sock.close()
        except OSError:
            pass


class ProxyState:
    def __init__(self, store):
        self.store = store
        self.loop = None
        self.client_sock = None
        self.client_pktinfo = False
        self.bind_host = UDP_LISTEN_HOST
        self.sessions = {}  # (入口IP, 客户端IP, 客户端端口) -> Session
        self.lock = threading.RLock()
        self.next_session_id = 1
        self.ports = PortAllocator(UPSTREAM_PORT_MIN, UPSTREAM_PORT_MAX, UPSTREAM_PORT_COOLDOWN)

    async def start(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        bind_host = UDP_LISTEN_HOST
        # 通配监听时才能且必须靠 IP_PKTINFO 区分入口；单地址监听照常。
        if _is_wildcard(bind_host):
            self.client_pktinfo = _enable_pktinfo(sock)
        sock.bind((bind_host, UDP_LISTEN_PORT))
        sock.setblocking(False)
        self.client_sock = sock
        self.bind_host = bind_host
        self.loop.add_reader(sock.fileno(), self._on_client_readable)
        self.loop.create_task(self._sweeper())
        print(f"[proxy] 客户端 UDP 监听 {bind_host}:{UDP_LISTEN_PORT}"
              f"（IP_PKTINFO {'启用' if self.client_pktinfo else '未启用'}）", flush=True)
        print(f"[proxy] 空闲会话超时 {SESSION_IDLE_TIMEOUT}s，当前目标 {self.store.get_target()}",
              flush=True)

    # ---- 客户端方向 ----
    def _on_client_readable(self):
        while True:
            try:
                data, ancdata, _flags, addr = self.client_sock.recvmsg(
                    65535, socket.CMSG_SPACE(_PKTINFO_LEN)
                )
            except BlockingIOError:
                return
            except OSError as exc:
                self.store.log("event", note=f"客户端接收失败: {exc}")
                return
            entrance, _ifindex = _packet_dst(ancdata)
            if entrance is None:
                # 单地址监听或平台不支持辅助数据：入口就是绑定地址。
                entrance = self.bind_host if not _is_wildcard(self.bind_host) else "0.0.0.0"
            self.on_client_datagram(data, addr, entrance)

    def on_client_datagram(self, data, addr, entrance):
        now = time.monotonic()
        key = (entrance, addr[0], addr[1])
        with self.lock:
            sess = self.sessions.get(key)
            if sess is not None and sess.closed:
                sess = None
            if (sess is not None and sess.state == "open"
                    and now - sess.last_activity > SESSION_IDLE_TIMEOUT):
                self._close_session(sess, "idle-timeout")
                sess = None
            if sess is None:
                # 建会话：入口 + 客户端地址共同决定会话，并固定到“当前”目标接收器。
                sess = Session(self.next_session_id, addr, entrance)
                self.next_session_id += 1
                self.sessions[key] = sess
                sess.pending.append(data)
                self.loop.create_task(self._open_session(sess))
                return
            sess.last_activity = now
            if sess.state == "opening":
                sess.pending.append(data)
                return
        self._forward(sess, data)

    async def _open_session(self, sess):
        target = self.store.get_target()  # 建会话时固定接收器
        client = fmt_addr(sess.client_addr)
        try:
            host, port = RECEIVERS[target]
            infos = await self.loop.getaddrinfo(
                host, port, family=socket.AF_INET, type=socket.SOCK_DGRAM
            )
            remote_addr = infos[0][4]
            upstream = None
            upstream_port = None
            last_err = None
            for _ in range(8):
                upstream_port = self.ports.allocate()
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                try:
                    sock.bind(("0.0.0.0", upstream_port))
                    sock.connect(remote_addr)  # connect：只收该接收器的回包
                    sock.setblocking(False)
                except OSError as exc:
                    last_err = exc
                    sock.close()
                    self.ports.discard(upstream_port)
                    continue
                upstream = UpstreamEndpoint(self, sess, sock)
                break
            if upstream is None:
                raise last_err or RuntimeError("无可用上游端口")
            with self.lock:
                if sess.closed:
                    upstream.close()
                    self.ports.release(upstream_port)
                    return
                sess.receiver_name = target
                sess.receiver_addr = remote_addr
                sess.upstream_port = upstream_port
                sess.upstream = upstream
                sess.state = "open"
                pending, sess.pending = sess.pending, []
            self.store.log("event", client=client, session_id=sess.id, receiver=target,
                           entrance=sess.entrance,
                           note=f"会话建立，入口 {sess.entrance}，固定到接收器 {target}"
                                f"（上游端口 {upstream_port}）")
            for data in pending:
                self._forward(sess, data)
        except Exception as exc:
            self.store.log("event", client=client, session_id=sess.id,
                           entrance=sess.entrance, note=f"会话建立失败: {exc}")
            self.close_session(sess, f"open-failed: {exc}")

    def _forward(self, sess, data):
        with self.lock:
            if sess.closed or sess.upstream is None:
                return
            sess.last_activity = time.monotonic()
        try:
            sess.upstream.sock.send(data)  # 已 connect，原样发往该会话的接收器
        except Exception as exc:
            self.store.log("event", client=fmt_addr(sess.client_addr), session_id=sess.id,
                           entrance=sess.entrance, note=f"转发失败: {exc}")
            return
        self.store.log("c2r", client=fmt_addr(sess.client_addr), session_id=sess.id,
                       receiver=sess.receiver_name, payload=preview(data),
                       entrance=sess.entrance)

    # ---- 接收器方向 ----
    def on_upstream_datagram(self, sess, data, addr=None):
        now = time.monotonic()
        with self.lock:
            if addr is not None and sess.receiver_addr is not None \
                    and addr != sess.receiver_addr:
                self.store.log("drop", client=fmt_addr(sess.client_addr), session_id=sess.id,
                               receiver=sess.receiver_name, payload=preview(data),
                               entrance=sess.entrance, note=f"回包来源异常 {fmt_addr(addr)}，已丢弃")
                return
            if sess.closed or self.sessions.get(sess.key) is not sess:
                self.store.log("drop", client=fmt_addr(sess.client_addr), session_id=sess.id,
                               receiver=sess.receiver_name, payload=preview(data),
                               entrance=sess.entrance, note="旧会话迟到的回包，已丢弃")
                return
            if now - sess.last_activity > SESSION_IDLE_TIMEOUT:
                self._close_session(sess, "idle-timeout")
                self.store.log("drop", client=fmt_addr(sess.client_addr), session_id=sess.id,
                               receiver=sess.receiver_name, payload=preview(data),
                               entrance=sess.entrance, note="回包到达时会话已过期，已丢弃")
                return
            sess.last_activity = now
        if not self._send_to_client(sess, data):
            return
        self.store.log("r2c", client=fmt_addr(sess.client_addr), session_id=sess.id,
                       receiver=sess.receiver_name, payload=preview(data),
                       entrance=sess.entrance)

    def _send_to_client(self, sess, data):
        """把回包原样送回客户端，源 IP 必须是终端访问的入口地址。"""
        try:
            if self.client_pktinfo:
                _send_from(self.client_sock, data, sess.client_addr, sess.entrance)
            else:
                self.client_sock.sendto(data, sess.client_addr)
            return True
        except OSError as exc:
            self.store.log("drop", client=fmt_addr(sess.client_addr), session_id=sess.id,
                           receiver=sess.receiver_name, payload=preview(data),
                           entrance=sess.entrance,
                           note=f"无法从入口 {sess.entrance} 发出回包，已丢弃: {exc}")
            return False

    # ---- 会话生命周期 ----
    def _close_session(self, sess, reason):
        """调用方需持有 self.lock。"""
        if sess.closed:
            return
        sess.closed = True
        sess.state = "closed"
        if self.sessions.get(sess.key) is sess:
            del self.sessions[sess.key]
        upstream, sess.upstream = sess.upstream, None
        port, sess.upstream_port = sess.upstream_port, None
        if upstream is not None:
            upstream.close()  # 迟到的回包到此端口将被系统丢弃
        if port is not None:
            self.ports.release(port)  # 进入冷却期，冷却内不再分配
        self.store.log("event", client=fmt_addr(sess.client_addr), session_id=sess.id,
                       receiver=sess.receiver_name, entrance=sess.entrance,
                       note=f"会话关闭（{reason}）")

    def close_session(self, sess, reason):
        with self.lock:
            self._close_session(sess, reason)

    async def _sweeper(self):
        while True:
            await asyncio.sleep(0.25)
            now = time.monotonic()
            with self.lock:
                expired = [s for s in self.sessions.values()
                           if not s.closed and now - s.last_activity > SESSION_IDLE_TIMEOUT]
                for sess in expired:
                    self._close_session(sess, "idle-timeout")

    def snapshot_sessions(self):
        now = time.monotonic()
        with self.lock:
            return [
                {
                    "id": s.id,
                    "client": fmt_addr(s.client_addr),
                    "entrance": s.entrance,
                    "local_ip": s.entrance,
                    "receiver": s.receiver_name or "?",
                    "upstream_port": s.upstream_port,
                    "state": s.state,
                    "idle": round(now - s.last_activity, 1),
                    "age": round(now - s.created, 1),
                }
                for s in sorted(self.sessions.values(), key=lambda x: x.id)
            ]


# ---------------- Flask 网页 ----------------
store = Store(DB_PATH)
state = ProxyState(store)
app = Flask(__name__)
app.secret_key = os.urandom(16)

_last_test = {}
_last_test_lock = threading.Lock()

DIRECTION_LABELS = {
    "c2r": "客户端→接收器",
    "r2c": "接收器→客户端",
    "event": "事件",
    "drop": "丢弃",
}


def _format_message(m):
    m = dict(m)
    m["ts"] = fmt_ts(m["ts"])
    m["direction"] = DIRECTION_LABELS.get(m["direction"], m["direction"])
    return m


@app.get("/")
def index():
    with _last_test_lock:
        lt = dict(_last_test) if _last_test else None
    return render_template(
        "index.html",
        target=store.get_target(),
        receivers={k: f"{h}:{p}" for k, (h, p) in RECEIVERS.items()},
        idle_timeout=SESSION_IDLE_TIMEOUT,
        udp_listen_port=UDP_LISTEN_PORT,
        test_entrances=TEST_ENTRANCES,
        default_entrance=TEST_ENTRANCES[0] if TEST_ENTRANCES else "127.0.0.1",
        last_test=lt,
    )


@app.get("/api/state")
def api_state():
    with _last_test_lock:
        lt = dict(_last_test) if _last_test else None
    return jsonify({
        "target": store.get_target(),
        "receivers": {k: f"{h}:{p}" for k, (h, p) in RECEIVERS.items()},
        "idle_timeout": SESSION_IDLE_TIMEOUT,
        "udp_listen_port": UDP_LISTEN_PORT,
        "test_entrances": TEST_ENTRANCES,
        "sessions": state.snapshot_sessions(),
        "messages": [_format_message(m) for m in store.recent_messages(100)],
        "last_test": lt,
    })


@app.post("/switch")
def switch():
    target = request.form.get("target", "")
    try:
        store.set_target(target)
        store.log("event", note=f"网页切换目标为接收器 {target}")
        flash(f"已切换到接收器 {target}。新会话走 {target}，旧会话继续走原接收器直至过期。", "ok")
    except Exception as exc:
        flash(f"切换保存失败，保留原目标 {store.get_target()}。原因：{exc}", "error")
    return redirect(url_for("index"))


@app.post("/test")
def test():
    global _last_test
    entrance = (
        request.form.get("entrance")
        or request.form.get("dest_ip")
        or request.form.get("test_ip")
        or request.form.get("entry")
        or ""
    ).strip()
    result = run_test(
        payload=request.form.get("payload", "PING"),
        delay_s=request.form.get("delay", "").strip(),
        client_port_s=request.form.get("client_port", "").strip(),
        wait_s=request.form.get("wait", "10").strip(),
        entrance=entrance,
    )
    with _last_test_lock:
        _last_test = result
    return redirect(url_for("index"))


def run_test(payload, delay_s, client_port_s, wait_s, entrance="", listen_port=None,
             entrances=None):
    """从网页后端经指定的代理入口发一条试传报文并等待回包。

    回包必须来自“入口 IP + 代理监听端口”，否则判为来源不符并报错。
    """
    listen_port = UDP_LISTEN_PORT if listen_port is None else listen_port
    entrances = TEST_ENTRANCES if entrances is None else entrances
    try:
        wait = min(max(float(wait_s), 0.5), 60.0)
    except ValueError:
        wait = 10.0
    if not entrance:
        entrance = entrances[0] if entrances else "127.0.0.1"
    expected_source = (entrance, listen_port)
    result = {"time": fmt_ts(time.time()), "payload": payload, "ok": False,
              "reply": "", "error": "", "rtt": None, "client": "", "receiver": None,
              "entrance": entrance,
              "expected_source": fmt_addr(expected_source),
              "reply_source": "", "reply_src_ip": "", "reply_src_port": None,
              "source_ok": False}
    if entrance not in entrances:
        result["error"] = f"入口 {entrance} 不可选（可选：{', '.join(entrances)}）"
        return result
    wire = payload
    try:
        delay = float(delay_s)
        if delay > 0:
            wire = f"DELAY:{delay}:{payload}"  # 接收器识别的延迟回包指令
    except ValueError:
        pass
    data = wire.encode()
    result["payload"] = wire
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        # 固定源地址，使不同入口的试传使用同一客户端 IP:端口。
        sock.bind((TEST_SOURCE_HOST, int(client_port_s) if client_port_s else 0))
        sock.settimeout(wait)
        result["client"] = fmt_addr(sock.getsockname())
        t0 = time.monotonic()
        sock.sendto(data, (entrance, listen_port))
        try:
            reply, src = sock.recvfrom(65535)
            result["rtt"] = round(time.monotonic() - t0, 3)
            result["reply_source"] = fmt_addr(src)
            result["reply_src_ip"] = src[0]
            result["reply_src_port"] = src[1]
            result["source_ok"] = (src[0], src[1]) == expected_source
            if result["source_ok"]:
                result["ok"] = True
                result["reply"] = preview(reply)
            else:
                # 回包内容仍展示，但来源不符必须报错。
                result["reply"] = preview(reply)
                result["error"] = (
                    f"回包来源不符：期望 {fmt_addr(expected_source)}，"
                    f"实际 {fmt_addr(src)}"
                )
        except socket.timeout:
            result["error"] = f"等待回包超时（{wait}s，入口 {entrance}）"
    except Exception as exc:
        result["error"] = f"发送失败：{exc}"
    finally:
        sock.close()
    time.sleep(0.05)  # 等代理把日志落库
    if result["client"]:
        result["receiver"] = store.latest_receiver_for_client(
            result["client"], entrance=entrance
        )
    return result


# ---------------- 启动：asyncio 线程 + Flask 主线程 ----------------
def _run_loop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    state.loop = loop
    loop.run_until_complete(state.start())
    loop.run_forever()


def main():
    t = threading.Thread(target=_run_loop, name="udp-proxy-loop", daemon=True)
    t.start()
    print(f"[proxy] 操作网页 http://{HTTP_HOST}:{HTTP_PORT}", flush=True)
    app.run(host=HTTP_HOST, port=HTTP_PORT, threaded=True, use_reloader=False)


if __name__ == "__main__":
    main()
