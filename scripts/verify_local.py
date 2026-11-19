#!/usr/bin/env python3
"""本地端到端验证：不依赖 Docker，直接以子进程启动两个接收器和代理，
逐项验证需求行为。用法：python3 scripts/verify_local.py
"""

import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.parse
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PROXY_PY = os.path.join(ROOT, "proxy", "app.py")
RECEIVER_PY = os.path.join(ROOT, "receiver", "receiver.py")

UDP_PORT = 19000
HTTP_PORT = 18080
BASE = f"http://127.0.0.1:{HTTP_PORT}"

_failures = []

# 本机是否支持 127.0.0.2（Linux 回环整段可用）。
try:
    _probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    _probe.bind(("127.0.0.2", 0))
    _probe.close()
    MULTI = True
except OSError:
    MULTI = False


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + (f"  -- {detail}" if detail and not cond else ""))
    if not cond:
        _failures.append(name)


def http_get(path):
    with urllib.request.urlopen(BASE + path, timeout=10) as r:
        return json.load(r)


def http_post(path, data):
    with urllib.request.urlopen(
        BASE + path, data=urllib.parse.urlencode(data).encode(), timeout=40
    ) as r:
        return r.status


def wait_http_up(timeout=20):
    t0 = time.time()
    while time.time() - t0 < timeout:
        try:
            http_get("/api/state")
            return True
        except Exception:
            time.sleep(0.3)
    return False


def udp_client(bind_ip="127.0.0.1", bind_port=0, dest="127.0.0.1"):
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind((bind_ip, bind_port))
    s.connect((dest, UDP_PORT))
    return s


def send_recv_from(sock, payload: bytes, dest_ip, timeout=5.0):
    """未 connect 的套接字：发往指定入口，返回 (回包, 源地址)。"""
    sock.settimeout(timeout)
    sock.sendto(payload, (dest_ip, UDP_PORT))
    return sock.recvfrom(65535)


def send_recv(sock, payload: bytes, timeout=5.0):
    sock.settimeout(timeout)
    sock.send(payload)
    return sock.recv(65535)


def log_entries(client=None, direction=None, text=None):
    msgs = http_get("/api/state")["messages"]
    out = []
    for m in msgs:
        if client and m["client"] != client:
            continue
        if direction and m["direction"] != direction:
            continue
        if text and text not in m["payload"] and text not in m["note"]:
            continue
        out.append(m)
    return out


def start_receiver(name, port, logdir):
    env = dict(os.environ, RECEIVER_NAME=name, LISTEN_PORT=str(port), REPLY_DELAY="0")
    log = open(os.path.join(logdir, f"receiver-{name}.log"), "w")
    return subprocess.Popen([sys.executable, RECEIVER_PY], env=env,
                            stdout=log, stderr=subprocess.STDOUT)


def start_proxy(db_path, logdir, tag, extra_env=None):
    env = dict(
        os.environ,
        UDP_LISTEN_PORT=str(UDP_PORT), HTTP_PORT=str(HTTP_PORT),
        RECEIVER_A_HOST="127.0.0.1", RECEIVER_A_PORT="19001",
        RECEIVER_B_HOST="127.0.0.1", RECEIVER_B_PORT="19002",
        SESSION_IDLE_TIMEOUT="5", DB_PATH=db_path,
    )
    if extra_env:
        env.update(extra_env)
    log = open(os.path.join(logdir, f"proxy-{tag}.log"), "w")
    return subprocess.Popen([sys.executable, PROXY_PY], env=env,
                            stdout=log, stderr=subprocess.STDOUT)


def check_reply_source_mismatch(procs, tmp):
    """负向验证：回包源地址与入口不一致时，网页试传必须报错。"""
    print("--- 回包来源不符验证 ---")
    if not MULTI:
        print("  （环境不支持 127.0.0.2，跳过）")
        return
    udp_port = UDP_PORT + 20  # 未被其他验证进程占用的端口
    # 用假代理占用该端口：绑定通配地址，从 127.0.0.1 回包；试传入口是 127.0.0.2。
    log = open(os.path.join(tmp, "fake-proxy.log"), "w")
    fake = subprocess.Popen(
        [sys.executable, os.path.join(ROOT, "scripts", "fake_proxy.py"), str(udp_port)],
        stdout=log, stderr=subprocess.STDOUT,
    )
    procs.append(fake)
    try:
        time.sleep(0.5)
        up = False
        for _ in range(50):
            try:
                urllib.request.urlopen(BASE + "/api/state", timeout=1)
                up = True
                break
            except Exception:
                time.sleep(0.2)
        if not up:
            check("来源不符验证环境启动", False, f"日志见 {tmp}")
            return
        # 直接验证 run_test 的来源校验逻辑（不依赖真实代理监听该端口）。
        sys.path.insert(0, os.path.join(ROOT, "proxy"))
        os.environ["DB_PATH"] = os.path.join(tmp, "mismatch.db")
        import importlib
        import app as proxy_app
        importlib.reload(proxy_app)
        lt = proxy_app.run_test("x", "0", "", "5", "127.0.0.2",
                                listen_port=udp_port,
                                entrances=["127.0.0.1", "127.0.0.2"])
        check("回包来源不符时试传报错",
              not lt["ok"] and "来源不符" in (lt["error"] or ""), str(lt))
        check("回包来源不符时仍显示实收来源",
              lt["reply_source"] == f"127.0.0.1:{udp_port}"
              and lt["expected_source"] == f"127.0.0.2:{udp_port}", str(lt))

        # 单地址监听的代理：试传仍应成功且来源为绑定地址。
        single_port = udp_port + 1
        log = open(os.path.join(tmp, "proxy-single-web.log"), "w")
        sp = subprocess.Popen(
            [sys.executable, PROXY_PY],
            env=dict(os.environ, DB_PATH=os.path.join(tmp, "single-web.db"),
                     UDP_LISTEN_HOST="127.0.0.1", UDP_LISTEN_PORT=str(single_port),
                     HTTP_PORT=str(HTTP_PORT + 3),
                     RECEIVER_A_HOST="127.0.0.1", RECEIVER_A_PORT="19001",
                     RECEIVER_B_HOST="127.0.0.1", RECEIVER_B_PORT="19002",
                     TEST_ENTRANCES="127.0.0.1"),
            stdout=log, stderr=subprocess.STDOUT,
        )
        procs.append(sp)
        up2 = False
        for _ in range(50):
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{HTTP_PORT + 3}/api/state",
                                       timeout=1)
                up2 = True
                break
            except Exception:
                time.sleep(0.2)
        check("单地址监听网页试传环境启动", up2)
        if up2:
            urllib.request.urlopen(
                f"http://127.0.0.1:{HTTP_PORT + 3}/test",
                data=urllib.parse.urlencode({"payload": "s1", "entrance": "127.0.0.1",
                                             "wait": "5"}).encode(),
                timeout=10,
            ).read()
            lt = json.loads(urllib.request.urlopen(
                f"http://127.0.0.1:{HTTP_PORT + 3}/api/state", timeout=10
            ).read())["last_test"]
            check("单地址监听网页试传成功",
                  lt and lt["ok"] and lt["reply"] == "s1", str(lt))
            check("单地址监听回包源地址为绑定地址",
                  lt and lt["reply_source"] == f"127.0.0.1:{single_port}", str(lt))

        # 无法从原入口地址发出回包：丢弃并记日志，绝不改从别的地址发出。
        pa = proxy_app
        client_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        client_sock.bind(("127.0.0.1", 0))
        proxy_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        proxy_sock.setsockopt(socket.IPPROTO_IP, pa.IP_PKTINFO, 1)
        proxy_sock.bind(("0.0.0.0", 0))
        pa.state.client_sock = proxy_sock
        pa.state.client_pktinfo = True
        try:
            probe = pa.Session(9999, client_sock.getsockname(), "10.255.255.1")
            client_sock.settimeout(0.5)
            sent = pa.state._send_to_client(probe, b"unreachable")
            got = None
            try:
                got = client_sock.recvfrom(100)
            except socket.timeout:
                pass
            check("无法从原入口发出时丢弃", not sent and got is None, str(got))
            dropped = [m for m in pa.store.recent_messages(20)
                       if "无法从入口" in m["note"]]
            check("丢弃回包记入日志", bool(dropped), str(dropped))
        finally:
            client_sock.close()
            proxy_sock.close()
            pa.state.client_sock = None
    finally:
        fake.terminate()
        fake.wait(timeout=5)


def check_multi_entrance(procs, tmp):
    """多入口验证：同一源 IP:端口分别访问 127.0.0.1 与 127.0.0.2。

    代理先切到 A，客户端经 127.0.0.1 建会话；再切到 B，同一源 IP:端口经
    127.0.0.2 建会话。两路互不串会话，回包源地址各自等于入口地址。
    """
    print("--- 多入口验证 ---")
    if not MULTI:
        print("  （环境不支持 127.0.0.2，跳过）")
        return
    http_post("/switch", {"target": "A"})
    time.sleep(6)  # 让已有会话空闲过期，确保从干净状态开始

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind(("127.0.0.1", 0))
    src = sock.getsockname()
    src_label = f"{src[0]}:{src[1]}"
    try:
        # 入口 127.0.0.1：建会话，固定到 A
        reply, got = send_recv_from(sock, b"entry1", "127.0.0.1")
        check("入口 127.0.0.1 回包正确", reply == b"entry1", str(reply))
        check("入口 127.0.0.1 回包源地址为 127.0.0.1:端口",
              got == ("127.0.0.1", UDP_PORT), str(got))

        # 切到 B，但旧会话（入口 127.0.0.1）仍走 A
        http_post("/switch", {"target": "B"})
        reply, got = send_recv_from(sock, b"entry1-again", "127.0.0.1")
        check("切 B 后入口 127.0.0.1 旧会话仍有回包", reply == b"entry1-again", str(reply))

        # 同一源 IP:端口访问入口 127.0.0.2：必须新建会话（走当前目标 B），
        # 不能复用 127.0.0.1 入口的会话。
        reply, got = send_recv_from(sock, b"entry2", "127.0.0.2")
        check("同源 IP:端口经 127.0.0.2 建新会话并有回包", reply == b"entry2", str(reply))
        check("入口 127.0.0.2 回包源地址为 127.0.0.2:端口",
              got == ("127.0.0.2", UDP_PORT), str(got))
        time.sleep(0.3)
        e = log_entries(src_label, "客户端→接收器", "entry2")
        check("入口 127.0.0.2 的新会话走接收器 B", e and e[0]["receiver"] == "B", str(e))
        check("日志记录了入口 IP 127.0.0.2", e and e[0]["entrance"] == "127.0.0.2", str(e))
        e = log_entries(src_label, "客户端→接收器", "entry1-again")
        check("入口 127.0.0.1 的旧会话仍走接收器 A", e and e[0]["receiver"] == "A", str(e))

        # 两路会话同时在活动会话表中，入口 IP 不同。
        sessions = [s for s in http_get("/api/state")["sessions"] if s["client"] == src_label]
        ips = sorted(s["local_ip"] for s in sessions)
        check("活动会话表列出两个入口 IP", ips == ["127.0.0.1", "127.0.0.2"], str(sessions))

        # 两路独立过期：只让入口 127.0.0.2 空闲超过 5 秒，127.0.0.1 持续保活。
        deadline = time.time() + 6
        while time.time() < deadline:
            send_recv_from(sock, b"keepalive", "127.0.0.1")
            time.sleep(1)
        sessions = [s for s in http_get("/api/state")["sessions"] if s["client"] == src_label]
        ips = sorted(s["local_ip"] for s in sessions)
        check("仅空闲的入口 127.0.0.2 会话过期",
              ips == ["127.0.0.1"], str(sessions))

        # 页面试传：可选 127.0.0.2 入口并显示实际回包源地址。
        http_post("/test", {"payload": "web2", "wait": "5", "delay": "0",
                            "client_port": "", "entrance": "127.0.0.2"})
        time.sleep(0.2)
        lt = http_get("/api/state")["last_test"]
        check("网页试传可选入口 127.0.0.2 且回包成功",
              lt and lt["ok"] and lt["reply"] == "web2" and lt["entrance"] == "127.0.0.2",
              str(lt))
        check("网页试传显示实收回包源地址",
              lt and lt["reply_source"] == f"127.0.0.2:{UDP_PORT}", str(lt))

        # 单地址监听照常工作。
        single_db = os.path.join(tmp, "single.db")
        single_env = {"UDP_LISTEN_HOST": "127.0.0.1", "HTTP_PORT": str(HTTP_PORT + 1),
                      "UDP_LISTEN_PORT": str(UDP_PORT + 1), "TEST_ENTRANCES": "127.0.0.1",
                      "RECEIVER_A_PORT": "19011", "RECEIVER_B_PORT": "19012"}
        log = open(os.path.join(tmp, "proxy-single.log"), "w")
        sp = subprocess.Popen(
            [sys.executable, PROXY_PY],
            env=dict(os.environ, DB_PATH=single_db, **single_env),
            stdout=log, stderr=subprocess.STDOUT,
        )
        procs.append(sp)
        up = False
        for _ in range(50):
            try:
                urllib.request.urlopen(f"http://127.0.0.1:{HTTP_PORT + 1}/api/state", timeout=1)
                up = True
                break
            except Exception:
                time.sleep(0.2)
        check("单地址监听代理启动", up)
        if up:
            s2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s2.bind(("127.0.0.1", 0))
            s2.settimeout(5)
            s2.sendto(b"single", ("127.0.0.1", UDP_PORT + 1))
            try:
                r, a = s2.recvfrom(65535)
            except socket.timeout:
                r, a = b"", None
            s2.close()
            check("单地址监听原样回包", r == b"single", repr(r))
            check("单地址监听回包源地址正确",
                  a == ("127.0.0.1", UDP_PORT + 1), str(a))
    finally:
        sock.close()


def main():
    tmp = tempfile.mkdtemp(prefix="proxy-verify-")
    db_path = os.path.join(tmp, "proxy.db")
    procs = [start_receiver("A", 19001, tmp), start_receiver("B", 19002, tmp),
             start_receiver("A2", 19011, tmp), start_receiver("B2", 19012, tmp)]
    proxy = start_proxy(db_path, tmp, "run1")
    procs.append(proxy)
    try:
        if not wait_http_up():
            check("代理启动", False, f"日志见 {tmp}")
            return 1
        print(f"--- 环境就绪（日志目录 {tmp}）---")

        # 1. 初始目标为 A，报文原样往返
        st = http_get("/api/state")
        check("初始目标为 A", st["target"] == "A")
        c1 = udp_client()
        client1 = f"127.0.0.1:{c1.getsockname()[1]}"
        m1 = b"m1-hello"
        check("报文原样回传", send_recv(c1, m1) == m1)
        time.sleep(0.2)
        e = log_entries(client1, "客户端→接收器", "m1-hello")
        check("m1 实际去向为接收器 A", e and e[0]["receiver"] == "A", str(e))

        # 2. 二进制原样透传
        blob = bytes(range(256))
        check("二进制报文原样透传", send_recv(c1, blob) == blob)

        # 3. 网页切换目标到 B
        http_post("/switch", {"target": "B"})
        check("切换后当前目标为 B", http_get("/api/state")["target"] == "B")

        # 4. 旧会话（空闲超时内）仍走原接收器 A
        m2 = b"m2-stick"
        check("旧会话报文仍有回包", send_recv(c1, m2) == m2)
        time.sleep(0.2)
        e = log_entries(client1, "客户端→接收器", "m2-stick")
        check("切换后旧会话继续走接收器 A", e and e[0]["receiver"] == "A", str(e))

        # 5. 空闲 5 秒过期后，同 IP:端口 重建会话走新接收器 B
        print("--- 等待 6 秒让会话空闲过期 ---")
        time.sleep(6)
        m3 = b"m3-newsession"
        check("新会话报文有回包", send_recv(c1, m3) == m3)
        time.sleep(0.2)
        e = log_entries(client1, "客户端→接收器", "m3-newsession")
        check("过期重建的新会话走接收器 B", e and e[0]["receiver"] == "B", str(e))

        # 6. 旧会话迟到的回包：同 IP:端口 重建会话也收不到
        c2 = udp_client()
        client2 = f"127.0.0.1:{c2.getsockname()[1]}"
        t0 = time.time()
        c2.send(b"DELAY:7:late1")          # 接收器 7 秒后才回包；会话 5 秒即过期
        time.sleep(6 - (time.time() - t0))  # 等到会话过期之后
        check("过期后新会话报文有回包", send_recv(c2, b"ping2", timeout=3) == b"ping2")
        # 旧回包在 t0+7 到达，等到 t0+9 确认没有迟到回包送进来
        got_late = False
        c2.settimeout(0.2)
        while time.time() - t0 < 9:
            try:
                if b"late1" in c2.recv(65535):
                    got_late = True
            except socket.timeout:
                pass
        check("旧会话迟到的回包未送达重建的会话", not got_late)
        time.sleep(0.3)
        e = log_entries(client2, "接收器→客户端", "late1")
        check("日志中无 late1 的回包下发记录", not e, str(e))

        # 7. 网页试传报文
        http_post("/test", {"payload": "webtest1", "wait": "5", "delay": "0",
                            "client_port": ""})
        time.sleep(0.2)
        lts = http_get("/api/state")["last_test"]
        check("网页试传成功且回包一致",
              lts and lts["ok"] and lts["reply"] == "webtest1", str(lts))
        check("网页试传显示实际去向为 B", lts and lts["receiver"] == "B", str(lts))
        check("网页试传回包源地址与入口一致",
              lts and lts["reply_source"] == f"127.0.0.1:{UDP_PORT}"
              and lts.get("source_ok"), str(lts))

        # 8. 重启：目标保留、会话清空
        c3 = udp_client()
        c3.send(b"before-restart")
        c3.settimeout(3)
        c3.recv(65535)
        proxy.terminate()
        proxy.wait(timeout=10)
        proxy = start_proxy(db_path, tmp, "run2")
        procs.append(proxy)
        check("代理重启后恢复", wait_http_up())
        st = http_get("/api/state")
        check("重启后目标保留为 B", st["target"] == "B")
        check("重启后会话已清空", st["sessions"] == [])
        c3.send(b"after-restart")  # 旧会话若未清空仍在 5 秒空闲窗口内
        c3.settimeout(3)
        c3.recv(65535)
        time.sleep(0.2)
        e = log_entries(f"127.0.0.1:{c3.getsockname()[1]}", "事件", "会话建立")
        check("重启后同客户端重建了全新会话", len(e) >= 1, str(e))

        # 9. 切换保存失败时保留原目标（进程内模拟数据库故障）
        sys.path.insert(0, os.path.join(ROOT, "proxy"))
        os.environ["DB_PATH"] = os.path.join(tmp, "other.db")
        import importlib
        import app as proxy_app
        importlib.reload(proxy_app)
        s = proxy_app.Store(os.path.join(tmp, "t.db"))
        s.set_target("B")
        s._conn.close()  # 模拟存储故障
        failed = False
        try:
            s.set_target("A")
        except Exception:
            failed = True
        check("保存失败时切换报错且保留原目标", failed and s.get_target() == "B")

        check_multi_entrance(procs, tmp)
        check_reply_source_mismatch(procs, tmp)

    finally:
        for p in procs:
            try:
                p.terminate()
            except Exception:
                pass
        for p in procs:
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()

    print("---")
    if _failures:
        print(f"共 {len(_failures)} 项失败: {_failures}")
        return 1
    print("全部验证通过")
    return 0


if __name__ == "__main__":
    sys.exit(main())
