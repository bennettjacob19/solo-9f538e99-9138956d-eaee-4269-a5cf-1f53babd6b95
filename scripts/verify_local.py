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
        BASE + path, data=urllib.parse.urlencode(data).encode(), timeout=60
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


def udp_client():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("127.0.0.1", UDP_PORT))
    return s


def udp_client_bound(port):
    """绑定固定源端口的终端（试传区/只送最新测试用）。"""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    s.bind(("127.0.0.1", port))
    s.connect(("127.0.0.1", UDP_PORT))
    return s


def send_recv(sock, payload: bytes, timeout=5.0):
    sock.settimeout(timeout)
    sock.send(payload)
    return sock.recv(65535)


def collect(sock, timeout, want=None):
    """收集回包直到超时或收够 want 条；返回 [(payload, 到达偏移秒)]。"""
    out = []
    t0 = time.monotonic()
    sock.settimeout(0.2)
    while time.monotonic() - t0 < timeout:
        if want is not None and len(out) >= want:
            break
        try:
            data = sock.recv(65535)
            out.append((data, round(time.monotonic() - t0, 2)))
        except socket.timeout:
            pass
    return out


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


def session_of(client):
    for s in http_get("/api/state")["sessions"]:
        if s["client"] == client:
            return s
    return None


def start_receiver(name, port, logdir):
    env = dict(os.environ, RECEIVER_NAME=name, LISTEN_PORT=str(port), REPLY_DELAY="0")
    log = open(os.path.join(logdir, f"receiver-{name}.log"), "w")
    return subprocess.Popen([sys.executable, RECEIVER_PY], env=env,
                            stdout=log, stderr=subprocess.STDOUT)


def start_proxy(db_path, logdir, tag):
    env = dict(
        os.environ,
        UDP_LISTEN_PORT=str(UDP_PORT), HTTP_PORT=str(HTTP_PORT),
        RECEIVER_A_HOST="127.0.0.1", RECEIVER_A_PORT="19001",
        RECEIVER_B_HOST="127.0.0.1", RECEIVER_B_PORT="19002",
        SESSION_IDLE_TIMEOUT="5", LATEST_ONLY_WAIT="2", DB_PATH=db_path,
    )
    log = open(os.path.join(logdir, f"proxy-{tag}.log"), "w")
    return subprocess.Popen([sys.executable, PROXY_PY], env=env,
                            stdout=log, stderr=subprocess.STDOUT)


def main():
    tmp = tempfile.mkdtemp(prefix="proxy-verify-")
    db_path = os.path.join(tmp, "proxy.db")
    procs = [start_receiver("A", 19001, tmp), start_receiver("B", 19002, tmp)]
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

        # 7. 「只送最新读数」：回包触发转发留存读数 + 替换计数
        http_post("/latest-only/add", {"ip": "127.0.0.1", "port": "31101"})
        st = http_get("/api/state")
        check("只送最新设置已生效（列表可见）", "127.0.0.1:31101" in st["latest_only"])
        c = udp_client_bound(31101)
        client = "127.0.0.1:31101"
        c.send(b"DELAY:0.6:L1-a")   # 立即转发，0.6s 后回包
        time.sleep(0.05)
        c.send(b"DELAY:0:L1-b")     # 等待期间留存
        c.send(b"DELAY:0:L1-c")     # 替换 L1-b
        reps = collect(c, 4, want=2)
        payloads = sorted(p for p, _ in reps)
        check("只送最新：实收 L1-a 与 L1-c 两条回包",
              payloads == sorted([b"DELAY:0.6:L1-a", b"DELAY:0:L1-c"]), str(reps))
        time.sleep(0.3)
        sess = session_of(client)
        check("只送最新：被替换读数 1 条", sess and sess["replaced"] == 1, str(sess))
        check("会话模式为只送最新", sess and sess["mode"] == "只送最新", str(sess))
        e = log_entries(client, "留存待发", "L1-c")
        check("留存待发有日志", bool(e), str(e))
        e = log_entries(client, "丢弃", "L1-b")
        check("被替换读数有丢弃日志", bool(e), str(e))

        # 8. 「只送最新读数」：等满 2 秒转发留存读数；旧读数的回包超时后到达
        #    被丢弃，不能结束新读数的等待；新读数自己的回包才有效
        http_post("/latest-only/add", {"ip": "127.0.0.1", "port": "31102"})
        c = udp_client_bound(31102)
        client = "127.0.0.1:31102"
        c.send(b"DELAY:2.5:S1")   # 立即转发；回包 2.5s 后到（超过 2s 等待 → 迟到）
        time.sleep(0.05)
        c.send(b"DELAY:1:S2")     # 留存；t=2 超时后转发，其回包 t=3 到（有效期内）
        # 时间线：t=0 发 S1；t=2 等满 2s 转发 S2；t=2.5 S1 的回包到达（应丢弃，
        # 不能结束 S2 的等待）；t=3 S2 的回包到达（应交付终端）
        reps = collect(c, 6)
        check("等满2秒转发留存读数：终端仅收到 S2 的回包",
              len(reps) == 1 and reps[0][0] == b"DELAY:1:S2", str(reps))
        check("旧回包未结束新读数等待（回包在 2.7~4.5s 到达）",
              reps and 2.7 <= reps[0][1] <= 4.5, str(reps))
        time.sleep(0.3)
        e = log_entries(client, "丢弃", "超时后到达")
        check("超时后到达的回包被丢弃并有日志", bool(e), str(e))

        # 9. 设置从下次建会话起生效：已建会话保持普通透传
        c = udp_client_bound(31103)
        client = "127.0.0.1:31103"
        check("设置前普通会话透传", send_recv(c, b"k1") == b"k1")
        http_post("/latest-only/add", {"ip": "127.0.0.1", "port": "31103"})
        c.send(b"DELAY:0.4:K-a")    # 已建会话不受设置影响
        time.sleep(0.05)
        c.send(b"K-b")
        c.send(b"K-c")
        reps = collect(c, 4, want=3)
        check("设置后旧会话仍普通透传（3 条全收）", len(reps) == 3, str(reps))
        print("--- 等待 6 秒让会话空闲过期 ---")
        time.sleep(6)
        c.send(b"DELAY:0.5:K-d")    # 过期重建，新会话按只送最新处理
        time.sleep(0.05)
        c.send(b"K-e")
        c.send(b"K-f")
        reps = collect(c, 4, want=2)
        payloads = sorted(p for p, _ in reps)
        check("过期重建后只送最新生效（实收 K-d 与 K-f）",
              payloads == sorted([b"DELAY:0.5:K-d", b"K-f"]), str(reps))

        # 10. 试传区：同一固定源端口连发，只送最新模式下报实收与替换数
        http_post("/latest-only/add", {"ip": "127.0.0.1", "port": "31104"})
        http_post("/test", {"source_port": "31104", "count": "4", "base": "BL",
                            "wait": "15", "d0": "0.5", "d1": "0", "d2": "0", "d3": "0"})
        lt = http_get("/api/state")["last_test"]
        check("试传区连发 4 条", lt and lt["sent"] == 4, str(lt))
        check("试传区实收 2 条回包", lt and lt["received"] == 2, str(lt))
        check("试传区被替换 2 条", lt and lt["replaced"] == 2, str(lt))
        check("试传区会话模式为只送最新", lt and lt["mode"] == "只送最新", str(lt))

        # 11. 试传区：普通会话照常透传（3 条全收，无替换）
        http_post("/test", {"source_port": "31105", "count": "3", "base": "NM",
                            "wait": "15", "d0": "0.2", "d1": "0", "d2": "0"})
        lt = http_get("/api/state")["last_test"]
        check("普通会话试传实收 3 条", lt and lt["received"] == 3, str(lt))
        check("普通会话试传无替换", lt and lt["replaced"] == 0, str(lt))
        check("普通会话试传模式为普通", lt and lt["mode"] == "普通", str(lt))

        # 12. 取消「只送最新」设置
        http_post("/latest-only/delete", {"client": "127.0.0.1:31101"})
        st = http_get("/api/state")
        check("取消后设置列表移除", "127.0.0.1:31101" not in st["latest_only"])

        # 13. 重启：目标与只送最新设置保留、会话清空
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
        check("重启后只送最新设置保留", "127.0.0.1:31104" in st["latest_only"], str(st))
        check("重启后会话已清空", st["sessions"] == [])
        c3.send(b"after-restart")  # 旧会话若未清空仍在 5 秒空闲窗口内
        c3.settimeout(3)
        c3.recv(65535)
        time.sleep(0.2)
        e = log_entries(f"127.0.0.1:{c3.getsockname()[1]}", "事件", "会话建立")
        check("重启后同客户端重建了全新会话", len(e) >= 1, str(e))

        # 14. 切换保存失败时保留原目标（进程内模拟数据库故障）
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

        # 15. 状态机级：新旧读数字节完全相同，旧回包也不能结束新读数的等待；
        #     重复回包丢弃（不依赖网络时序，直接驱动代理状态机）
        sm_store = proxy_app.Store(os.path.join(tmp, "sm.db"))
        ps = proxy_app.ProxyState(sm_store)
        sent_up, sent_down = [], []  # 发往接收器的读数 / 交给终端的回包

        class FakeTransport:
            def __init__(self, sink):
                self.sink = sink

            def sendto(self, data, addr=None):
                self.sink.append(data)

            def close(self):
                pass

        sess = proxy_app.Session(1, ("127.0.0.1", 40000))
        sess.latest_only = True
        sess.state = "open"
        sess.receiver_name = "A"
        sess.transport = FakeTransport(sent_up)
        ps.sessions[sess.client_addr] = sess
        ps.client_transport = FakeTransport(sent_down)
        ps._arm_timer = lambda s: None  # 定时器改由手动触发
        ps._on_reading(sess, b"SAME")             # 无等待 → 立即转发
        ps._on_reading(sess, b"SAME")             # 字节完全相同 → 留存待发
        ok = sent_up == [b"SAME"] and sess.pending_reading == b"SAME"
        ps._wait_timeout(sess, sess.wait_since)   # 等满 2 秒 → 转发留存读数
        ok = ok and sent_up == [b"SAME", b"SAME"] and sess.stale_replies == 1
        ps.on_upstream_datagram(sess, b"SAME")    # 旧读数的迟到回包（字节相同）
        ok = ok and sent_down == [] and sess.awaiting  # 未交付，新读数仍在等回包
        ps.on_upstream_datagram(sess, b"SAME")    # 新读数的回包 → 原样交付终端
        ok = ok and sent_down == [b"SAME"] and not sess.awaiting
        ps.on_upstream_datagram(sess, b"SAME")    # 重复回包 → 丢弃
        ok = ok and sent_down == [b"SAME"]
        check("相同字节：旧回包不结束新等待、重复回包丢弃", ok)

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
