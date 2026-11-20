#!/usr/bin/env python3
"""遥测 UDP 代理 + 操作网页。

功能要点：
- 按客户端 IP:端口建立会话，空闲超时（默认 5 秒）后过期；
- 建会话时把该会话固定到当前目标接收器（A/B），会话期间不随切换改变；
- 每个会话使用一条独立的、已 connect 的上游 UDP 套接字（独立本地端口），
  因此回包天然只能属于一个会话：只把该会话对应接收器的回包送回该客户端；
- 会话过期即关闭其上游端口，且端口进入冷却期不再分配，
  所以旧会话迟到的回包不可能落到同 IP:端口重建的新会话里；
- 普通会话报文与回包原样透传，不做任何修改；
- 「只送最新读数」：值班员在控制台按终端 IP:端口 启用，从下次建会话起生效。
  启用后没有读数在等回包时，新读数立即转发；等待期间只留最后收到的一条
  待发读数，收到当前读数的回包或等满 2 秒后，再转发留存的读数；有效回包
  原样交给终端，超时后到达或重复的回包丢弃。回包按转发先后一一对应核销，
  因此即使新旧读数字节相同，旧回包也不能结束新读数的等待。
  会话过期即清掉待发读数；
- 当前目标与「只送最新」设置保存在 SQLite，重启后保留；会话全在内存，重启即清空；
- 切换目标时先落库再生效，保存失败则保留原目标；
- Flask 网页可切换目标、按终端设置只送最新、从同一固定源端口连发 3~8 条
  试传读数（逐条设置回包延迟，结果显示实收回包和被替换的读数条数）、
  查看会话与报文日志（实际去向/回包/留存/替换/丢弃）。
"""

import asyncio
import ipaddress
import os
import socket
import sqlite3
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
# 「只送最新读数」模式：等当前读数回包的最长秒数；等满即转发留存的最新读数，
# 未等到的回包按迟到处理（再到即丢弃）。
LATEST_ONLY_WAIT = float(os.environ.get("LATEST_ONLY_WAIT", "2"))
UPSTREAM_PORT_MIN = int(os.environ.get("UPSTREAM_PORT_MIN", "20000"))
UPSTREAM_PORT_MAX = int(os.environ.get("UPSTREAM_PORT_MAX", "29999"))
# 上游端口关闭后的冷却时间：冷却期内不再分配该端口，
# 保证旧会话迟到的回包不会被复用同端口的新会话收到。应大于接收器最大回包延迟。
UPSTREAM_PORT_COOLDOWN = float(os.environ.get("UPSTREAM_PORT_COOLDOWN", "30"))
LOG_KEEP = int(os.environ.get("LOG_KEEP", "1000"))

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


def fmt_addr(addr):
    return f"{addr[0]}:{addr[1]}"


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


# ---------------- SQLite：目标 + 只送最新设置持久化 + 报文日志 ----------------
class Store:
    """线程安全的 SQLite 存取。目标(target)与只送最新终端设置落库，重启后保留。"""

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
            "CREATE TABLE IF NOT EXISTS latest_only ("
            "  client TEXT PRIMARY KEY, created REAL NOT NULL)"
        )
        self._conn.execute(
            """CREATE TABLE IF NOT EXISTS message_log (
                   id INTEGER PRIMARY KEY AUTOINCREMENT,
                   ts REAL NOT NULL,
                   client TEXT NOT NULL DEFAULT '',
                   session_id INTEGER NOT NULL DEFAULT 0,
                   direction TEXT NOT NULL,
                   receiver TEXT NOT NULL DEFAULT '',
                   payload TEXT NOT NULL DEFAULT '',
                   note TEXT NOT NULL DEFAULT '')"""
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
        self._latest_only = {
            r[0] for r in self._conn.execute("SELECT client FROM latest_only")
        }
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

    # ---- 「只送最新读数」终端设置（按 终端IP:端口，持久化）----
    def list_latest_only(self):
        with self._lock:
            return sorted(self._latest_only)

    def is_latest_only(self, client):
        with self._lock:
            return client in self._latest_only

    def add_latest_only(self, client):
        with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO latest_only(client, created) VALUES(?,?)",
                (client, time.time()),
            )
            self._conn.commit()
            self._latest_only.add(client)

    def remove_latest_only(self, client):
        with self._lock:
            self._conn.execute("DELETE FROM latest_only WHERE client=?", (client,))
            self._conn.commit()
            self._latest_only.discard(client)

    def log(self, direction, client="", session_id=0, receiver="", payload="", note=""):
        try:
            with self._lock:
                self._conn.execute(
                    "INSERT INTO message_log(ts,client,session_id,direction,receiver,payload,note)"
                    " VALUES(?,?,?,?,?,?,?)",
                    (time.time(), client, session_id, direction, receiver, payload, note),
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
                "SELECT id,ts,client,session_id,direction,receiver,payload,note "
                "FROM message_log ORDER BY id DESC LIMIT ?",
                (limit,),
            ).fetchall()
        cols = ("id", "ts", "client", "session_id", "direction", "receiver", "payload", "note")
        return [dict(zip(cols, r)) for r in rows]


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
        "id", "client_addr", "receiver_name", "receiver_addr",
        "upstream_port", "transport", "state", "closed",
        "created", "last_activity", "pending",
        # 「只送最新读数」模式状态
        "latest_only",        # 建会话时按终端 IP:端口 的设置确定，会话期间不变
        "awaiting",           # 是否有一条已转发读数在等回包
        "wait_since",         # 当前等待起始时刻（monotonic）
        "wait_handle",        # 等满 LATEST_ONLY_WAIT 秒的定时器
        "pending_reading",    # 等待期间留存的最新一条待发读数（bytes 或 None）
        "stale_replies",      # 已超时但回包尚未到达的读数条数（其回包再到即丢弃）
        "replaced_count",     # 留存期间被更新读数替换、从未发出的读数条数
        "forwarded_count",    # 实际转发给接收器的读数条数
    )

    def __init__(self, sid, client_addr):
        self.id = sid
        self.client_addr = client_addr
        self.receiver_name = ""
        self.receiver_addr = None
        self.upstream_port = None
        self.transport = None
        self.state = "opening"  # opening -> open -> closed
        self.closed = False
        self.created = time.monotonic()
        self.last_activity = self.created
        self.pending = []
        self.latest_only = False
        self.awaiting = False
        self.wait_since = 0.0
        self.wait_handle = None
        self.pending_reading = None
        self.stale_replies = 0
        self.replaced_count = 0
        self.forwarded_count = 0


class ClientProtocol(asyncio.DatagramProtocol):
    """面向遥测终端的 UDP 端。"""

    def __init__(self, state):
        self.state = state

    def datagram_received(self, data, addr):
        self.state.on_client_datagram(data, addr)

    def error_received(self, exc):
        pass


class UpstreamProtocol(asyncio.DatagramProtocol):
    """每个会话一条、已 connect 到其接收器的上游 UDP 端。
    只有该会话对应接收器的回包会到达这里。"""

    def __init__(self, state):
        self.state = state
        self.session = None

    def datagram_received(self, data, addr):
        sess = self.session
        if sess is not None:
            self.state.on_upstream_datagram(sess, data)

    def error_received(self, exc):
        pass  # 如对端 ICMP 不可达，忽略


class ProxyState:
    def __init__(self, store):
        self.store = store
        self.loop = None
        self.client_transport = None
        self.sessions = {}  # client_addr -> Session（仅事件循环线程修改）
        self.lock = threading.RLock()
        self.next_session_id = 1
        self.ports = PortAllocator(UPSTREAM_PORT_MIN, UPSTREAM_PORT_MAX, UPSTREAM_PORT_COOLDOWN)

    async def start(self):
        transport, _ = await self.loop.create_datagram_endpoint(
            lambda: ClientProtocol(self),
            local_addr=(UDP_LISTEN_HOST, UDP_LISTEN_PORT),
        )
        self.client_transport = transport
        self.loop.create_task(self._sweeper())
        print(f"[proxy] 客户端 UDP 监听 {UDP_LISTEN_HOST}:{UDP_LISTEN_PORT}", flush=True)
        print(f"[proxy] 空闲会话超时 {SESSION_IDLE_TIMEOUT}s，"
              f"只送最新等待 {LATEST_ONLY_WAIT}s，当前目标 {self.store.get_target()}",
              flush=True)

    # ---- 客户端方向 ----
    def on_client_datagram(self, data, addr):
        now = time.monotonic()
        with self.lock:
            sess = self.sessions.get(addr)
            if sess is not None and sess.closed:
                sess = None
            if (sess is not None and sess.state == "open"
                    and now - sess.last_activity > SESSION_IDLE_TIMEOUT):
                self._close_session(sess, "idle-timeout")
                sess = None
            if sess is None:
                # 建会话：固定到“当前”目标接收器；
                # 「只送最新读数」按终端 IP:端口 的当前设置确定，从本次建会话起生效
                sess = Session(self.next_session_id, addr)
                self.next_session_id += 1
                sess.latest_only = self.store.is_latest_only(fmt_addr(addr))
                self.sessions[addr] = sess
                sess.pending.append(data)
                self.loop.create_task(self._open_session(sess))
                return
            sess.last_activity = now
            if sess.state == "opening":
                sess.pending.append(data)
                return
        self._on_reading(sess, data)

    async def _open_session(self, sess):
        target = self.store.get_target()  # 建会话时固定接收器
        host, port = RECEIVERS[target]
        client = fmt_addr(sess.client_addr)
        try:
            transport = None
            upstream_port = None
            last_err = None
            for _ in range(8):
                upstream_port = self.ports.allocate()
                try:
                    transport, proto = await self.loop.create_datagram_endpoint(
                        lambda: UpstreamProtocol(self),
                        local_addr=("0.0.0.0", upstream_port),
                        remote_addr=(host, port),  # connect：只收该接收器的回包
                    )
                    break
                except OSError as exc:
                    last_err = exc
                    self.ports.discard(upstream_port)
            if transport is None:
                raise last_err or RuntimeError("无可用上游端口")
            with self.lock:
                if sess.closed:
                    transport.close()
                    self.ports.release(upstream_port)
                    return
                sess.receiver_name = target
                sess.receiver_addr = (host, port)
                sess.upstream_port = upstream_port
                sess.transport = transport
                sess.state = "open"
                proto.session = sess
                pending, sess.pending = sess.pending, []
            mode = "只送最新" if sess.latest_only else "普通透传"
            self.store.log("event", client=client, session_id=sess.id, receiver=target,
                           note=f"会话建立（{mode}），固定到接收器 {target}（上游端口 {upstream_port}）")
            for data in pending:
                self._on_reading(sess, data)
        except Exception as exc:
            self.store.log("event", client=client, session_id=sess.id,
                           note=f"会话建立失败: {exc}")
            self.close_session(sess, f"open-failed: {exc}")

    # ---- 「只送最新读数」状态机 ----
    def _on_reading(self, sess, data):
        """终端来了一条读数。普通会话立即原样转发；只送最新会话：
        无读数等回包时立即转发，等待期间只留最后收到的一条待发。"""
        with self.lock:
            if sess.closed or sess.state != "open":
                return
            now = time.monotonic()
            sess.last_activity = now
            if sess.latest_only:
                if sess.awaiting:
                    old = sess.pending_reading
                    sess.pending_reading = data
                    if old is not None:
                        sess.replaced_count += 1
                        self.store.log("drop", client=fmt_addr(sess.client_addr),
                                       session_id=sess.id, receiver=sess.receiver_name,
                                       payload=preview(old),
                                       note="被更新的读数替换，未发送")
                    self.store.log("hold", client=fmt_addr(sess.client_addr),
                                   session_id=sess.id, receiver=sess.receiver_name,
                                   payload=preview(data),
                                   note="等待回包期间只留最新一条，待发")
                    return
                sess.awaiting = True
                sess.wait_since = now
                self._arm_timer(sess)
        self._send_reading(sess, data)

    def _send_reading(self, sess, data):
        """实际把一条读数原样发往该会话固定的接收器。"""
        with self.lock:
            if sess.closed or sess.transport is None:
                return
            sess.last_activity = time.monotonic()
            sess.forwarded_count += 1
            transport = sess.transport
            receiver = sess.receiver_name
        try:
            transport.sendto(data)  # 已 connect，原样发往该会话的接收器
        except Exception as exc:
            self.store.log("event", client=fmt_addr(sess.client_addr), session_id=sess.id,
                           note=f"转发失败: {exc}")
            return
        self.store.log("c2r", client=fmt_addr(sess.client_addr), session_id=sess.id,
                       receiver=receiver, payload=preview(data))

    def _arm_timer(self, sess):
        """等回包最长 LATEST_ONLY_WAIT 秒；调用方需持有 self.lock。"""
        self._cancel_timer(sess)
        sess.wait_handle = self.loop.call_later(
            LATEST_ONLY_WAIT, self._wait_timeout, sess, sess.wait_since)

    def _cancel_timer(self, sess):
        handle, sess.wait_handle = sess.wait_handle, None
        if handle is not None:
            handle.cancel()

    def _wait_timeout(self, sess, token):
        """等满 LATEST_ONLY_WAIT 秒无回包：解除等待，转发留存的最新读数；
        未等到的回包记为迟到（stale），再到即丢弃。"""
        nxt = None
        with self.lock:
            if sess.closed or not sess.awaiting or sess.wait_since != token:
                return
            sess.wait_handle = None
            sess.stale_replies += 1
            nxt = sess.pending_reading
            sess.pending_reading = None
            if nxt is not None:
                sess.wait_since = time.monotonic()
                self._arm_timer(sess)
                self.store.log("event", client=fmt_addr(sess.client_addr),
                               session_id=sess.id, receiver=sess.receiver_name,
                               note=f"等满 {LATEST_ONLY_WAIT}s 无回包，转发留存的最新读数；"
                                    f"旧回包再到将按迟到丢弃")
            else:
                sess.awaiting = False
                self.store.log("event", client=fmt_addr(sess.client_addr),
                               session_id=sess.id, receiver=sess.receiver_name,
                               note=f"等满 {LATEST_ONLY_WAIT}s 无回包，解除等待；"
                                    f"该读数回包再到将按迟到丢弃")
        if nxt is not None:
            self._send_reading(sess, nxt)

    # ---- 接收器方向 ----
    def on_upstream_datagram(self, sess, data):
        now = time.monotonic()
        nxt = None
        with self.lock:
            if sess.closed or self.sessions.get(sess.client_addr) is not sess:
                self.store.log("drop", client=fmt_addr(sess.client_addr), session_id=sess.id,
                               receiver=sess.receiver_name, payload=preview(data),
                               note="旧会话迟到的回包，已丢弃")
                return
            if now - sess.last_activity > SESSION_IDLE_TIMEOUT:
                self._close_session(sess, "idle-timeout")
                self.store.log("drop", client=fmt_addr(sess.client_addr), session_id=sess.id,
                               receiver=sess.receiver_name, payload=preview(data),
                               note="回包到达时会话已过期，已丢弃")
                return
            sess.last_activity = now
            if sess.latest_only:
                if sess.stale_replies > 0:
                    # 回包按转发先后一一对应：先核销已超时的旧读数。
                    # 即使新旧读数字节相同，旧回包也不能结束新读数的等待。
                    sess.stale_replies -= 1
                    self.store.log("drop", client=fmt_addr(sess.client_addr),
                                   session_id=sess.id, receiver=sess.receiver_name,
                                   payload=preview(data),
                                   note="超时后到达的回包，已丢弃")
                    return
                if not sess.awaiting:
                    self.store.log("drop", client=fmt_addr(sess.client_addr),
                                   session_id=sess.id, receiver=sess.receiver_name,
                                   payload=preview(data),
                                   note="重复的回包，已丢弃")
                    return
                # 有效回包：结束当前等待；如有留存读数，立即转发
                sess.awaiting = False
                self._cancel_timer(sess)
                nxt = sess.pending_reading
                sess.pending_reading = None
                if nxt is not None:
                    sess.awaiting = True
                    sess.wait_since = now
                    self._arm_timer(sess)
        self.client_transport.sendto(data, sess.client_addr)  # 有效回包原样交给终端
        self.store.log("r2c", client=fmt_addr(sess.client_addr), session_id=sess.id,
                       receiver=sess.receiver_name, payload=preview(data))
        if nxt is not None:
            self._send_reading(sess, nxt)

    # ---- 会话生命周期 ----
    def _close_session(self, sess, reason):
        """调用方需持有 self.lock。"""
        if sess.closed:
            return
        sess.closed = True
        sess.state = "closed"
        if self.sessions.get(sess.client_addr) is sess:
            del self.sessions[sess.client_addr]
        self._cancel_timer(sess)
        if sess.pending_reading is not None:
            # 会话过期便清掉待发读数
            self.store.log("drop", client=fmt_addr(sess.client_addr), session_id=sess.id,
                           receiver=sess.receiver_name, payload=preview(sess.pending_reading),
                           note=f"会话关闭（{reason}），清掉待发读数")
            sess.pending_reading = None
        sess.pending = []
        sess.awaiting = False
        sess.stale_replies = 0
        transport, sess.transport = sess.transport, None
        port, sess.upstream_port = sess.upstream_port, None
        if transport is not None:
            transport.close()  # 迟到的回包到此端口将被系统丢弃
        if port is not None:
            self.ports.release(port)  # 进入冷却期，冷却内不再分配
        self.store.log("event", client=fmt_addr(sess.client_addr), session_id=sess.id,
                       receiver=sess.receiver_name, note=f"会话关闭（{reason}）")

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
                    "mode": "只送最新" if s.latest_only else "普通",
                    "receiver": s.receiver_name or "?",
                    "upstream_port": s.upstream_port,
                    "state": s.state,
                    "awaiting": s.awaiting,
                    "has_pending": s.pending_reading is not None,
                    "replaced": s.replaced_count,
                    "forwarded": s.forwarded_count,
                    "idle": round(now - s.last_activity, 1),
                    "age": round(now - s.created, 1),
                }
                for s in sorted(self.sessions.values(), key=lambda x: x.id)
                if not s.closed
            ]

    def session_stats(self, client):
        """供试传区读取某终端会话的实时统计；无会话时返回 None。"""
        with self.lock:
            for s in self.sessions.values():
                if not s.closed and fmt_addr(s.client_addr) == client:
                    return {
                        "latest_only": s.latest_only,
                        "awaiting": s.awaiting,
                        "has_pending": s.pending_reading is not None,
                        "replaced": s.replaced_count,
                        "forwarded": s.forwarded_count,
                    }
        return None


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
    "hold": "留存待发",
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
        latest_only_wait=LATEST_ONLY_WAIT,
        udp_listen_port=UDP_LISTEN_PORT,
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
        "latest_only_wait": LATEST_ONLY_WAIT,
        "latest_only": store.list_latest_only(),
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


@app.post("/latest-only/add")
def latest_only_add():
    ip_s = request.form.get("ip", "").strip()
    port_s = request.form.get("port", "").strip()
    try:
        ip = str(ipaddress.ip_address(ip_s))
        port = int(port_s)
        if not 1 <= port <= 65535:
            raise ValueError
    except ValueError:
        flash(f"无效的终端地址：{ip_s}:{port_s}", "error")
        return redirect(url_for("index"))
    client = f"{ip}:{port}"
    store.add_latest_only(client)
    store.log("event", client=client, note=f"设置「只送最新读数」（下次建会话起生效）")
    flash(f"已为 {client} 启用「只送最新读数」，从该终端下次建会话起生效；"
          f"已建立的会话保持原行为。", "ok")
    return redirect(url_for("index"))


@app.post("/latest-only/delete")
def latest_only_delete():
    client = request.form.get("client", "").strip()
    store.remove_latest_only(client)
    store.log("event", client=client, note=f"取消「只送最新读数」（下次建会话起生效）")
    flash(f"已取消 {client} 的「只送最新读数」，从下次建会话起生效。", "ok")
    return redirect(url_for("index"))


@app.post("/test")
def test():
    global _last_test
    result = run_burst_test(
        source_port_s=request.form.get("source_port", "").strip(),
        count_s=request.form.get("count", "").strip(),
        base=request.form.get("base", "").strip(),
        wait_s=request.form.get("wait", "").strip(),
        delay_strs=[request.form.get(f"d{i}", "") for i in range(8)],
    )
    with _last_test_lock:
        _last_test = result
    return redirect(url_for("index"))


def run_burst_test(source_port_s, count_s, base, wait_s, delay_strs):
    """试传区：从同一固定源端口连发 3~8 条实际 UDP 读数（逐条设置回包延迟），
    收集实收回包，并从会话统计读出被替换的读数条数。"""
    result = {"time": fmt_ts(time.time()), "client": "", "mode": "", "sent": 0,
              "readings": [], "replies": [], "received": 0, "replaced": 0,
              "forwarded": 0, "error": ""}
    try:
        src_port = int(source_port_s)
        if not 1 <= src_port <= 65535:
            raise ValueError
    except ValueError:
        result["error"] = "试传区要求填写固定源端口（1~65535）"
        return result
    try:
        count = int(count_s)
        if not 3 <= count <= 8:
            raise ValueError
    except ValueError:
        result["error"] = "连发条数须为 3~8"
        return result
    delays = []
    for i in range(count):
        try:
            delays.append(min(max(float(delay_strs[i]), 0.0), 10.0))
        except (ValueError, IndexError):
            delays.append(0.0)
    try:
        wait = min(max(float(wait_s), 2.0), 90.0) if wait_s else 30.0
    except ValueError:
        wait = 30.0
    base = base or "LEVEL"
    readings = []
    for i, d in enumerate(delays):
        body = f"{base}-{i + 1}"
        readings.append(f"DELAY:{d}:{body}" if d > 0 else body)  # 接收器识别的延迟回包指令
    result["sent"] = count
    result["readings"] = [
        {"i": i + 1, "delay": delays[i], "payload": readings[i]} for i in range(count)
    ]
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("0.0.0.0", src_port))  # 同一固定源端口
        sock.connect(("127.0.0.1", UDP_LISTEN_PORT))
        client = fmt_addr(sock.getsockname())
        result["client"] = client
        before = state.session_stats(client) or {}
        t0 = time.monotonic()
        for wire in readings:  # 连发
            sock.send(wire.encode())
            time.sleep(0.01)
        deadline = t0 + wait
        last_reply = None
        while True:
            now = time.monotonic()
            stats = state.session_stats(client)
            if stats:
                result["mode"] = "只送最新" if stats["latest_only"] else "普通"
            if len(result["replies"]) >= count:
                break
            if (last_reply is not None and now - last_reply > 0.5 and (
                    stats is None or (stats["latest_only"] and not stats["awaiting"]
                                      and not stats["has_pending"]))):
                break  # 会话已安静：不会再有新的有效回包
            remaining = deadline - now
            if remaining <= 0:
                break
            sock.settimeout(min(0.2, remaining))
            try:
                reply = sock.recv(65535)
            except socket.timeout:
                continue
            result["replies"].append(
                {"payload": preview(reply), "at": round(time.monotonic() - t0, 2)})
            last_reply = time.monotonic()
        after = state.session_stats(client) or {}
        result["received"] = len(result["replies"])
        result["replaced"] = max(0, after.get("replaced", 0) - before.get("replaced", 0))
        result["forwarded"] = max(0, after.get("forwarded", 0) - before.get("forwarded", 0))
        if not result["replies"]:
            result["error"] = f"等待 {wait}s 内未收到任何回包"
    except OSError as exc:
        result["error"] = f"发送失败：{exc}"
    finally:
        sock.close()
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
