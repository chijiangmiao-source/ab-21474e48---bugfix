"""后端代码测试：持久层单元测试 + HTTP/SSE 集成测试。

运行：python -m unittest discover -s tests -v
"""
import json
import os
import socket
import sys
import threading
import unittest
import urllib.error
import urllib.request

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "app"))

import server as server_mod  # noqa: E402
from store import (  # noqa: E402
    ConflictError,
    GenerationClosedError,
    NotFoundError,
    RETENTION,
    Store,
    canonical_payload,
)


# ------------------------------------------------------------------ 工具

def http_json(port, method, path, body=None, headers=None):
    url = f"http://127.0.0.1:{port}{path}"
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode()
        try:
            return exc.code, json.loads(raw)
        except json.JSONDecodeError:
            return exc.code, {"raw": raw}


class SSEClient:
    """最小 SSE 客户端：原始 socket 逐行解析事件。"""

    def __init__(self, port, path, headers=None):
        self.sock = socket.create_connection(("127.0.0.1", port), timeout=10)
        req = f"GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\n"
        for k, v in (headers or {}).items():
            req += f"{k}: {v}\r\n"
        req += "Connection: close\r\n\r\n"
        self.sock.sendall(req.encode())
        self.f = self.sock.makefile("rb")
        self.status_line = self.f.readline().decode().strip()
        self.headers = {}
        while True:
            line = self.f.readline().decode().strip()
            if not line:
                break
            k, _, v = line.partition(":")
            self.headers[k.strip().lower()] = v.strip()

    @property
    def status(self):
        return int(self.status_line.split()[1])

    def read_event(self, timeout=10):
        """读取一个事件块；心跳与 retry 行被跳过。流关闭时抛 EOFError。"""
        self.sock.settimeout(timeout)
        while True:
            ev = {}
            data_lines = []
            while True:
                line = self.f.readline()
                if not line:
                    raise EOFError("事件流已关闭")
                line = line.decode().rstrip("\r\n")
                if line == "":
                    break
                if line.startswith(":"):
                    continue  # 心跳注释
                field, _, value = line.partition(":")
                value = value.lstrip(" ")
                if field == "event":
                    ev["event"] = value
                elif field == "data":
                    data_lines.append(value)
                elif field == "id":
                    ev["id"] = value
            if data_lines:
                ev["data"] = json.loads("\n".join(data_lines))
                return ev
            # 无 data 的块（如 retry 行）继续读下一块

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


class ServerFixture:
    def __init__(self):
        self.store = Store(":memory:")
        self.httpd = server_mod.make_server("127.0.0.1", 0, self.store)
        self.port = self.httpd.server_address[1]
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)
        self.thread.start()

    def stop(self):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.store.close()

    def post(self, path, body=None):
        return http_json(self.port, "POST", path, body)

    def get(self, path):
        return http_json(self.port, "GET", path)

    def new_gen(self):
        status, data = self.post("/api/acquisitions")
        assert status == 201, data
        return data["code"]


# ------------------------------------------------------------------ 持久层

class StoreTest(unittest.TestCase):
    def setUp(self):
        self.store = Store(":memory:")

    def tearDown(self):
        self.store.close()

    def test_seq_strictly_increasing_and_totals(self):
        code, _ = self.store.create_acquisition()
        r1 = self.store.append_frame(code, "op-1", {"ch0": 1.5, "ch1": 2})
        r2 = self.store.append_frame(code, "op-2", {"ch0": -0.5})
        r3 = self.store.append_frame(code, "op-3", {"ch2": 7})
        self.assertEqual((r1.seq, r2.seq, r3.seq), (1, 2, 3))
        self.assertEqual(r3.totals, {"ch0": 1.0, "ch1": 2, "ch2": 7})
        snap = self.store.get_snapshot(code)
        self.assertEqual(snap.high_water, 3)

    def test_idempotent_retry_returns_original_seq(self):
        code, _ = self.store.create_acquisition()
        first = self.store.append_frame(code, "op-x", {"ch0": 1})
        again = self.store.append_frame(code, "op-x", {"ch0": 1})
        self.assertFalse(first.replayed)
        self.assertTrue(again.replayed)
        self.assertEqual(again.seq, first.seq)
        self.assertEqual(self.store.get_snapshot(code).high_water, 1)

    def test_conflicting_reuse_rejected_without_event(self):
        code, _ = self.store.create_acquisition()
        self.store.append_frame(code, "op-x", {"ch0": 1})
        with self.assertRaises(ConflictError):
            self.store.append_frame(code, "op-x", {"ch0": 2})
        # 稳定拒绝：重试仍是冲突，且不产生新帧
        with self.assertRaises(ConflictError):
            self.store.append_frame(code, "op-x", {"ch0": 2})
        self.assertEqual(self.store.get_snapshot(code).high_water, 1)

    def test_append_to_superseded_generation_rejected(self):
        old_code, _ = self.store.create_acquisition()
        self.store.append_frame(old_code, "op-1", {"ch0": 1})
        new_code, superseded = self.store.create_acquisition()
        self.assertIn(old_code, superseded)
        with self.assertRaises(GenerationClosedError):
            self.store.append_frame(old_code, "op-2", {"ch0": 1})
        # 新代不受影响
        r = self.store.append_frame(new_code, "op-1", {"ch0": 5})
        self.assertEqual(r.seq, 1)

    def test_unknown_code_rejected(self):
        with self.assertRaises(NotFoundError):
            self.store.append_frame("gen-00000000", "op-1", {"ch0": 1})

    def test_retention_keeps_last_32_frames(self):
        code, _ = self.store.create_acquisition()
        for i in range(RETENTION + 8):
            self.store.append_frame(code, f"op-{i}", {"ch0": 1})
        frames, lo = self.store.frames_after(code, 0)
        self.assertEqual(lo, 9)  # 40 - 32 + 1
        self.assertEqual(len(frames), RETENTION)
        self.assertEqual(frames[0]["seq"], 9)
        # 压缩不丢累计值
        self.assertEqual(self.store.get_snapshot(code).totals["ch0"], RETENTION + 8)

    def test_concurrent_duplicate_append_persists_one_frame(self):
        code, _ = self.store.create_acquisition()
        barrier = threading.Barrier(8)
        results = []
        lock = threading.Lock()

        def worker():
            barrier.wait()
            r = self.store.append_frame(code, "op-shared", {"ch0": 3})
            with lock:
                results.append(r)

        threads = [threading.Thread(target=worker) for _ in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        self.assertEqual(len(results), 8)
        self.assertEqual({r.seq for r in results}, {1})
        self.assertEqual(sum(1 for r in results if not r.replayed), 1)
        self.assertEqual(self.store.get_snapshot(code).high_water, 1)
        self.assertEqual(self.store.get_snapshot(code).totals["ch0"], 3)

    def test_sparse_wide_magnitude_trajectory_is_exact(self):
        """稀疏帧 + 极大/极小增量混合：固化的逐帧累计必须与正向累计完全一致。"""
        code, _ = self.store.create_acquisition()
        # seq 1：signal +10000.5（页面新开一代后收到的第一帧）
        self.store.append_frame(code, "sig-init", {"signal": 10000.5})
        # seq 2..32：monitor 连续 31 次 +1（事件流中断期间经同代追加接口写入）
        for i in range(RETENTION - 1):
            self.store.append_frame(code, f"mon-{i:02d}", {"monitor": 1})
        # seq 33：signal +0.5
        self.store.append_frame(code, "sig-half", {"signal": 0.5})
        # seq 34：signal -10000000000000000
        self.store.append_frame(code, "sig-huge", {"signal": -10_000_000_000_000_000})

        snap = self.store.get_snapshot(code)
        self.assertEqual(snap.high_water, 34)
        # 权威汇总：看似一直正确
        self.assertEqual(snap.totals["signal"], -9_999_999_999_990_000)
        self.assertEqual(snap.totals["monitor"], 31)

        # 留存窗口恰好覆盖序号 3..34，连续不重放
        seqs = [f["seq"] for f in snap.frames]
        self.assertEqual(seqs, list(range(3, 35)))
        by_seq = {f["seq"]: f for f in snap.frames}
        # 关键回归点：seq 33 的 signal 累计必须是 10001，而非浮点抵消后的 10000
        self.assertEqual(by_seq[33]["totals"]["signal"], 10001)
        self.assertEqual(by_seq[33]["totals"]["monitor"], 31)
        # seq 34 最终值
        self.assertEqual(by_seq[34]["totals"]["signal"], -9_999_999_999_990_000)
        self.assertEqual(by_seq[34]["totals"]["monitor"], 31)
        # 窗口起点 seq 3：signal 未变，monitor 已累计 seq 2、3 两次 +1
        self.assertEqual(by_seq[3]["totals"], {"signal": 10000.5, "monitor": 2})
        # monitor 逐帧累计：seq i（2..32 留存部分从 3 起）为 i-1
        for i in range(3, 33):
            self.assertEqual(by_seq[i]["totals"]["monitor"], i - 1)

        # frames_after 与快照同源
        frames, lo = self.store.frames_after(code, 1)
        self.assertEqual(lo, 3)
        self.assertEqual([f["seq"] for f in frames], list(range(3, 35)))
        self.assertEqual(frames[30]["totals"]["signal"], 10001)  # seq 33

        # 继续追加：从序号 35 与现有权威汇总继续
        r = self.store.append_frame(code, "continue-1", {"monitor": 1})
        self.assertEqual(r.seq, 35)
        self.assertEqual(r.totals["monitor"], 32)
        self.assertEqual(r.totals["signal"], -9_999_999_999_990_000)
        snap2 = self.store.get_snapshot(code)
        self.assertEqual(snap2.high_water, 35)
        self.assertEqual([f["seq"] for f in snap2.frames], list(range(4, 36)))

    def test_persisted_frame_totals_survive_reopen(self):
        import os
        import tempfile

        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            store = Store(path)
            code, _ = store.create_acquisition()
            store.append_frame(code, "op-1", {"ch0": 10000.5})
            for i in range(RETENTION + 2):
                store.append_frame(code, f"op-{i+2}", {"ch0": 0.5})
            store.close()

            reopened = Store(path)
            snap = reopened.get_snapshot(code)
            self.assertEqual(snap.high_water, RETENTION + 3)
            self.assertEqual(snap.totals["ch0"], 10000.5 + 0.5 * (RETENTION + 2))
            # 固化的逐帧累计在重开后依旧精确，且末帧等于权威汇总
            self.assertEqual(snap.frames[-1]["totals"]["ch0"], snap.totals["ch0"])
            self.assertEqual(snap.frames[0]["seq"], 4)
            reopened.close()
        finally:
            for suffix in ("", "-wal", "-shm"):
                if os.path.exists(path + suffix):
                    os.unlink(path + suffix)

    def test_legacy_schema_without_totals_column_backfills(self):
        """旧库（frames 无 totals 列）打开时自动迁移并补填，末帧等于权威汇总。"""
        import os
        import sqlite3
        import tempfile
        import time

        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            raw = sqlite3.connect(path)
            raw.execute(
                "CREATE TABLE acquisitions (id INTEGER PRIMARY KEY AUTOINCREMENT,"
                " code TEXT UNIQUE, status TEXT, created_at REAL)"
            )
            raw.execute(
                "CREATE TABLE snapshots (acquisition_id INTEGER PRIMARY KEY,"
                " high_water INTEGER, totals TEXT)"
            )
            raw.execute(
                "CREATE TABLE frames (id INTEGER PRIMARY KEY AUTOINCREMENT,"
                " acquisition_id INTEGER, seq INTEGER, operation_id TEXT,"
                " params_hash TEXT, payload TEXT, created_at REAL)"
            )
            raw.execute(
                "INSERT INTO acquisitions(id, code, status, created_at)"
                " VALUES (1, 'gen-legacy', 'active', ?)",
                (time.time(),),
            )
            raw.execute(
                "INSERT INTO snapshots(acquisition_id, high_water, totals)"
                " VALUES (1, 3, ?)",
                (canonical_payload({"ch0": 6}),),
            )
            for seq, value in ((1, 1), (2, 2), (3, 3)):
                raw.execute(
                    "INSERT INTO frames(acquisition_id, seq, operation_id,"
                    " params_hash, payload, created_at) VALUES (1,?,?,?,?,?)",
                    (seq, f"op-{seq}", "x", canonical_payload({"ch0": value}),
                     time.time()),
                )
            raw.commit()
            raw.close()

            store = Store(path)
            snap = store.get_snapshot("gen-legacy")
            self.assertEqual(
                [f["totals"]["ch0"] for f in snap.frames], [1, 3, 6]
            )
            store.close()
        finally:
            for suffix in ("", "-wal", "-shm"):
                if os.path.exists(path + suffix):
                    os.unlink(path + suffix)


# ------------------------------------------------------------------ HTTP/SSE

class HttpTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fx = ServerFixture()

    @classmethod
    def tearDownClass(cls):
        cls.fx.stop()

    def test_health(self):
        status, data = self.fx.get("/health")
        self.assertEqual(status, 200)
        self.assertEqual(data["status"], "ok")

    def test_index_page_served(self):
        req = urllib.request.Request(f"http://127.0.0.1:{self.fx.port}/")
        with urllib.request.urlopen(req, timeout=10) as resp:
            body = resp.read().decode()
        self.assertEqual(resp.status, 200)
        self.assertIn("低温阵列扫描监控", body)

    def test_append_idempotent_conflict_and_unknown(self):
        code = self.fx.new_gen()
        status, data = self.fx.post(f"/api/acquisitions/{code}/frames",
                                    {"operationId": "op-a", "deltas": {"ch0": 1}})
        self.assertEqual(status, 201)
        self.assertEqual(data["seq"], 1)
        self.assertFalse(data["replayed"])

        # 同参重试 -> 原序号
        status, data = self.fx.post(f"/api/acquisitions/{code}/frames",
                                    {"operationId": "op-a", "deltas": {"ch0": 1}})
        self.assertEqual(status, 200)
        self.assertTrue(data["replayed"])
        self.assertEqual(data["seq"], 1)

        # 异参复用 -> 稳定 409
        for _ in range(2):
            status, data = self.fx.post(f"/api/acquisitions/{code}/frames",
                                        {"operationId": "op-a", "deltas": {"ch0": 9}})
            self.assertEqual(status, 409)
            self.assertEqual(data["error"], "operation_conflict")

        # 未知代号 -> 404
        status, _ = self.fx.post("/api/acquisitions/gen-ffffffff/frames",
                                 {"operationId": "op-b", "deltas": {"ch0": 1}})
        self.assertEqual(status, 404)

        # 参数非法 -> 400
        status, _ = self.fx.post(f"/api/acquisitions/{code}/frames",
                                 {"operationId": "op-c", "deltas": {}})
        self.assertEqual(status, 400)

    def test_sse_snapshot_first_then_live_frames(self):
        code = self.fx.new_gen()
        self.fx.post(f"/api/acquisitions/{code}/frames",
                     {"operationId": "op-1", "deltas": {"ch0": 1}})
        client = SSEClient(self.fx.port, f"/api/acquisitions/{code}/stream")
        try:
            self.assertEqual(client.status, 200)
            first = client.read_event()
            self.assertEqual(first["event"], "snapshot")
            self.assertEqual(first["data"]["code"], code)
            self.assertEqual(first["data"]["highWater"], 1)
            self.assertEqual(first["data"]["totals"], {"ch0": 1})

            # 订阅建立后追加，应收到增量帧
            self.fx.post(f"/api/acquisitions/{code}/frames",
                         {"operationId": "op-2", "deltas": {"ch0": 2}})
            ev = client.read_event()
            self.assertEqual(ev["event"], "frame")
            self.assertEqual(ev["data"]["seq"], 2)
            self.assertEqual(ev["id"], "2")
        finally:
            client.close()

    def test_sse_reconnect_with_cursor_replays_gap(self):
        code = self.fx.new_gen()
        for i in range(1, 6):
            self.fx.post(f"/api/acquisitions/{code}/frames",
                         {"operationId": f"op-{i}", "deltas": {"ch0": i}})
        client = SSEClient(self.fx.port, f"/api/acquisitions/{code}/stream?cursor=2")
        try:
            seqs = [client.read_event()["data"]["seq"] for _ in range(3)]
            self.assertEqual(seqs, [3, 4, 5])
        finally:
            client.close()

    def test_sse_last_event_id_used_as_cursor(self):
        code = self.fx.new_gen()
        for i in range(3):
            self.fx.post(f"/api/acquisitions/{code}/frames",
                         {"operationId": f"op-{i}", "deltas": {"ch0": 1}})
        client = SSEClient(self.fx.port, f"/api/acquisitions/{code}/stream",
                           headers={"Last-Event-ID": "1"})
        try:
            ev = client.read_event()
            self.assertEqual(ev["event"], "frame")
            self.assertEqual(ev["data"]["seq"], 2)
        finally:
            client.close()

    def test_sse_reset_when_cursor_expired(self):
        code = self.fx.new_gen()
        for i in range(RETENTION + 5):  # 37 帧，前 5 帧被压缩
            self.fx.post(f"/api/acquisitions/{code}/frames",
                         {"operationId": f"op-{i}", "deltas": {"ch0": 1}})
        client = SSEClient(self.fx.port, f"/api/acquisitions/{code}/stream?cursor=2")
        try:
            ev = client.read_event()
            self.assertEqual(ev["event"], "reset")
            self.assertEqual(ev["data"]["reason"], "cursor_expired")
            self.assertEqual(ev["data"]["highWater"], RETENTION + 5)
            # 重置携带完整累计值
            self.assertEqual(ev["data"]["totals"], {"ch0": RETENTION + 5})
        finally:
            client.close()

    def test_compacted_reconnect_yields_same_cumulative(self):
        code = self.fx.new_gen()
        expected = {}
        for i in range(RETENTION + 10):
            deltas = {"ch0": 1, "ch1": i % 3}
            for k, v in deltas.items():
                expected[k] = expected.get(k, 0) + v
            self.fx.post(f"/api/acquisitions/{code}/frames",
                         {"operationId": f"op-{i}", "deltas": deltas})
        # 全新连接拿快照：累计值须与逐帧求和一致
        client = SSEClient(self.fx.port, f"/api/acquisitions/{code}/stream")
        try:
            ev = client.read_event()
            self.assertEqual(ev["event"], "snapshot")
            self.assertEqual(ev["data"]["totals"], expected)
            self.assertEqual(ev["data"]["highWater"], RETENTION + 10)
        finally:
            client.close()

    def test_sparse_frames_snapshot_reset_and_followup_agree(self):
        """构造报告中的稀疏帧与断链游标：首次快照、过期游标重置、后续增量
        必须看到同一条累计轨迹（seq 33 signal=10001，seq 34 为最终大负值）。"""
        code = self.fx.new_gen()

        def append(op, deltas):
            status, data = self.fx.post(f"/api/acquisitions/{code}/frames",
                                        {"operationId": op, "deltas": deltas})
            assert status == 201, (status, data)
            return data

        append("sig-init", {"signal": 10000.5})               # seq 1
        for i in range(RETENTION - 1):                        # seq 2..32
            append(f"mon-{i:02d}", {"monitor": 1})
        append("sig-half", {"signal": 0.5})                   # seq 33
        append("sig-huge", {"signal": -10_000_000_000_000_000})  # seq 34

        final_totals = {"signal": -9_999_999_999_990_000, "monitor": 31}

        def assert_trajectory(payload):
            self.assertEqual(payload["highWater"], 34)
            self.assertEqual(payload["totals"], final_totals)
            frames = payload["frames"]
            seqs = [f["seq"] for f in frames]
            self.assertEqual(seqs, list(range(3, 35)))  # 恰好 3..34，连续不重放
            by_seq = {f["seq"]: f for f in frames}
            self.assertEqual(by_seq[33]["totals"]["signal"], 10001)
            self.assertEqual(by_seq[33]["totals"]["monitor"], 31)
            self.assertEqual(by_seq[34]["totals"], final_totals)
            # 逐帧 totals 与自身 deltas 自洽（留存窗口从 seq 3 起，
            # 基值取 seq 2 末：signal 10000.5、monitor 1）
            prev = {"signal": 10000.5, "monitor": 1}
            for f in frames:
                calc = dict(prev)
                for k, v in f["deltas"].items():
                    calc[k] = calc.get(k, 0) + v
                self.assertEqual(f["totals"], calc, f"seq {f['seq']} 累计轨迹不自洽")
                prev = calc

        # 1) 首次连接直接读同一代快照
        fresh = SSEClient(self.fx.port, f"/api/acquisitions/{code}/stream")
        try:
            ev = fresh.read_event()
            self.assertEqual(ev["event"], "snapshot")
            assert_trajectory(ev["data"])
        finally:
            fresh.close()

        # 2) 页面以游标 1 重连：前两帧已超出最近 32 帧窗口 -> 完整重置
        stale = SSEClient(self.fx.port, f"/api/acquisitions/{code}/stream?cursor=1")
        try:
            ev = stale.read_event()
            self.assertEqual(ev["event"], "reset")
            self.assertEqual(ev["data"]["reason"], "cursor_expired")
            assert_trajectory(ev["data"])

            # 3) 重置后继续增量：序号 35，从现有权威汇总继续
            status, data = self.fx.post(
                f"/api/acquisitions/{code}/frames",
                {"operationId": "continue-mon", "deltas": {"monitor": 1}})
            self.assertEqual(status, 201)
            self.assertEqual(data["seq"], 35)
            self.assertEqual(data["totals"]["monitor"], 32)
            self.assertEqual(data["totals"]["signal"], -9_999_999_999_990_000)

            ev = stale.read_event()
            self.assertEqual(ev["event"], "frame")
            self.assertEqual(ev["id"], "35")
            self.assertEqual(ev["data"]["totals"]["monitor"], 32)
            self.assertEqual(ev["data"]["totals"]["signal"], -9_999_999_999_990_000)
        finally:
            stale.close()

        # 4) 有效游标补发缺口（游标 33 -> 34、35），补发轨迹同样精确
        gap = SSEClient(self.fx.port, f"/api/acquisitions/{code}/stream?cursor=33")
        try:
            ev34 = gap.read_event()
            self.assertEqual(ev34["event"], "frame")
            self.assertEqual(ev34["data"]["seq"], 34)
            self.assertEqual(ev34["data"]["totals"], final_totals)
            ev35 = gap.read_event()
            self.assertEqual(ev35["event"], "frame")
            self.assertEqual(ev35["data"]["seq"], 35)
            self.assertEqual(ev35["data"]["totals"]["monitor"], 32)
        finally:
            gap.close()

    def test_generation_isolation(self):
        old_code = self.fx.new_gen()
        self.fx.post(f"/api/acquisitions/{old_code}/frames",
                     {"operationId": "op-1", "deltas": {"ch0": 1}})
        old_stream = SSEClient(self.fx.port, f"/api/acquisitions/{old_code}/stream")
        try:
            self.assertEqual(old_stream.read_event()["event"], "snapshot")

            new_code = self.fx.new_gen()

            # 旧代流收到 superseded 后关闭
            ev = old_stream.read_event()
            self.assertEqual(ev["event"], "superseded")

            # 旧代追加 -> 410，不产生事件
            status, data = self.fx.post(f"/api/acquisitions/{old_code}/frames",
                                        {"operationId": "op-2", "deltas": {"ch0": 1}})
            self.assertEqual(status, 410)
            self.assertEqual(data["error"], "generation_closed")

            # 新代独立从 1 开始编号
            status, data = self.fx.post(f"/api/acquisitions/{new_code}/frames",
                                        {"operationId": "op-1", "deltas": {"ch9": 4}})
            self.assertEqual(status, 201)
            self.assertEqual(data["seq"], 1)
            self.assertEqual(data["totals"], {"ch9": 4})
        finally:
            old_stream.close()

    def test_concurrent_duplicate_append_over_http(self):
        code = self.fx.new_gen()
        barrier = threading.Barrier(6)
        outcomes = []
        lock = threading.Lock()

        def worker():
            barrier.wait()
            status, data = self.fx.post(
                f"/api/acquisitions/{code}/frames",
                {"operationId": "op-race", "deltas": {"ch0": 5}})
            with lock:
                outcomes.append((status, data))

        threads = [threading.Thread(target=worker) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        seqs = {d["seq"] for _, d in outcomes}
        self.assertEqual(seqs, {1}, "并发重复追加必须收敛到同一序号")
        self.assertEqual(sum(1 for s, _ in outcomes if s == 201), 1)
        self.assertEqual(sum(1 for s, _ in outcomes if s == 200), 5)
        # 只落一帧
        client = SSEClient(self.fx.port, f"/api/acquisitions/{code}/stream")
        try:
            ev = client.read_event()
            self.assertEqual(ev["data"]["highWater"], 1)
            self.assertEqual(ev["data"]["totals"], {"ch0": 5})
        finally:
            client.close()


if __name__ == "__main__":
    unittest.main()
