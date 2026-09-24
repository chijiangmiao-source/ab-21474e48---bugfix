"""API/HTTP 冒烟复核：对运行中的应用服务执行端到端检查。

用法：APP_URL=http://app:8080 python verify/smoke.py
全部通过时退出码为 0，否则为 1。
"""
import json
import os
import socket
import sys
import threading
import time
import urllib.error
import urllib.request
from urllib.parse import urlparse

APP_URL = os.environ.get("APP_URL", "http://127.0.0.1:8080").rstrip("/")
RETENTION = 32

_failures = []


def check(name, cond, detail=""):
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + (f" —— {detail}" if detail and not cond else ""), flush=True)
    if not cond:
        _failures.append(name)


def http_json(method, path, body=None, headers=None):
    req = urllib.request.Request(APP_URL + path, method=method)
    data = None
    if body is not None:
        data = json.dumps(body).encode()
        req.add_header("Content-Type", "application/json")
    for k, v in (headers or {}).items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, data=data, timeout=10) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        try:
            return exc.code, json.loads(exc.read().decode())
        except json.JSONDecodeError:
            return exc.code, {}


class SSEClient:
    def __init__(self, path, headers=None):
        parsed = urlparse(APP_URL)
        self.sock = socket.create_connection((parsed.hostname, parsed.port or 80), timeout=10)
        req = f"GET {path} HTTP/1.1\r\nHost: {parsed.hostname}\r\n"
        for k, v in (headers or {}).items():
            req += f"{k}: {v}\r\n"
        req += "Connection: close\r\n\r\n"
        self.sock.sendall(req.encode())
        self.f = self.sock.makefile("rb")
        self.status = int(self.f.readline().decode().split()[1])
        while True:
            if not self.f.readline().strip():
                break

    def read_event(self, timeout=10):
        self.sock.settimeout(timeout)
        while True:
            ev, data_lines = {}, []
            while True:
                line = self.f.readline()
                if not line:
                    raise EOFError("事件流已关闭")
                line = line.decode().rstrip("\r\n")
                if line == "":
                    break
                if line.startswith(":"):
                    continue
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

    def close(self):
        try:
            self.sock.close()
        except OSError:
            pass


def wait_ready(timeout=60):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            status, data = http_json("GET", "/health")
            if status == 200 and data.get("status") == "ok":
                return True
        except Exception:
            pass
        time.sleep(1)
    return False


def main():
    if not wait_ready():
        check("服务就绪", False, f"{APP_URL}/health 在 60s 内未就绪")
        return 1
    check("服务就绪 /health", True)

    # 监看页可访问
    try:
        with urllib.request.urlopen(APP_URL + "/", timeout=10) as resp:
            body = resp.read().decode()
        check("监看页可访问", resp.status == 200 and "低温阵列扫描监控" in body)
    except Exception as exc:
        check("监看页可访问", False, str(exc))

    # 新开一代采集
    status, gen = http_json("POST", "/api/acquisitions")
    check("新开采集返回代号", status == 201 and bool(gen.get("code")), f"status={status}")
    code = gen["code"]

    # 追加帧 + 幂等重试
    status, r1 = http_json("POST", f"/api/acquisitions/{code}/frames",
                           {"operationId": "smoke-1", "deltas": {"ch0": 1.5, "ch1": 2}})
    check("追加帧返回序号 1", status == 201 and r1.get("seq") == 1, f"status={status}")
    status, r2 = http_json("POST", f"/api/acquisitions/{code}/frames",
                           {"operationId": "smoke-1", "deltas": {"ch0": 1.5, "ch1": 2}})
    check("同参重试返回原序号", status == 200 and r2.get("replayed") and r2.get("seq") == 1,
          f"status={status} body={r2}")

    # 异参复用稳定拒绝
    status, c1 = http_json("POST", f"/api/acquisitions/{code}/frames",
                           {"operationId": "smoke-1", "deltas": {"ch0": 9}})
    check("异参复用稳定拒绝(409)", status == 409 and c1.get("error") == "operation_conflict",
          f"status={status}")

    # 并发重复追加只落一帧
    barrier = threading.Barrier(8)
    outcomes, lock = [], threading.Lock()

    def racer():
        barrier.wait()
        s, d = http_json("POST", f"/api/acquisitions/{code}/frames",
                         {"operationId": "smoke-race", "deltas": {"ch2": 4}})
        with lock:
            outcomes.append((s, d))

    threads = [threading.Thread(target=racer) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    seqs = {d.get("seq") for _, d in outcomes}
    check("并发重复追加只落一帧",
          len(outcomes) == 8 and seqs == {2}
          and sum(1 for s, _ in outcomes if s == 201) == 1,
          f"outcomes={outcomes}")

    # SSE：快照先行，再收增量
    client = SSEClient(f"/api/acquisitions/{code}/stream")
    try:
        ev = client.read_event()
        check("SSE 首发快照带高水位",
              ev.get("event") == "snapshot" and ev["data"].get("highWater") == 2
              and ev["data"]["totals"] == {"ch0": 1.5, "ch1": 2, "ch2": 4},
              f"event={ev}")
        http_json("POST", f"/api/acquisitions/{code}/frames",
                  {"operationId": "smoke-2", "deltas": {"ch0": 0.5}})
        ev = client.read_event()
        check("SSE 增量帧连续", ev.get("event") == "frame" and ev["data"].get("seq") == 3,
              f"event={ev}")
    finally:
        client.close()

    # 压缩后重连：游标过旧 -> 完整重置，累计值一致
    expected = {"ch0": 2.0, "ch1": 2, "ch2": 4}
    for i in range(RETENTION + 6):
        http_json("POST", f"/api/acquisitions/{code}/frames",
                  {"operationId": f"smoke-bulk-{i}", "deltas": {"ch0": 1, "ch3": i % 2}})
        expected["ch0"] += 1
        expected["ch3"] = expected.get("ch3", 0) + i % 2
    client = SSEClient(f"/api/acquisitions/{code}/stream?cursor=1")
    try:
        ev = client.read_event()
        check("游标过旧触发完整重置",
              ev.get("event") == "reset" and ev["data"].get("reason") == "cursor_expired",
              f"event={ev}")
        check("重置后累计值一致", ev["data"].get("totals") == expected
              and ev["data"].get("highWater") == RETENTION + 9,
              f"totals={ev['data'].get('totals')} expected={expected}")
    finally:
        client.close()

    # 有效游标补发缺口
    high = RETENTION + 9
    client = SSEClient(f"/api/acquisitions/{code}/stream?cursor={high - 2}")
    try:
        seqs = [client.read_event()["data"]["seq"] for _ in range(2)]
        check("有效游标补发缺口帧", seqs == [high - 1, high], f"seqs={seqs}")
    finally:
        client.close()

    # 代际隔离：新开一代后旧代拒绝追加，旧流收到 superseded
    old_stream = SSEClient(f"/api/acquisitions/{code}/stream")
    try:
        old_stream.read_event()  # 快照，确保订阅已建立
        status, gen2 = http_json("POST", "/api/acquisitions")
        new_code = gen2["code"]
        ev = old_stream.read_event()
        check("旧代连接收到取代事件", ev.get("event") == "superseded", f"event={ev}")
        status, d = http_json("POST", f"/api/acquisitions/{code}/frames",
                              {"operationId": "smoke-late", "deltas": {"ch0": 1}})
        check("旧代追加被拒绝(410)", status == 410 and d.get("error") == "generation_closed",
              f"status={status}")
        status, d = http_json("POST", f"/api/acquisitions/{new_code}/frames",
                              {"operationId": "smoke-new-1", "deltas": {"ch7": 3}})
        check("新代独立编号", status == 201 and d.get("seq") == 1
              and d.get("totals") == {"ch7": 3}, f"status={status} body={d}")
    finally:
        old_stream.close()

    if _failures:
        print(f"\n冒烟复核失败 {len(_failures)} 项: {', '.join(_failures)}", flush=True)
        return 1
    print("\n冒烟复核全部通过", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
