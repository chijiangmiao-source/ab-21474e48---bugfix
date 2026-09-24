"""低温阵列扫描 —— HTTP/SSE 服务。

路由：
  GET  /health                              健康检查
  GET  /                                    监看页（frontend/dist）
  GET  /app.js, /styles.css                 前端静态资源
  POST /api/acquisitions                    新开一代采集
  POST /api/acquisitions/{code}/frames      按操作标识追加帧（幂等）
  GET  /api/acquisitions/{code}/stream      事件流（SSE）
      - 首次连接：先发带高水位的 snapshot，再发增量 frame
      - 重连携带 ?cursor=N 或 Last-Event-ID：补发缺口帧
      - 游标过旧（缺口帧已被压缩）：发 reset 事件供客户端原子替换
      - 代被取代：向旧代订阅者发 superseded 后关闭
"""
from __future__ import annotations

import json
import os
import queue
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs

from store import (
    ConflictError,
    GenerationClosedError,
    NotFoundError,
    Store,
    ValidationError,
)

HEARTBEAT_SECONDS = 15
MAX_BODY_BYTES = 64 * 1024
DIST_DIR = os.environ.get(
    "DIST_DIR", os.path.join(os.path.dirname(__file__), "..", "frontend", "dist")
)

CONTENT_TYPES = {
    ".html": "text/html; charset=utf-8",
    ".js": "text/javascript; charset=utf-8",
    ".css": "text/css; charset=utf-8",
    ".json": "application/json; charset=utf-8",
}


class Bus:
    """内存事件总线：按代号分发事件给 SSE 订阅者。"""

    def __init__(self):
        self._lock = threading.Lock()
        self._subs: dict[str, set[queue.Queue]] = {}

    def subscribe(self, code: str) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=512)
        with self._lock:
            self._subs.setdefault(code, set()).add(q)
        return q

    def unsubscribe(self, code: str, q: queue.Queue) -> None:
        with self._lock:
            subs = self._subs.get(code)
            if subs is not None:
                subs.discard(q)
                if not subs:
                    self._subs.pop(code, None)

    def publish(self, code: str, event: dict) -> None:
        with self._lock:
            subs = list(self._subs.get(code, ()))
        for q in subs:
            try:
                q.put_nowait(event)
            except queue.Full:
                # 慢消费者：塞入关闭哨兵，客户端断开后会带游标重连修复。
                try:
                    q.put_nowait(None)
                except queue.Full:
                    pass


def sse_format(event: dict) -> bytes:
    lines = []
    if event.get("id") is not None:
        lines.append(f"id: {event['id']}")
    if event.get("event"):
        lines.append(f"event: {event['event']}")
    data = event.get("data")
    if not isinstance(data, str):
        data = json.dumps(data, ensure_ascii=False)
    for line in data.splitlines() or [""]:
        lines.append(f"data: {line}")
    return ("\n".join(lines) + "\n\n").encode("utf-8")


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "CryoArray/1.0"

    # 由 server 实例注入
    @property
    def store(self) -> Store:
        return self.server.store  # type: ignore[attr-defined]

    @property
    def bus(self) -> Bus:
        return self.server.bus  # type: ignore[attr-defined]

    def log_message(self, fmt, *args):  # 静默访问日志，避免刷屏
        pass

    # ------------------------------------------------------------- 工具

    def _send_json(self, status: int, obj: dict) -> None:
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _send_error_json(self, status: int, error: str, message: str) -> None:
        self._send_json(status, {"error": error, "message": message})

    def _read_json_body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0 or length > MAX_BODY_BYTES:
            raise ValidationError("请求体缺失或超过大小限制")
        raw = self.rfile.read(length)
        try:
            body = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ValidationError("请求体必须是合法 JSON")
        if not isinstance(body, dict):
            raise ValidationError("请求体必须是 JSON 对象")
        return body

    def _send_file(self, path: str) -> None:
        ext = os.path.splitext(path)[1]
        ctype = CONTENT_TYPES.get(ext, "application/octet-stream")
        try:
            with open(path, "rb") as f:
                body = f.read()
        except OSError:
            self._send_error_json(404, "not_found", "资源不存在")
            return
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(body)

    # ------------------------------------------------------------- GET

    def do_GET(self) -> None:  # noqa: N802 - http.server 约定
        parsed = urlparse(self.path)
        path = parsed.path
        try:
            if path == "/health":
                self._send_json(200, {"status": "ok", "time": time.time()})
            elif path == "/":
                self._send_file(os.path.join(DIST_DIR, "index.html"))
            elif path in ("/app.js", "/styles.css"):
                self._send_file(os.path.join(DIST_DIR, path.lstrip("/")))
            elif path.startswith("/api/acquisitions/") and path.endswith("/stream"):
                code = path[len("/api/acquisitions/") : -len("/stream")]
                self._handle_stream(code, parse_qs(parsed.query))
            else:
                self._send_error_json(404, "not_found", "路径不存在")
        except BrokenPipeError:
            pass
        except Exception as exc:  # pragma: no cover - 兜底
            try:
                self._send_error_json(500, "internal_error", str(exc))
            except Exception:
                pass

    # ------------------------------------------------------------- POST

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path
        try:
            if path == "/api/acquisitions":
                self._handle_create_acquisition()
            elif path.startswith("/api/acquisitions/") and path.endswith("/frames"):
                code = path[len("/api/acquisitions/") : -len("/frames")]
                self._handle_append_frame(code)
            else:
                self._send_error_json(404, "not_found", "路径不存在")
        except ValidationError as exc:
            self._send_error_json(400, "invalid_request", str(exc))
        except BrokenPipeError:
            pass
        except Exception as exc:  # pragma: no cover - 兜底
            try:
                self._send_error_json(500, "internal_error", str(exc))
            except Exception:
                pass

    def _handle_create_acquisition(self) -> None:
        code, superseded = self.store.create_acquisition()
        for old_code in superseded:
            # 通知旧代订阅者：本代已被取代，随后关闭其流。
            self.bus.publish(
                old_code,
                {"event": "superseded", "data": {"code": old_code, "by": code}},
            )
            self.bus.publish(old_code, None)  # 关闭哨兵
        self._send_json(
            201, {"code": code, "highWater": 0, "totals": {}, "superseded": superseded}
        )

    def _handle_append_frame(self, code: str) -> None:
        body = self._read_json_body()
        operation_id = body.get("operationId")
        deltas = body.get("deltas")
        try:
            result = self.store.append_frame(code, operation_id, deltas)
        except NotFoundError as exc:
            self._send_error_json(404, "not_found", str(exc))
            return
        except GenerationClosedError as exc:
            self._send_error_json(410, "generation_closed", str(exc))
            return
        except ConflictError as exc:
            self._send_error_json(409, "operation_conflict", str(exc))
            return

        payload = {
            "code": result.code,
            "seq": result.seq,
            "highWater": result.high_water,
            "operationId": result.operation_id,
            "deltas": result.deltas,
            "totals": result.totals,
            "replayed": result.replayed,
        }
        if not result.replayed:
            # 事务提交后再广播，保证订阅者看到的序号与快照一致。
            self.bus.publish(
                result.code,
                {"event": "frame", "id": result.seq, "data": payload},
            )
            self._send_json(201, payload)
        else:
            self._send_json(200, payload)

    # ------------------------------------------------------------- SSE

    def _handle_stream(self, code: str, query: dict) -> None:
        try:
            snap = self.store.get_snapshot(code)
        except NotFoundError as exc:
            self._send_error_json(404, "not_found", str(exc))
            return

        # 游标：优先 Last-Event-ID（浏览器自动重连），其次查询参数。
        cursor = None
        last_event_id = self.headers.get("Last-Event-ID")
        raw_cursor = last_event_id if last_event_id else (query.get("cursor") or [None])[0]
        if raw_cursor is not None:
            try:
                cursor = int(raw_cursor)
            except (TypeError, ValueError):
                self._send_error_json(400, "invalid_request", "游标必须是整数")
                return
            if cursor < 0:
                self._send_error_json(400, "invalid_request", "游标必须非负")
                return

        # 先订阅再读库，二者之间到达的帧会进入队列，随后按序号去重，不漏不重。
        q = self.bus.subscribe(code)
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream; charset=utf-8")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("X-Accel-Buffering", "no")
            self.end_headers()
            self.close_connection = True  # 流以连接关闭为界

            self.wfile.write(b"retry: 2000\n\n")
            last_sent = 0

            if cursor is None:
                # 首次连接：发带高水位的快照，之后只发增量。
                snap = self.store.get_snapshot(code)  # 订阅后重读，拿到最新高水位
                self.wfile.write(
                    sse_format(
                        {
                            "event": "snapshot",
                            "data": {
                                "code": code,
                                "status": snap.status,
                                "highWater": snap.high_water,
                                "totals": snap.totals,
                                "frames": snap.frames,
                            },
                        }
                    )
                )
                last_sent = snap.high_water
            else:
                frames, lo = self.store.frames_after(code, cursor)
                snap = self.store.get_snapshot(code)
                if cursor + 1 < lo or cursor > snap.high_water:
                    # 游标过旧（或来自未来）：完整重置，客户端原子替换。
                    self.wfile.write(
                        sse_format(
                            {
                                "event": "reset",
                                "data": {
                                    "code": code,
                                    "status": snap.status,
                                    "highWater": snap.high_water,
                                    "totals": snap.totals,
                                    "frames": snap.frames,
                                    "reason": "cursor_expired",
                                },
                            }
                        )
                    )
                    last_sent = snap.high_water
                else:
                    for frame in frames:
                        self.wfile.write(
                            sse_format(
                                {
                                    "event": "frame",
                                    "id": frame["seq"],
                                    "data": {
                                        "code": code,
                                        "seq": frame["seq"],
                                        "operationId": frame["operationId"],
                                        "deltas": frame["deltas"],
                                        "totals": frame["totals"],
                                    },
                                }
                            )
                        )
                        last_sent = frame["seq"]

            self.wfile.flush()

            # 代已结束且没有活跃订阅价值时，补发 superseded 后由客户端决定。
            if snap.status != "active":
                self.wfile.write(
                    sse_format(
                        {
                            "event": "superseded",
                            "data": {"code": code},
                        }
                    )
                )
                self.wfile.flush()
                return

            while True:
                try:
                    event = q.get(timeout=HEARTBEAT_SECONDS)
                except queue.Empty:
                    self.wfile.write(b":hb\n\n")
                    self.wfile.flush()
                    continue
                if event is None:  # 关闭哨兵（代被取代 / 慢消费者）
                    return
                seq = event.get("id")
                if isinstance(seq, int) and seq <= last_sent:
                    continue  # 订阅前已补发的帧，去重
                self.wfile.write(sse_format(event))
                self.wfile.flush()
                if isinstance(seq, int):
                    last_sent = seq
                if event.get("event") == "superseded":
                    return
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            self.bus.unsubscribe(code, q)


def make_server(host: str, port: int, store: Store) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), Handler)
    server.store = store  # type: ignore[attr-defined]
    server.bus = Bus()  # type: ignore[attr-defined]
    server.daemon_threads = True
    return server


def main() -> None:
    port = int(os.environ.get("PORT", "8080"))
    db_path = os.environ.get("DB_PATH", ":memory:")
    if db_path != ":memory:":
        os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)
    store = Store(db_path)
    server = make_server("0.0.0.0", port, store)
    print(f"低温阵列扫描服务已启动: 0.0.0.0:{port} (db={db_path})", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
        store.close()


if __name__ == "__main__":
    main()
