"""低温阵列扫描 —— 持久化层。

职责：
- 一代采集（acquisition）对应一个代号 code，同一时刻仅一代为 active。
- 追加帧（frame）在**一次事务**内完成：校验操作标识幂等性、分配严格递增序号、
  更新快照（高水位 + 各通道累计值）、压缩（仅保留最近 RETENTION 帧）。
- 相同操作标识 + 相同参数重试 -> 返回原序号（replayed）。
- 相同操作标识 + 不同参数 -> 稳定拒绝（ConflictError），不产生任何事件。
- 向已结束的旧代追加 -> GenerationClosedError。
"""
from __future__ import annotations

import hashlib
import json
import secrets
import sqlite3
import threading
import time
from dataclasses import dataclass, field

RETENTION = 32  # 每代仅保留最近 32 帧
MAX_CHANNELS = 64
MAX_OPERATION_ID_LEN = 128


class StoreError(Exception):
    """持久层错误基类。"""


class NotFoundError(StoreError):
    """代号不存在。"""


class GenerationClosedError(StoreError):
    """采集代已被新代替换（或不存在于 active 状态），拒绝追加。"""


class ConflictError(StoreError):
    """操作标识被异参复用，稳定拒绝。"""


class ValidationError(StoreError):
    """请求参数非法。"""


@dataclass(frozen=True)
class AppendResult:
    code: str
    seq: int
    high_water: int
    totals: dict
    deltas: dict
    operation_id: str
    replayed: bool = False


@dataclass(frozen=True)
class Snapshot:
    code: str
    status: str
    high_water: int
    totals: dict
    frames: list[dict] = field(default_factory=list)


def canonical_payload(deltas: dict) -> str:
    """参数规范化：键排序的紧凑 JSON，保证同参数得到同哈希。"""
    return json.dumps(deltas, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def validate_deltas(deltas) -> dict:
    if not isinstance(deltas, dict) or not deltas:
        raise ValidationError("deltas 必须是非空对象")
    if len(deltas) > MAX_CHANNELS:
        raise ValidationError(f"通道数超过上限 {MAX_CHANNELS}")
    clean = {}
    for name, value in deltas.items():
        if not isinstance(name, str) or not name or len(name) > 64:
            raise ValidationError("通道名必须为 1..64 字符的字符串")
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValidationError(f"通道 {name} 的增量必须为有限数值")
        if value != value or value in (float("inf"), float("-inf")):
            raise ValidationError(f"通道 {name} 的增量必须为有限数值")
        clean[name] = value
    return clean


def validate_operation_id(operation_id) -> str:
    if not isinstance(operation_id, str) or not operation_id:
        raise ValidationError("operationId 必须是非空字符串")
    if len(operation_id) > MAX_OPERATION_ID_LEN:
        raise ValidationError(f"operationId 长度超过 {MAX_OPERATION_ID_LEN}")
    return operation_id


class Store:
    """SQLite 持久化。所有写操作经 _lock 串行化，并以 BEGIN IMMEDIATE 开启事务，
    因此并发重复追加只会落一帧，其余请求在提交后读到已存在的操作标识并返回原序号。"""

    def __init__(self, path: str = ":memory:"):
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._create_schema()

    def _create_schema(self) -> None:
        with self._lock, self._conn:
            self._conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS acquisitions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    code TEXT NOT NULL UNIQUE,
                    status TEXT NOT NULL DEFAULT 'active',
                    created_at REAL NOT NULL
                );
                CREATE TABLE IF NOT EXISTS snapshots (
                    acquisition_id INTEGER PRIMARY KEY REFERENCES acquisitions(id),
                    high_water INTEGER NOT NULL DEFAULT 0,
                    totals TEXT NOT NULL DEFAULT '{}'
                );
                CREATE TABLE IF NOT EXISTS frames (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    acquisition_id INTEGER NOT NULL REFERENCES acquisitions(id),
                    seq INTEGER NOT NULL,
                    operation_id TEXT NOT NULL,
                    params_hash TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    totals TEXT,
                    created_at REAL NOT NULL,
                    UNIQUE (acquisition_id, seq),
                    UNIQUE (acquisition_id, operation_id)
                );
                """
            )
            # 旧库迁移：逐帧累计值改为追加时固化（避免从最终汇总反向消减时
            # 大数与小增量混合导致浮点灾难性抵消）。
            frame_cols = {
                r["name"]
                for r in self._conn.execute("PRAGMA table_info(frames)")
            }
            if "totals" not in frame_cols:
                self._conn.execute("ALTER TABLE frames ADD COLUMN totals TEXT")
                self._backfill_legacy_frame_totals()

    def _backfill_legacy_frame_totals(self) -> None:
        """为旧版本写入、缺少固化累计值的帧补填 totals（尽力而为）。

        基值取“快照权威汇总减去留存帧增量之和”，再正向累计；正常量级数据与历史
        行为一致，且最后一帧恰好等于权威汇总。极端大小量级混合的旧数据中间轨迹
        无法从压缩后的留存帧精确复原（这正是旧实现的缺陷），新数据在追加时即
        固化累计值，不再依赖此路径。
        """
        acq_rows = self._conn.execute("SELECT id FROM acquisitions").fetchall()
        for acq in acq_rows:
            snap = self._conn.execute(
                "SELECT totals FROM snapshots WHERE acquisition_id=?", (acq["id"],)
            ).fetchone()
            if snap is None:
                continue
            base = json.loads(snap["totals"])
            frame_rows = self._conn.execute(
                "SELECT seq, payload FROM frames WHERE acquisition_id=? ORDER BY seq ASC",
                (acq["id"],),
            ).fetchall()
            # 先扣除全部留存增量得到留存窗口起点前的基值
            for row in frame_rows:
                for name, value in json.loads(row["payload"]).items():
                    base[name] = base.get(name, 0) - value
            running = base
            for row in frame_rows:
                deltas = json.loads(row["payload"])
                for name, value in deltas.items():
                    running[name] = running.get(name, 0) + value
                self._conn.execute(
                    "UPDATE frames SET totals=? WHERE acquisition_id=? AND seq=?",
                    (canonical_payload(running), acq["id"], row["seq"]),
                )

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------ 代管理

    def create_acquisition(self) -> tuple[str, list[str]]:
        """开启新一代采集。返回 (新代号, 被取代的旧代号列表)。"""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                old = [
                    r["code"]
                    for r in self._conn.execute(
                        "SELECT code FROM acquisitions WHERE status='active'"
                    )
                ]
                self._conn.execute(
                    "UPDATE acquisitions SET status='superseded' WHERE status='active'"
                )
                for _ in range(8):  # 代号碰撞重试
                    code = "gen-" + secrets.token_hex(4)
                    try:
                        cur = self._conn.execute(
                            "INSERT INTO acquisitions(code, status, created_at)"
                            " VALUES (?, 'active', ?)",
                            (code, time.time()),
                        )
                        break
                    except sqlite3.IntegrityError:
                        continue
                else:  # pragma: no cover - 理论上不可达
                    raise StoreError("无法分配唯一代号")
                self._conn.execute(
                    "INSERT INTO snapshots(acquisition_id, high_water, totals)"
                    " VALUES (?, 0, '{}')",
                    (cur.lastrowid,),
                )
                self._conn.commit()
                return code, old
            except Exception:
                self._conn.rollback()
                raise

    def _get_acquisition(self, code: str) -> sqlite3.Row:
        row = self._conn.execute(
            "SELECT id, code, status, created_at FROM acquisitions WHERE code=?",
            (code,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"代号 {code} 不存在")
        return row

    # ------------------------------------------------------------------ 追加帧

    def append_frame(self, code: str, operation_id: str, deltas: dict) -> AppendResult:
        """在一次事务中追加一帧（或幂等返回原帧）。"""
        operation_id = validate_operation_id(operation_id)
        deltas = validate_deltas(deltas)
        payload = canonical_payload(deltas)
        params_hash = hashlib.sha256(payload.encode("utf-8")).hexdigest()

        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                acq = self._conn.execute(
                    "SELECT id, code, status FROM acquisitions WHERE code=?", (code,)
                ).fetchone()
                if acq is None:
                    raise NotFoundError(f"代号 {code} 不存在")

                existing = self._conn.execute(
                    "SELECT seq, params_hash, payload FROM frames"
                    " WHERE acquisition_id=? AND operation_id=?",
                    (acq["id"], operation_id),
                ).fetchone()
                if existing is not None:
                    # 命中已存在的操作标识：不再产生任何事件。
                    snap = self._conn.execute(
                        "SELECT high_water, totals FROM snapshots WHERE acquisition_id=?",
                        (acq["id"],),
                    ).fetchone()
                    self._conn.commit()
                    if existing["params_hash"] != params_hash:
                        raise ConflictError(
                            f"操作标识 {operation_id!r} 已被不同参数使用"
                        )
                    return AppendResult(
                        code=code,
                        seq=existing["seq"],
                        high_water=snap["high_water"],
                        totals=json.loads(snap["totals"]),
                        deltas=json.loads(existing["payload"]),
                        operation_id=operation_id,
                        replayed=True,
                    )

                if acq["status"] != "active":
                    raise GenerationClosedError(f"采集代 {code} 已被新代替换")

                snap = self._conn.execute(
                    "SELECT high_water, totals FROM snapshots WHERE acquisition_id=?",
                    (acq["id"],),
                ).fetchone()
                seq = snap["high_water"] + 1
                totals = json.loads(snap["totals"])
                for name, value in deltas.items():
                    totals[name] = totals.get(name, 0) + value

                self._conn.execute(
                    "INSERT INTO frames(acquisition_id, seq, operation_id,"
                    " params_hash, payload, totals, created_at)"
                    " VALUES (?,?,?,?,?,?,?)",
                    (
                        acq["id"],
                        seq,
                        operation_id,
                        params_hash,
                        payload,
                        canonical_payload(totals),
                        time.time(),
                    ),
                )
                self._conn.execute(
                    "UPDATE snapshots SET high_water=?, totals=? WHERE acquisition_id=?",
                    (seq, canonical_payload(totals), acq["id"]),
                )
                # 压缩：仅保留最近 RETENTION 帧，累计值由快照承载。
                self._conn.execute(
                    "DELETE FROM frames WHERE acquisition_id=? AND seq<=?",
                    (acq["id"], seq - RETENTION),
                )
                self._conn.commit()
                return AppendResult(
                    code=code,
                    seq=seq,
                    high_water=seq,
                    totals=totals,
                    deltas=deltas,
                    operation_id=operation_id,
                    replayed=False,
                )
            except Exception:
                self._conn.rollback()
                raise

    # ------------------------------------------------------------------ 读取

    def _retained_frames(self, acquisition_id: int) -> list[dict]:
        """读取留存帧。每帧的 totals 是追加时正向累计并固化的值，
        与快照权威汇总同源，无需（也不得）从最终汇总反向消减重建
        —— 反向消减在大数量级与微小增量混合时会发生浮点灾难性抵消。"""
        rows = self._conn.execute(
            "SELECT seq, operation_id, payload, totals FROM frames"
            " WHERE acquisition_id=? ORDER BY seq ASC",
            (acquisition_id,),
        ).fetchall()
        frames = []
        for row in rows:
            totals = json.loads(row["totals"]) if row["totals"] is not None else {}
            frames.append(
                {
                    "seq": row["seq"],
                    "operationId": row["operation_id"],
                    "deltas": json.loads(row["payload"]),
                    "totals": totals,
                }
            )
        return frames

    def get_snapshot(self, code: str) -> Snapshot:
        with self._lock:
            acq = self._get_acquisition(code)
            snap = self._conn.execute(
                "SELECT high_water, totals FROM snapshots WHERE acquisition_id=?",
                (acq["id"],),
            ).fetchone()
            totals = json.loads(snap["totals"])
            return Snapshot(
                code=code,
                status=acq["status"],
                high_water=snap["high_water"],
                totals=totals,
                frames=self._retained_frames(acq["id"]),
            )

    def frames_after(self, code: str, cursor: int) -> tuple[list[dict], int]:
        """返回 (seq > cursor 的留存帧, 留存的最小序号)。游标过旧时调用方据此发重置。"""
        with self._lock:
            acq = self._get_acquisition(code)
            retained = self._retained_frames(acq["id"])
            lo_row = self._conn.execute(
                "SELECT MIN(seq) AS lo FROM frames WHERE acquisition_id=?",
                (acq["id"],),
            ).fetchone()
            lo = lo_row["lo"] if lo_row["lo"] is not None else 1
            frames = [frame for frame in retained if frame["seq"] > cursor]
            return frames, lo
