"""SQLite 访问（薄封装，无 ORM）。

表：
- meta(key,value)                  seed 版本
- batches(...)                     已入库批次，sha256 用于重复文件判定
- fact_orders(...)                 明细事实，仅 committed 批次写入
- batch_aggregates(...)            按 site 聚合快照
- import_attempts(...)             每次上传的结论（含回滚记录）/review 用
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from . import settings
from .money import money

SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (
  key TEXT PRIMARY KEY,
  value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS batches (
  batch_id TEXT PRIMARY KEY,
  filename TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  encoding TEXT NOT NULL,
  bom INTEGER NOT NULL DEFAULT 0,
  row_count INTEGER NOT NULL,
  total_amount_cents INTEGER NOT NULL,
  total_qty INTEGER NOT NULL,
  forced INTEGER NOT NULL DEFAULT 0,
  superseded_by TEXT,
  created_at TEXT NOT NULL,
  note TEXT
);
CREATE INDEX IF NOT EXISTS idx_batches_sha ON batches(sha256);

CREATE TABLE IF NOT EXISTS fact_orders (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  batch_id TEXT NOT NULL,
  row_no INTEGER NOT NULL,
  order_id INTEGER NOT NULL,
  site TEXT NOT NULL,
  category TEXT NOT NULL,
  order_date TEXT NOT NULL,
  amount_cents INTEGER NOT NULL,
  qty INTEGER NOT NULL,
  memo TEXT
);
CREATE INDEX IF NOT EXISTS idx_fact_batch ON fact_orders(batch_id);

CREATE TABLE IF NOT EXISTS batch_aggregates (
  batch_id TEXT NOT NULL,
  site TEXT NOT NULL,
  amount_cents INTEGER NOT NULL,
  qty INTEGER NOT NULL,
  row_count INTEGER NOT NULL,
  PRIMARY KEY (batch_id, site)
);

CREATE TABLE IF NOT EXISTS import_attempts (
  attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
  filename TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  encoding TEXT,
  status TEXT NOT NULL,
  error_count INTEGER NOT NULL DEFAULT 0,
  row_count INTEGER NOT NULL DEFAULT 0,
  batch_id TEXT,
  detail TEXT,
  errors_json TEXT,
  created_at TEXT NOT NULL
);

-- ---------------------------------------------------------------- 退货
-- 退货批次：sha256 唯一，是「同一份文件重试不重复入账」的数据库级护栏。
CREATE TABLE IF NOT EXISTS return_batches (
  return_batch_id TEXT PRIMARY KEY,
  filename TEXT NOT NULL,
  sha256 TEXT NOT NULL UNIQUE,
  declared_site TEXT,
  encoding TEXT NOT NULL,
  bom INTEGER NOT NULL DEFAULT 0,
  row_count INTEGER NOT NULL,
  total_qty INTEGER NOT NULL,
  total_amount_cents INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  note TEXT
);
CREATE INDEX IF NOT EXISTS idx_return_batches_sha ON return_batches(sha256);

-- 退货明细：仅已提交批次存在；order_id 指向原始订单。
CREATE TABLE IF NOT EXISTS return_facts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  return_batch_id TEXT NOT NULL,
  row_no INTEGER NOT NULL,
  line_no INTEGER NOT NULL,
  return_id INTEGER NOT NULL,
  order_id INTEGER NOT NULL,
  site TEXT NOT NULL,
  category TEXT NOT NULL,
  return_date TEXT,
  amount_cents INTEGER NOT NULL,
  qty INTEGER NOT NULL,
  memo TEXT,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_return_facts_batch ON return_facts(return_batch_id);
CREATE INDEX IF NOT EXISTS idx_return_facts_order ON return_facts(order_id);
CREATE UNIQUE INDEX IF NOT EXISTS uq_return_facts_line ON return_facts(return_batch_id, return_id);

-- 批次内站点汇总快照（每成功提交一份文件写入一组）。
CREATE TABLE IF NOT EXISTS return_aggregates (
  return_batch_id TEXT NOT NULL,
  site TEXT NOT NULL,
  qty INTEGER NOT NULL,
  amount_cents INTEGER NOT NULL,
  row_count INTEGER NOT NULL,
  PRIMARY KEY (return_batch_id, site)
);

-- 站点累计退货汇总：与明细同一事务维护；失败整份回滚，旧值保留。
CREATE TABLE IF NOT EXISTS return_site_totals (
  site TEXT PRIMARY KEY,
  qty_returned INTEGER NOT NULL DEFAULT 0,
  amount_returned_cents INTEGER NOT NULL DEFAULT 0,
  row_count INTEGER NOT NULL DEFAULT 0,
  last_return_batch_id TEXT,
  updated_at TEXT NOT NULL
);

-- 订单累计退货台账：跨历史批次 + 并发批次统一在此累加。
-- order_qty / order_amount_cents 是首次退货时从原始订单冻结的上限，
-- UPDATE 时带条件，数据库层再兜底一次「累计不越界」。
CREATE TABLE IF NOT EXISTS return_order_totals (
  order_id INTEGER PRIMARY KEY,
  order_qty INTEGER NOT NULL,
  order_amount_cents INTEGER NOT NULL,
  qty_returned INTEGER NOT NULL DEFAULT 0,
  amount_returned_cents INTEGER NOT NULL DEFAULT 0,
  last_return_batch_id TEXT,
  updated_at TEXT NOT NULL
);

-- 每次退货上传的结论（冲突清单、失败前旧汇总快照）。
CREATE TABLE IF NOT EXISTS return_attempts (
  attempt_id INTEGER PRIMARY KEY AUTOINCREMENT,
  filename TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  encoding TEXT,
  status TEXT NOT NULL,
  error_count INTEGER NOT NULL DEFAULT 0,
  row_count INTEGER NOT NULL DEFAULT 0,
  return_batch_id TEXT,
  declared_site TEXT,
  detail TEXT,
  errors_json TEXT,
  conflicts_json TEXT,
  previous_summary_json TEXT,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_return_attempts_sha ON return_attempts(sha256);
"""

# 允许的上传策略（避免路由随意传值）
STATUSES = ("committed", "duplicate_file", "rejected", "encoding_failed", "rolled_back")
RETURN_STATUSES = ("committed", "duplicate_file", "rejected", "encoding_failed",
                   "rolled_back", "conflict")


def now_iso() -> str:
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def connect(path: Path | str | None = None) -> sqlite3.Connection:
    target = Path(path) if path else settings.DB_PATH
    target.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(target), isolation_level=None, timeout=15)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)


def get_meta(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else None


def set_meta(conn: sqlite3.Connection, key: str, value: str) -> None:
    conn.execute(
        "INSERT INTO meta(key, value) VALUES(?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        (key, value),
    )


def count_rows(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) AS c FROM fact_orders").fetchone()["c"]


def find_batch_by_sha(conn: sqlite3.Connection, sha256: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM batches WHERE sha256 = ? ORDER BY created_at ASC LIMIT 1", (sha256,)
    ).fetchone()


def list_batches(conn: sqlite3.Connection, limit: int = 30) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM batches ORDER BY created_at DESC, batch_id DESC LIMIT ?", (limit,)
    ).fetchall()
    return [
        {
            "batch_id": r["batch_id"],
            "filename": r["filename"],
            "sha256": r["sha256"],
            "encoding": r["encoding"],
            "bom": bool(r["bom"]),
            "row_count": r["row_count"],
            "total_amount": f"{r['total_amount_cents'] // 100}.{r['total_amount_cents'] % 100:02d}",
            "total_qty": r["total_qty"],
            "forced": bool(r["forced"]),
            "superseded_by": r["superseded_by"],
            "created_at": r["created_at"],
        }
        for r in rows
    ]


def get_batch(conn: sqlite3.Connection, batch_id: str) -> dict | None:
    row = conn.execute("SELECT * FROM batches WHERE batch_id = ?", (batch_id,)).fetchone()
    if not row:
        return None
    agg = [
        {
            "site": a["site"],
            "amount": f"{a['amount_cents'] // 100}.{a['amount_cents'] % 100:02d}",
            "amount_cents": a["amount_cents"],
            "qty": a["qty"],
            "row_count": a["row_count"],
        }
        for a in conn.execute(
            "SELECT * FROM batch_aggregates WHERE batch_id = ? ORDER BY amount_cents DESC",
            (batch_id,),
        ).fetchall()
    ]
    sample = [
        dict(r) for r in conn.execute(
            "SELECT row_no, order_id, site, category, order_date, amount_cents, qty, memo "
            "FROM fact_orders WHERE batch_id = ? ORDER BY row_no LIMIT 20",
            (batch_id,),
        ).fetchall()
    ]
    for r in sample:
        r["amount"] = f"{r['amount_cents'] // 100}.{r['amount_cents'] % 100:02d}"
    return {
        "batch_id": row["batch_id"],
        "filename": row["filename"],
        "sha256": row["sha256"],
        "encoding": row["encoding"],
        "bom": bool(row["bom"]),
        "row_count": row["row_count"],
        "total_amount": f"{row['total_amount_cents'] // 100}.{row['total_amount_cents'] % 100:02d}",
        "total_amount_cents": row["total_amount_cents"],
        "total_qty": row["total_qty"],
        "forced": bool(row["forced"]),
        "superseded_by": row["superseded_by"],
        "created_at": row["created_at"],
        "note": row["note"],
        "aggregates": agg,
        "sample_rows": sample,
    }


def list_attempts(conn: sqlite3.Connection, limit: int = 30) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM import_attempts ORDER BY attempt_id DESC LIMIT ?", (limit,)
    ).fetchall()
    return [dict(r) for r in rows]


def get_attempt(conn: sqlite3.Connection, attempt_id: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM import_attempts WHERE attempt_id = ?", (attempt_id,)
    ).fetchone()
    if not row:
        return None
    data = dict(row)
    try:
        data["errors"] = json.loads(data.get("errors_json") or "[]")
    except json.JSONDecodeError:
        data["errors"] = []
    return data


def record_attempt(conn: sqlite3.Connection, *, filename: str, sha256: str, status: str,
                   encoding: str = "", error_count: int = 0, row_count: int = 0,
                   batch_id: str | None = None, detail: str = "",
                   errors: list[dict] | None = None) -> int:
    payload = json.dumps(errors[:200] if errors else [], ensure_ascii=False)
    cur = conn.execute(
        "INSERT INTO import_attempts(filename, sha256, encoding, status, error_count,"
        " row_count, batch_id, detail, errors_json, created_at)"
        " VALUES(?,?,?,?,?,?,?,?,?,?)",
        (filename, sha256, encoding, status, error_count, row_count, batch_id, detail,
         payload, now_iso()),
    )
    return int(cur.lastrowid or 0)


def stats(conn: sqlite3.Connection) -> dict:
    batches = conn.execute("SELECT COUNT(*) AS c FROM batches").fetchone()["c"]
    fact = count_rows(conn)
    rejected = conn.execute(
        "SELECT COUNT(*) AS c FROM import_attempts WHERE status IN ('rejected','encoding_failed')"
    ).fetchone()["c"]
    sites = conn.execute("SELECT COUNT(DISTINCT site) AS c FROM fact_orders").fetchone()["c"]
    return {"batches": batches, "fact_rows": fact, "rejected_attempts": rejected, "sites": sites}


# ---------------------------------------------------------------- 退货


def count_return_rows(conn: sqlite3.Connection) -> int:
    return conn.execute("SELECT COUNT(*) AS c FROM return_facts").fetchone()["c"]


def find_return_batch_by_sha(conn: sqlite3.Connection, sha256: str) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM return_batches WHERE sha256 = ? LIMIT 1", (sha256,)
    ).fetchone()


def list_return_batches(conn: sqlite3.Connection, limit: int = 30) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM return_batches ORDER BY created_at DESC, return_batch_id DESC LIMIT ?",
        (limit,),
    ).fetchall()
    return [
        {
            "return_batch_id": r["return_batch_id"],
            "filename": r["filename"],
            "sha256": r["sha256"],
            "declared_site": r["declared_site"],
            "encoding": r["encoding"],
            "bom": bool(r["bom"]),
            "row_count": r["row_count"],
            "total_qty": r["total_qty"],
            "total_amount": money(r["total_amount_cents"]),
            "total_amount_cents": r["total_amount_cents"],
            "created_at": r["created_at"],
            "note": r["note"],
        }
        for r in rows
    ]


def get_return_batch(conn: sqlite3.Connection, batch_id: str) -> dict | None:
    row = conn.execute(
        "SELECT * FROM return_batches WHERE return_batch_id = ?", (batch_id,)
    ).fetchone()
    if not row:
        return None
    aggregates = [
        {
            "site": a["site"],
            "qty": a["qty"],
            "amount_cents": a["amount_cents"],
            "amount": money(a["amount_cents"]),
            "row_count": a["row_count"],
        }
        for a in conn.execute(
            "SELECT * FROM return_aggregates WHERE return_batch_id = ? ORDER BY amount_cents DESC",
            (batch_id,),
        ).fetchall()
    ]
    facts = [
        {
            "row_no": f["row_no"],
            "line_no": f["line_no"],
            "return_id": f["return_id"],
            "order_id": f["order_id"],
            "site": f["site"],
            "category": f["category"],
            "return_date": f["return_date"],
            "qty": f["qty"],
            "amount_cents": f["amount_cents"],
            "amount": money(f["amount_cents"]),
            "memo": f["memo"],
        }
        for f in conn.execute(
            "SELECT * FROM return_facts WHERE return_batch_id = ? ORDER BY row_no LIMIT 50",
            (batch_id,),
        ).fetchall()
    ]
    return {
        "return_batch_id": row["return_batch_id"],
        "filename": row["filename"],
        "sha256": row["sha256"],
        "declared_site": row["declared_site"],
        "encoding": row["encoding"],
        "bom": bool(row["bom"]),
        "row_count": row["row_count"],
        "total_qty": row["total_qty"],
        "total_amount": money(row["total_amount_cents"]),
        "total_amount_cents": row["total_amount_cents"],
        "created_at": row["created_at"],
        "note": row["note"],
        "aggregates": aggregates,
        "facts": facts,
    }


def record_return_attempt(conn: sqlite3.Connection, *, filename: str, sha256: str,
                          status: str, encoding: str = "", error_count: int = 0,
                          row_count: int = 0, return_batch_id: str | None = None,
                          declared_site: str = "", detail: str = "",
                          errors: list[dict] | None = None,
                          conflicts: list[dict] | None = None,
                          previous_summary: list[dict] | None = None) -> int:
    payload = json.dumps(errors[:200] if errors else [], ensure_ascii=False)
    conflict_payload = json.dumps(conflicts or [], ensure_ascii=False)
    summary_payload = json.dumps(previous_summary or [], ensure_ascii=False)
    cur = conn.execute(
        "INSERT INTO return_attempts(filename, sha256, encoding, status, error_count,"
        " row_count, return_batch_id, declared_site, detail, errors_json, conflicts_json,"
        " previous_summary_json, created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (filename, sha256, encoding, status, error_count, row_count, return_batch_id,
         declared_site, detail, payload, conflict_payload, summary_payload, now_iso()),
    )
    return int(cur.lastrowid or 0)


def list_return_attempts(conn: sqlite3.Connection, limit: int = 30) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM return_attempts ORDER BY attempt_id DESC LIMIT ?", (limit,)
    ).fetchall()
    out = []
    for r in rows:
        data = dict(r)
        data.pop("errors_json", None)
        try:
            data["conflicts"] = json.loads(data.pop("conflicts_json") or "[]")
        except json.JSONDecodeError:
            data["conflicts"] = []
        try:
            data["previous_summary"] = json.loads(data.pop("previous_summary_json") or "[]")
        except json.JSONDecodeError:
            data["previous_summary"] = []
        out.append(data)
    return out


def get_return_attempt(conn: sqlite3.Connection, attempt_id: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM return_attempts WHERE attempt_id = ?", (attempt_id,)
    ).fetchone()
    if not row:
        return None
    data = dict(row)
    for source, target in (("errors_json", "errors"),
                           ("conflicts_json", "conflicts"),
                           ("previous_summary_json", "previous_summary")):
        raw = data.pop(source, None)
        try:
            data[target] = json.loads(raw or "[]")
        except json.JSONDecodeError:
            data[target] = []
    return data


def return_site_summary(conn: sqlite3.Connection) -> list[dict]:
    """站点累计退货汇总（与明细同事务维护）。"""
    return [
        {
            "site": r["site"],
            "qty": r["qty_returned"],
            "amount_cents": r["amount_returned_cents"],
            "amount": money(r["amount_returned_cents"]),
            "row_count": r["row_count"],
            "last_return_batch_id": r["last_return_batch_id"],
            "updated_at": r["updated_at"],
        }
        for r in conn.execute(
            "SELECT * FROM return_site_totals ORDER BY amount_returned_cents DESC, site"
        ).fetchall()
    ]


def return_order_ledger(conn: sqlite3.Connection, order_id: int) -> dict | None:
    row = conn.execute(
        "SELECT * FROM return_order_totals WHERE order_id = ?", (order_id,)
    ).fetchone()
    if not row:
        return None
    return {
        "order_id": row["order_id"],
        "order_qty": row["order_qty"],
        "order_amount_cents": row["order_amount_cents"],
        "order_amount": money(row["order_amount_cents"]),
        "qty_returned": row["qty_returned"],
        "qty_remaining": max(row["order_qty"] - row["qty_returned"], 0),
        "amount_returned_cents": row["amount_returned_cents"],
        "amount_returned": money(row["amount_returned_cents"]),
        "amount_remaining_cents": max(
            row["order_amount_cents"] - row["amount_returned_cents"], 0
        ),
        "amount_remaining": money(max(
            row["order_amount_cents"] - row["amount_returned_cents"], 0
        )),
        "last_return_batch_id": row["last_return_batch_id"],
        "updated_at": row["updated_at"],
    }


def return_ledger(conn: sqlite3.Connection, limit: int = 50) -> list[dict]:
    return [
        {
            "order_id": r["order_id"],
            "order_qty": r["order_qty"],
            "qty_returned": r["qty_returned"],
            "qty_remaining": max(r["order_qty"] - r["qty_returned"], 0),
            "order_amount": money(r["order_amount_cents"]),
            "amount_returned": money(r["amount_returned_cents"]),
            "amount_remaining": money(max(
                r["order_amount_cents"] - r["amount_returned_cents"], 0
            )),
            "last_return_batch_id": r["last_return_batch_id"],
            "updated_at": r["updated_at"],
        }
        for r in conn.execute(
            "SELECT * FROM return_order_totals ORDER BY updated_at DESC, order_id LIMIT ?",
            (limit,),
        ).fetchall()
    ]


def find_order_canonical(conn: sqlite3.Connection, order_id: int) -> sqlite3.Row | None:
    """原始订单的权威行。

    同一 order_id 可能因 force_new_batch 重灌而存在多条明细：优先取未被 superseded
    的批次中的那条；若全部已被替代（理论上不会），退回最早一条。退货上限在首次
    退货时冻结，之后不再随重灌变化。
    """
    row = conn.execute(
        "SELECT f.order_id, f.site, f.category, f.qty, f.amount_cents, f.batch_id"
        " FROM fact_orders f LEFT JOIN batches b ON b.batch_id = f.batch_id"
        " WHERE f.order_id = ? AND (b.superseded_by IS NULL OR b.batch_id IS NULL)"
        " ORDER BY f.id ASC LIMIT 1",
        (order_id,),
    ).fetchone()
    if row:
        return row
    return conn.execute(
        "SELECT order_id, site, category, qty, amount_cents, batch_id"
        " FROM fact_orders WHERE order_id = ? ORDER BY id ASC LIMIT 1",
        (order_id,),
    ).fetchone()


def return_stats(conn: sqlite3.Connection) -> dict:
    batches = conn.execute("SELECT COUNT(*) AS c FROM return_batches").fetchone()["c"]
    facts = count_return_rows(conn)
    conflicts = conn.execute(
        "SELECT COUNT(*) AS c FROM return_attempts WHERE status = 'conflict'"
    ).fetchone()["c"]
    qty = conn.execute(
        "SELECT COALESCE(SUM(qty),0) AS q FROM return_facts"
    ).fetchone()["q"]
    cents = conn.execute(
        "SELECT COALESCE(SUM(amount_cents),0) AS a FROM return_facts"
    ).fetchone()["a"]
    return {
        "return_batches": batches,
        "return_rows": facts,
        "return_qty": qty,
        "return_amount_cents": cents,
        "return_amount": money(cents),
        "conflict_attempts": conflicts,
    }
