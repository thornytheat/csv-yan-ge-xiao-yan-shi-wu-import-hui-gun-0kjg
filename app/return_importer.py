"""退货事务导入：跨历史与并发批次的累计退货控制。

核心约束（在一次 `BEGIN IMMEDIATE` 写事务内完成，SQLite 写锁天然串行化并发提交）：

1. **按订单累计**：每个 order_id 的「历史已退件数 + 本文件件数」不得超过原始订单件数；
   「历史已退金额 + 本文件退款金额」不得超过原始订单金额。上限在**首次退货时冻结**
   到 `return_order_totals`，后续 force_new_batch 重灌订单也不会放宽已发生退货的上限。
2. **整份原子提交**：退货批次 + 明细 + 批次站点汇总 + 站点累计汇总 + 订单累计台账
   全部在同一事务内写入；任一步异常 → `ROLLBACK`，明细/汇总/台账都回到旧值。
3. **越界整份回滚**：只要有任一订单越界，整份文件不落任何一行，返回冲突订单清单
   （订单号、原始上限、历史已退、本文件请求、超出量），并在上传记录里保留
   「本次之前」的站点汇总与订单台账快照，证明旧账未被改写。
4. **幂等重试**：同 sha256 已成功入账 → `duplicate_file`，写入 0 行；
   `return_batches.sha256` 上的唯一索引是并发下的数据库级兜底。
"""
from __future__ import annotations

import sqlite3
from datetime import datetime

from . import returns, store
from .money import money as return_money


def _new_return_batch_id(conn: sqlite3.Connection) -> str:
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    base = f"R-{stamp}"
    candidate = base
    suffix = 1
    while conn.execute(
        "SELECT 1 FROM return_batches WHERE return_batch_id = ?", (candidate,)
    ).fetchone():
        suffix += 1
        candidate = f"{base}-{suffix}"
    return candidate


def _safe_rollback(conn: sqlite3.Connection) -> None:
    """回滚可能发生在 BEGIN 之前（如等锁超时），无事务时静默忽略。"""
    try:
        conn.execute("ROLLBACK")
    except sqlite3.OperationalError:
        pass


def _snapshot(conn: sqlite3.Connection, order_ids: list[int], sites: list[str]) -> dict:
    """回滚前旧账快照：站点累计汇总 + 相关订单台账（只读，不写库）。"""
    site_totals = {s["site"]: s for s in store.return_site_summary(conn)}
    site_rows = [
        site_totals.get(site, {
            "site": site, "qty": 0, "amount_cents": 0, "amount": "0.00",
            "row_count": 0, "last_return_batch_id": None,
        })
        for site in sorted(set(sites))
    ]
    order_rows = []
    for oid in sorted(set(order_ids)):
        ledger = store.return_order_ledger(conn, oid)
        if ledger:
            order_rows.append(ledger)
            continue
        order = store.find_order_canonical(conn, oid)
        if order is None:
            order_rows.append({
                "order_id": oid, "order_qty": None, "order_amount_cents": None,
                "qty_returned": 0, "amount_returned_cents": 0,
                "amount_returned": "0.00", "qty_remaining": None,
                "amount_remaining": "0.00",
            })
        else:
            order_rows.append({
                "order_id": oid, "order_qty": int(order["qty"]),
                "order_amount_cents": int(order["amount_cents"]),
                "order_amount": return_money(int(order["amount_cents"])),
                "qty_returned": 0, "qty_remaining": int(order["qty"]),
                "amount_returned_cents": 0, "amount_returned": "0.00",
                "amount_remaining_cents": int(order["amount_cents"]),
                "amount_remaining": return_money(int(order["amount_cents"])),
                "last_return_batch_id": None,
            })
    return {
        "sites": site_rows,
        "orders": order_rows,
        "captured_at": store.now_iso(),
    }


def _eval_conflicts(conn: sqlite3.Connection, summary: dict) -> list[dict]:
    """在同一事务内比对「原始订单上限」与「历史累计 + 本文件请求」。

    上限口径：已存在退货台账的订单以台账里冻结的 order_qty/order_amount_cents 为准
    （即使原订单后来被 force_new_batch 重灌也不放宽）；首次退货取原始订单行。
    """
    conflicts: list[dict] = []
    for bucket in summary["by_order"]:
        oid = int(bucket["order_id"])
        order = store.find_order_canonical(conn, oid)
        if order is None:
            conflicts.append({
                "order_id": oid,
                "code": "order_not_found",
                "label": returns.RETURN_CODE_LABELS["order_not_found"],
                "line_nos": bucket["line_nos"],
                "return_ids": bucket["return_ids"],
                "order_qty": None,
                "order_amount": None,
                "order_amount_cents": None,
                "already_qty": 0,
                "already_amount": "0.00",
                "already_amount_cents": 0,
                "requested_qty": bucket["qty"],
                "requested_amount": return_money(bucket["amount_cents"]),
                "requested_amount_cents": bucket["amount_cents"],
                "exceeded_qty": bucket["qty"],
                "exceeded_amount": return_money(bucket["amount_cents"]),
                "exceeded_amount_cents": bucket["amount_cents"],
                "qty_remaining": 0,
                "amount_remaining": "0.00",
                "message": (f"订单 {oid} 不在已入账订单中；退货必须指向历史订单，"
                            "请确认 order_id 是否写错或对应订单尚未入库。"),
            })
            continue

        ledger = store.return_order_ledger(conn, oid)
        if ledger:
            already_qty = ledger["qty_returned"]
            already_cents = ledger["amount_returned_cents"]
            limit_qty = ledger["order_qty"]
            limit_cents = ledger["order_amount_cents"]
        else:
            already_qty = 0
            already_cents = 0
            limit_qty = int(order["qty"])
            limit_cents = int(order["amount_cents"])

        qty_over = already_qty + bucket["qty"] - limit_qty
        cents_over = already_cents + bucket["amount_cents"] - limit_cents
        if qty_over <= 0 and cents_over <= 0:
            continue

        codes = []
        if qty_over > 0:
            codes.append("qty_exceeded")
        if cents_over > 0:
            codes.append("amount_exceeded")
        conflicts.append({
            "order_id": oid,
            "code": "+".join(codes),
            "label": " / ".join(returns.RETURN_CODE_LABELS[c] for c in codes),
            "line_nos": bucket["line_nos"],
            "return_ids": bucket["return_ids"],
            "order_qty": limit_qty,
            "order_amount": return_money(limit_cents),
            "order_amount_cents": limit_cents,
            "already_qty": already_qty,
            "already_amount": return_money(already_cents),
            "already_amount_cents": already_cents,
            "requested_qty": bucket["qty"],
            "requested_amount": return_money(bucket["amount_cents"]),
            "requested_amount_cents": bucket["amount_cents"],
            "exceeded_qty": max(qty_over, 0),
            "exceeded_amount": return_money(max(cents_over, 0)),
            "exceeded_amount_cents": max(cents_over, 0),
            "qty_remaining": max(limit_qty - already_qty, 0),
            "amount_remaining": return_money(max(limit_cents - already_cents, 0)),
            "message": (
                f"订单 {oid}：原件数 {limit_qty}、原金额 {return_money(limit_cents)}；"
                f"历史已退 {already_qty} 件 / {return_money(already_cents)}；"
                f"本文件再退 {bucket['qty']} 件 / {return_money(bucket['amount_cents'])}，"
                f"超出 {max(qty_over, 0)} 件 / {return_money(max(cents_over, 0))}。"
            ),
        })
    conflicts.sort(key=lambda c: c["order_id"])
    return conflicts


def import_returns(
    conn: sqlite3.Connection,
    *,
    filename: str,
    sha256: str,
    rows: list[dict],
    summary: dict,
    encoding: str,
    bom: bool,
    declared_site: str = "",
    fail_after: int | None = None,
    note: str = "",
    return_batch_id: str | None = None,
) -> dict:
    """返回导入结论；业务冲突与故障都以结果返回，不抛业务异常。"""
    rows_before = store.count_return_rows(conn)
    sites = sorted({r["site"] for r in rows})
    order_ids = [int(b["order_id"]) for b in summary["by_order"]]

    # 快速路径：同 sha256 已入账（事务内还会再判一次，防并发双写）。
    existing = store.find_return_batch_by_sha(conn, sha256)
    if existing:
        attempt_id = store.record_return_attempt(
            conn, filename=filename, sha256=sha256, status="duplicate_file",
            encoding=encoding, row_count=len(rows), declared_site=declared_site,
            return_batch_id=existing["return_batch_id"],
            detail="同 sha256 退货文件已入账，未写入任何行",
        )
        return {
            "status": "duplicate_file",
            "return_batch_id": existing["return_batch_id"],
            "existing_filename": existing["filename"],
            "existing_created_at": existing["created_at"],
            "attempt_id": attempt_id,
            "rows_before": rows_before,
            "rows_after": rows_before,
            "written": 0,
            "duplicate_hint": (
                "同一份退货文件（sha256 相同）已入账，重试不会重复入账。"
                "若确需重新入账，请先核对原批次并冲正后使用新文件。"
            ),
        }

    # 文件级站点约束：各站点退货文件独立提交，一份文件只允许一个站点。
    if len(sites) > 1:
        detail = (f"退货文件含多个站点（{', '.join(sites)}）；各站点须独立提交，"
                  "整份文件未写入。")
        attempt_id = store.record_return_attempt(
            conn, filename=filename, sha256=sha256, status="rejected", encoding=encoding,
            error_count=len(sites), row_count=len(rows),
            declared_site=declared_site, detail=detail,
            errors=[{
                "line_no": 1, "row_no": 0, "column": "site", "code": "multi_site_file",
                "label": returns.RETURN_CODE_LABELS["multi_site_file"],
                "raw": ",".join(sites),
                "reason": detail,
            }],
        )
        return {
            "status": "rejected", "stage": "return_site", "detail": detail,
            "attempt_id": attempt_id, "rows_before": rows_before,
            "rows_after": rows_before, "written": 0, "sites": sites,
        }

    if declared_site and sites and declared_site not in sites:
        detail = (f"申报站点 {declared_site} 与文件内容站点 {sites[0]} 不一致；整份文件未写入。")
        attempt_id = store.record_return_attempt(
            conn, filename=filename, sha256=sha256, status="rejected", encoding=encoding,
            error_count=1, row_count=len(rows), declared_site=declared_site, detail=detail,
        )
        return {
            "status": "rejected", "stage": "return_site", "detail": detail,
            "attempt_id": attempt_id, "rows_before": rows_before,
            "rows_after": rows_before, "written": 0,
        }

    if not summary.get("consistent"):
        detail = "退货聚合自检未通过：分组/订单/Decimal 口径不一致；未写入。"
        attempt_id = store.record_return_attempt(
            conn, filename=filename, sha256=sha256, status="rolled_back",
            encoding=encoding, error_count=1, row_count=len(rows),
            declared_site=declared_site, detail=detail,
        )
        return {"status": "rolled_back", "detail": detail, "attempt_id": attempt_id,
                "rows_before": rows_before, "rows_after": rows_before, "written": 0}

    batch_id = return_batch_id
    written = 0
    previous: dict = {"sites": [], "orders": []}
    try:
        conn.execute("BEGIN IMMEDIATE")

        # 批次号在写事务内分配：写锁已串行化并发提交，避免同一秒时间戳撞唯一键。
        if not batch_id:
            batch_id = _new_return_batch_id(conn)

        # 事务内二次判重：并发提交同一文件时，后到者在此命中，避免双写。
        if store.find_return_batch_by_sha(conn, sha256):
            _safe_rollback(conn)
            existing = store.find_return_batch_by_sha(conn, sha256)
            attempt_id = store.record_return_attempt(
                conn, filename=filename, sha256=sha256, status="duplicate_file",
                encoding=encoding, row_count=len(rows), declared_site=declared_site,
                return_batch_id=existing["return_batch_id"] if existing else None,
                detail="并发提交命中同 sha256 已入账文件，未重复写入",
            )
            return {
                "status": "duplicate_file",
                "return_batch_id": existing["return_batch_id"] if existing else None,
                "attempt_id": attempt_id, "rows_before": rows_before,
                "rows_after": rows_before, "written": 0,
                "duplicate_hint": "同一份退货文件已入账（并发重试），未重复入账。",
            }

        # 旧账快照：用于回滚后证明「旧版汇总与旧账未被改写」。
        previous = _snapshot(conn, order_ids, sites)

        conflicts = _eval_conflicts(conn, summary)
        if conflicts:
            _safe_rollback(conn)
            detail = (f"累计退货越界：{len(conflicts)} 个订单冲突，整份文件回滚，"
                      "未写入任何退货明细。")
            attempt_id = store.record_return_attempt(
                conn, filename=filename, sha256=sha256, status="conflict",
                encoding=encoding, error_count=len(conflicts), row_count=len(rows),
                declared_site=declared_site, detail=detail,
                conflicts=conflicts, previous_summary=previous,
            )
            return {
                "status": "conflict", "detail": detail, "attempt_id": attempt_id,
                "conflicts": conflicts, "previous_summary": previous,
                "rows_before": rows_before, "rows_after": rows_before, "written": 0,
            }

        # 1) 批次
        conn.execute(
            "INSERT INTO return_batches(return_batch_id, filename, sha256, declared_site,"
            " encoding, bom, row_count, total_qty, total_amount_cents, created_at, note)"
            " VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (batch_id, filename, sha256, sites[0] if sites else declared_site, encoding,
             1 if bom else 0, len(rows), int(summary["total_qty"]),
             int(summary["total_amount_cents"]), store.now_iso(), note),
        )

        # 2) 明细
        for row in rows:
            conn.execute(
                "INSERT INTO return_facts(return_batch_id, row_no, line_no, return_id,"
                " order_id, site, category, return_date, amount_cents, qty, memo, created_at)"
                " VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
                (batch_id, int(row["row_no"]), int(row["line_no"]), int(row["return_id"]),
                 int(row["order_id"]), row["site"], row["category"], row["return_date"],
                 int(row["amount_cents"]), int(row["qty"]), row.get("memo") or None,
                 store.now_iso()),
            )
            written += 1
            if fail_after is not None and written >= fail_after:
                raise RuntimeError(f"注入故障：写入第 {written} 行退货明细后中断（用于证明事务回滚）")

        # 3) 批次级站点汇总
        for group in summary["groups"]:
            conn.execute(
                "INSERT INTO return_aggregates(return_batch_id, site, qty, amount_cents,"
                " row_count) VALUES(?,?,?,?,?)",
                (batch_id, group["site"], int(group["qty"]), int(group["amount_cents"]),
                 int(group["row_count"])),
            )

        # 4) 站点累计汇总（UPSERT）
        for group in summary["groups"]:
            conn.execute(
                "INSERT INTO return_site_totals(site, qty_returned, amount_returned_cents,"
                " row_count, last_return_batch_id, updated_at) VALUES(?,?,?,?,?,?)"
                " ON CONFLICT(site) DO UPDATE SET"
                " qty_returned = qty_returned + excluded.qty_returned,"
                " amount_returned_cents = amount_returned_cents + excluded.amount_returned_cents,"
                " row_count = row_count + excluded.row_count,"
                " last_return_batch_id = excluded.last_return_batch_id,"
                " updated_at = excluded.updated_at",
                (group["site"], int(group["qty"]), int(group["amount_cents"]),
                 int(group["row_count"]), batch_id, store.now_iso()),
            )

        # 5) 订单累计台账（UPSERT，并带条件兜底：累计不得越过冻结上限）
        for bucket in summary["by_order"]:
            oid = int(bucket["order_id"])
            order = store.find_order_canonical(conn, oid)
            # _eval_conflicts 已确认存在；这里仍防御性判空。
            if order is None:
                raise RuntimeError(f"订单 {oid} 在事务内消失")
            conn.execute(
                "INSERT INTO return_order_totals(order_id, order_qty, order_amount_cents,"
                " qty_returned, amount_returned_cents, last_return_batch_id, updated_at)"
                " VALUES(?,?,?,?,?,?,?)"
                " ON CONFLICT(order_id) DO UPDATE SET"
                " qty_returned = qty_returned + excluded.qty_returned,"
                " amount_returned_cents = amount_returned_cents + excluded.amount_returned_cents,"
                " last_return_batch_id = excluded.last_return_batch_id,"
                " updated_at = excluded.updated_at",
                (oid, int(order["qty"]), int(order["amount_cents"]),
                 int(bucket["qty"]), int(bucket["amount_cents"]), batch_id, store.now_iso()),
            )
            guard = conn.execute(
                "UPDATE return_order_totals SET qty_returned = qty_returned"
                " WHERE order_id = ? AND qty_returned <= order_qty"
                " AND amount_returned_cents <= order_amount_cents",
                (oid,),
            )
            if guard.rowcount != 1:
                raise RuntimeError(
                    f"订单 {oid} 累计退货越界（数据库兜底校验失败），回滚整份文件"
                )

        conn.execute("COMMIT")
    except sqlite3.IntegrityError as exc:
        _safe_rollback(conn)
        # sha256 唯一索引兜底：并发重复文件。
        existing = store.find_return_batch_by_sha(conn, sha256)
        if existing:
            attempt_id = store.record_return_attempt(
                conn, filename=filename, sha256=sha256, status="duplicate_file",
                encoding=encoding, row_count=len(rows), declared_site=declared_site,
                return_batch_id=existing["return_batch_id"],
                detail="唯一索引拦截并发重复文件，未重复入账",
            )
            return {
                "status": "duplicate_file",
                "return_batch_id": existing["return_batch_id"],
                "attempt_id": attempt_id, "rows_before": rows_before,
                "rows_after": rows_before, "written": 0,
                "duplicate_hint": "同一份退货文件已入账（并发重试），未重复入账。",
            }
        rows_after = store.count_return_rows(conn)
        detail = f"事务回滚：{exc}；退货明细 {rows_before} → {rows_after}（应保持不变）。"
        attempt_id = store.record_return_attempt(
            conn, filename=filename, sha256=sha256, status="rolled_back",
            encoding=encoding, error_count=1, row_count=len(rows),
            declared_site=declared_site, detail=detail, previous_summary=previous,
        )
        return {"status": "rolled_back", "detail": detail, "attempt_id": attempt_id,
                "rows_before": rows_before, "rows_after": rows_after, "written": 0,
                "error": str(exc)}
    except Exception as exc:  # noqa: BLE001 - 事务失败必须整体回滚
        _safe_rollback(conn)
        rows_after = store.count_return_rows(conn)
        detail = f"事务回滚：{exc}；退货明细 {rows_before} → {rows_after}（应保持不变）。"
        attempt_id = store.record_return_attempt(
            conn, filename=filename, sha256=sha256, status="rolled_back",
            encoding=encoding, error_count=1, row_count=len(rows),
            declared_site=declared_site, detail=detail, previous_summary=previous,
        )
        return {"status": "rolled_back", "detail": detail,
                "return_batch_id": batch_id, "attempt_id": attempt_id,
                "rows_before": rows_before, "rows_after": rows_after, "written": 0,
                "error": str(exc)}

    rows_after = store.count_return_rows(conn)
    attempt_id = store.record_return_attempt(
        conn, filename=filename, sha256=sha256, status="committed", encoding=encoding,
        row_count=len(rows), return_batch_id=batch_id, declared_site=declared_site,
        detail="退货整份入账（明细 + 站点汇总 + 订单累计台账同事务提交）",
    )
    return {
        "status": "committed",
        "return_batch_id": batch_id,
        "attempt_id": attempt_id,
        "rows_before": rows_before,
        "rows_after": rows_after,
        "written": written,
        "declared_site": sites[0] if sites else declared_site,
        "site_summary": store.return_site_summary(conn),
    }
