"""退货编排：字节 → 编码探测 → 退货表头契约 → 行校验 → 聚合 → 事务入账。

与订单管道共用编码探测与 CSV 解析；区别在于退货必须按订单核对累计上限，
这只能在写事务里做，因此全部交给 `return_importer`。
"""
from __future__ import annotations

import hashlib
import sqlite3

from . import encoding, report, return_importer, returns, store
from .pipeline import parse_csv


def _empty_return_report() -> dict:
    data = report.build_report([])
    data["advice"] = "整份退货文件未入账；库内退货明细与站点汇总保持原值。"
    return data


def run_returns(
    raw: bytes,
    filename: str,
    extra_policy: str = "reject",
    declared_site: str = "",
    conn: sqlite3.Connection | None = None,
    fail_after: int | None = None,
    return_batch_id: str | None = None,
) -> dict:
    own_conn = conn is None
    conn = conn or store.connect()
    sha = hashlib.sha256(raw).hexdigest()
    try:
        result: dict = {
            "filename": filename,
            "sha256": sha,
            "bytes": len(raw),
            "extra_policy": extra_policy,
            "declared_site": declared_site,
            "sample_hex": encoding.sample_hex(raw),
        }

        # 1) 编码探测（与订单一致）
        try:
            probe = encoding.probe(raw)
        except encoding.EncodingError as exc:
            attempt_id = store.record_return_attempt(
                conn, filename=filename, sha256=sha, status="encoding_failed",
                declared_site=declared_site, detail=exc.message,
            )
            before = store.count_return_rows(conn)
            result.update({
                "status": "encoding_failed",
                "detail": exc.message,
                "encoding": {
                    "encoding": None, "bom": raw.startswith(encoding.UTF8_BOM),
                    "sample_hex": exc.sample_hex, "attempts": exc.attempts,
                },
                "suggestion": exc.message,
                "attempt_id": attempt_id,
                "rows_before": before, "rows_after": before,
            })
            return result

        result["encoding"] = probe.evidence()

        # 2) 退货表头契约
        header, data, blank_lines = parse_csv(probe.text)
        contract_result = returns.check_contract(header, extra_policy)
        result["contract"] = contract_result
        result["data_row_count"] = len(data)
        result["skipped_blank_lines"] = blank_lines

        if not contract_result["ok"]:
            detail = "；".join(e["message"] for e in contract_result["errors"])
            header_errors = [
                {"line_no": 1, "row_no": 0, "column": ",".join(e.get("columns", [])),
                 "code": e["code"], "label": "退货表头契约", "raw": "", "reason": e["message"]}
                for e in contract_result["errors"]
            ]
            attempt_id = store.record_return_attempt(
                conn, filename=filename, sha256=sha, status="rejected",
                encoding=probe.encoding, error_count=len(contract_result["errors"]),
                row_count=len(data), declared_site=declared_site,
                detail=f"退货表头契约未通过：{detail}", errors=header_errors,
            )
            before = store.count_return_rows(conn)
            result.update({
                "status": "rejected", "stage": "contract", "detail": detail,
                "attempt_id": attempt_id, "report": _empty_return_report(),
                "rows_before": before, "rows_after": before,
            })
            return result

        # 3) 行级校验
        validation = returns.validate(data, contract_result["header"],
                                      contract_result["columns_used"])
        result["row_validation"] = {
            "ok": validation.ok,
            "row_count": validation.row_count,
            "error_count": validation.error_count,
            "codes": sorted(set(validation.codes())),
        }
        result["report"] = report.build_report(validation.error_dicts())

        if not validation.ok:
            detail = (f"退货表头契约通过，但 {validation.error_count} 处行级错误，"
                      f"整份文件未入账（首处：第 {result['report']['first_line']} 行）。")
            attempt_id = store.record_return_attempt(
                conn, filename=filename, sha256=sha, status="rejected",
                encoding=probe.encoding, error_count=validation.error_count,
                row_count=len(data), declared_site=declared_site,
                detail=detail, errors=validation.error_dicts(),
            )
            before = store.count_return_rows(conn)
            result.update({
                "status": "rejected", "stage": "rows", "detail": detail,
                "attempt_id": attempt_id,
                "rows_before": before, "rows_after": before,
            })
            return result

        # 4) 聚合自证
        summary = returns.summarize(validation.rows)
        result["aggregate"] = summary
        result["declared_site"] = validation.declared_site or declared_site

        # 5) 事务入账（累计上限 + 冲突整份回滚 + 幂等）
        outcome = return_importer.import_returns(
            conn, filename=filename, sha256=sha, rows=validation.rows,
            summary=summary, encoding=probe.encoding, bom=probe.bom,
            declared_site=declared_site, fail_after=fail_after,
            return_batch_id=return_batch_id,
        )
        result["import"] = outcome
        result["status"] = outcome["status"]
        result["return_batch_id"] = outcome.get("return_batch_id")
        result["batch_id"] = outcome.get("return_batch_id")
        result["rows_before"] = outcome.get("rows_before")
        result["rows_after"] = outcome.get("rows_after")
        result["written"] = outcome.get("written", 0)
        result["attempt_id"] = outcome.get("attempt_id")
        if outcome.get("error"):
            result["error"] = outcome["error"]
            result["stage"] = outcome.get("stage", "returns")
        if outcome["status"] == "conflict":
            result["conflicts"] = outcome.get("conflicts", [])
            result["previous_summary"] = outcome.get("previous_summary", {})
            result["detail"] = outcome.get("detail", "")
        elif outcome["status"] in ("rejected", "rolled_back"):
            result["stage"] = outcome.get("stage", "returns")
            result["detail"] = outcome.get("detail", "")
        elif outcome["status"] == "duplicate_file":
            result["detail"] = outcome.get("duplicate_hint") or outcome.get("detail", "")
        if outcome.get("duplicate_hint"):
            result["duplicate_hint"] = outcome["duplicate_hint"]
        return result
    finally:
        if own_conn:
            conn.close()
