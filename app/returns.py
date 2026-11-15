"""退货 CSV：契约、行级校验与聚合（纯函数，不碰数据库）。

与订单 CSV 的差异：
- 主键是 `return_id`（退货单号），同样要求文件内严格递增；
- 每行必须给出 `order_id`，指向**历史上已入账的原始订单**；
- 一份退货文件只允许一个站点（各站点独立提交），否则整份拒绝；
- 退货件数 / 退款金额的「累计上限」不在本模块判定（需要查库 + 事务锁），
  由 `return_importer` 在 `BEGIN IMMEDIATE` 内统一核对。
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from decimal import Decimal, InvalidOperation

from . import contract
from .money import money
from .rows import DATE_RE, DEC_RE, INT_RE, RowError, RowValidation, _cell

RETURN_REQUIRED = ["return_id", "order_id", "site", "category", "return_date", "amount", "qty"]

RETURN_COLUMN_SPECS: dict[str, dict] = {
    "return_id": {"required": True, "type": "int", "max_length": 32,
                  "label": "退货单号（主键，需严格递增）"},
    "order_id": {"required": True, "type": "int", "max_length": 32,
                 "label": "原订单号（须已入库）"},
    "site": {"required": True, "type": "enum", "enum": contract.SITES,
             "max_length": 32, "label": "退货处理站点"},
    "category": {"required": True, "type": "enum", "enum": contract.CATEGORIES,
                 "max_length": 32, "label": "品类"},
    "return_date": {"required": True, "type": "date", "max_length": 10,
                    "format": "YYYY-MM-DD", "label": "退货日期"},
    "amount": {"required": True, "type": "decimal", "max_length": 16, "scale": 2,
               "min": "0.01", "label": "退款金额"},
    "qty": {"required": True, "type": "int", "max_length": 8, "min": 1, "max": 100000,
            "label": "退货件数"},
    "memo": {"required": False, "type": "string", "max_length": 60, "label": "备注"},
}

# 退货专属错误码（行级 / 文件级 / 累计上限）
RETURN_CODE_LABELS = {
    "multi_site_file": "文件含多个站点",
    "order_not_found": "原订单不存在",
    "qty_exceeded": "累计退货件数超限",
    "amount_exceeded": "累计退款金额超限",
}


@dataclass
class ReturnValidation(RowValidation):
    declared_site: str = ""


def check_contract(header_cells: list[str], extra_policy: str = "reject") -> dict:
    """退货表头契约：与订单共用实现，只是列定义不同。"""
    return contract.check_columns(
        header_cells, extra_policy, RETURN_COLUMN_SPECS, RETURN_REQUIRED,
    )


def validate(records: list[list[str]], header: list[str], columns_used: list[str]) -> ReturnValidation:
    """逐行校验，规则顺序与订单一致：必填 → 长度 → 类型/枚举 → 业务规则。"""
    header_len = len(header)
    index = contract.index_of(header, columns_used)
    result = ReturnValidation(row_count=len(records))
    if not records:
        result.ok = False
        result.errors.append(RowError(0, 1, "", "empty_data", "", "文件只有表头，没有数据行。"))
        return result

    prev_id: int | None = None

    for row_no, record in enumerate(records, start=1):
        line_no = row_no + 1
        per_cell: dict[str, RowError] = {}

        def add(column: str, code: str, raw: str, reason: str) -> None:
            if column in per_cell:
                return
            per_cell[column] = RowError(row_no, line_no, column, code, raw, reason)

        if len(record) != header_len:
            add("", "column_count", str(len(record)),
                f"本行有 {len(record)} 列，表头有 {header_len} 列；请检查是否漏了逗号或字段里含未转义的逗号。")
            result.errors.append(per_cell[""])
            continue

        values: dict[str, str] = {c: _cell(record, index[c]) for c in columns_used}

        # 1) 必填
        for name in RETURN_REQUIRED:
            if name in values and not values[name]:
                add(name, "required", "", f"{RETURN_COLUMN_SPECS[name]['label']}为空，契约要求必填。")

        # 2) 长度
        for name, raw in values.items():
            if name in per_cell or not raw:
                continue
            limit = RETURN_COLUMN_SPECS[name].get("max_length")
            if limit and len(raw) > limit:
                add(name, "length", raw, f"长度 {len(raw)} 超过上限 {limit}（按字符计，含空格已去首尾）。")

        # 3) 类型 / 枚举 / 业务规则
        parsed: dict[str, object] = {}

        for name in ("return_id", "order_id", "qty"):
            if name not in values or name in per_cell or not values[name]:
                continue
            raw = values[name]
            if not INT_RE.match(raw):
                add(name, "int_type", raw, f"「{raw}」不是整数（只允许可选正负号 + 数字）。")
                continue
            number = int(raw)
            if name == "order_id" and number <= 0:
                add(name, "int_range", raw, f"原订单号必须为正整数，当前 {number}。")
                continue
            if name == "qty" and (number < 1 or number > 100000):
                add(name, "int_range", raw, f"退货件数需在 1~100000，当前 {number}。")
                continue
            parsed[name] = number

        if "return_date" in values and "return_date" not in per_cell and values["return_date"]:
            raw = values["return_date"]
            if not DATE_RE.match(raw):
                add("return_date", "date_format", raw, f"日期需为 YYYY-MM-DD，「{raw}」不符合。")
            else:
                try:
                    parsed["return_date"] = date.fromisoformat(raw)
                except ValueError as exc:
                    add("return_date", "date_invalid", raw,
                        f"「{raw}」不是真实存在的日期（{exc}），例如 2 月没有 30 日。")

        if "amount" in values and "amount" not in per_cell and values["amount"]:
            raw = values["amount"]
            if not DEC_RE.match(raw):
                add("amount", "decimal_type", raw,
                    "退款金额需为纯数字（如 128.50），不允许千分位、货币符号、空格。")
            else:
                try:
                    amount = Decimal(raw)
                except InvalidOperation:
                    add("amount", "decimal_type", raw, f"「{raw}」无法解析为金额。")
                else:
                    exponent = -amount.as_tuple().exponent  # type: ignore[arg-type]
                    if exponent > 2:
                        add("amount", "decimal_scale", raw,
                            f"小数位 {exponent} 位，超过 2 位；请先在上游四舍五入到分。")
                    elif amount <= 0:
                        add("amount", "decimal_range", raw, f"退款金额需大于 0，当前 {raw}。")
                    elif amount > Decimal("99999999.99"):
                        add("amount", "decimal_range", raw,
                            f"金额 {raw} 超过上限 99999999.99，疑似单位错误。")
                    else:
                        parsed["amount_cents"] = int(amount * 100)

        for name in ("site", "category"):
            if name not in values or name in per_cell or not values[name]:
                continue
            allowed = RETURN_COLUMN_SPECS[name]["enum"]
            if values[name] not in allowed:
                add(name, "enum", values[name],
                    f"「{values[name]}」不在允许值内：{', '.join(allowed)}。")

        # 4) 主键（退货单号）严格递增
        if "return_id" not in per_cell and "return_id" in parsed:
            rid = int(parsed["return_id"])  # type: ignore[arg-type]
            raw = values["return_id"]
            if prev_id is not None and rid == prev_id:
                add("return_id", "duplicate", raw,
                    f"退货单号 {raw} 与上一行相同；契约要求 return_id 严格递增。")
            elif prev_id is not None and rid < prev_id:
                add("return_id", "out_of_order", raw,
                    f"退货单号 {raw} 小于上一行 {prev_id}；契约要求严格递增。")
            else:
                prev_id = rid

        for err in per_cell.values():
            result.errors.append(err)

        if not per_cell:
            result.rows.append({
                "row_no": row_no,
                "line_no": line_no,
                "return_id": int(parsed["return_id"]),  # type: ignore[arg-type]
                "order_id": int(parsed["order_id"]),    # type: ignore[arg-type]
                "site": values.get("site", ""),
                "category": values.get("category", ""),
                "return_date": parsed["return_date"].isoformat(),  # type: ignore[union-attr]
                "amount_cents": int(parsed["amount_cents"]),  # type: ignore[arg-type]
                "amount_raw": values.get("amount", ""),
                "qty": int(parsed["qty"]),  # type: ignore[arg-type]
                "memo": values.get("memo", ""),
            })

    result.ok = not result.errors
    result.rows.sort(key=lambda r: r["row_no"])
    sites = {r["site"] for r in result.rows}
    result.declared_site = next(iter(sites)) if len(sites) == 1 else ""
    return result


def summarize(rows: list[dict]) -> dict:
    """按站点 + 按订单两条口径汇总，并用独立口径自证。"""
    sites: dict[str, dict] = {}
    by_order: dict[int, dict] = {}
    linear_cents = 0
    linear_qty = 0

    for row in rows:
        site = row["site"]
        bucket = sites.setdefault(site, {
            "site": site, "amount_cents": 0, "qty": 0, "row_count": 0,
        })
        bucket["amount_cents"] += int(row["amount_cents"])
        bucket["qty"] += int(row["qty"])
        bucket["row_count"] += 1

        oid = int(row["order_id"])
        order_bucket = by_order.setdefault(oid, {
            "order_id": oid, "amount_cents": 0, "qty": 0, "row_count": 0,
            "line_nos": [], "return_ids": [],
        })
        order_bucket["amount_cents"] += int(row["amount_cents"])
        order_bucket["qty"] += int(row["qty"])
        order_bucket["row_count"] += 1
        order_bucket["line_nos"].append(row["line_no"])
        order_bucket["return_ids"].append(row["return_id"])

        linear_cents += int(row["amount_cents"])
        linear_qty += int(row["qty"])

    group_cents = sum(b["amount_cents"] for b in sites.values())
    group_qty = sum(b["qty"] for b in sites.values())
    group_rows = sum(b["row_count"] for b in sites.values())

    decimal_cents = 0
    decimal_error = ""
    try:
        total = sum((Decimal(row["amount_raw"]) for row in rows), Decimal("0"))
        decimal_cents = int(total * 100)
    except Exception as exc:  # pragma: no cover - 防御性
        decimal_error = str(exc)

    checks = [
        {"name": "组内退款和 == 明细线性累加",
         "ok": group_cents == linear_cents,
         "detail": f"分组 {group_cents} 分 vs 明细 {linear_cents} 分"},
        {"name": "组内行数之和 == 明细行数",
         "ok": group_rows == len(rows),
         "detail": f"分组 {group_rows} 行 vs 明细 {len(rows)} 行"},
        {"name": "组内件数之和 == 明细件数累加",
         "ok": group_qty == linear_qty,
         "detail": f"分组 {group_qty} 件 vs 明细 {linear_qty} 件"},
        {"name": "按订单汇总 == 明细线性累加",
         "ok": (sum(b["amount_cents"] for b in by_order.values()) == linear_cents
                and sum(b["qty"] for b in by_order.values()) == linear_qty),
         "detail": (f"订单口径 {sum(b['qty'] for b in by_order.values())} 件 / "
                    f"{sum(b['amount_cents'] for b in by_order.values())} 分")},
        {"name": "Decimal 原始字符串复核一致",
         "ok": (not decimal_error) and decimal_cents == linear_cents,
         "detail": (f"Decimal 复核 {decimal_cents} 分 vs 明细 {linear_cents} 分"
                    if not decimal_error else f"Decimal 复核异常：{decimal_error}")},
    ]

    ordered = sorted(sites.values(), key=lambda b: (-b["amount_cents"], b["site"]))
    for bucket in ordered:
        bucket["amount"] = money(bucket["amount_cents"])

    return {
        "groups": ordered,
        "by_order": [by_order[k] for k in sorted(by_order)],
        "total_amount_cents": linear_cents,
        "total_amount": money(linear_cents),
        "total_qty": linear_qty,
        "row_count": len(rows),
        "checks": checks,
        "consistent": all(c["ok"] for c in checks),
    }
