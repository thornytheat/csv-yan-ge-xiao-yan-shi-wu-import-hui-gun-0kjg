"""金额格式化（纯函数，无 DB / 业务依赖，供 store 与 returns 共用）。"""
from __future__ import annotations


def money(cents: int) -> str:
    """整数分 → 两位小数字符串；负数保留符号（用于台账展示）。"""
    sign = "-" if cents < 0 else ""
    cents = abs(int(cents))
    return f"{sign}{cents // 100}.{cents % 100:02d}"
