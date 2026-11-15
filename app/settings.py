"""设置与路径。"""
from __future__ import annotations

import os
from pathlib import Path

APP_NAME = "csvgate · CSV 严格校验与事务导入"
APP_SHORT = "csvgate"
BASE_DIR = Path(__file__).resolve().parent.parent
SAMPLE_DIR = BASE_DIR / "sample_data"
DATA_DIR = Path(os.environ.get("CSVGATE_DATA_DIR", str(BASE_DIR / "data")))
DB_PATH = DATA_DIR / "csvgate.sqlite3"

SEED_VERSION = "seed-2026-11-07.1"
SEED_BATCH_ID = "B-20261107-001"
SEED_FILENAME = "seed_20261107.csv"

SAMPLE_FILES = [
    {"name": "normal_500.csv", "title": "正常 500 行（UTF-8）",
     "desc": "主键严格递增、6 个站点，应整体入库成功"},
    {"name": "gbk_120.csv", "title": "GBK 120 行",
     "desc": "GB18030 编码 + 中文备注列，验证编码探测与解码"},
    {"name": "broken.csv", "title": "5 类错误样例",
     "desc": "重复主键/倒序/非法日期/超长/枚举越界各 1 条，应整体回滚"},
    {"name": "return_beijing_1.csv", "title": "退货 · beijing 首次提交",
     "desc": "引用 seed 历史订单，件数/金额均在上限内，应整份入账"},
    {"name": "return_beijing_2_conflict.csv", "title": "退货 · beijing 越界冲突",
     "desc": "订单 700001 已退满后再退，整份回滚并列出冲突订单"},
    {"name": "return_shanghai_1.csv", "title": "退货 · shanghai 独立提交",
     "desc": "与 beijing 文件互不影响，验证站点隔离与跨批次累计"},
    {"name": "return_multi_site.csv", "title": "退货 · 混站文件",
     "desc": "一份文件含多个站点，应整份拒绝（各站点须独立提交）"},
    {"name": "return_shanghai_2_amount_conflict.csv", "title": "退货 · shanghai 金额越界",
     "desc": "件数未超但累计退款金额越界，整份回滚并列出冲突订单"},
    {"name": "contract.json", "title": "列契约 contract.json",
     "desc": "列定义、类型、枚举与主键规则"},
]

RETURN_SAMPLE_FILES = [s for s in SAMPLE_FILES if s["name"].startswith("return_")]

EXTRA_POLICIES = [
    {"value": "reject", "label": "reject（默认）：出现契约外列即拒绝整份文件"},
    {"value": "warn", "label": "warn：契约外列忽略并告警，其余照常校验"},
]
