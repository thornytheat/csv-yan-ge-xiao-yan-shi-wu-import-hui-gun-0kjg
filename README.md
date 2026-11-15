# csvgate · CSV 严格校验与事务导入回滚

上游每天给一份站点订单 CSV，要求「**全对才入库，错了要知道第几行错哪列**」。
本工具把这份人工核对变成 6 个可复核的后端模块，任一行非法则整份回滚，库内明细行数不变。

使用者：数据平台组入库值班同学。

## 能力

| 模块 | 落地位置 | 做什么 |
|---|---|---|
| 1 上传与编码探测 | `app/encoding.py` | BOM → UTF-8-SIG；严格 UTF-8 失败 → GB18030；都失败 → 400 + 建议。响应记录是否 BOM、前 16 字节摘要、逐次尝试 |
| 2 表头契约 | `app/contract.py` | 必需列缺失=错误；额外列 `reject`/`warn`；列顺序无关；表头去首尾空白；重复列名=错误 |
| 3 行级校验 | `app/rows.py` | 主键严格递增（重复 `duplicate` / 倒序 `out_of_order`，逐行与上一行比较）、int/date/decimal/enum、必填、长度上限、日期真实性 |
| 4 分组聚合 | `app/aggregate.py` | 按 `site` 求和，并用三条独立口径自证一致 |
| 5 事务导入 | `app/importer.py` | 单事务写 批次+明细+聚合；异常即 ROLLBACK；sha256 重复 → `duplicate_file`，可 `force_new_batch` |
| 6 错误报告 | `app/report.py` | 按错误类型汇总 + 逐行依据（行号/列名/原值/原因）+ 修正建议 |

编排在 `app/pipeline.py`，路由 `app/main.py` 只做参数解析与渲染。

## 退货入账（订单到货后的第二段流程）

退货 CSV 是**另一份文件、另一套契约**（`app/returns.py`）：主键是 `return_id`，
每行用 `order_id` 指向已入账的历史订单。入口：`/returns` 页面、`POST /api/returns/import`。

| 模块 | 落地位置 | 做什么 |
|---|---|---|
| 退货契约 / 行校验 / 聚合 | `app/returns.py` | 7 个必需列 + 可选 `memo`；退货单号严格递增；日期/金额/枚举规则与订单一致；按站点 + 按订单双口径汇总并自证 |
| 退货事务入账 | `app/return_importer.py` | 单事务内核对累计上限、写明细/站点汇总/订单台账；冲突整份回滚；sha256 幂等 |
| 退货编排 | `app/return_pipeline.py` | 编码探测 → 契约 → 行校验 → 聚合 → 事务入账 |
| 退货页面 | `templates/returns.html` | 上传、站点累计汇总、订单累计台账、批次与上传记录、冲突清单与回滚前快照 |

核心规则：

1. **各站点独立提交**：一份退货文件只允许一个站点，混站文件直接拒绝（`422`，
   `stage=return_site`）；也可填 `declared_site` 做预校验，与文件内容不一致即拒绝。
   不同站点的文件互不影响。
2. **同一订单累计受控**：每次入账都在 `BEGIN IMMEDIATE` 写事务里按 `order_id` 合并本文件各行，
   再核对「历史已退 + 本文件」是否超过原始订单的件数与金额。上限在**首次退货时冻结**到
   `return_order_totals`，之后即使订单被 `force_new_batch` 重灌也不放宽。
3. **并发安全**：SQLite 写锁把并发提交串行化，第二个事务看到的是第一次提交后的累计值；
   同 sha256 并发提交由事务内二次判重 + `return_batches.sha256` 唯一索引兜底，只入账一次。
4. **越界整份回滚**：任一订单越界 → 整份文件 0 行写入（合法行也不入账），返回 `409` 与冲突订单清单
   （原件数/原金额、历史已退、本文件请求、超出量、文件行号），并保留上传前的站点汇总与订单台账快照，
   可证明旧汇总和旧账未被改写。
5. **重试幂等**：同一份文件（sha256 相同）重复提交 → `duplicate_file`，写入 0 行，累计值不变。
6. **中途失败**：批次、明细、站点汇总、订单台账在同一事务；任一步异常 → `ROLLBACK`，
   退货明细行数、站点汇总、订单台账全部保持旧值。

退货样例：`return_beijing_1.csv`（成功）、`return_beijing_2_conflict.csv`（件数+金额越界）、
`return_shanghai_1.csv`（另一站点独立提交）、`return_shanghai_2_amount_conflict.csv`（仅金额越界）、
`return_multi_site.csv`（混站拒绝）。均引用 seed 历史订单 700001~700060。

## 核心规则

1. **编码探测**（证据可查）
   - 前 3 字节是 `EF BB BF` → 按 `utf-8-sig` 解码，BOM 剥离；
   - 否则严格 UTF-8；失败再试 `gb18030`（GBK 超集）；
   - 都失败 → HTTP 400，附三次尝试的失败字节偏移与修复建议。
   - 判定是**确定性**的：纯 ASCII 文件一律算 `utf-8`。
2. **行级错误只报第一处**：同一行同一列按「必填 → 长度 → 类型/枚举 → 业务」取第一个失败原因，
   避免一个超长 site 值同时被报成长度错和枚举错、干扰计数。
3. **主键严格递增**：与「上一条主键可解析的行」比较；相等 → `duplicate`，更小 → `out_of_order`，更大 → 通过并更新基线。
4. **日期真实性**：先匹配 `YYYY-MM-DD`，再用 `date.fromisoformat` 判真实存在（`2026-02-30` → `date_invalid`）。
5. **聚合自证**：分组累加 cents、明细单趟线性累加、`Decimal` 原始字符串复核、组内行数和=明细行数，四条必须同时成立。
6. **事务与重复文件**
   - 全对：`BEGIN IMMEDIATE` 写批次 → 明细 → 聚合 → `COMMIT`；中途异常 `ROLLBACK`，明细行数保持写入前值。
   - 同 `sha256` 已入库且未勾选 `force_new_batch` → 返回 `duplicate_file`，写入 0 行，返回既有批号。
   - 勾选 `force_new_batch` → 新建批次（`-F2`/`-F3` 后缀），旧批次保留且 `superseded_by` 指向新批次，
     语义是「同一份文件被有意重灌，重灌痕迹可追溯」，不是覆盖删除。

## 启动

```bash
cd batch-20261114b/projects/gb-623
cp .env.example .env          # 可选，默认端口 18212
docker compose up -d --build
curl -s http://127.0.0.1:18212/health
```

- 页面：<http://127.0.0.1:18212/>
- 数据卷：`csvgate-data:/app/data`（SQLite `csvgate.sqlite3`）
- 启动即幂等 seed：`meta.seed_version = seed-2026-11-07.1`，批次 `B-20261107-001`（60 行历史周批次）；
  重复启动不会重复写入。**重建数据库**：`docker compose down -v && docker compose up -d --build`。

## 演示步骤

1. `/workbench` 下载 `normal_500.csv` 并上传 → 结论「已入库」，得批号、站点聚合表、明细前 20 行。
2. 上传 `broken.csv` → HTTP 422，结论「整体回滚」，列出 5 条逐行错误（重复主键 / 倒序 / 非法日期 / 超长 / 枚举越界）。
   刷新首页或 `/health` 看 `fact_rows` 未变。
3. 上传 `gbk_120.csv` → 编码探测显示 `gb18030`，120 行入库。
4. 再上传一次 `normal_500.csv` → `duplicate_file`，写入 0 行；到 `/review` 点「验证重复导入」看只读判定。
5. 勾选 `force_new_batch` 再传 `normal_500.csv` → 新批次，旧批次标 superseded。

退货演示：

1. `/returns` 上传 `return_beijing_1.csv` → 入账 4 行，站点汇总与订单台账同步更新。
2. 再传 `return_beijing_2_conflict.csv` → 409，列出订单 700001 冲突（已退满后再退）；
   页面同时展示回滚前旧汇总快照，证明旧账未改写。
3. 传 `return_shanghai_1.csv` → 另一站点独立入账；再传 `return_shanghai_2_amount_conflict.csv`
   → 件数未超但退款金额越界，409 回滚。
4. 重复上传 `return_beijing_1.csv` → `duplicate_file`，累计值不变。
5. 传 `return_multi_site.csv` → 422，混站文件须拆分后按站点单独提交。

命令行等价（验收脚本用的就是这些）：

```bash
curl -s http://127.0.0.1:18212/health
curl -s -F "file=@sample_data/normal_500.csv" -F "extra_policy=reject" http://127.0.0.1:18212/api/import
curl -s -o /tmp/broken.json -w '%{http_code}\n' -F "file=@sample_data/broken.csv" http://127.0.0.1:18212/api/import
python3 -c "import json;d=json.load(open('/tmp/broken.json'));print(d['report']['total'],[e['code'] for e in d['report']['rows']])"
curl -s -F "file=@sample_data/return_beijing_1.csv" http://127.0.0.1:18212/api/returns/import
curl -s -o /tmp/rc.json -w '%{http_code}\n' -F "file=@sample_data/return_beijing_2_conflict.csv" http://127.0.0.1:18212/api/returns/import
python3 -c "import json;d=json.load(open('/tmp/rc.json'));print(d['status'],[c['order_id'] for c in d['conflicts']])"
curl -s -F "file=@sample_data/return_beijing_1.csv" http://127.0.0.1:18212/api/returns/import
```

## API / 页面

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/health` | 健康检查 + 当前明细行数 + seed 版本 |
| POST | `/api/import` | multipart：`file`、`extra_policy`、`force_new_batch`；200 已入库/重复文件、400 编码失败、422 校验拒绝 |
| POST | `/workbench` | 页面表单同管道；通过 → 303 跳结果页，拒绝 → 422 直接渲染错误表 |
| POST | `/api/contract-check` | 只检查表头（含 `warn` 策略演练） |
| POST | `/api/replay-check` | 按 sha256 判定「再次上传会不会重复」 |
| GET | `/api/batches` | 批次列表 + 统计 |
| GET | `/api/samples/{name}` | 样例下载 |
| POST | `/api/returns/import` | multipart：`file`、`extra_policy`、`declared_site`；200 已入账/重复文件、422 校验拒绝、409 累计越界回滚 |
| POST | `/returns` | 退货页面表单；409 时渲染冲突订单清单与回滚前快照 |
| POST | `/api/returns/contract-check` | 退货表头自检（独立入口） |
| POST | `/api/returns/replay-check` | 按 sha256 判定退货文件重试结论 |
| GET | `/api/returns` | 站点累计汇总 + 订单台账 + 退货批次/上传记录 |
| GET | `/api/returns/order/{order_id}` | 单个订单的原始订单与累计退货台账 |
| 页面 | `/`、`/workbench`、`/returns`、`/result/{batch_id}`、`/result/attempt/{id}`、`/review` | 首页/工作台/退货入账/结果/错误明细/历史复核 |

## 目录

```text
app/            main.py settings.py pipeline.py seed.py
                encoding.py contract.py rows.py aggregate.py importer.py report.py store.py
                returns.py return_pipeline.py return_importer.py
                templates/ static/
sample_data/    normal_500.csv gbk_120.csv seed_20261107.csv broken.csv contract.json
                return_beijing_1.csv return_beijing_2_conflict.csv
                return_shanghai_1.csv return_shanghai_2_amount_conflict.csv return_multi_site.csv
                make_samples.py
tests/          test_encoding.py test_rows.py test_importer.py test_api.py
                test_returns.py test_returns_api.py conftest.py
Dockerfile docker-compose.yml requirements.txt .env.example
```

## 已知限制

- CSV 解析用标准库 `csv.reader`：支持引号包裹字段，但**行号**按「数据行 + 1」换算，
  字段内含换行时行号会偏移（当前样例与常见导出不含换行字段）。
- 编码只覆盖 UTF-8 / UTF-8-BOM / GB18030；UTF-16/UTF-32 会走到 400 并给出提示，不做自动识别。
- 分隔符固定逗号，不做 `;`/`\t` 自动猜测；SJIS、Big5 等会按 GB18030 误判成功的概率存在（GBK 超集覆盖广），
  需要在契约里约定来源编码。
- 单次上传上限 8MB（约 10 万行量级）；不做流式分片。
- 明细与批次不提供删除接口；重灌靠 `force_new_batch` + `superseded_by` 追溯。
- 退货累计上限按「首次退货时的原始订单」冻结；若上游事后修正订单件数/金额，需另行冲正台账
  （当前不提供台账修订接口）。
- 退货不提供撤销/冲正接口：已入账退货只能通过新文件退回剩余额度，`return_batches` 只增不改。
