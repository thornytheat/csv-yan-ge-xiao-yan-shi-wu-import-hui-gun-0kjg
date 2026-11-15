"""退货入账：跨批次累计上限、站点独立提交、整份回滚、旧账不变、幂等重试、并发。"""
from __future__ import annotations

import threading

import pytest

from app import aggregate, pipeline, return_importer, return_pipeline, returns, store

RETURN_HEADER = ["return_id", "order_id", "site", "category", "return_date", "amount", "qty"]


@pytest.fixture()
def conn(tmp_path):
    connection = store.connect(tmp_path / "returns.sqlite3")
    store.init_schema(connection)
    yield connection
    connection.close()


def seed_orders(conn) -> None:
    """入账一份历史订单：订单 1001..1005，件数 5、金额 10.00。"""
    data = [
        ["beijing", "grocery", str(1001 + i), "2026-11-05", "10.00", "5"]
        for i in range(5)
    ]
    result = pipeline.run(
        ("site,category,order_id,order_date,amount,qty\n"
         + "".join(",".join(r) + "\n" for r in data)).encode(),
        "orders.csv", conn=conn, batch_id="B-ORDERS",
    )
    assert result["status"] == "committed", result
    assert result["written"] == 5


def csv_bytes(rows: list[tuple]) -> bytes:
    text = ",".join(RETURN_HEADER) + "\n"
    text += "".join(",".join(str(c) for c in row) + "\n" for row in rows)
    return text.encode()


def run(conn, rows, filename="r.csv", **kw):
    return return_pipeline.run_returns(csv_bytes(rows), filename, conn=conn, **kw)


def test_return_commits_and_updates_ledger_and_site_summary(conn):
    seed_orders(conn)
    out = run(conn, [
        (1, 1001, "beijing", "grocery", "2026-11-10", "4.00", 2),
        (2, 1002, "beijing", "grocery", "2026-11-10", "3.00", 1),
    ])
    assert out["status"] == "committed", out
    assert out["written"] == 2
    assert store.count_return_rows(conn) == 2

    batch = store.get_return_batch(conn, out["return_batch_id"])
    assert batch["declared_site"] == "beijing"
    assert batch["total_qty"] == 3
    assert batch["total_amount"] == "7.00"
    assert [g["site"] for g in batch["aggregates"]] == ["beijing"]

    ledger = store.return_order_ledger(conn, 1001)
    assert ledger["qty_returned"] == 2
    assert ledger["qty_remaining"] == 3
    assert ledger["amount_returned"] == "4.00"
    assert ledger["amount_remaining"] == "6.00"

    sight = {s["site"]: s for s in store.return_site_summary(conn)}
    assert sight["beijing"]["qty"] == 3
    assert sight["beijing"]["amount"] == "7.00"


def test_cumulative_qty_limit_across_batches(conn):
    seed_orders(conn)
    first = run(conn, [(1, 1001, "beijing", "grocery", "2026-11-10", "4.00", 4)],
                "first.csv")
    assert first["status"] == "committed"

    second = run(conn, [(2, 1001, "beijing", "grocery", "2026-11-11", "2.00", 2)],
                 "second.csv")
    assert second["status"] == "conflict"
    assert second["written"] == 0
    assert [c["order_id"] for c in second["conflicts"]] == [1001]
    conflict = second["conflicts"][0]
    assert "qty_exceeded" in conflict["code"]
    assert conflict["exceeded_qty"] == 1
    assert conflict["already_qty"] == 4
    # 旧账未被改写
    assert store.return_order_ledger(conn, 1001)["qty_returned"] == 4
    assert store.count_return_rows(conn) == 1


def test_cumulative_amount_limit_even_when_qty_fits(conn):
    seed_orders(conn)
    first = run(conn, [(1, 1003, "beijing", "grocery", "2026-11-10", "8.00", 1)])
    assert first["status"] == "committed"

    second = run(conn, [(2, 1003, "beijing", "grocery", "2026-11-11", "2.01", 1)],
                 "amount.csv")
    assert second["status"] == "conflict"
    conflict = second["conflicts"][0]
    assert conflict["code"] == "amount_exceeded"
    assert conflict["exceeded_amount"] == "0.01"
    assert conflict["already_amount"] == "8.00"
    assert store.return_order_ledger(conn, 1003)["amount_returned_cents"] == 800


def test_same_order_multiple_rows_are_aggregated_before_check(conn):
    seed_orders(conn)
    # 单行各 3 件都在上限内，但同一订单合计 6 件超过 5 件 -> 整份回滚
    out = run(conn, [
        (1, 1004, "beijing", "grocery", "2026-11-10", "3.00", 3),
        (2, 1004, "beijing", "grocery", "2026-11-10", "3.00", 3),
    ])
    assert out["status"] == "conflict"
    conflict = out["conflicts"][0]
    assert conflict["requested_qty"] == 6
    assert conflict["line_nos"] == [2, 3]
    assert store.count_return_rows(conn) == 0
    assert store.return_order_ledger(conn, 1004) is None


def test_conflict_file_rolls_back_entire_file_not_just_offending_rows(conn):
    seed_orders(conn)
    out = run(conn, [
        (1, 1002, "beijing", "grocery", "2026-11-10", "1.00", 1),   # 合法
        (2, 1005, "beijing", "grocery", "2026-11-10", "99.00", 9),  # 金额/件数超界
    ])
    assert out["status"] == "conflict"
    assert out["written"] == 0
    assert store.count_return_rows(conn) == 0
    assert store.return_order_ledger(conn, 1002) is None          # 合法行也未入账
    assert store.return_site_summary(conn) == []
    assert store.list_return_batches(conn) == []


def test_order_not_found_is_conflict(conn):
    seed_orders(conn)
    out = run(conn, [(1, 999999, "beijing", "grocery", "2026-11-10", "1.00", 1)])
    assert out["status"] == "conflict"
    assert out["conflicts"][0]["code"] == "order_not_found"
    assert store.count_return_rows(conn) == 0


def test_multi_site_file_is_rejected(conn):
    seed_orders(conn)
    out = run(conn, [
        (1, 1001, "beijing", "grocery", "2026-11-10", "1.00", 1),
        (2, 1002, "shanghai", "grocery", "2026-11-10", "1.00", 1),
    ])
    assert out["status"] == "rejected"
    assert out["stage"] == "return_site"
    assert store.count_return_rows(conn) == 0
    assert store.list_return_attempts(conn)[0]["status"] == "rejected"


def test_declared_site_mismatch_is_rejected(conn):
    seed_orders(conn)
    out = run(conn, [(1, 1001, "beijing", "grocery", "2026-11-10", "1.00", 1)],
              declared_site="shanghai")
    assert out["status"] == "rejected"
    assert "不一致" in out["detail"]
    assert store.count_return_rows(conn) == 0


def test_sites_submit_independently_and_accumulate_per_order(conn):
    seed_orders(conn)
    bj = run(conn, [(1, 1001, "beijing", "grocery", "2026-11-10", "2.00", 1)],
             "bj.csv", declared_site="beijing")
    sh = run(conn, [(2, 1001, "shanghai", "grocery", "2026-11-10", "2.00", 1)],
             "sh.csv", declared_site="shanghai")
    assert bj["status"] == sh["status"] == "committed"
    # 同一订单的两笔退货来自不同站点文件，累计仍统一受控
    ledger = store.return_order_ledger(conn, 1001)
    assert ledger["qty_returned"] == 2
    sight = {s["site"]: s for s in store.return_site_summary(conn)}
    assert sight["beijing"]["qty"] == 1 and sight["shanghai"]["qty"] == 1


def test_failure_midway_rolls_back_details_and_summaries(conn):
    seed_orders(conn)
    # 先入账一笔，形成非空旧账
    first = run(conn, [(1, 1005, "beijing", "grocery", "2026-11-10", "1.00", 1)],
                "first.csv")
    assert first["status"] == "committed"
    before_rows = store.count_return_rows(conn)
    before_sites = store.return_site_summary(conn)
    before_ledger = store.return_ledger(conn)

    out = run(conn, [
        (2, 1001, "beijing", "grocery", "2026-11-10", "1.00", 1),
        (3, 1002, "beijing", "grocery", "2026-11-10", "1.00", 1),
    ], "second.csv", fail_after=1)

    assert out["status"] == "rolled_back"
    assert "注入故障" in out["error"]
    assert store.count_return_rows(conn) == before_rows
    assert store.return_site_summary(conn) == before_sites
    assert store.return_ledger(conn) == before_ledger
    assert len(store.list_return_batches(conn)) == 1
    attempt = store.list_return_attempts(conn)[0]
    assert attempt["status"] == "rolled_back"
    # 回滚记录保留上传前的旧汇总快照，可证明旧值未被改写；
    # 订单快照只覆盖本文件涉及的订单（1001/1002），其余台账由上面的完整比对保证。
    assert attempt["previous_summary"]["sites"] == before_sites
    assert [o["order_id"] for o in attempt["previous_summary"]["orders"]] == [1001, 1002]
    assert all(o["qty_returned"] == 0 for o in attempt["previous_summary"]["orders"])


def test_retry_same_file_is_idempotent(conn):
    seed_orders(conn)
    rows = [(1, 1001, "beijing", "grocery", "2026-11-10", "2.00", 1)]
    first = run(conn, rows, "same.csv")
    assert first["status"] == "committed"
    count = store.count_return_rows(conn)
    ledger = store.return_order_ledger(conn, 1001)

    retry = run(conn, rows, "same.csv")
    assert retry["status"] == "duplicate_file"
    assert retry["written"] == 0
    assert retry["return_batch_id"] == first["return_batch_id"]
    assert store.count_return_rows(conn) == count
    assert store.return_order_ledger(conn, 1001) == ledger


def test_concurrent_submissions_serialized_by_write_lock(conn, tmp_path):
    """两个连接同时为同一订单各提交 4 件（上限 5），只能成功一个。"""
    seed_orders(conn)
    db = tmp_path / "returns.sqlite3"
    payloads = {
        "a": csv_bytes([(1, 1001, "beijing", "grocery", "2026-11-10", "4.00", 4)]),
        "b": csv_bytes([(2, 1001, "beijing", "grocery", "2026-11-10", "4.00", 4)]),
    }
    results: dict[str, dict] = {}

    def worker(name: str) -> None:
        connection = store.connect(db)
        try:
            results[name] = return_pipeline.run_returns(payloads[name], f"{name}.csv",
                                                        conn=connection)
        finally:
            connection.close()

    threads = [threading.Thread(target=worker, args=(name,)) for name in ("a", "b")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    statuses = sorted(r["status"] for r in results.values())
    assert statuses == ["committed", "conflict"], results
    ledger = store.return_order_ledger(conn, 1001)
    assert ledger["qty_returned"] == 4                    # 只入账一份
    assert store.count_return_rows(conn) == 1


def test_concurrent_identical_file_double_write_guard(conn, tmp_path):
    """两个连接同时提交同一份文件：后到者命中幂等或唯一索引，只入账一次。"""
    seed_orders(conn)
    db = tmp_path / "returns.sqlite3"
    payload = csv_bytes([(1, 1001, "beijing", "grocery", "2026-11-10", "2.00", 1)])
    results: dict[str, dict] = {}

    def worker(name: str) -> None:
        connection = store.connect(db)
        try:
            results[name] = return_pipeline.run_returns(payload, "same.csv",
                                                        conn=connection)
        finally:
            connection.close()

    threads = [threading.Thread(target=worker, args=(name,)) for name in ("a", "b")]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert sorted(r["status"] for r in results.values()) == ["committed", "duplicate_file"]
    assert store.count_return_rows(conn) == 1
    assert store.return_order_ledger(conn, 1001)["qty_returned"] == 1


def test_many_concurrent_files_never_exceed_order_limit(conn, tmp_path):
    """20 个并发文件各退 1 件（上限 5 件）：恰好 5 份入账，台账/汇总/明细三者一致。"""
    seed_orders(conn)
    db = tmp_path / "returns.sqlite3"
    statuses: list[str] = []
    lock = threading.Lock()

    def worker(i: int) -> None:
        connection = store.connect(db)
        payload = csv_bytes([(2000 + i, 1002, "beijing", "grocery",
                              "2026-11-10", "1.00", 1)])
        try:
            outcome = return_pipeline.run_returns(payload, f"r{i}.csv", conn=connection)
        finally:
            connection.close()
        with lock:
            statuses.append(outcome["status"])

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert statuses.count("committed") == 5, statuses
    assert statuses.count("conflict") == 15, statuses

    ledger = store.return_order_ledger(conn, 1002)
    assert ledger["qty_returned"] == 5
    assert ledger["qty_remaining"] == 0
    assert store.count_return_rows(conn) == 5
    site = {s["site"]: s for s in store.return_site_summary(conn)}["beijing"]
    assert site["qty"] == 5 and site["row_count"] == 5
    assert len(store.list_return_batches(conn)) == 5


def test_row_validation_errors_reject_whole_file(conn):
    seed_orders(conn)
    out = run(conn, [
        (1, 1001, "beijing", "grocery", "2026-11-10", "1.00", 1),
        (1, 1002, "beijing", "grocery", "2026-11-10", "1.00", 1),   # return_id 重复
        (3, 1003, "beijing", "furniture", "2026-11-10", "1.00", 1),  # 枚举越界
    ])
    assert out["status"] == "rejected"
    assert out["stage"] == "rows"
    codes = sorted(out["row_validation"]["codes"])
    assert codes == ["duplicate", "enum"]
    assert store.count_return_rows(conn) == 0


def test_return_aggregate_inconsistency_blocks_write(conn, monkeypatch):
    seed_orders(conn)
    original = returns.summarize

    def broken(rows):
        data = original(rows)
        data["total_amount_cents"] += 1
        data["consistent"] = False
        return data

    monkeypatch.setattr(returns, "summarize", broken)
    out = run(conn, [(1, 1001, "beijing", "grocery", "2026-11-10", "1.00", 1)])
    assert out["status"] == "rolled_back"
    assert store.count_return_rows(conn) == 0


def test_force_new_batch_order_reingest_does_not_relax_frozen_limit(conn):
    """订单被 force_new_batch 重灌后，已发生退货的上限仍按首次退货时冻结值。"""
    seed_orders(conn)
    first = run(conn, [(1, 1001, "beijing", "grocery", "2026-11-10", "5.00", 5)])
    assert first["status"] == "committed"
    assert store.return_order_ledger(conn, 1001)["qty_remaining"] == 0

    # 重灌一份「件数更大」的同 order_id 订单
    data = "site,category,order_id,order_date,amount,qty\n"
    data += "beijing,grocery,1001,2026-11-05,20.00,9\n"
    forced = pipeline.run(data.encode(), "orders_v2.csv", conn=conn, force_new_batch=True)
    assert forced["status"] == "committed"

    # 上限仍冻结在 5 件 / 10.00
    out = run(conn, [(2, 1001, "beijing", "grocery", "2026-11-11", "1.00", 1)])
    assert out["status"] == "conflict"
    assert out["conflicts"][0]["order_qty"] == 5
