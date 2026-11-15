"""退货 HTTP 端到端：页面上传、409 冲突清单、幂等重试、站点独立性、旧汇总不变。"""
from __future__ import annotations

from app import store


def upload_returns(client, path, name=None, **form):
    data = {"extra_policy": "reject", **form}
    return client.post(
        "/api/returns/import",
        files={"file": (name or path.name, path.read_bytes(), "text/csv")},
        data=data,
    )


def return_rows_http(client):
    return client.get("/api/returns").json()["stats"]["return_rows"]


def test_returns_page_renders(client):
    res = client.get("/returns")
    assert res.status_code == 200
    assert "退货入账" in res.text
    assert "站点累计退货汇总" in res.text
    assert "订单累计退货台账" in res.text


def test_first_beijing_file_commits(client, sample_dir):
    before = return_rows_http(client)
    res = upload_returns(client, sample_dir / "return_beijing_1.csv")
    assert res.status_code == 200
    body = res.json()
    assert body["status"] == "committed"
    assert body["written"] == 4
    assert body["return_batch_id"].startswith("R-")
    assert body["rows_before"] == before
    assert body["rows_after"] == before + 4
    assert body["aggregate"]["consistent"] is True
    assert body["aggregate"]["groups"][0]["site"] == "beijing"

    api = client.get("/api/returns").json()
    sight = {s["site"]: s for s in api["summary"]}
    assert sight["beijing"]["qty"] == 7              # 1+2+3+1
    assert sight["beijing"]["amount"] == "48.56"     # 12.00+10.11+12.19+14.26


def test_conflict_returns_409_with_order_list_and_old_summary_unchanged(client, sample_dir):
    assert upload_returns(client, sample_dir / "return_beijing_1.csv").status_code == 200
    before_rows = return_rows_http(client)
    before_summary = client.get("/api/returns").json()["summary"]
    before_ledger = client.get("/api/returns").json()["batches"]

    res = upload_returns(client, sample_dir / "return_beijing_2_conflict.csv")
    assert res.status_code == 409
    body = res.json()
    assert body["status"] == "conflict"
    assert body["written"] == 0
    assert [c["order_id"] for c in body["conflicts"]] == [700001]
    conflict = body["conflicts"][0]
    assert conflict["exceeded_qty"] == 1
    assert conflict["already_qty"] == 1
    assert conflict["order_qty"] == 1
    assert body["previous_summary"]["sites"] == before_summary
    assert body["rows_before"] == body["rows_after"] == before_rows

    # 旧汇总与旧批次未被改写
    after = client.get("/api/returns").json()
    assert after["summary"] == before_summary
    assert after["batches"] == before_ledger
    assert after["stats"]["return_rows"] == before_rows
    assert after["stats"]["conflict_attempts"] == 1


def test_retry_same_file_does_not_double_count(client, sample_dir):
    first = upload_returns(client, sample_dir / "return_beijing_1.csv").json()
    rows_after = return_rows_http(client)
    summary = client.get("/api/returns").json()["summary"]

    retry = upload_returns(client, sample_dir / "return_beijing_1.csv")
    assert retry.status_code == 200
    body = retry.json()
    assert body["status"] == "duplicate_file"
    assert body["written"] == 0
    assert body["return_batch_id"] == first["return_batch_id"]

    after = client.get("/api/returns").json()
    assert after["stats"]["return_rows"] == rows_after
    assert after["summary"] == summary

    replay = client.post("/api/returns/replay-check", json={"sha256": first["sha256"]})
    assert replay.json()["verdict"] == "duplicate_file"


def test_two_sites_submit_independently_but_share_order_limit(client, sample_dir):
    # beijing 与 shanghai 文件各自提交成功
    assert upload_returns(client, sample_dir / "return_beijing_1.csv").status_code == 200
    assert upload_returns(client, sample_dir / "return_shanghai_1.csv").status_code == 200

    summary = {s["site"]: s for s in client.get("/api/returns").json()["summary"]}
    assert summary["beijing"]["qty"] == 7
    assert summary["shanghai"]["qty"] == 5

    # shanghai 第二份文件：件数未超但金额越界 → 409，且 beijing 汇总不受影响
    res = upload_returns(client, sample_dir / "return_shanghai_2_amount_conflict.csv")
    assert res.status_code == 409
    body = res.json()
    assert [c["code"] for c in body["conflicts"]] == ["amount_exceeded"]
    after = {s["site"]: s for s in client.get("/api/returns").json()["summary"]}
    assert after["beijing"] == summary["beijing"]
    assert after["shanghai"] == summary["shanghai"]


def test_multi_site_file_is_rejected_422(client, sample_dir):
    res = upload_returns(client, sample_dir / "return_multi_site.csv")
    assert res.status_code == 422
    body = res.json()
    assert body["status"] == "rejected"
    assert body["stage"] == "return_site"
    assert return_rows_http(client) == 0


def test_declared_site_form_field(client, sample_dir):
    ok = upload_returns(client, sample_dir / "return_beijing_1.csv", declared_site="beijing")
    assert ok.json()["status"] == "committed"

    wrong = upload_returns(client, sample_dir / "return_shanghai_1.csv",
                           declared_site="beijing", name="sh_wrong.csv")
    assert wrong.status_code == 422
    assert "不一致" in wrong.json()["detail"]


def test_return_contract_check_and_order_ledger_api(client, sample_dir):
    bad = client.post("/api/returns/contract-check",
                      json={"header": "return_id,order_id,site,category,return_date,amount"})
    body = bad.json()
    assert body["ok"] is False
    assert body["missing"] == ["qty"]

    good = client.post("/api/returns/contract-check",
                       json={"header": "qty,return_id,order_id,site,category,return_date,amount"})
    assert good.json()["ok"] is True

    upload_returns(client, sample_dir / "return_beijing_1.csv")
    order = client.get("/api/returns/order/700007").json()
    assert order["ledger"]["qty_returned"] == 2
    assert order["ledger"]["qty_remaining"] == 2
    assert client.get("/api/returns/order/123456").status_code == 404


def test_returns_form_renders_conflict_page(client, sample_dir):
    upload_returns(client, sample_dir / "return_beijing_1.csv")
    res = client.post(
        "/returns",
        files={"file": ("return_beijing_2_conflict.csv",
                        (sample_dir / "return_beijing_2_conflict.csv").read_bytes(),
                        "text/csv")},
        data={"extra_policy": "reject"},
        follow_redirects=False,
    )
    assert res.status_code == 409
    assert "冲突订单" in res.text
    assert "700001" in res.text
    assert "回滚前旧账快照" in res.text


def test_return_row_error_page_renders(client):
    text = ("return_id,order_id,site,category,return_date,amount,qty\n"
            "1,700001,beijing,grocery,2026-11-10,12.00,1\n"
            "1,700002,beijing,grocery,2026-11-10,1.00,1\n")
    res = client.post("/returns",
                      files={"file": ("bad.csv", text.encode(), "text/csv")},
                      data={"extra_policy": "reject"},
                      follow_redirects=False)
    assert res.status_code == 422
    assert "主键重复" in res.text
    assert return_rows_http(client) == 0


def test_health_unaffected_by_returns(client, sample_dir):
    before = client.get("/health").json()["fact_rows"]
    upload_returns(client, sample_dir / "return_beijing_1.csv")
    assert client.get("/health").json()["fact_rows"] == before      # 订单明细不变
