from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
import threading
from unittest.mock import MagicMock, call, patch

import pandas as pd
import pytest
import requests

from zer0share.finance.catalog import FINANCE_SPECS
from zer0share.finance.fetcher import RateLimiter, fetch_financial
from zer0share.finance.storage import FinanceStore
from zer0share.api import LocalPro
from zer0share.storage import MetaStore, SnapshotStore
from zer0share.sync import SyncRuntime
from zer0share.sync.finance import FinanceSyncJob
from zer0share.trading_calendar import TradingCalendar


def row(table="income", date="20251001", code="000001.SZ", **values):
    return {"ts_code": code, "end_date": "20250630", FINANCE_SPECS[table].date_column: date, **values}


@pytest.fixture(autouse=True)
def no_finance_network_wait():
    with patch("zer0share.finance.fetcher.time.sleep") as sleep:
        yield sleep


@pytest.mark.parametrize("table", list(FINANCE_SPECS))
def test_download_requests_all_source_fields(table, no_finance_network_wait):
    pro = MagicMock()
    getattr(pro, table).return_value = pd.DataFrame([row(table, revenue=12.5)])
    df = fetch_financial(pro, table, "000001.SZ", "20250101", "20261231")
    params = {"start_date": "20250101", "end_date": "20261231"} if FINANCE_SPECS[table].supports_range else {}
    if table == "fina_mainbz":
        assert getattr(pro, table).call_args_list == [
            call(ts_code="000001.SZ", fields=",".join(FINANCE_SPECS[table].columns), **params, type=t)
            for t in ("P", "D", "I", "S")
        ]
        assert no_finance_network_wait.call_args_list == [call(1.5)] * 4
        return
    getattr(pro, table).assert_called_once_with(
        ts_code="000001.SZ", fields=",".join(FINANCE_SPECS[table].columns), **params
    )
    assert df.iloc[0]["revenue"] == 12.5
    no_finance_network_wait.assert_called_once_with(1.5)


def test_capped_response_is_split_and_every_request_throttled(monkeypatch, no_finance_network_wait):
    monkeypatch.setitem(FINANCE_SPECS, "income", replace(FINANCE_SPECS["income"], row_limit=2))
    pro = MagicMock()
    first, second = row(date="20250101"), row(date="20250102")
    pro.income.side_effect = [pd.DataFrame([first, second]), pd.DataFrame([first]), pd.DataFrame([second])]
    df = fetch_financial(pro, "income", "000001.SZ", "20250101", "20250102")
    assert list(df.ann_date) == ["20250101", "20250102"]
    assert [(c.kwargs["start_date"], c.kwargs["end_date"]) for c in pro.income.call_args_list] == [
        ("20250101", "20250102"), ("20250101", "20250101"), ("20250102", "20250102")
    ]
    assert no_finance_network_wait.call_args_list == [call(1.5)] * 3


def test_unsplittable_cap_is_not_accepted_as_complete(monkeypatch):
    monkeypatch.setitem(FINANCE_SPECS, "income", replace(FINANCE_SPECS["income"], row_limit=1))
    pro = MagicMock()
    pro.income.return_value = pd.DataFrame([row(date="20250101")])
    with pytest.raises(RuntimeError, match="cap"):
        fetch_financial(pro, "income", "000001.SZ", "20250101", "20250101")


def test_dividend_filters_locally_without_unsupported_date_parameters():
    pro = MagicMock()
    pro.dividend.return_value = pd.DataFrame([row("dividend", "20241231"), row("dividend", "20251231")])
    df = fetch_financial(pro, "dividend", "000001.SZ", "20250101", "20261231")
    assert list(df.end_date) == ["20251231"]
    assert "start_date" not in pro.dividend.call_args.kwargs


def test_permission_error_is_not_retried_but_request_is_throttled(no_finance_network_wait):
    pro = MagicMock()
    pro.income.side_effect = Exception("权限不足")
    with pytest.raises(Exception, match="权限不足"):
        fetch_financial(pro, "income", "000001.SZ", "20000101", "20261019")
    assert pro.income.call_count == 1
    no_finance_network_wait.assert_called_once_with(1.5)


def test_transient_network_error_retries():
    pro = MagicMock()
    pro.income.side_effect = [requests.ConnectionError("reset"), pd.DataFrame([row()])]
    assert len(fetch_financial(pro, "income", "000001.SZ", "20250101", "20261231")) == 1
    assert pro.income.call_count == 2


@pytest.mark.parametrize("bad", [None, pd.DataFrame([row(code="000002.SZ")]), pd.DataFrame([row(date=None)])])
def test_invalid_response_is_not_treated_as_empty(bad):
    pro = MagicMock()
    pro.income.return_value = bad
    with pytest.raises(ValueError):
        fetch_financial(pro, "income", "000001.SZ", "20250101", "20261231")


def test_refresh_replaces_changes_and_deletions_but_keeps_older_data(tmp_path):
    store = FinanceStore(tmp_path, FINANCE_SPECS["income"])
    old = pd.DataFrame([row(date="20240101", revenue=1), row(date="20250101", revenue=2), row(date="20260101", revenue=3)])
    store.replace_range("000001.SZ", "20000101", "20261231", old)
    new = pd.DataFrame([row(date="20250101", revenue=22, source_extension="kept")])
    store.replace_range("000001.SZ", "20250101", "20261231", new)
    df = store.read("000001.SZ")
    assert list(df.ann_date) == ["20240101", "20250101"]
    assert list(df.revenue) == [1, 22]
    assert df.iloc[1].source_extension == "kept"
    store.replace_range("000001.SZ", "20250101", "20261231", new)
    assert len(store.read("000001.SZ")) == 2
    store.replace_range("000001.SZ", "20250101", "20261231", pd.DataFrame())
    assert list(store.read("000001.SZ").ann_date) == ["20240101"]


def test_preserves_source_duplicates_and_report_types(tmp_path):
    store = FinanceStore(tmp_path, FINANCE_SPECS["income"])
    df = pd.DataFrame([row(report_type="1"), row(report_type="5"), row(report_type="5")])
    store.replace_range("000001.SZ", "20250101", "20261231", df)
    assert list(store.read("000001.SZ").report_type) == ["1", "5", "5"]


def test_empty_partition_has_typed_optional_fields(tmp_path):
    store = FinanceStore(tmp_path, FINANCE_SPECS["dividend"])
    store.replace_range("000001.SZ", "20000101", "20261231", pd.DataFrame())
    assert store.read("000001.SZ").empty
    assert "base_share" in store.read("000001.SZ")


def test_failed_atomic_write_preserves_previous_file(tmp_path):
    store = FinanceStore(tmp_path, FINANCE_SPECS["income"])
    store.replace_range("000001.SZ", "20250101", "20261231", pd.DataFrame([row(revenue=1)]))
    with patch("zer0share.finance.storage.pq.write_table", side_effect=OSError("disk full")):
        with pytest.raises(OSError):
            store.replace_range("000001.SZ", "20250101", "20261231", pd.DataFrame([row(revenue=2)]))
    assert store.read("000001.SZ").iloc[0].revenue == 1
    assert not list(store.table_dir.rglob("*.tmp"))


@pytest.fixture
def runtime(tmp_path):
    SnapshotStore(tmp_path / "stock" / "basic" / "data.parquet").write(
        pd.DataFrame({"ts_code": ["000001.SZ", "000002.SZ"], "list_status": ["L", "D"]})
    )
    with MetaStore(tmp_path / "meta.duckdb") as meta:
        yield SyncRuntime(TradingCalendar(meta, today_fn=lambda: "20261019"), MagicMock(), meta)


@pytest.mark.parametrize("table", list(FINANCE_SPECS))
def test_first_full_sync_then_only_dates_after_last_success(tmp_path, runtime, table):
    fetch = MagicMock(return_value=pd.DataFrame())
    job = FinanceSyncJob(FINANCE_SPECS[table], fetch, tmp_path)
    job.run(runtime)
    assert fetch.call_args_list == [call(table, code, "20000101", "20261019") for code in ("000001.SZ", "000002.SZ")]
    fetch.reset_mock()
    job.run(runtime)
    fetch.assert_not_called()
    assert runtime.meta.get_last_date(table) == "20261019"
    runtime.calendar._today_fn = lambda: "20261020"
    job.run(runtime)
    assert fetch.call_args_list == [call(table, code, "20261020", "20261020") for code in ("000001.SZ", "000002.SZ")]
    # Gaps and year boundaries do not reset the cursor to a calendar-year window.
    runtime.calendar._today_fn = lambda: "20270101"
    fetch.reset_mock()
    job.run(runtime)
    assert fetch.call_args_list == [call(table, code, "20261021", "20270101") for code in ("000001.SZ", "000002.SZ")]


def test_partial_first_sync_resumes_without_losing_uninitialized_stock_history(tmp_path, runtime):
    fetch = MagicMock(side_effect=[pd.DataFrame(), RuntimeError("permission")])
    job = FinanceSyncJob(FINANCE_SPECS["income"], fetch, tmp_path)
    with pytest.raises(RuntimeError):
        job.run(runtime)
    assert runtime.meta.get_last_date("income") is None
    assert runtime.meta.get_last_date("finance:income:000001.SZ") == "20261019"
    assert runtime.meta.get_last_date("finance:income:000002.SZ") is None
    fetch.side_effect = None
    fetch.return_value = pd.DataFrame()
    fetch.reset_mock()
    job.run(runtime)
    assert fetch.call_args_list == [
        call("income", "000002.SZ", "20000101", "20261019"),
    ]


def test_incremental_sync_preserves_existing_history(tmp_path, runtime):
    def fetch(table, code, start, end):
        return pd.DataFrame([row(code=code, date=end, revenue=1 if start == "20000101" else 2)])
    job = FinanceSyncJob(FINANCE_SPECS["income"], fetch, tmp_path)
    job.run(runtime)
    runtime.calendar._today_fn = lambda: "20261020"
    job.run(runtime)
    data = job._store.read("000001.SZ")
    assert list(data.ann_date) == ["20261019", "20261020"]
    assert list(data.revenue) == [1, 2]
    assert runtime.meta.get_last_date("finance:income:000001.SZ") == "20261020"


def test_new_stock_still_receives_full_history_after_other_stocks_are_current(tmp_path, runtime):
    fetch = MagicMock(return_value=pd.DataFrame())
    job = FinanceSyncJob(FINANCE_SPECS["income"], fetch, tmp_path)
    job.run(runtime)
    SnapshotStore(tmp_path / "stock/basic/data.parquet").write(
        pd.DataFrame({"ts_code": ["000001.SZ", "000002.SZ", "000003.SZ"]})
    )
    fetch.reset_mock()
    job.run(runtime)
    fetch.assert_called_once_with("income", "000003.SZ", "20000101", "20261019")


def test_incremental_uses_each_stock_cursor_instead_of_table_summary(tmp_path, runtime):
    fetch = MagicMock(return_value=pd.DataFrame())
    job = FinanceSyncJob(FINANCE_SPECS["income"], fetch, tmp_path)
    job.run(runtime)
    runtime.meta.update_last_date("finance:income:000002.SZ", "20261017")
    fetch.reset_mock()
    job.run(runtime)
    fetch.assert_called_once_with("income", "000002.SZ", "20261018", "20261019")


def test_manual_history_refresh_does_not_rewind_incremental_cursor(tmp_path, runtime):
    fetch = MagicMock(return_value=pd.DataFrame())
    job = FinanceSyncJob(FINANCE_SPECS["income"], fetch, tmp_path)
    job.run(runtime)
    fetch.reset_mock()
    job.run(runtime, start_date="20250101", end_date="20251231")
    assert fetch.call_args_list == [call("income", code, "20250101", "20251231") for code in ("000001.SZ", "000002.SZ")]
    assert runtime.meta.get_last_date("finance:income:000001.SZ") == "20261019"
    fetch.reset_mock()
    runtime.calendar._today_fn = lambda: "20261020"
    job.run(runtime)
    assert fetch.call_args_list == [call("income", code, "20261020", "20261020") for code in ("000001.SZ", "000002.SZ")]


def test_narrow_manual_sync_does_not_mark_initial_history_complete(tmp_path, runtime):
    fetch = MagicMock(return_value=pd.DataFrame())
    job = FinanceSyncJob(FINANCE_SPECS["income"], fetch, tmp_path)
    job.run(runtime, start_date="20200101", end_date="20201231")
    assert runtime.meta.get_last_date("finance:income:000001.SZ") is None
    fetch.reset_mock()
    job.run(runtime)
    assert fetch.call_args_list[0] == call("income", "000001.SZ", "20000101", "20261019")


def test_lost_partition_and_new_stock_receive_full_history(tmp_path, runtime):
    fetch = MagicMock(return_value=pd.DataFrame())
    job = FinanceSyncJob(FINANCE_SPECS["income"], fetch, tmp_path)
    job.run(runtime)
    job._store.path("000001.SZ").unlink()
    fetch.reset_mock()
    job.run(runtime)
    assert fetch.call_args_list[0] == call("income", "000001.SZ", "20000101", "20261019")


def test_failure_keeps_existing_data_and_progress(tmp_path, runtime):
    fetch = MagicMock(side_effect=lambda table, code, start, end: pd.DataFrame([row(code=code, revenue=1)]))
    job = FinanceSyncJob(FINANCE_SPECS["income"], fetch, tmp_path)
    job.run(runtime)
    fetch.side_effect = requests.ConnectionError("offline")
    runtime.calendar._today_fn = lambda: "20261020"
    with pytest.raises(requests.ConnectionError):
        job.run(runtime)
    assert job._store.read("000001.SZ").iloc[0].revenue == 1
    assert runtime.meta.get_last_date("income") == "20261019"


def test_finance_requires_basic_but_not_trade_calendar(tmp_path, runtime):
    (tmp_path / "stock" / "basic" / "data.parquet").unlink()
    with pytest.raises(RuntimeError, match="basic"):
        FinanceSyncJob(FINANCE_SPECS["income"], MagicMock(), tmp_path).run(runtime)


@pytest.mark.parametrize("table", list(FINANCE_SPECS))
def test_local_api_source_fields_code_period_and_empty_results(tmp_path, table):
    store = FinanceStore(tmp_path, FINANCE_SPECS[table])
    store.replace_range("000001.SZ", "20000101", "20261231", pd.DataFrame([row(table, "20251001", revenue=12.5)]))
    store.replace_range("000002.SZ", "20000101", "20261231", pd.DataFrame())
    pro = LocalPro(tmp_path)
    df = getattr(pro, table)(ts_code="000001.SZ", start_date="20250101", end_date="20261231")
    assert len(df) == 1
    assert df.iloc[0].revenue == 12.5
    assert pro.query(table, ts_code="000002.SZ").empty
    assert pro.query(table, ts_code="000003.SZ").empty
    assert len(pro.query(table, period=df.iloc[0].end_date, fields="ts_code,end_date")) == 1


def test_query_filters_dates_types_and_pagination(tmp_path):
    store = FinanceStore(tmp_path, FINANCE_SPECS["income"])
    store.replace_range("000001.SZ", "20000101", "20261231", pd.DataFrame([
        row(date="20250101", report_type="1"), row(date="20250102", report_type="5"),
    ]))
    pro = LocalPro(tmp_path)
    assert len(pro.income(ann_date="20250101", report_type="1")) == 1
    assert len(pro.income(limit=1, offset=1)) == 1
    with pytest.raises(ValueError, match="unknown fields"):
        pro.income(fields="not_a_source_field")
    with pytest.raises(ValueError):
        pro.income(start_date="20251301")
    with pytest.raises(ValueError):
        pro.income(start_date="20260101", end_date="20250101")
    with pytest.raises(ValueError):
        pro.income(limit=-1)
    with pytest.raises(ValueError):
        pro.income(ts_code="../../data")


def test_mainbz_downloads_and_preserves_all_source_classifications(tmp_path):
    pro = MagicMock()
    pro.fina_mainbz.side_effect = lambda **kw: pd.DataFrame([
        row("fina_mainbz", "20251231", bz_code=kw["type"], bz_item="同名项目", bz_sales=10)
    ])
    df = fetch_financial(pro, "fina_mainbz", "000001.SZ", "20250101", "20261019")
    store = FinanceStore(tmp_path, FINANCE_SPECS["fina_mainbz"])
    store.replace_range("000001.SZ", "20250101", "20261019", df)
    assert set(store.read("000001.SZ").bz_code) == {"P", "D", "I", "S"}
    assert len(LocalPro(tmp_path).fina_mainbz(type="P")) == 1


def test_disclosure_only_passes_supported_source_parameters():
    pro = MagicMock()
    pro.disclosure_date.return_value = pd.DataFrame([
        row("disclosure_date", "20241231"), row("disclosure_date", "20251231", modify_date="20260301"),
    ])
    df = fetch_financial(pro, "disclosure_date", "000001.SZ", "20250101", "20261019")
    assert list(df.end_date) == ["20251231"]
    assert df.iloc[0].modify_date == "20260301"
    assert set(pro.disclosure_date.call_args.kwargs) == {"ts_code", "fields"}


@pytest.mark.parametrize("table", ["dividend", "disclosure_date"])
def test_full_stock_endpoint_cap_aborts_instead_of_overwriting(monkeypatch, table):
    monkeypatch.setitem(FINANCE_SPECS, table, replace(FINANCE_SPECS[table], row_limit=1))
    pro = MagicMock()
    getattr(pro, table).return_value = pd.DataFrame([row(table)])
    with pytest.raises(RuntimeError, match="cap"):
        fetch_financial(pro, table, "000001.SZ", "20250101", "20261019")


def test_resume_covers_gap_after_multiple_years_without_sync(tmp_path, runtime):
    fetch = MagicMock(return_value=pd.DataFrame())
    job = FinanceSyncJob(FINANCE_SPECS["income"], fetch, tmp_path)
    job.run(runtime, start_date="20000101", end_date="20231231")
    fetch.reset_mock()
    job.run(runtime)
    assert fetch.call_args_list[0] == call("income", "000001.SZ", "20240101", "20261019")


@pytest.mark.parametrize("code", ["000001.SZ", "600018.SH", "920001.BJ", "T600018.SH"])
def test_finance_partition_accepts_source_stock_codes(tmp_path, code):
    store = FinanceStore(tmp_path, FINANCE_SPECS["dividend"])
    assert store.path(code) == tmp_path / "finance" / "dividend" / f"ts_code={code}" / "data.parquet"


@pytest.mark.parametrize("code", ["../600018.SH", "T600018/SH", "T600018.SH/../", "T600018.SH\n", "TT600018.SH"])
def test_finance_partition_still_rejects_invalid_paths(tmp_path, code):
    with pytest.raises(ValueError, match="invalid stock code"):
        FinanceStore(tmp_path, FINANCE_SPECS["dividend"]).path(code)


def test_historical_prefixed_code_download_storage_query_and_refresh(tmp_path, runtime):
    SnapshotStore(tmp_path / "stock" / "basic" / "data.parquet").write(pd.DataFrame({
        "ts_code": ["600018.SH", "T600018.SH"], "list_status": ["L", "D"],
    }))
    pro = MagicMock()
    pro.dividend.side_effect = lambda **kw: pd.DataFrame([
        row("dividend", "20251231", code=kw["ts_code"], cash_div=0.1 if kw["ts_code"].startswith("T") else 0.2)
    ])
    job = FinanceSyncJob(
        FINANCE_SPECS["dividend"],
        lambda table, code, start, end: fetch_financial(pro, table, code, start, end),
        tmp_path,
    )
    job.run(runtime)
    assert [c.kwargs["ts_code"] for c in pro.dividend.call_args_list] == ["600018.SH", "T600018.SH"]
    assert runtime.meta.get_last_date("finance:dividend:T600018.SH") == "20261019"
    local = LocalPro(tmp_path)
    historical = local.dividend(ts_code="T600018.SH", period="20251231")
    assert historical.iloc[0].ts_code == "T600018.SH"
    assert historical.iloc[0].cash_div == 0.1
    assert local.dividend(ts_code="600018.SH").iloc[0].cash_div == 0.2
    job.run(runtime)
    assert len(local.dividend(ts_code="T600018.SH")) == 1


# --- Rate limiter ---

def test_rate_limiter_waits_the_configured_interval(monkeypatch):
    limiter = RateLimiter(requests_per_minute=100)  # 0.6s interval
    clock = {"t": 0.0}
    sleeps = []
    monkeypatch.setattr("zer0share.finance.fetcher.time.monotonic", lambda: clock["t"])

    def fake_sleep(seconds):
        clock["t"] += seconds
        sleeps.append(seconds)

    monkeypatch.setattr("zer0share.finance.fetcher.time.sleep", fake_sleep)
    limiter.wait()
    limiter.wait()
    limiter.wait()
    assert sleeps == [0.6, 0.6]
    assert clock["t"] == pytest.approx(1.2)


def test_rate_limiter_serializes_concurrent_waiters(monkeypatch):
    limiter = RateLimiter(requests_per_minute=100)
    clock = {"t": 0.0}
    monkeypatch.setattr("zer0share.finance.fetcher.time.monotonic", lambda: clock["t"])

    def fake_sleep(seconds):
        clock["t"] += seconds

    monkeypatch.setattr("zer0share.finance.fetcher.time.sleep", fake_sleep)
    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda _: limiter.wait(), range(10)))
    # 10 requests at 0.6s spacing => the clock advanced 9 intervals.
    assert clock["t"] == pytest.approx(0.6 * 9)


def test_rate_limiter_rejects_nonpositive_rate():
    with pytest.raises(ValueError, match="positive"):
        RateLimiter(0)


# --- Concurrent sync job ---

def test_concurrent_run_fetches_in_parallel(tmp_path, runtime):
    # A barrier forces both fetches to be in flight at once: if the executor ran
    # them sequentially the barrier would time out and the run would raise.
    barrier = threading.Barrier(2)

    def fetch(table, code, start, end):
        barrier.wait(timeout=5)
        return pd.DataFrame()

    job = FinanceSyncJob(FINANCE_SPECS["income"], fetch, tmp_path, max_workers=2)
    job.run(runtime)
    assert runtime.meta.get_last_date("finance:income:000001.SZ") == "20261019"
    assert runtime.meta.get_last_date("finance:income:000002.SZ") == "20261019"


def test_concurrent_failure_keeps_earlier_stock_data(tmp_path, runtime):
    def fetch(table, code, start, end):
        if code == "000002.SZ":
            raise RuntimeError("permission")
        return pd.DataFrame([row(code=code, revenue=1)])

    job = FinanceSyncJob(FINANCE_SPECS["income"], fetch, tmp_path, max_workers=2)
    with pytest.raises(RuntimeError):
        job.run(runtime)
    assert job._store.read("000001.SZ").iloc[0].revenue == 1
    assert runtime.meta.get_last_date("finance:income:000001.SZ") == "20261019"
    assert runtime.meta.get_last_date("finance:income:000002.SZ") is None
