from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import pandas as pd
import pytest

from zer0share.catalog import FINA_AUDIT_SPEC
from zer0share.fetcher import TushareFetcher
from zer0share.pipeline import Pipeline
from zer0share.query.repository import DailyTableSpec, TableSpec
from zer0share.schema import FINA_AUDIT_COLS
from zer0share.sources import DataSources
from zer0share.storage import SnapshotStore, TickerPartitionStore
from zer0share.sync._jobs import TickerSyncJob, TickerSyncResult
from zer0share.sync.fina_audit import (
    build_jobs, get_fina_audit_last_date, get_fina_audit_tickers, merge_fina_audit,
)


TICKERS = ["000001.SZ", "000333.SZ", "600036.SH"]


def audit(**values):
    row = dict(
        ts_code="000001.SZ", ann_date="20240430", end_date="20231231",
        audit_result="标准无保留意见", audit_fees=100.0,
        audit_agency="原始事务所", audit_sign="张三、李四",
    )
    row.update(values)
    return row


def frame(*rows):
    return pd.DataFrame(rows, columns=FINA_AUDIT_COLS)


def write_basic(data_dir, codes=TICKERS):
    basic = pd.DataFrame({
        "ts_code": codes,
        "symbol": [code.split(".")[0] for code in codes],
        "market": ["主板"] * len(codes),
    })
    SnapshotStore(data_dir / "stock" / "basic" / "data.parquet").write(basic)
    return basic


@pytest.fixture
def cfg(tmp_path):
    return SimpleNamespace(
        data_dir=tmp_path, db_path=tmp_path / "meta.duckdb",
        ricequant=SimpleNamespace(enabled=False),
    )


@pytest.fixture(autouse=True)
def sleep():
    with patch("zer0share.sync._jobs.time.sleep") as mocked:
        yield mocked


def test_schema_and_non_daily_catalog():
    assert FINA_AUDIT_COLS == [
        "ts_code", "ann_date", "end_date", "audit_result", "audit_fees",
        "audit_agency", "audit_sign",
    ]
    assert isinstance(FINA_AUDIT_SPEC, TableSpec)
    assert not isinstance(FINA_AUDIT_SPEC, DailyTableSpec)
    assert FINA_AUDIT_SPEC.columns == FINA_AUDIT_COLS
    assert FINA_AUDIT_SPEC.name == FINA_AUDIT_SPEC.sync_table == "fina_audit"
    assert FINA_AUDIT_SPEC.path_parts == ("stock", "financial", "fina_audit")
    assert FINA_AUDIT_SPEC.parquet_pattern == "ts_code=*/data.parquet"
    assert FINA_AUDIT_SPEC.first_date == "20100101"
    assert "trade_date" not in FINA_AUDIT_SPEC.columns


def test_merge_combines_and_deduplicates_identical_rows():
    old = frame(audit(end_date="20221231", ann_date="20230430"), audit())
    fetched = frame(audit(), audit(end_date="20241231", ann_date="20250430"))
    expected = frame(
        audit(end_date="20221231", ann_date="20230430"), audit(),
        audit(end_date="20241231", ann_date="20250430"),
    )
    result = merge_fina_audit(old, fetched)
    pd.testing.assert_frame_equal(result, expected)
    pd.testing.assert_frame_equal(merge_fina_audit(result, fetched), expected)
    assert len(old) == len(fetched) == 2


@pytest.mark.parametrize(
    "revision",
    [
        {"ann_date": "20240501"}, {"audit_result": "保留意见"},
        {"audit_agency": "另一家事务所"}, {"audit_sign": "另一位会计师"},
    ],
)
def test_merge_preserves_pit_and_opinion_versions(revision):
    result = merge_fina_audit(frame(audit()), frame(audit(**revision)))
    assert len(result) == 2
    assert result.iloc[0].to_dict() == audit()
    assert result.iloc[1].to_dict() == audit(**revision)


def test_merge_same_key_fetched_fees_win():
    result = merge_fina_audit(frame(audit(audit_fees=100.0)), frame(audit(audit_fees=120.0)))
    pd.testing.assert_frame_equal(result, frame(audit(audit_fees=120.0)))


@pytest.mark.parametrize("missing", [None, float("nan"), pd.NA])
def test_merge_missing_key_fields_are_stable(missing):
    old = frame(audit(audit_agency=None, audit_sign=None))
    fetched = frame(audit(audit_agency=missing, audit_sign=missing, audit_fees=120.0))
    result = merge_fina_audit(old, fetched)
    assert len(result) == 1
    assert result.iloc[0].audit_fees == 120.0
    assert pd.isna(result.iloc[0].audit_agency)
    pd.testing.assert_frame_equal(merge_fina_audit(result, fetched), result)


def test_merge_stably_sorts_ticker_period_and_announcement():
    expected = frame(
        audit(ann_date="20240401"), audit(), audit(audit_result="保留意见"),
        audit(end_date="20241231", ann_date="20250430"), audit(ts_code="600036.SH"),
    )
    result = merge_fina_audit(
        expected.iloc[[4, 3, 1, 0]].copy(), expected.iloc[[2]].copy(),
    )
    pd.testing.assert_frame_equal(result, expected)


@pytest.mark.parametrize("existing_empty, fetched_empty", [(True, False), (False, True), (True, True)])
def test_merge_handles_empty_inputs(existing_empty, fetched_empty):
    result = merge_fina_audit(
        pd.DataFrame() if existing_empty else frame(audit()),
        pd.DataFrame() if fetched_empty else frame(audit()),
    )
    assert list(result.columns) == FINA_AUDIT_COLS
    assert len(result) == (0 if existing_empty and fetched_empty else 1)


def test_last_date_uses_ann_date_not_report_period():
    existing = frame(
        audit(ann_date="20240430", end_date="20231231"),
        audit(ann_date="20250501", end_date="20221231"),
        audit(ann_date=None, end_date="20251231"),
    )
    assert get_fina_audit_last_date(existing) == "20250501"


@pytest.mark.parametrize("existing", [pd.DataFrame(), frame(audit(ann_date=None)), frame(audit(ann_date=pd.NA))])
def test_last_date_empty_or_all_missing_is_none(existing):
    assert get_fina_audit_last_date(existing) is None


def test_universe_uses_basic_keeps_st_new_delisted_and_beijing(cfg):
    codes = [*TICKERS, "688001.SH", "300001.SZ", "920001.BJ", "830001.BJ", TICKERS[0]]
    basic = write_basic(cfg.data_dir, codes)
    basic["name"] = ["*ST测试", "新股", "退市测试", "科创", "创业", "北交", "北交旧码", "重复"]
    basic["list_date"] = "20260501"
    basic["list_status"] = ["L", "L", "D", "L", "L", "L", "P", "L"]
    basic.loc[basic.ts_code.str.endswith("BJ"), "market"] = "北交所"
    SnapshotStore(cfg.data_dir / "stock/basic/data.parquet").write(basic)
    assert get_fina_audit_tickers(cfg.data_dir) == sorted(set(codes))


def test_universe_excludes_non_a_share_entities(cfg):
    codes = [
        "000001.SZ", "200001.SZ", "900901.SH", "689009.SH", "00700.HK",
        "510300.SH", "159919.SZ", "IF2406.CFX", "10000001.SH", "000001.SH", "399001.SZ",
    ]
    basic = write_basic(cfg.data_dir, codes)
    basic.loc[basic.ts_code == "689009.SH", "market"] = "CDR"
    SnapshotStore(cfg.data_dir / "stock/basic/data.parquet").write(basic)
    assert get_fina_audit_tickers(cfg.data_dir) == ["000001.SZ"]


@pytest.mark.parametrize("contents", ["missing", "empty", "non_a_share"])
def test_empty_universe_raises_instead_of_false_success(cfg, contents):
    if contents == "empty":
        write_basic(cfg.data_dir, [])
    elif contents == "non_a_share":
        write_basic(cfg.data_dir, ["200001.SZ", "00700.HK"])
    with pytest.raises(RuntimeError, match="fina_audit:.*basic"):
        get_fina_audit_tickers(cfg.data_dir)


def test_job_wires_existing_generic_executor_and_store(cfg):
    fetcher = Mock()
    job, = build_jobs(cfg, fetcher)
    assert type(job) is TickerSyncJob
    assert job.table_name == "fina_audit"
    assert job.supports_date_range
    assert job.first_date == "20100101"
    assert job.overlap_days == 120
    assert job.fetch_range is fetcher.fetch_fina_audit
    assert job.merge is merge_fina_audit
    assert job.get_last_date is get_fina_audit_last_date
    assert isinstance(job.read_existing.__self__, TickerPartitionStore)
    assert job.read_existing.__self__ is job.write_ticker.__self__
    write_basic(cfg.data_dir)
    assert job.get_tickers() == TICKERS


def test_mock_tushare_to_pipeline_to_parquet_with_overlap(cfg):
    write_basic(cfg.data_dir)
    with patch("zer0share.fetcher.ts.pro_api") as pro_api:
        client = pro_api.return_value
        client.fina_audit.side_effect = lambda **args: frame(audit(ts_code=args["ts_code"]))
        fetcher = TushareFetcher("fake")
        with Pipeline(cfg, DataSources(tushare=fetcher), Mock()) as pipeline:
            pipeline._runtime.calendar._today_fn = lambda: "20240531"
            job = pipeline.registry["fina_audit"]
            assert isinstance(job, TickerSyncJob)
            pipeline.run("fina_audit")
            assert client.fina_audit.call_args_list == [
                call(ts_code=code, start_date="20100101", end_date="20240531", fields=",".join(FINA_AUDIT_COLS))
                for code in TICKERS
            ]
            store = job.read_existing.__self__
            for code in TICKERS:
                pd.testing.assert_frame_equal(store.read(code), frame(audit(ts_code=code)))
                assert (cfg.data_dir / "stock/financial/fina_audit" / f"ts_code={code}" / "data.parquet").exists()

            client.fina_audit.reset_mock()
            client.fina_audit.side_effect = lambda **args: frame(
                audit(ts_code=args["ts_code"], audit_fees=120.0),
                audit(ts_code=args["ts_code"], ann_date="20240501", audit_result="保留意见"),
            )
            pipeline.run("fina_audit")
            assert client.fina_audit.call_args_list == [
                call(ts_code=code, start_date="20240101", end_date="20240531", fields=",".join(FINA_AUDIT_COLS))
                for code in TICKERS
            ]
            for code in TICKERS:
                data = store.read(code)
                assert len(data) == 2
                assert data.ann_date.tolist() == ["20240430", "20240501"]
                assert data.audit_fees.tolist() == [120.0, 100.0]
            assert pipeline._runtime.meta.get_last_date("fina_audit") is None


def test_pipeline_reports_failed_tickers_after_successful_tickers_are_written(cfg):
    write_basic(cfg.data_dir)
    fetcher = Mock()

    def fetch(ticker, start, end):
        if ticker == TICKERS[0]:
            raise RuntimeError("API down")
        return frame(audit(ts_code=ticker))

    fetcher.fetch_fina_audit.side_effect = fetch
    with Pipeline(cfg, DataSources(tushare=fetcher), Mock()) as pipeline:
        with pytest.raises(RuntimeError, match="incomplete.*failed=1.*000001.SZ"):
            pipeline.run("fina_audit", "20100101", "20240531")
        assert fetcher.fetch_fina_audit.call_count == 6
        assert pipeline.registry["fina_audit"].read_existing(TICKERS[0]).empty
        for ticker in TICKERS[1:]:
            assert not pipeline.registry["fina_audit"].read_existing(ticker).empty


def test_pipeline_run_all_checks_incomplete_ticker_result(cfg):
    with Pipeline(cfg, DataSources(tushare=Mock()), Mock()) as pipeline:
        for job in pipeline.registry.values():
            job.run = Mock(return_value=None)
        pipeline.registry["fina_audit"].run.return_value = TickerSyncResult(
            total=1, failed=1, failed_tickers=[TICKERS[0]],
        )
        with pytest.raises(RuntimeError, match="incomplete"):
            pipeline.run_all()
