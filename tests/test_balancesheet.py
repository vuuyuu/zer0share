from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import pandas as pd
import pytest
from click.testing import CliRunner

from zer0share import dateutil
from zer0share.catalog import BALANCESHEET_SPEC
from zer0share.cli import cli
from zer0share.fetcher import TushareFetcher
from zer0share.pipeline import Pipeline
from zer0share.profiles import IPO_SCORE_DATED_TABLES, IPO_SCORE_TABLES
from zer0share.query.repository import DailyTableSpec, TableSpec
from zer0share.schema import BALANCESHEET_COLS
from zer0share.sources import DataSources
from zer0share.storage import SnapshotStore, TickerPartitionStore
from zer0share.sync._jobs import TickerSyncJob
from zer0share.sync.balancesheet import (
    BALANCESHEET_KEY, build_jobs, get_balancesheet_effective_announcement_date,
    get_balancesheet_last_date, merge_balancesheet,
)
from zer0share.sync.fina_audit import get_fina_audit_tickers


def balancesheet(**changes):
    row = dict(
        ts_code="600000.SH", ann_date="20240430", f_ann_date="20240501",
        end_date="20231231", report_type="1", comp_type="2", update_flag="0",
        total_assets=100.0, total_liab=80.0, depos=20.0,
        depos_ib_deposits=15.0, loan_oth_bank=5.0,
    )
    row.update(changes)
    return row


def frame(*rows):
    return pd.DataFrame(rows, columns=BALANCESHEET_COLS)


@pytest.fixture
def cfg(tmp_path):
    return SimpleNamespace(
        data_dir=tmp_path, db_path=tmp_path / "meta.duckdb",
        ricequant=SimpleNamespace(enabled=False),
    )


@pytest.fixture
def api():
    with patch("zer0share.fetcher.ts.pro_api") as factory:
        yield factory.return_value, TushareFetcher("fake")


@pytest.fixture(autouse=True)
def no_sleep():
    with patch("zer0share.sync._jobs.time.sleep"):
        yield


def test_official_schema_catalog_profile_and_job(cfg):
    assert len(BALANCESHEET_COLS) == len(set(BALANCESHEET_COLS)) == 158
    assert BALANCESHEET_COLS[:7] == [
        "ts_code", "ann_date", "f_ann_date", "end_date",
        "report_type", "comp_type", "end_type",
    ]
    assert BALANCESHEET_COLS[-1] == "update_flag"
    assert {"depos", "depos_ib_deposits", "loan_oth_bank", "cb_borr"} <= set(BALANCESHEET_COLS)
    assert isinstance(BALANCESHEET_SPEC, TableSpec)
    assert not isinstance(BALANCESHEET_SPEC, DailyTableSpec)
    assert BALANCESHEET_SPEC.columns == BALANCESHEET_COLS
    assert BALANCESHEET_SPEC.path_parts == ("stock", "financial", "balancesheet")
    assert BALANCESHEET_SPEC.first_date == "20100101"
    assert IPO_SCORE_TABLES[-2:] == ("balancesheet", "cashflow")
    assert "balancesheet" not in IPO_SCORE_DATED_TABLES
    assert "cashflow" not in IPO_SCORE_DATED_TABLES

    fetcher = Mock()
    job, = build_jobs(cfg, fetcher)
    assert type(job) is TickerSyncJob
    assert job.table_name == "balancesheet" and job.supports_date_range
    assert job.first_date == "20100101" and job.overlap_days == 1825
    assert job.get_tickers.func is get_fina_audit_tickers
    assert job.fetch_range is fetcher.fetch_balancesheet
    assert job.merge is merge_balancesheet
    assert job.get_last_date is get_balancesheet_last_date
    assert isinstance(job.read_existing.__self__, TickerPartitionStore)
    assert job.read_existing.__self__ is job.write_ticker.__self__
    SnapshotStore(cfg.data_dir / "stock/basic/data.parquet").write(pd.DataFrame({
        "ts_code": ["600000.SH"], "symbol": ["600000"], "market": ["主板"],
    }))
    assert job.get_tickers() == ["600000.SH"]


def test_fetch_explicit_fields_schema_dates_and_special_bank_fields(api):
    client, fetcher = api
    client.balancesheet.return_value = frame(
        balancesheet(f_ann_date=pd.Timestamp("2024-05-01"))
    )[BALANCESHEET_COLS[::-1]].drop(columns=["lease_liab"])
    result = fetcher.fetch_balancesheet("600000.SH", "20240101", "20241231")
    client.balancesheet.assert_called_once_with(
        ts_code="600000.SH", start_date="20240101", end_date="20241231",
        fields=",".join(BALANCESHEET_COLS),
    )
    assert list(result) == BALANCESHEET_COLS
    assert result.f_ann_date.iloc[0] == "20240501"
    assert result.lease_liab.isna().all()
    assert result.depos.iloc[0] == 20.0


@pytest.mark.parametrize("empty", [None, pd.DataFrame()])
def test_empty_fetch_keeps_full_schema(api, empty):
    client, fetcher = api
    client.balancesheet.return_value = empty
    result = fetcher.fetch_balancesheet("600000.SH", "20240101", "20241231")
    assert result.empty and list(result) == BALANCESHEET_COLS


def test_announcement_chunks_have_no_gap_and_concat_nonempty_parts(api):
    client, fetcher = api
    client.balancesheet.side_effect = [
        frame(balancesheet(ann_date="20120430", f_ann_date="20120501")), None,
        frame(balancesheet(ann_date="20180430", f_ann_date="20180501")), pd.DataFrame(),
        frame(balancesheet(ann_date="20240430", f_ann_date="20240501")),
        frame(balancesheet(ann_date="20260930", f_ann_date="20261001")),
    ]
    result = fetcher.fetch_balancesheet("600000.SH", "20100101", "20261001")
    bounds = [(c.kwargs["start_date"], c.kwargs["end_date"])
              for c in client.balancesheet.call_args_list]
    assert bounds == [
        ("20100101", "20121231"), ("20130101", "20151231"),
        ("20160101", "20181231"), ("20190101", "20211231"),
        ("20220101", "20241231"), ("20250101", "20261001"),
    ]
    assert all(dateutil.add_days(left[1], 1) == right[0]
               for left, right in zip(bounds, bounds[1:]))
    assert all(c.kwargs["fields"] == ",".join(BALANCESHEET_COLS)
               for c in client.balancesheet.call_args_list)
    assert result.ann_date.tolist() == ["20120430", "20180430", "20240430", "20260930"]


def test_saturated_chunk_is_bisected_and_one_day_fails(api):
    client, fetcher = api
    client.balancesheet.side_effect = [
        pd.concat([frame(balancesheet())] * 100),
        pd.concat([frame(balancesheet())] * 60),
        pd.concat([frame(balancesheet())] * 60),
    ]
    assert len(fetcher.fetch_balancesheet("600000.SH", "20200101", "20221231")) == 120
    calls = client.balancesheet.call_args_list
    assert calls[1].kwargs["start_date"] == "20200101"
    assert dateutil.add_days(calls[1].kwargs["end_date"], 1) == calls[2].kwargs["start_date"]
    client.balancesheet.reset_mock()
    client.balancesheet.side_effect = None
    client.balancesheet.return_value = pd.concat([frame(balancesheet())] * 100)
    with pytest.raises(RuntimeError, match="saturated single announcement day"):
        fetcher.fetch_balancesheet("600000.SH", "20240430", "20240430")


@pytest.mark.parametrize("row, expected", [
    (balancesheet(), "20240501"),
    (balancesheet(f_ann_date=None), "20240430"),
    (balancesheet(f_ann_date="", ann_date="20240430"), "20240430"),
    (balancesheet(f_ann_date=None, ann_date=None), None),
])
def test_effective_announcement_date(row, expected):
    assert get_balancesheet_effective_announcement_date(pd.Series(row)) == expected


def test_last_date_uses_effective_announcement_not_report_period():
    existing = frame(
        balancesheet(ann_date="20240510", f_ann_date="20240501", end_date="20261231"),
        balancesheet(ann_date="20240430", f_ann_date=None, end_date="20211231"),
        balancesheet(ann_date="20240502", f_ann_date="20240506", end_date="20221231"),
    )
    assert get_balancesheet_last_date(existing) == "20240506"


@pytest.mark.parametrize("change", [
    {"ann_date": "20240501"}, {"f_ann_date": "20240502"},
    {"report_type": "2"}, {"comp_type": "1"}, {"update_flag": "1"},
])
def test_distinct_versions_remain(change):
    result = merge_balancesheet(frame(balancesheet()), frame(balancesheet(**change)))
    assert len(result) == 2
    assert result[BALANCESHEET_KEY].drop_duplicates().shape[0] == 2


def test_same_complete_key_new_value_wins_and_missing_keys_are_stable(cfg):
    old = frame(balancesheet(total_assets=100.0))
    new = frame(balancesheet(total_assets=150.0, total_liab=125.0))
    result = merge_balancesheet(old, new)
    assert len(result) == 1 and result.total_assets.iloc[0] == 150.0
    pd.testing.assert_frame_equal(merge_balancesheet(result, new), result)
    nulls = dict(ann_date=None, f_ann_date=None, report_type=None, comp_type=None, update_flag=None)
    stable = merge_balancesheet(frame(balancesheet(**nulls)), frame(balancesheet(**{
        key: float("nan") for key in nulls
    }, total_assets=200.0)))
    store = TickerPartitionStore(cfg.data_dir / "stock/financial/balancesheet")
    store.write("600000.SH", stable)
    assert len(merge_balancesheet(store.read("600000.SH"), stable)) == 1


def test_pipeline_incremental_and_parquet(cfg, api):
    client, fetcher = api
    SnapshotStore(cfg.data_dir / "stock/basic/data.parquet").write(pd.DataFrame({
        "ts_code": ["600000.SH"], "symbol": ["600000"], "market": ["主板"],
    }))
    client.balancesheet.side_effect = [None, None, None, None, frame(balancesheet())]
    with Pipeline(cfg, DataSources(tushare=fetcher), Mock()) as pipeline:
        pipeline._runtime.calendar._today_fn = lambda: "20240531"
        pipeline.run("balancesheet")
        path = cfg.data_dir / "stock/financial/balancesheet/ts_code=600000.SH/data.parquet"
        assert path.exists()
        client.balancesheet.reset_mock()
        client.balancesheet.side_effect = None
        client.balancesheet.return_value = frame(balancesheet(total_assets=200.0))
        pipeline.run("balancesheet")
        assert client.balancesheet.call_args_list[0].kwargs["start_date"] == dateutil.add_days("20240501", -1825)


def test_cli_explicit_dates_and_profile_init():
    with patch("zer0share.cli._make_pipeline") as factory:
        result = CliRunner().invoke(cli, [
            "sync", "--table", "balancesheet", "--start-date", "20100101",
            "--end-date", "20240531",
        ])
        assert result.exit_code == 0, result.output
        factory.return_value.__enter__.return_value.run.assert_called_once_with(
            "balancesheet", start_date="20100101", end_date="20240531",
        )
    with patch("zer0share.cli._make_pipeline") as factory:
        result = CliRunner().invoke(cli, ["sync", "--ipo-score", "--init"])
        assert result.exit_code == 0, result.output
        assert call("balancesheet", start_date=None, end_date=None) in (
            factory.return_value.__enter__.return_value.run.call_args_list
        )
