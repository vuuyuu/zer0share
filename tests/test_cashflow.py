from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd
import pytest

from zer0share.catalog import CASHFLOW_SPEC
from zer0share.fetcher import TushareFetcher
from zer0share.profiles import IPO_SCORE_DATED_TABLES, IPO_SCORE_TABLES
from zer0share.query.repository import TableSpec
from zer0share.schema import CASHFLOW_COLS
from zer0share.storage import TickerPartitionStore
from zer0share.sync.cashflow import (
    CASHFLOW_KEY, build_jobs, get_cashflow_effective_announcement_date,
    get_cashflow_last_date, merge_cashflow,
)


def row(**changes):
    value = dict(ts_code="600000.SH", ann_date="20240430", f_ann_date="20240501",
                 end_date="20231231", comp_type="2", report_type="1", end_type="4",
                 update_flag="0", net_profit=10.0, n_cashflow_act=8.0,
                 n_cashflow_inv_act=-3.0, n_cash_flows_fnc_act=2.0,
                 n_depos_incr_fi=1.0, net_cash_rece_sec=2.0)
    value.update(changes)
    return value


def frame(*rows):
    return pd.DataFrame(rows, columns=CASHFLOW_COLS)


@pytest.fixture
def api():
    with patch("zer0share.fetcher.ts.pro_api") as factory:
        yield factory.return_value, TushareFetcher("fake")


def test_schema_spec_profile_and_job(tmp_path):
    assert len(CASHFLOW_COLS) == len(set(CASHFLOW_COLS)) == 97
    assert CASHFLOW_COLS[:7] == ["ts_code", "ann_date", "f_ann_date", "end_date", "comp_type", "report_type", "end_type"]
    assert CASHFLOW_COLS[-1] == "update_flag"
    assert {"net_profit", "n_cashflow_act", "n_cashflow_inv_act", "n_cash_flows_fnc_act", "n_depos_incr_fi", "net_cash_rece_sec"} <= set(CASHFLOW_COLS)
    assert isinstance(CASHFLOW_SPEC, TableSpec) and CASHFLOW_SPEC.first_date == "20100101"
    assert CASHFLOW_SPEC.path_parts == ("stock", "financial", "cashflow")
    assert IPO_SCORE_TABLES[-1] == "cashflow" and "cashflow" not in IPO_SCORE_DATED_TABLES
    cfg = SimpleNamespace(data_dir=tmp_path)
    fetcher = Mock()
    job, = build_jobs(cfg, fetcher)
    assert job.first_date == "20100101" and job.overlap_days == 1825
    assert job.fetch_range is fetcher.fetch_cashflow and isinstance(job.read_existing.__self__, TickerPartitionStore)


def test_fetch_schema_dates_chunks_and_saturation(api):
    client, fetcher = api
    client.cashflow.return_value = frame(row(f_ann_date=pd.Timestamp("2024-05-01")))[CASHFLOW_COLS[::-1]].drop(columns="end_bal_cash")
    result = fetcher.fetch_cashflow("600000.SH", "20240101", "20241231")
    client.cashflow.assert_called_once_with(ts_code="600000.SH", start_date="20240101", end_date="20241231", fields=",".join(CASHFLOW_COLS))
    assert list(result) == CASHFLOW_COLS and result.f_ann_date.iloc[0] == "20240501" and result.end_bal_cash.isna().all()
    client.cashflow.reset_mock()
    client.cashflow.side_effect = [frame(row()), None, frame(row(ann_date="20240430")), None, None, None]
    fetcher.fetch_cashflow("600000.SH", "20100101", "20261001")
    bounds = [(call.kwargs["start_date"], call.kwargs["end_date"]) for call in client.cashflow.call_args_list]
    assert bounds == [("20100101", "20121231"), ("20130101", "20151231"), ("20160101", "20181231"), ("20190101", "20211231"), ("20220101", "20241231"), ("20250101", "20261001")]
    client.cashflow.reset_mock(); client.cashflow.side_effect = [pd.concat([frame(row())] * 100), frame(row()), frame(row())]
    assert len(fetcher.fetch_cashflow("600000.SH", "20200101", "20221231")) == 2


def test_pit_merge_and_null_key_stability():
    assert get_cashflow_effective_announcement_date(pd.Series(row())) == "20240501"
    assert get_cashflow_effective_announcement_date(pd.Series(row(f_ann_date=None))) == "20240430"
    existing = frame(row(end_date="20261231"), row(ann_date="20240502", f_ann_date="20240506", end_date="20211231"))
    assert get_cashflow_last_date(existing) == "20240506"
    changed = merge_cashflow(frame(row()), frame(row(n_cashflow_act=99.0)))
    assert len(changed) == 1 and changed.n_cashflow_act.iloc[0] == 99.0 and changed.end_type.iloc[0] == "4"
    versions = merge_cashflow(frame(row()), frame(row(update_flag="1")))
    assert len(versions) == 2 and versions[CASHFLOW_KEY].drop_duplicates().shape[0] == 2
    null = dict(ann_date=None, f_ann_date=None, report_type=None, comp_type=None, update_flag=None)
    assert len(merge_cashflow(frame(row(**null)), frame(row(**{k: float("nan") for k in null})))) == 1
