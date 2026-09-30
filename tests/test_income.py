from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import pandas as pd
import pytest
from click.testing import CliRunner

from zer0share import dateutil
from zer0share.catalog import INCOME_SPEC
from zer0share.cli import cli
from zer0share.fetcher import TushareFetcher
from zer0share.pipeline import Pipeline
from zer0share.profiles import IPO_SCORE_DATED_TABLES, IPO_SCORE_TABLES
from zer0share.query.repository import DailyTableSpec, TableSpec
from zer0share.schema import INCOME_COLS
from zer0share.sources import DataSources
from zer0share.storage import SnapshotStore, TickerPartitionStore
from zer0share.sync._jobs import TickerSyncJob
from zer0share.sync.fina_audit import get_fina_audit_tickers
from zer0share.sync.income import (
    INCOME_KEY, build_jobs, get_income_effective_announcement_date,
    get_income_last_date, merge_income,
)


def income(**changes):
    row = dict(
        ts_code='000001.SZ', ann_date='20240430', f_ann_date='20240501',
        end_date='20231231', report_type='1', comp_type='2',
        update_flag='0', revenue=100.0, n_income=12.0,
    )
    row.update(changes)
    return row


def frame(*rows):
    return pd.DataFrame(rows, columns=INCOME_COLS)


@pytest.fixture
def cfg(tmp_path):
    return SimpleNamespace(
        data_dir=tmp_path, db_path=tmp_path / 'meta.duckdb',
        ricequant=SimpleNamespace(enabled=False),
    )


@pytest.fixture
def api():
    with patch('zer0share.fetcher.ts.pro_api') as factory:
        yield factory.return_value, TushareFetcher('fake')


@pytest.fixture(autouse=True)
def no_sleep():
    with patch('zer0share.sync._jobs.time.sleep'):
        yield


def test_official_schema_catalog_profile_and_job(cfg):
    assert len(INCOME_COLS) == len(set(INCOME_COLS)) == 94
    assert INCOME_COLS[:7] == [
        'ts_code', 'ann_date', 'f_ann_date', 'end_date',
        'report_type', 'comp_type', 'end_type',
    ]
    assert INCOME_COLS[-1] == 'update_flag'
    assert {'revenue', 'n_income', 'n_income_attr_p', 'rd_exp'} <= set(INCOME_COLS)
    assert isinstance(INCOME_SPEC, TableSpec)
    assert not isinstance(INCOME_SPEC, DailyTableSpec)
    assert INCOME_SPEC.columns == INCOME_COLS
    assert INCOME_SPEC.path_parts == ('stock', 'financial', 'income')
    assert INCOME_SPEC.first_date == '20100101'
    assert IPO_SCORE_TABLES[-6:] == ('fina_audit', 'fina_indicator', 'dividend', 'income', 'balancesheet', 'cashflow')
    assert 'cashflow' not in IPO_SCORE_DATED_TABLES
    assert 'income' not in IPO_SCORE_DATED_TABLES

    fetcher = Mock()
    job, = build_jobs(cfg, fetcher)
    assert type(job) is TickerSyncJob
    assert job.table_name == 'income' and job.supports_date_range
    assert job.first_date == '20100101' and job.overlap_days == 1825
    assert job.get_tickers.func is get_fina_audit_tickers
    assert job.fetch_range is fetcher.fetch_income
    assert job.merge is merge_income
    assert job.get_last_date is get_income_last_date
    assert isinstance(job.read_existing.__self__, TickerPartitionStore)
    assert job.read_existing.__self__ is job.write_ticker.__self__
    with pytest.raises(RuntimeError, match='basic'):
        job.get_tickers()
    SnapshotStore(cfg.data_dir/'stock/basic/data.parquet').write(pd.DataFrame({
        'ts_code': ['000001.SZ'], 'symbol': ['000001'], 'market': ['主板'],
    }))
    assert job.get_tickers() == ['000001.SZ']


def test_fetch_explicit_fields_schema_dates_and_single_small_range(api):
    client, fetcher = api
    client.income.return_value = frame(income(f_ann_date=pd.Timestamp('2024-05-01')))[INCOME_COLS[::-1]].drop(columns=['rd_exp'])
    result = fetcher.fetch_income('000001.SZ', '20240101', '20241231')
    client.income.assert_called_once_with(
        ts_code='000001.SZ', start_date='20240101', end_date='20241231',
        fields=','.join(INCOME_COLS),
    )
    assert list(result) == INCOME_COLS
    assert result.f_ann_date.iloc[0] == '20240501'
    assert result.rd_exp.isna().all()
    assert result.revenue.iloc[0] == 100.


@pytest.mark.parametrize('empty', [None, pd.DataFrame()])
def test_empty_fetch_keeps_full_schema(api, empty):
    client, fetcher = api
    client.income.return_value = empty
    result = fetcher.fetch_income('000001.SZ', '20240101', '20241231')
    assert result.empty and list(result) == INCOME_COLS


def test_announcement_chunks_have_no_gap_and_concat_nonempty_parts(api):
    client, fetcher = api
    client.income.side_effect = [
        frame(income(ann_date='20120430', f_ann_date='20120501')),
        None,
        frame(income(ann_date='20180430', f_ann_date='20180501')),
        pd.DataFrame(),
        frame(income(ann_date='20240430', f_ann_date='20240501')),
        frame(income(ann_date='20260930', f_ann_date='20261001')),
    ]
    result = fetcher.fetch_income('000001.SZ', '20100101', '20261001')
    bounds = [(c.kwargs['start_date'], c.kwargs['end_date']) for c in client.income.call_args_list]
    assert bounds == [
        ('20100101','20121231'), ('20130101','20151231'),
        ('20160101','20181231'), ('20190101','20211231'),
        ('20220101','20241231'), ('20250101','20261001'),
    ]
    assert all(dateutil.add_days(left[1], 1) == right[0] for left, right in zip(bounds, bounds[1:]))
    assert all(c.kwargs['fields'] == ','.join(INCOME_COLS) for c in client.income.call_args_list)
    assert result.ann_date.tolist() == ['20120430','20180430','20240430','20260930']


def test_leap_day_chunk_and_invalid_range(api):
    client, fetcher = api
    client.income.return_value = None
    fetcher.fetch_income('000001.SZ', '20200229', '20230301')
    assert [(c.kwargs['start_date'], c.kwargs['end_date']) for c in client.income.call_args_list] == [
        ('20200229', '20221231'), ('20230101', '20230301'),
    ]
    with pytest.raises(ValueError):
        fetcher.fetch_income('000001.SZ', '20240102', '20240101')


def test_saturated_chunk_is_bisected_without_fetcher_dedup(api):
    client, fetcher = api
    client.income.side_effect = [
        pd.concat([frame(income())] * 100),
        pd.concat([frame(income())] * 60),
        pd.concat([frame(income())] * 60),
    ]
    result = fetcher.fetch_income('000001.SZ', '20200101', '20221231')
    assert len(result) == 120
    calls = client.income.call_args_list
    assert calls[1].kwargs['start_date'] == '20200101'
    assert dateutil.add_days(calls[1].kwargs['end_date'], 1) == calls[2].kwargs['start_date']
    assert calls[2].kwargs['end_date'] == '20221231'


def test_saturated_one_day_fails_and_api_error_propagates(api):
    client, fetcher = api
    client.income.return_value = pd.concat([frame(income())] * 100)
    with pytest.raises(RuntimeError, match='saturated single announcement day'):
        fetcher.fetch_income('000001.SZ', '20240430', '20240430')
    client.income.side_effect = RuntimeError('API unavailable')
    with pytest.raises(RuntimeError, match='API unavailable'):
        fetcher.fetch_income('000001.SZ', '20240101', '20241231')


@pytest.mark.parametrize('row, expected', [
    (income(), '20240501'),
    (income(f_ann_date=None), '20240430'),
    (income(f_ann_date='', ann_date='20240430'), '20240430'),
    (income(f_ann_date=None, ann_date=None), None),
])
def test_effective_announcement_date(row, expected):
    assert get_income_effective_announcement_date(pd.Series(row)) == expected


def test_last_date_uses_per_row_effective_announcement_not_report_period():
    assert get_income_last_date(pd.DataFrame()) is None
    assert get_income_last_date(frame(income(ann_date=None, f_ann_date=None))) is None
    existing = frame(
        income(ann_date='20240510', f_ann_date='20240501', end_date='20261231'),
        income(ann_date='20240430', f_ann_date=None, end_date='20211231'),
        income(ann_date='20240502', f_ann_date='20240506', end_date='20221231'),
    )
    assert get_income_last_date(existing) == '20240506'


@pytest.mark.parametrize('change', [
    {'ann_date':'20240501'}, {'f_ann_date':'20240502'},
    {'report_type':'2'}, {'comp_type':'1'}, {'update_flag':'1'},
])
def test_distinct_versions_remain(change):
    result = merge_income(frame(income()), frame(income(**change)))
    assert len(result) == 2
    assert result[INCOME_KEY].drop_duplicates().shape[0] == 2
    assert list(result) == INCOME_COLS  # No fabricated revision timestamp.


def test_same_complete_key_new_value_wins_and_replay_is_stable():
    old = frame(income(revenue=100.))
    new = frame(income(revenue=150., n_income=25.))
    result = merge_income(old, new)
    assert len(result) == 1 and result.revenue.iloc[0] == 150.
    assert result.n_income.iloc[0] == 25.
    pd.testing.assert_frame_equal(merge_income(result, new), result)


def test_missing_keys_are_idempotent_through_parquet(cfg):
    nulls = dict(ann_date=None, f_ann_date=None, report_type=None, comp_type=None, update_flag=None)
    old = frame(income(**nulls))
    new = frame(income(**{k: float('nan') for k in nulls}, revenue=200.))
    result = merge_income(old, new)
    assert len(result) == 1 and result.revenue.iloc[0] == 200.
    store = TickerPartitionStore(cfg.data_dir/'stock/financial/income')
    store.write('000001.SZ', result)
    for _ in range(2):
        result = merge_income(store.read('000001.SZ'), new)
        assert len(result) == 1
        store.write('000001.SZ', result)
    assert store.read('000001.SZ').revenue.iloc[0] == 200.
    assert merge_income(pd.DataFrame(), pd.DataFrame()).empty


def test_sorted_versions_retain_all_report_types_and_flags():
    rows = frame(
        income(end_date='20231231', report_type='2', update_flag='1'),
        income(end_date='20221231'),
        income(end_date='20231231', report_type='1', update_flag='0'),
    )
    result = merge_income(pd.DataFrame(), rows)
    assert result.end_date.tolist() == ['20221231', '20231231', '20231231']
    assert result.report_type.tolist() == ['1','1','2']
    assert result.update_flag.tolist() == ['0','0','1']


def test_pipeline_initial_retry_incremental_and_parquet(cfg, api):
    client, fetcher = api
    SnapshotStore(cfg.data_dir/'stock/basic/data.parquet').write(pd.DataFrame({
        'ts_code': ['000001.SZ'], 'symbol': ['000001'], 'market': ['主板'],
    }))
    client.income.side_effect = [
        None, RuntimeError('temporary'),  # Failed second chunk restarts ticker.
        None, None, None, None, frame(income()),
    ]
    with Pipeline(cfg, DataSources(tushare=fetcher), Mock()) as pipeline:
        job = pipeline.registry['income']
        pipeline._runtime.calendar._today_fn = lambda: '20240531'
        pipeline.run('income')
        starts = [c.kwargs['start_date'] for c in client.income.call_args_list]
        assert starts == [
            '20100101','20130101','20100101','20130101',
            '20160101','20190101','20220101',
        ]
        path = cfg.data_dir/'stock/financial/income/ts_code=000001.SZ/data.parquet'
        assert path.exists() and len(job.read_existing('000001.SZ')) == 1
        client.income.reset_mock()
        client.income.side_effect = None
        client.income.return_value = frame(income(revenue=200.))
        pipeline.run('income')
        assert client.income.call_args_list[0].kwargs['start_date'] == dateutil.add_days('20240501', -1825)
        after = job.read_existing('000001.SZ')
        assert len(after) == 1 and after.revenue.iloc[0] == 200.
        pipeline.run('income')
        pd.testing.assert_frame_equal(job.read_existing('000001.SZ'), after)
        assert pipeline._runtime.meta.get_last_date('income') is None


def test_cli_explicit_dates_and_profile_init():
    with patch('zer0share.cli._make_pipeline') as factory:
        result = CliRunner().invoke(cli, [
            'sync', '--table', 'income', '--start-date', '20100101',
            '--end-date', '20240531',
        ])
        assert result.exit_code == 0, result.output
        factory.return_value.__enter__.return_value.run.assert_called_once_with(
            'income', start_date='20100101', end_date='20240531',
        )
    with patch('zer0share.cli._make_pipeline') as factory:
        result = CliRunner().invoke(cli, ['sync', '--ipo-score', '--init'])
        assert result.exit_code == 0, result.output
        calls = factory.return_value.__enter__.return_value.run.call_args_list
        assert call('income', start_date=None, end_date=None) in calls
