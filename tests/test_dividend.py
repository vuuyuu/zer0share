from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import pandas as pd
import pytest
from click.testing import CliRunner

from zer0share.catalog import DIVIDEND_SPEC
from zer0share.cli import cli
from zer0share.fetcher import TushareFetcher
from zer0share.pipeline import Pipeline
from zer0share.profiles import IPO_SCORE_DATED_TABLES, IPO_SCORE_TABLES
from zer0share.query.repository import DailyTableSpec, TableSpec
from zer0share.schema import DIVIDEND_COLS
from zer0share.sources import DataSources
from zer0share.storage import SnapshotStore, TickerPartitionStore
from zer0share.sync._jobs import TickerSyncJob
from zer0share.sync.dividend import (
    DIVIDEND_KEY, build_jobs, get_dividend_last_date, merge_dividend,
)
from zer0share.sync.fina_audit import get_fina_audit_tickers

CODES = ['000001.SZ', '000333.SZ', '600000.SH', '600036.SH', '600941.SH']


def dividend(**changes):
    row = dict(
        ts_code='000001.SZ', end_date='20231231', ann_date='20240430',
        div_proc='预案', stk_div=0.0, stk_bo_rate=0.0, stk_co_rate=0.0,
        cash_div=0.10, cash_div_tax=0.12, record_date=None, ex_date=None,
        pay_date=None, div_listdate=None, imp_ann_date=None,
        base_date=None, base_share=None,
    )
    row.update(changes)
    return row


def frame(*rows):
    return pd.DataFrame(rows, columns=DIVIDEND_COLS)


@pytest.fixture
def cfg(tmp_path):
    return SimpleNamespace(
        data_dir=tmp_path, db_path=tmp_path/'meta.duckdb',
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


def write_basic(data_dir, codes=CODES):
    basic = pd.DataFrame({
        'ts_code': codes, 'symbol': [c.split('.')[0] for c in codes],
        'market': ['主板'] * len(codes),
    })
    SnapshotStore(data_dir/'stock/basic/data.parquet').write(basic)


def test_schema_catalog_profile_and_job(cfg):
    assert DIVIDEND_COLS == [
        'ts_code', 'end_date', 'ann_date', 'div_proc', 'stk_div',
        'stk_bo_rate', 'stk_co_rate', 'cash_div', 'cash_div_tax',
        'record_date', 'ex_date', 'pay_date', 'div_listdate',
        'imp_ann_date', 'base_date', 'base_share',
    ]
    assert isinstance(DIVIDEND_SPEC, TableSpec)
    assert not isinstance(DIVIDEND_SPEC, DailyTableSpec)
    assert DIVIDEND_SPEC.columns == DIVIDEND_COLS
    assert DIVIDEND_SPEC.path_parts == ('stock', 'financial', 'dividend')
    assert DIVIDEND_SPEC.first_date == '20100101'
    assert tuple(
        table for table in IPO_SCORE_TABLES
        if table in {'fina_audit', 'fina_indicator', 'dividend'}
    ) == ('fina_audit', 'fina_indicator', 'dividend')
    assert 'dividend' not in IPO_SCORE_DATED_TABLES
    assert 'cashflow' not in IPO_SCORE_TABLES

    fetcher = Mock()
    job, = build_jobs(cfg, fetcher)
    assert type(job) is TickerSyncJob
    assert job.table_name == 'dividend' and job.supports_date_range
    assert job.first_date == '20100101' and job.overlap_days == 0
    assert job.fetch_range is fetcher.fetch_dividend
    assert job.merge is merge_dividend
    assert job.get_last_date is get_dividend_last_date
    assert job.get_tickers.func is get_fina_audit_tickers
    assert isinstance(job.read_existing.__self__, TickerPartitionStore)
    assert job.read_existing.__self__ is job.write_ticker.__self__
    with pytest.raises(RuntimeError, match='basic'):
        job.get_tickers()
    write_basic(cfg.data_dir)
    assert job.get_tickers() == CODES


def test_fetch_only_ticker_and_explicit_fields_regardless_of_callback_dates(api):
    client, fetcher = api
    raw = frame(dividend(), dividend(div_proc='实施', ann_date='20240520',
                                      imp_ann_date='20240520', record_date='20240528',
                                      ex_date='20240529', pay_date='20240529'))
    client.dividend.return_value = raw[DIVIDEND_COLS[::-1]]
    first = fetcher.fetch_dividend('000001.SZ', '20100101', '20201231')
    second = fetcher.fetch_dividend('000001.SZ', '20250101', '20261001')
    assert client.dividend.call_args_list == [
        call(ts_code='000001.SZ', fields=','.join(DIVIDEND_COLS)),
        call(ts_code='000001.SZ', fields=','.join(DIVIDEND_COLS)),
    ]
    pd.testing.assert_frame_equal(first, second)
    assert list(first) == DIVIDEND_COLS
    assert first.div_proc.tolist() == ['预案', '实施']
    assert first.cash_div_tax.tolist() == [0.12, 0.12]
    assert first.cash_div.tolist() == [0.1, 0.1]
    assert first.loc[1, 'record_date'] == '20240528'


@pytest.mark.parametrize('empty', [None, pd.DataFrame()])
def test_empty_fetch_has_schema(api, empty):
    client, fetcher = api
    client.dividend.return_value = empty
    result = fetcher.fetch_dividend('600000.SH', '20100101', '20261001')
    assert result.empty and list(result) == DIVIDEND_COLS
    client.dividend.assert_called_once_with(ts_code='600000.SH', fields=','.join(DIVIDEND_COLS))


def test_fetch_aligns_missing_optional_columns_and_normalizes_dates(api):
    client, fetcher = api
    client.dividend.return_value = frame(dividend(ann_date=pd.Timestamp('2024-04-30'))).drop(columns=['base_date', 'base_share'])
    result = fetcher.fetch_dividend('000001.SZ', '20100101', '20261001')
    assert result.ann_date.iloc[0] == '20240430'
    assert result.base_date.isna().all() and result.base_share.isna().all()
    assert list(result) == DIVIDEND_COLS


def test_saturated_single_request_fails_instead_of_saving_truncated_history(api):
    client, fetcher = api
    client.dividend.return_value = pd.concat([frame(dividend())] * 2000, ignore_index=True)
    with pytest.raises(RuntimeError, match='2000-row truncation'):
        fetcher.fetch_dividend('000001.SZ', '20100101', '20261001')


def test_last_date_never_anchors_full_history():
    assert get_dividend_last_date(pd.DataFrame()) is None
    assert get_dividend_last_date(frame(dividend(ann_date='20261001', ex_date='20261002'))) is None


@pytest.mark.parametrize('change', [
    {'ann_date': '20240501'}, {'div_proc': '决案'},
    {'record_date': '20240528'}, {'ex_date': '20240529'},
    {'pay_date': '20240603'}, {'imp_ann_date': '20240520'},
])
def test_distinct_identity_parts_are_retained(change):
    result = merge_dividend(frame(dividend()), frame(dividend(**change)))
    assert len(result) == 2
    assert result[DIVIDEND_KEY].drop_duplicates().shape[0] == 2


def test_updated_content_wins_without_becoming_identity():
    existing = frame(dividend())
    fetched = frame(dividend(
        cash_div=0.14, cash_div_tax=0.18, stk_div=0.3,
        stk_bo_rate=0.2, stk_co_rate=0.1, div_listdate='20240601',
        base_date='20231231', base_share=1234.0,
    ))
    result = merge_dividend(existing, fetched)
    assert len(result) == 1
    for column in ('cash_div', 'cash_div_tax', 'stk_div', 'stk_bo_rate',
                   'stk_co_rate', 'div_listdate', 'base_date', 'base_share'):
        assert result.loc[0, column] == fetched.loc[0, column]
    pd.testing.assert_frame_equal(merge_dividend(result, fetched), result)
    assert list(result) == DIVIDEND_COLS


def test_all_stages_and_dates_survive_stable_sort():
    rows = [
        dividend(div_proc='实施', ann_date='20240520', imp_ann_date='20240520',
                 record_date='20240528', ex_date='20240529', pay_date='20240530'),
        dividend(),
        dividend(div_proc='决案', ann_date='20240502'),
        dividend(end_date='20221231'),
    ]
    result = merge_dividend(pd.DataFrame(), frame(*rows))
    assert result.end_date.tolist() == ['20221231', '20231231', '20231231', '20231231']
    assert result.div_proc.tolist() == ['预案', '预案', '决案', '实施']
    assert result.loc[3, ['ann_date', 'imp_ann_date', 'record_date', 'ex_date', 'pay_date']].tolist() == [
        '20240520', '20240520', '20240528', '20240529', '20240530',
    ]


def test_missing_date_keys_are_idempotent_across_parquet(cfg):
    old = frame(dividend(record_date=None, ex_date=None, pay_date=None, imp_ann_date=None))
    new = frame(dividend(record_date=float('nan'), ex_date=pd.NA, pay_date=None,
                         imp_ann_date=float('nan'), cash_div_tax=0.2))
    result = merge_dividend(old, new)
    assert len(result) == 1 and result.cash_div_tax.iloc[0] == 0.2
    store = TickerPartitionStore(cfg.data_dir/'stock/financial/dividend')
    store.write('000001.SZ', result)
    for _ in range(2):
        result = merge_dividend(store.read('000001.SZ'), new)
        assert len(result) == 1
        store.write('000001.SZ', result)
    assert store.read('000001.SZ').cash_div_tax.iloc[0] == 0.2
    assert merge_dividend(pd.DataFrame(), pd.DataFrame()).empty


def test_pipeline_full_history_replay_and_storage(cfg, api):
    client, fetcher = api
    write_basic(cfg.data_dir, ['000001.SZ'])
    client.dividend.return_value = frame(dividend())
    with Pipeline(cfg, DataSources(tushare=fetcher), Mock()) as pipeline:
        job = pipeline.registry['dividend']
        job.ticker_sleep = 0
        pipeline._runtime.calendar._today_fn = lambda: '20261001'
        pipeline.run('dividend')
        path = cfg.data_dir/'stock/financial/dividend/ts_code=000001.SZ/data.parquet'
        assert path.exists()
        assert len(job.read_existing('000001.SZ')) == 1
        client.dividend.return_value = frame(dividend(cash_div_tax=0.2), dividend(
            div_proc='实施', ann_date='20240520', imp_ann_date='20240520',
            record_date='20240528', ex_date='20240529', pay_date='20240530',
        ))
        pipeline.run('dividend')
        after = job.read_existing('000001.SZ')
        assert len(after) == 2 and after.cash_div_tax.iloc[0] == 0.2
        pipeline.run('dividend')
        pd.testing.assert_frame_equal(job.read_existing('000001.SZ'), after)
        assert client.dividend.call_args_list == [
            call(ts_code='000001.SZ', fields=','.join(DIVIDEND_COLS))
        ] * 3
        assert pipeline._runtime.meta.get_last_date('dividend') is None


def test_cli_accepts_dividend_dates_but_full_fetch_ignores_them():
    with patch('zer0share.cli._make_pipeline') as factory:
        result = CliRunner().invoke(cli, [
            'sync', '--table', 'dividend', '--start-date', '20100101',
            '--end-date', '20261001',
        ])
        assert result.exit_code == 0, result.output
        factory.return_value.__enter__.return_value.run.assert_called_once_with(
            'dividend', start_date='20100101', end_date='20261001',
        )
