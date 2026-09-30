from types import SimpleNamespace
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from click.testing import CliRunner

from zer0share import dateutil
from zer0share.catalog import FINA_INDICATOR_SPEC
from zer0share.cli import cli
from zer0share.fetcher import TushareFetcher
from zer0share.pipeline import Pipeline
from zer0share.schema import FINA_INDICATOR_COLS
from zer0share.sources import DataSources
from zer0share.storage import SnapshotStore, TickerPartitionStore
from zer0share.sync._jobs import TickerSyncJob
from zer0share.sync.fina_audit import get_fina_audit_tickers
from zer0share.sync.fina_indicator import (
    FINA_INDICATOR_KEY, build_jobs, get_fina_indicator_last_date,
    merge_fina_indicator,
)


def frame(**changes):
    row = dict(ts_code='000001.SZ', end_date='20231231', ann_date='20240430', update_flag='0', roe=10.)
    row.update(changes)
    return pd.DataFrame([row], columns=FINA_INDICATOR_COLS)


@pytest.fixture
def cfg(tmp_path):
    return SimpleNamespace(data_dir=tmp_path, db_path=tmp_path / 'meta.duckdb', ricequant=SimpleNamespace(enabled=False))


@pytest.fixture
def api():
    with patch('zer0share.fetcher.ts.pro_api') as factory:
        yield factory.return_value, TushareFetcher('fake')


@pytest.fixture(autouse=True)
def no_sleep():
    with patch('zer0share.sync._jobs.time.sleep'):
        yield


def test_schema_catalog_and_job(cfg):
    assert len(FINA_INDICATOR_COLS) == len(set(FINA_INDICATOR_COLS)) == 167
    assert FINA_INDICATOR_COLS[:3] == ['ts_code', 'ann_date', 'end_date']
    assert FINA_INDICATOR_COLS[-3:] == ['equity_yoy', 'rd_exp', 'update_flag']
    assert {'fcff', 'fcfe', 'roic', 'netdebt', 'ebit_to_interest'} <= set(FINA_INDICATOR_COLS)
    assert FINA_INDICATOR_SPEC.columns == FINA_INDICATOR_COLS
    assert FINA_INDICATOR_SPEC.path_parts == ('stock', 'financial', 'fina_indicator')
    fetcher = Mock()
    job, = build_jobs(cfg, fetcher)
    assert type(job) is TickerSyncJob
    assert job.table_name == 'fina_indicator'
    assert job.supports_date_range
    assert job.first_date == '20100101' and job.overlap_days == 1825
    assert job.get_tickers.func is get_fina_audit_tickers
    assert job.fetch_range is fetcher.fetch_fina_indicator
    assert job.merge is merge_fina_indicator
    assert job.get_last_date is get_fina_indicator_last_date
    assert isinstance(job.read_existing.__self__, TickerPartitionStore)
    assert job.read_existing.__self__ is job.write_ticker.__self__
    with pytest.raises(RuntimeError, match='basic'):
        job.get_tickers()


def test_fetch_schema_fields_and_missing_columns(api):
    client, fetcher = api
    client.fina_indicator.return_value = frame().iloc[:, ::-1].drop(columns='rd_exp')
    result = fetcher.fetch_fina_indicator('000001.SZ', '20230101', '20241231')
    client.fina_indicator.assert_called_once_with(ts_code='000001.SZ', start_date='20230101', end_date='20241231', fields=','.join(FINA_INDICATOR_COLS))
    assert list(result) == FINA_INDICATOR_COLS
    assert result.rd_exp.isna().all()
    assert result.roe.iloc[0] == 10


@pytest.mark.parametrize('empty', [None, pd.DataFrame()])
def test_empty_schema(api, empty):
    client, fetcher = api
    client.fina_indicator.return_value = empty
    result = fetcher.fetch_fina_indicator('000001.SZ', '20230101', '20241231')
    assert result.empty and list(result) == FINA_INDICATOR_COLS


def test_five_year_chunks_with_empty_and_concat(api):
    client, fetcher = api
    client.fina_indicator.side_effect = [frame(end_date='20141231'), None, frame(end_date='20241231'), frame(end_date='20260630')]
    result = fetcher.fetch_fina_indicator('000001.SZ', '20100101', '20261001')
    bounds = [(c.kwargs['start_date'], c.kwargs['end_date']) for c in client.fina_indicator.call_args_list]
    assert bounds == [('20100101','20141231'),('20150101','20191231'),('20200101','20241231'),('20250101','20261001')]
    for left, right in zip(bounds, bounds[1:]):
        assert dateutil.add_days(left[1], 1) == right[0]
    assert result.end_date.tolist() == ['20141231', '20241231', '20260630']
    assert all(c.kwargs['fields'] == ','.join(FINA_INDICATOR_COLS) for c in client.fina_indicator.call_args_list)


def test_chunk_leap_day_and_invalid_range(api):
    client, fetcher = api
    client.fina_indicator.return_value = None
    fetcher.fetch_fina_indicator('000001.SZ', '20200229', '20250301')
    assert [(c.kwargs['start_date'], c.kwargs['end_date']) for c in client.fina_indicator.call_args_list] == [('20200229','20241231'),('20250101','20250301')]
    with pytest.raises(ValueError):
        fetcher.fetch_fina_indicator('000001.SZ', '20250301', '20200229')


def test_saturated_chunk_is_split_and_total_can_exceed_100(api):
    client, fetcher = api
    client.fina_indicator.side_effect = [pd.concat([frame()] * 100), pd.concat([frame()] * 60), pd.concat([frame()] * 60)]
    result = fetcher.fetch_fina_indicator('000001.SZ', '20200101', '20241231')
    assert len(result) == 120  # No final PIT dedup in the fetcher.
    calls = client.fina_indicator.call_args_list
    assert calls[1].kwargs['start_date'] == '20200101'
    assert dateutil.add_days(calls[1].kwargs['end_date'], 1) == calls[2].kwargs['start_date']
    assert calls[2].kwargs['end_date'] == '20241231'


def test_saturated_single_day_fails(api):
    client, fetcher = api
    client.fina_indicator.return_value = pd.concat([frame()] * 100)
    with pytest.raises(RuntimeError, match='100-row limit'):
        fetcher.fetch_fina_indicator('000001.SZ', '20231231', '20231231')


def test_merge_versions_new_wins_and_stable_sort():
    old = pd.concat([frame(), frame(ann_date='20240501'), frame(end_date='20221231')])
    new = pd.concat([frame(roe=20.), frame(update_flag='1')])
    result = merge_fina_indicator(old, new)
    assert len(result) == 4
    assert result.roe.tolist() == [10.,20.,10.,10.]
    assert result.update_flag.tolist() == ['0','0','1','0']
    assert list(result) == FINA_INDICATOR_COLS  # No fabricated revision timestamp.
    pd.testing.assert_frame_equal(merge_fina_indicator(result, new), result)


@pytest.mark.parametrize('column', FINA_INDICATOR_KEY)
def test_missing_key_normalization_survives_replay(column, tmp_path):
    old = frame(**{column: None})
    new = pd.concat([frame(**{column: float('nan')}, roe=20.), frame(**{column: pd.NA}, roe=30.)])
    result = merge_fina_indicator(old, new)
    assert len(result) == 1 and result.roe.iloc[0] == 30.
    store = TickerPartitionStore(tmp_path)
    store.write('000001.SZ', result)
    pd.testing.assert_frame_equal(merge_fina_indicator(store.read('000001.SZ'), new), result)


def test_numeric_and_string_flags_are_stable():
    result = merge_fina_indicator(frame(update_flag=1.), frame(update_flag='1', roe=20.))
    assert len(result) == 1 and result.roe.iloc[0] == 20.
    assert merge_fina_indicator(pd.DataFrame(), pd.DataFrame()).empty


@pytest.mark.parametrize('existing, expected', [
    (pd.DataFrame(), None), (frame(end_date=None), None),
    (pd.concat([frame(end_date=None), frame(), frame(end_date='20240630', ann_date='20200101')]), '20240630'),
])
def test_last_date_uses_report_period(existing, expected):
    assert get_fina_indicator_last_date(existing) == expected


def test_pipeline_initial_incremental_explicit_and_retry(cfg, api):
    client, fetcher = api
    code = '000001.SZ'
    SnapshotStore(cfg.data_dir / 'stock/basic/data.parquet').write(pd.DataFrame({'ts_code':[code], 'market':['主板'], 'symbol':['000001']}))
    with Pipeline(cfg, DataSources(tushare=fetcher), Mock()) as pipeline:
        job = pipeline.registry['fina_indicator']
        # A second-chunk failure must replay the entire ticker fetch.
        client.fina_indicator.side_effect = [None, RuntimeError('temporary'), None, None, frame()]
        pipeline.run('fina_indicator', end_date='20240531')
        starts = [c.kwargs['start_date'] for c in client.fina_indicator.call_args_list]
        assert starts == ['20100101','20150101','20100101','20150101','20200101']
        path = cfg.data_dir / 'stock/financial/fina_indicator/ts_code=000001.SZ/data.parquet'
        assert path.exists() and len(job.read_existing(code)) == 1
        client.fina_indicator.reset_mock()
        client.fina_indicator.side_effect = None
        client.fina_indicator.return_value = frame(roe=20.)
        pipeline.run('fina_indicator', end_date='20240531')
        assert client.fina_indicator.call_args_list[0].kwargs['start_date'] == dateutil.add_days('20231231', -1825)
        result = job.read_existing(code)
        assert len(result) == 1 and result.roe.iloc[0] == 20.
        pipeline.run('fina_indicator', end_date='20240531')
        pd.testing.assert_frame_equal(job.read_existing(code), result)
        client.fina_indicator.reset_mock()
        pipeline.run('fina_indicator', '20240101', '20240531')
        assert client.fina_indicator.call_args.kwargs['start_date'] == '20240101'
        assert pipeline._runtime.meta.get_last_date('fina_indicator') is None


def test_cli_explicit_dates():
    with patch('zer0share.cli._make_pipeline') as factory:
        result = CliRunner().invoke(cli, ['sync','--table','fina_indicator','--start-date','20100101','--end-date','20240531'])
        assert result.exit_code == 0, result.output
        factory.return_value.__enter__.return_value.run.assert_called_once_with('fina_indicator', start_date='20100101', end_date='20240531')
