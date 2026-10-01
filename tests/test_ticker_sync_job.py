import weakref
from types import SimpleNamespace
from unittest.mock import Mock, call, patch

import pandas as pd
import pytest

from zer0share.sync._jobs import SyncJob, TickerSyncJob


TICKERS = ["000001.SZ", "000333.SZ", "600036.SH"]


@pytest.fixture(autouse=True)
def sleep():
    with patch("zer0share.sync._jobs.time.sleep") as mocked:
        yield mocked


@pytest.fixture
def rt():
    return SimpleNamespace(
        calendar=SimpleNamespace(today=Mock(return_value="20240531")),
        meta=Mock(),
        notifier=Mock(),
    )


@pytest.fixture
def job():
    return TickerSyncJob(
        table_name="fake_history",
        get_tickers=Mock(return_value=TICKERS),
        fetch_range=Mock(side_effect=lambda ticker, start, end: pd.DataFrame({"value": [ticker]})),
        read_existing=Mock(side_effect=lambda ticker: pd.DataFrame()),
        write_ticker=Mock(),
        merge=Mock(side_effect=lambda existing, fetched: fetched),
        first_date="20050101",
        get_last_date=Mock(return_value=None),
    )


def test_explicit_range_and_independent_ticker_execution(job, rt, sleep):
    result = job.run(rt, "20240101", "20240301")

    assert isinstance(job, SyncJob)
    assert job.supports_date_range
    job.get_tickers.assert_called_once_with()
    assert job.fetch_range.call_args_list == [
        call(ticker, "20240101", "20240301") for ticker in TICKERS
    ]
    assert job.read_existing.call_args_list == [call(ticker) for ticker in TICKERS]
    assert [c.args[0] for c in job.write_ticker.call_args_list] == TICKERS
    assert job.merge.call_count == 3
    assert (result.total, result.success, result.empty, result.failed) == (3, 3, 0, 0)
    assert result.failed_tickers == []
    assert result.complete
    assert sleep.call_args_list == [call(0.2), call(0.2)]
    rt.calendar.today.assert_not_called()
    assert rt.meta.mock_calls == []


def test_first_sync_uses_dataset_first_date_and_runtime_today(job, rt):
    result = job.run(rt)
    assert result.complete
    assert job.fetch_range.call_args_list == [
        call(ticker, "20050101", "20240531") for ticker in TICKERS
    ]
    job.get_last_date.assert_not_called()
    rt.calendar.today.assert_called_once_with()


@pytest.mark.parametrize(
    "last_date, overlap, expected",
    [
        ("20240430", 120, "20240101"),
        ("20240301", 2, "20240228"),
        ("20240430", 0, "20240430"),
        (None, 120, "20050101"),
        ("20050102", 2, "20041231"),
    ],
)
def test_existing_history_uses_generic_date_hook(job, rt, last_date, overlap, expected):
    existing = pd.DataFrame({"dataset_date": [last_date], "value": [1]})
    job.read_existing.side_effect = None
    job.read_existing.return_value = existing
    job.get_last_date.side_effect = lambda frame: frame["dataset_date"].iloc[0]
    job.overlap_days = overlap

    result = job.run(rt, end_date="20240501")

    assert result.complete
    assert job.fetch_range.call_args_list == [
        call(ticker, expected, "20240501") for ticker in TICKERS
    ]
    assert job.get_last_date.call_count == 3
    assert all(c.args[0] is existing for c in job.get_last_date.call_args_list)
    rt.calendar.today.assert_not_called()


def test_explicit_start_overrides_overlap(job, rt):
    existing = pd.DataFrame({"dataset_date": ["20240430"]})
    job.read_existing.side_effect = None
    job.read_existing.return_value = existing
    job.get_last_date.side_effect = AssertionError("explicit start must bypass date hook")

    assert job.run(rt, start_date="20240201").complete
    assert job.fetch_range.call_args_list == [
        call(ticker, "20240201", "20240531") for ticker in TICKERS
    ]
    job.get_last_date.assert_not_called()
    assert all(c.args[0] is existing for c in job.merge.call_args_list)


def test_empty_fetch_is_not_failure_and_does_not_merge_or_write(job, rt, sleep):
    job.fetch_range.side_effect = lambda *args: pd.DataFrame()
    result = job.run(rt)

    assert (result.success, result.empty, result.failed) == (0, 3, 0)
    assert result.complete
    assert result.failed_tickers == []
    job.merge.assert_not_called()
    job.write_ticker.assert_not_called()
    assert job.fetch_range.call_count == 3
    assert sleep.call_args_list == [call(0.2), call(0.2)]


@pytest.mark.parametrize("failures", [1, 2, 3])
def test_transient_fetch_failure_retries_then_succeeds(job, rt, sleep, failures):
    frame = pd.DataFrame({"value": [1]})
    job.get_tickers.return_value = TICKERS[:1]
    job.fetch_range.side_effect = [RuntimeError("temporary")] * failures + [frame]

    result = job.run(rt)

    assert (result.success, result.empty, result.failed) == (1, 0, 0)
    assert result.complete
    assert job.fetch_range.call_args_list == [
        call(TICKERS[0], "20050101", "20240531")
    ] * (failures + 1)
    assert sleep.call_args_list == [call(delay) for delay in (5, 15, 45)[:failures]]
    job.read_existing.assert_called_once_with(TICKERS[0])
    job.merge.assert_called_once()
    job.write_ticker.assert_called_once()


def test_mixed_batch_permanent_failure_does_not_stop_other_tickers(job, rt, sleep):
    def fetch(ticker, start, end):
        if ticker == TICKERS[0]:
            raise RuntimeError("permanent")
        if ticker == TICKERS[1]:
            return pd.DataFrame()
        return pd.DataFrame({"value": [1]})

    job.fetch_range.side_effect = fetch
    with patch("zer0share.sync._jobs.logger") as log:
        result = job.run(rt)

    assert (result.total, result.success, result.empty, result.failed) == (3, 1, 1, 1)
    assert result.failed_tickers == TICKERS[:1]
    assert not result.complete
    assert [c.args[0] for c in job.fetch_range.call_args_list] == [TICKERS[0]] * 4 + TICKERS[1:]
    assert sleep.call_args_list == [call(5), call(15), call(45), call(0.2), call(0.2)]
    job.merge.assert_called_once()
    job.write_ticker.assert_called_once()
    assert job.write_ticker.call_args.args[0] == TICKERS[2]
    log.info.assert_called_once_with("Ticker sync start: table=fake_history tickers=3")
    log.error.assert_called_once_with(
        "Ticker sync failed: table=fake_history ticker=000001.SZ error=permanent"
    )
    assert "success=1 empty=1 failed=1" in log.warning.call_args.args[0]
    assert "failed_tickers=['000001.SZ']" in log.warning.call_args.args[0]
    assert rt.meta.mock_calls == []


@pytest.mark.parametrize("bad_result", [None, "invalid payload"])
def test_non_dataframe_fetch_is_retried_and_failed_not_empty(job, rt, sleep, bad_result):
    job.get_tickers.return_value = TICKERS[:1]
    job.fetch_range.side_effect = None
    job.fetch_range.return_value = bad_result
    result = job.run(rt)
    assert (result.success, result.empty, result.failed) == (0, 0, 1)
    assert not result.complete
    assert job.fetch_range.call_count == 4
    assert sleep.call_args_list == [call(5), call(15), call(45)]
    job.merge.assert_not_called()
    job.write_ticker.assert_not_called()


@pytest.mark.parametrize("stage", ["read_existing", "get_last_date", "merge", "write_ticker"])
def test_callback_failure_is_failed_without_retry_and_batch_continues(job, rt, sleep, stage):
    existing = pd.DataFrame({"value": [1]})
    job.read_existing.side_effect = None
    job.read_existing.return_value = existing
    callback = getattr(job, stage)
    normal_result = None if stage in ("get_last_date", "write_ticker") else existing
    callback.side_effect = [RuntimeError("callback failed"), normal_result, normal_result]

    result = job.run(rt)

    assert (result.success, result.empty, result.failed) == (2, 0, 1)
    assert result.failed_tickers == TICKERS[:1]
    assert not result.complete
    assert callback.call_count == 3
    assert sleep.call_args_list == [call(0.2), call(0.2)]
    expected_fetches = 2 if stage in ("read_existing", "get_last_date") else 3
    assert job.fetch_range.call_count == expected_fetches


def test_merge_receives_original_frames_and_write_receives_merged_frame(job, rt):
    existing = pd.DataFrame({"version": [1]})
    fetched = pd.DataFrame({"version": [2]})
    merged = pd.DataFrame({"version": [1, 2]})
    job.get_tickers.return_value = TICKERS[:1]
    job.read_existing.side_effect = None
    job.read_existing.return_value = existing
    job.fetch_range.side_effect = None
    job.fetch_range.return_value = fetched
    job.merge.side_effect = None
    job.merge.return_value = merged

    assert job.run(rt).complete
    job.merge.assert_called_once()
    assert job.merge.call_args.args[0] is existing
    assert job.merge.call_args.args[1] is fetched
    job.write_ticker.assert_called_once()
    assert job.write_ticker.call_args.args == (TICKERS[0], merged)


def test_immediate_write_and_frames_released_before_next_ticker(job, rt):
    events = []
    frames = []

    def frame():
        value = pd.DataFrame({"value": [1]})
        frames.append(weakref.ref(value))
        return value

    def read(ticker):
        assert all(ref() is None for ref in frames)
        events.append(("read", ticker))
        return frame()

    def fetch(ticker, start, end):
        events.append(("fetch", ticker))
        return frame()

    def merge(existing, fetched):
        events.append(("merge", None))
        return frame()

    def write(ticker, merged):
        assert merged is frames[-1]()
        events.append(("write", ticker))

    # Plain callbacks avoid mock call histories retaining the DataFrames.
    job.read_existing = read
    job.fetch_range = fetch
    job.merge = merge
    job.write_ticker = write
    job.get_last_date = lambda existing: None
    with patch("pandas.concat", side_effect=AssertionError("executor must not concatenate")):
        assert job.run(rt).complete

    assert events == [
        event for ticker in TICKERS
        for event in [("read", ticker), ("fetch", ticker), ("merge", None), ("write", ticker)]
    ]
    assert all(ref() is None for ref in frames)


@pytest.mark.parametrize("ticker_sleep, expected", [(0, []), (0.5, [call(0.5), call(0.5)])])
def test_ticker_sleep_is_configurable(job, rt, sleep, ticker_sleep, expected):
    job.ticker_sleep = ticker_sleep
    job.run(rt)
    assert sleep.call_args_list == expected


def test_empty_universe_returns_complete_zero_counts(job, rt, sleep):
    job.get_tickers.return_value = []
    result = job.run(rt)
    assert (result.total, result.success, result.empty, result.failed) == (0, 0, 0, 0)
    assert result.complete
    job.read_existing.assert_not_called()
    job.fetch_range.assert_not_called()
    sleep.assert_not_called()


def test_universe_failure_propagates(job, rt):
    job.get_tickers.side_effect = RuntimeError("universe unavailable")
    with pytest.raises(RuntimeError, match="universe unavailable"):
        job.run(rt)
    job.fetch_range.assert_not_called()


@pytest.mark.parametrize(
    "start, end", [("20240601", "20240531"), ("20240230", None), (None, "20240230")],
)
def test_invalid_explicit_range_raises_before_loading_universe(job, rt, start, end):
    with pytest.raises(ValueError):
        job.run(rt, start, end)
    job.get_tickers.assert_not_called()


def test_invalid_automatic_range_is_failed_not_empty(job, rt):
    job.get_last_date.return_value = "20260101"
    job.read_existing.side_effect = lambda ticker: pd.DataFrame({"value": [1]})
    result = job.run(rt)
    assert (result.success, result.empty, result.failed) == (0, 0, 3)
    assert result.failed_tickers == TICKERS
    assert not result.complete
    job.fetch_range.assert_not_called()


@pytest.mark.parametrize("kwargs", [{"overlap_days": -1}, {"ticker_sleep": -0.1}])
def test_negative_overlap_or_sleep_rejected(job, kwargs):
    with pytest.raises(ValueError, match="non-negative"):
        TickerSyncJob(
            "fake_history", job.get_tickers, job.fetch_range, job.read_existing,
            job.write_ticker, job.merge, "20050101", job.get_last_date, **kwargs,
        )
