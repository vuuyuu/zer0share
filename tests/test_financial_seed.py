from collections.abc import Callable
from unittest.mock import Mock, patch

import pandas as pd
import pytest
from click.testing import CliRunner

from zer0share.cli import cli
from zer0share.schema import (
    BALANCESHEET_COLS,
    CASHFLOW_COLS,
    DIVIDEND_COLS,
    FINA_AUDIT_COLS,
    FINA_INDICATOR_COLS,
    INCOME_COLS,
)
from zer0share.seed import (
    FINANCIAL_TABLES,
    FinancialSeedRunner,
    FinancialSeedState,
    SeedTaskStatus,
    classify_error,
    read_manifest,
)
from zer0share.sync._jobs import TickerSyncJob
from zer0share.sync.balancesheet import merge_balancesheet
from zer0share.sync.cashflow import merge_cashflow
from zer0share.sync.dividend import merge_dividend
from zer0share.sync.fina_audit import merge_fina_audit
from zer0share.sync.fina_indicator import merge_fina_indicator
from zer0share.sync.income import merge_income


def write_manifest(tmp_path, contents: str) -> object:
    path = tmp_path / "tickers.txt"
    path.write_text(contents, encoding="utf-8")
    return read_manifest(path)


def build_jobs(
    behavior: Callable[[str, str, str], object] | None = None,
    ticker_sleep: float = 0,
) -> dict[str, TickerSyncJob]:
    behavior = behavior or (lambda ticker, start, end: pd.DataFrame({"ticker": [ticker]}))
    jobs = {}
    for table in FINANCIAL_TABLES:
        jobs[table] = TickerSyncJob(
            table_name=table,
            get_tickers=lambda: [],
            fetch_range=behavior,
            read_existing=lambda ticker: pd.DataFrame(),
            write_ticker=lambda ticker, frame: None,
            merge=lambda existing, fetched: fetched,
            first_date="20100101",
            get_last_date=lambda existing: None,
            ticker_sleep=ticker_sleep,
        )
    return jobs


def task_for(state: FinancialSeedState, run_id: str, table: str, ticker: str):
    return next(task for task in state.eligible_tasks(run_id) if task.table_name == table and task.ticker == ticker)


def test_manifest_ignores_comments_deduplicates_sorts_and_hashes_canonical_tickers(tmp_path):
    first = write_manifest(tmp_path, "# policy universe\n600036.SH\n\n000001.SZ\n600036.SH\n")
    second_path = tmp_path / "same-tickers.txt"
    second_path.write_text("000001.SZ\n600036.SH\n", encoding="utf-8")
    second = read_manifest(second_path)

    assert first.tickers == ("000001.SZ", "600036.SH")
    assert first.sha256 == second.sha256


def test_init_creates_exactly_one_pending_task_per_ticker_and_table(tmp_path):
    manifest = write_manifest(tmp_path, "\n".join(f"{i:06d}.SZ" for i in range(10)))
    with FinancialSeedState(tmp_path / "state.sqlite") as state:
        run_id = state.create_run(manifest, FINANCIAL_TABLES, "20100101", "20250630")
        summary = state.summary(run_id)
        tasks = state.eligible_tasks(run_id)

    assert len(tasks) == 60
    assert summary["total"] == {
        "PENDING": 60, "RUNNING": 0, "SUCCESS": 0, "VALID_EMPTY": 0, "FAILED": 0,
    }
    assert {(task.table_name, task.ticker) for task in tasks} == {
        (table, ticker) for table in FINANCIAL_TABLES for ticker in manifest.tickers
    }


def test_runner_recovers_running_task_and_skips_terminal_states_unless_failed_retry_is_requested(tmp_path):
    manifest = write_manifest(tmp_path, "000001.SZ\n000002.SZ\n000003.SZ\n000004.SZ\n000005.SZ\n")
    calls: list[str] = []
    jobs = build_jobs(lambda ticker, start, end: calls.append(ticker) or pd.DataFrame({"value": [ticker]}))
    with FinancialSeedState(tmp_path / "state.sqlite") as state:
        run_id = state.create_run(manifest, ("income",), "20100101", "20250630")
        state.finish(task_for(state, run_id, "income", "000001.SZ"), SeedTaskStatus.SUCCESS)
        state.finish(task_for(state, run_id, "income", "000002.SZ"), SeedTaskStatus.VALID_EMPTY)
        state.finish(task_for(state, run_id, "income", "000003.SZ"), SeedTaskStatus.FAILED, error_type="API_ERROR")
        state.mark_running(task_for(state, run_id, "income", "000005.SZ"))

        result = FinancialSeedRunner(state, jobs).run(run_id)
        retry = FinancialSeedRunner(state, jobs).run(run_id, retry_failed=True)
        summary = state.summary(run_id)

    assert calls == ["000004.SZ", "000005.SZ", "000003.SZ"]
    assert (result.attempted, result.success, result.valid_empty, result.failed) == (2, 2, 0, 0)
    assert (retry.attempted, retry.success, retry.valid_empty, retry.failed) == (1, 1, 0, 0)
    assert summary["total"] == {
        "PENDING": 0, "RUNNING": 0, "SUCCESS": 4, "VALID_EMPTY": 1, "FAILED": 0,
    }


def test_failed_task_retries_only_when_requested_and_fetch_empty_fails_closed(tmp_path):
    manifest = write_manifest(tmp_path, "000001.SZ\n000002.SZ\n")
    calls: list[str] = []

    def fetch(ticker, start, end):
        calls.append(ticker)
        return pd.DataFrame() if ticker == "000001.SZ" else pd.DataFrame({"value": [ticker]})

    with FinancialSeedState(tmp_path / "state.sqlite") as state:
        run_id = state.create_run(manifest, ("income",), "20100101", "20250630")
        first = FinancialSeedRunner(state, build_jobs(fetch)).run(run_id)
        second = FinancialSeedRunner(state, build_jobs(fetch)).run(run_id)
        third = FinancialSeedRunner(state, build_jobs(lambda *_: pd.DataFrame({"value": [1]}))).run(
            run_id, retry_failed=True
        )
        summary = state.summary(run_id)

    assert (first.success, first.failed) == (1, 1)
    assert second.attempted == 0
    assert (third.success, third.failed) == (1, 0)
    assert calls == ["000001.SZ", "000002.SZ"]
    assert summary["total"] == {
        "PENDING": 0, "RUNNING": 0, "SUCCESS": 2, "VALID_EMPTY": 0, "FAILED": 0,
    }


def test_manifest_mismatch_and_task_filter_limit_are_fail_closed(tmp_path):
    manifest = write_manifest(tmp_path, "000001.SZ\n000002.SZ\n")
    changed = write_manifest(tmp_path, "000001.SZ\n000003.SZ\n")
    with FinancialSeedState(tmp_path / "state.sqlite") as state:
        run_id = state.create_run(manifest, ("income", "cashflow"), "20100101", "20250630")
        with pytest.raises(ValueError, match="manifest hash"):
            state.verify_manifest(run_id, changed)
        result = FinancialSeedRunner(state, build_jobs()).run(run_id, table_name="income", limit=1)
        summary = state.summary(run_id)

    assert result.attempted == 1
    assert summary["total"]["SUCCESS"] == 1
    assert summary["total"]["PENDING"] == 3


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        (RuntimeError("Tushare API unavailable"), "API_ERROR"),
        (RuntimeError("429 rate limit"), "RATE_LIMIT"),
        (KeyError("missing expected column"), "SCHEMA_ERROR"),
        (RuntimeError("pyarrow parquet write failed"), "SERIALIZATION_ERROR"),
        (OSError("disk full"), "WRITE_ERROR"),
        (RuntimeError("unexpected"), "UNKNOWN_ERROR"),
    ],
)
def test_error_classification(error, expected):
    assert classify_error(error) == expected


def test_runner_records_failure_and_preserves_inter_ticker_sleep(tmp_path):
    manifest = write_manifest(tmp_path, "000001.SZ")
    with FinancialSeedState(tmp_path / "state.sqlite") as state:
        run_id = state.create_run(manifest, ("income",), "20100101", "20250630")
        with patch("zer0share.seed.runner.time.sleep") as sleep:
            result = FinancialSeedRunner(
                state,
                build_jobs(lambda *_: (_ for _ in ()).throw(RuntimeError("Tushare API broken")), ticker_sleep=0.2),
            ).run(run_id)
        failed = state.failed_tasks(run_id)

    assert (result.success, result.failed) == (0, 1)
    assert [call.args for call in sleep.call_args_list] == [(5,), (15,), (45,), (0.2,)]
    assert failed == [{
        "table_name": "income", "ticker": "000001.SZ", "error_type": "API_ERROR",
        "error_message": "Tushare API broken",
    }]


def test_replaying_each_financial_job_uses_existing_merge_key_without_duplicate_rows():
    fixtures = {
        "income": (INCOME_COLS, merge_income, {"f_ann_date": "20250430", "report_type": "1", "comp_type": "1", "update_flag": "0"}),
        "balancesheet": (BALANCESHEET_COLS, merge_balancesheet, {"f_ann_date": "20250430", "report_type": "1", "comp_type": "1", "update_flag": "0"}),
        "cashflow": (CASHFLOW_COLS, merge_cashflow, {"f_ann_date": "20250430", "report_type": "1", "comp_type": "1", "update_flag": "0"}),
        "fina_indicator": (FINA_INDICATOR_COLS, merge_fina_indicator, {"update_flag": "0"}),
        "dividend": (DIVIDEND_COLS, merge_dividend, {"div_proc": "实施", "record_date": "20250501", "ex_date": "20250502", "pay_date": "20250503", "imp_ann_date": "20250430"}),
        "fina_audit": (FINA_AUDIT_COLS, merge_fina_audit, {"audit_result": "标准无保留意见", "audit_agency": "test", "audit_sign": "test"}),
    }
    for table, (columns, merge, extra) in fixtures.items():
        row = {column: None for column in columns}
        row.update({"ts_code": "000001.SZ", "end_date": "20241231", "ann_date": "20250430", **extra})
        fetched = pd.DataFrame([row], columns=columns)
        written: dict[str, pd.DataFrame] = {}
        job = TickerSyncJob(
            table_name=table,
            get_tickers=lambda: [],
            fetch_range=lambda *_: fetched.copy(),
            read_existing=lambda ticker: written.get(ticker, pd.DataFrame(columns=columns)),
            write_ticker=lambda ticker, frame: written.__setitem__(ticker, frame),
            merge=merge,
            first_date="20100101",
            get_last_date=lambda existing: None,
            ticker_sleep=0,
        )

        assert job._sync_ticker("000001.SZ", "20100101", "20250630") == "SUCCESS"
        assert job._sync_ticker("000001.SZ", "20100101", "20250630") == "SUCCESS"
        assert len(written["000001.SZ"]) == 1, table


def test_seed_financial_init_and_status_use_config_data_directory(tmp_path):
    manifest_path = tmp_path / "tickers.txt"
    manifest_path.write_text("600036.SH\n000001.SZ\n", encoding="utf-8")
    config = Mock(data_dir=tmp_path / "data")
    with patch("zer0share.cli.load_config", return_value=config):
        result = CliRunner().invoke(cli, [
            "seed-financial", "init", "--manifest", str(manifest_path),
            "--tables", "income,dividend", "--start-date", "20100101", "--end-date", "20250630",
        ])
        assert result.exit_code == 0, result.output
        run_id = result.output.split("run_id=", 1)[1].split()[0]
        with FinancialSeedState(config.data_dir / "ops" / "financial_seed_state.sqlite") as state:
            task = task_for(state, run_id, "income", "000001.SZ")
            state.finish(task, SeedTaskStatus.FAILED, error_type="API_ERROR", error_message="test failure")
        status = CliRunner().invoke(cli, ["seed-financial", "status", "--run-id", run_id, "--show-failed"])

    assert status.exit_code == 0, status.output
    assert "PENDING=3" in status.output
    assert "FAILED=1" in status.output
    assert "income:" in status.output
    assert "dividend:" in status.output
    assert "FAILED income 000001.SZ API_ERROR: test failure" in status.output
