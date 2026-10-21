from contextlib import contextmanager
from pathlib import Path
import plistlib
import subprocess
import sys
import time
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.install_macos_sync import agent_config
from zer0share import macos_sync as task


def calendar(root, dates=("20261020", "20261019", "20261018"), flags=(True, True, False)):
    path = root / "data" / "stock" / "trade_cal" / "exchange=SSE" / "data.parquet"
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.table({"cal_date": dates, "is_open": flags}), path)


@pytest.fixture
def root(tmp_path, monkeypatch):
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "settings.toml").write_text(
        '[paths]\ndata_dir="data"\ndb_path="db/meta.duckdb"\n'
    )
    calendar(tmp_path)
    monkeypatch.setattr(task, "ROOT", tmp_path)
    monkeypatch.setattr(task, "today", lambda: "20261020")
    return tmp_path


def test_calendar_distinguishes_trading_day_holiday_and_missing_date(root):
    assert task.is_trading_day(root / "data", "20261020")
    assert not task.is_trading_day(root / "data", "20261018")
    with pytest.raises(RuntimeError, match="未唯一覆盖"):
        task.is_trading_day(root / "data", "20270101")


def test_missing_duplicate_and_invalid_calendar_do_not_silently_skip(root):
    with pytest.raises(RuntimeError, match="不存在"):
        task.is_trading_day(root / "missing", "20261020")
    calendar(root, ("20261020", "20261020"), (True, True))
    with pytest.raises(RuntimeError, match="未唯一覆盖"):
        task.is_trading_day(root / "data", "20261020")
    calendar(root, ("20261020",), (None,))
    with pytest.raises(RuntimeError, match="无效"):
        task.is_trading_day(root / "data", "20261020")


@pytest.mark.parametrize("group", ["stock", "finance"])
def test_check_only_does_not_lock_or_download(root, monkeypatch, group):
    lock = MagicMock(side_effect=AssertionError("must not lock"))
    run = MagicMock(side_effect=AssertionError("must not download"))
    monkeypatch.setattr(task, "sync_lock", lock)
    monkeypatch.setattr(task, "run_command", run)
    assert task.main([group, "--uv", sys.executable, "--check-only"]) == 0
    assert not (root / "logs").exists()


def test_nontrading_day_skips_even_when_uv_is_not_available(root, monkeypatch):
    monkeypatch.setattr(task, "today", lambda: "20261018")
    run = MagicMock()
    monkeypatch.setattr(task, "run_command", run)
    assert task.main(["stock", "--uv", "/missing/uv"]) == 0
    run.assert_not_called()


@pytest.mark.parametrize("group", ["stock", "finance"])
def test_trading_day_runs_exact_cli_from_project_and_propagates_exit_code(root, monkeypatch, group):
    run = MagicMock(return_value=7)
    monkeypatch.setattr(task, "run_command", run)
    assert task.main([group, "--uv", sys.executable]) == 7
    command = [sys.executable, "run", "python", "main.py", "sync", f"--{group}"]
    if group == "finance":
        command += ["--end-date", "20261019"]
    run.assert_called_once_with(command, root)


def test_missing_calendar_aborts_without_running_sync(root, monkeypatch):
    monkeypatch.setattr(task, "today", lambda: "20270101")
    run = MagicMock()
    monkeypatch.setattr(task, "run_command", run)
    assert task.main(["finance", "--uv", sys.executable]) == 1
    run.assert_not_called()


def test_cross_day_wait_skips_without_running_old_task(root, monkeypatch):
    monkeypatch.setattr(task, "wait_for_database", MagicMock(side_effect=task.ExpiredTask("crossed day")))
    run = MagicMock()
    monkeypatch.setattr(task, "run_command", run)
    assert task.main(["finance", "--uv", sys.executable]) == 0
    run.assert_not_called()


def test_wait_timeout_is_reported(root):
    with pytest.raises(TimeoutError):
        task.check_wait("20261020", time.monotonic() - 1)
    with pytest.raises(task.ExpiredTask):
        task.check_wait("20261019", time.monotonic() + 10)


def test_mutex_excludes_another_process_and_releases_after_job(root):
    path = root / "logs" / "launchd" / "sync.lock"
    source = (
        'import fcntl,sys\nf=open(sys.argv[1],"a+")\n'
        'try: fcntl.flock(f,fcntl.LOCK_EX|fcntl.LOCK_NB)\n'
        'except BlockingIOError: sys.exit(2)\n'
    )
    with task.sync_lock(path, "20261020", time.monotonic() + 10):
        assert subprocess.run([sys.executable, "-c", source, str(path)]).returncode == 2
    assert subprocess.run([sys.executable, "-c", source, str(path)]).returncode == 0


def test_mutex_waits_for_previous_job(root, monkeypatch):
    real_flock = task.fcntl.flock
    attempts = []
    def flock(fd, flags):
        attempts.append(flags)
        if len(attempts) == 1:
            raise BlockingIOError()
        return real_flock(fd, flags)
    monkeypatch.setattr(task.fcntl, "flock", flock)
    sleep = MagicMock()
    monkeypatch.setattr(task.time, "sleep", sleep)
    with task.sync_lock(root / "sync.lock", "20261020", time.monotonic() + 10):
        pass
    sleep.assert_called_once_with(task.POLL_SECONDS)
    assert len(attempts) == 3


def test_waits_for_database_owned_by_a_manual_sync_process(root, monkeypatch):
    path = root / "manual.duckdb"
    source = (
        'import duckdb,sys\nc=duckdb.connect(sys.argv[1])\n'
        'print("READY",flush=True)\nsys.stdin.readline()\nc.close()\n'
    )
    child = subprocess.Popen([sys.executable, "-c", source, str(path)], stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        assert child.stdout.readline().strip() == "READY"
        sleep = MagicMock(side_effect=lambda _: child.communicate(input="stop\n", timeout=10))
        monkeypatch.setattr(task.time, "sleep", sleep)
        task.wait_for_database(path, "20261020", time.monotonic() + 10)
        sleep.assert_called_once_with(task.POLL_SECONDS)
    finally:
        if child.poll() is None:
            child.kill()
            child.communicate()


@pytest.mark.parametrize("group,hour", [("stock", 17), ("finance", 2)])
def test_launch_agent_schedule_absolute_paths_and_no_immediate_execution(group, hour):
    root = Path("/Users/example/Quant Lab")
    config = plistlib.loads(plistlib.dumps(agent_config(group, root, Path("/Users/example/.local/bin/uv"))))
    assert config["StartCalendarInterval"] == {"Hour": hour, "Minute": 0}
    assert config["WorkingDirectory"] == str(root)
    assert config["ProgramArguments"] == [str(root / ".venv/bin/python"), "-m", "zer0share.macos_sync",
                                          group, "--uv", "/Users/example/.local/bin/uv"]
    assert config["RunAtLoad"] is False
    assert config["KeepAlive"] is False
    assert config["EnvironmentVariables"]["TZ"] == "Asia/Shanghai"


@pytest.mark.parametrize("execution_day,reference_day,is_open", [
    ("20261017", "20261016", True),  # Saturday runs Friday's sync.
    ("20261019", "20261018", False),  # Monday skips Sunday.
    ("20270101", "20261231", True),
    ("20240301", "20240229", True),
])
def test_finance_uses_previous_calendar_day_and_execution_day_for_waits(
    root, monkeypatch, execution_day, reference_day, is_open, caplog
):
    calendar(root, (reference_day,), (is_open,))
    monkeypatch.setattr(task, "today", lambda: execution_day)
    run = MagicMock(return_value=0)
    wait = MagicMock()
    monkeypatch.setattr(task, "run_command", run)
    monkeypatch.setattr(task, "wait_for_database", wait)
    with caplog.at_level("INFO"):
        assert task.main(["finance", "--uv", sys.executable]) == 0
    assert execution_day in caplog.text
    assert reference_day in caplog.text
    if is_open:
        assert run.call_args.args[0][-2:] == ["--end-date", reference_day]
        assert wait.call_args.args[1] == execution_day
    else:
        run.assert_not_called()
        wait.assert_not_called()
