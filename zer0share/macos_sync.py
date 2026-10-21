"""Trading-day gate and process mutex for the two macOS LaunchAgents.

The actual sync runs through the CLI from the project directory. Overnight
finance runs check the previous calendar day and cap data coverage at that day.
"""

import argparse
from contextlib import contextmanager
from datetime import datetime, timedelta
import fcntl
import logging
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time
import tomllib
from zoneinfo import ZoneInfo

import duckdb
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
LOGGER = logging.getLogger("zer0share.macos_sync")
POLL_SECONDS = 30
WAIT_SECONDS = 12 * 60 * 60


def today() -> str:
    return datetime.now(ZoneInfo("Asia/Shanghai")).strftime("%Y%m%d")


def configured_paths(root: Path) -> tuple[Path, Path]:
    with (root / "config" / "settings.toml").open("rb") as file:
        paths = tomllib.load(file)["paths"]
    return root / paths["data_dir"], root / paths["db_path"]


def is_trading_day(data_dir: Path, day: str) -> bool:
    path = data_dir / "stock" / "trade_cal" / "exchange=SSE" / "data.parquet"
    if not path.exists():
        raise RuntimeError("本地 SSE 交易日历不存在，请先运行 sync --table trade_cal")
    rows = pq.ParquetFile(path).read(columns=["cal_date", "is_open"]).to_pylist()
    matches = [row for row in rows if str(row["cal_date"]).replace("-", "") == day]
    if len(matches) != 1:
        raise RuntimeError(f"本地交易日历未唯一覆盖 {day}，请更新 sync --table trade_cal")
    value = matches[0]["is_open"]
    if value in (True, 1, "1"):
        return True
    if value in (False, 0, "0"):
        return False
    raise RuntimeError(f"本地交易日历 {day} 的 is_open 无效")


class ExpiredTask(RuntimeError):
    """A delayed task should not unexpectedly run on a different day."""


def check_wait(day: str, deadline: float) -> None:
    if today() != day:
        raise ExpiredTask("等待已跨日，跳过旧任务，等待下次正常触发")
    if time.monotonic() >= deadline:
        raise TimeoutError("等待其他同步结束超过 12 小时，本次任务退出")


@contextmanager
def sync_lock(path: Path, day: str, deadline: float):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a+") as file:
        # Other scheduled syncs use the same advisory lock. Manual CLI runs do
        # not, so the database availability check below also handles those.
        waiting = False
        while True:
            check_wait(day, deadline)
            try:
                fcntl.flock(file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if not waiting:
                    LOGGER.info("另一项定时同步尚未结束，等待互斥锁")
                    waiting = True
                time.sleep(POLL_SECONDS)
        try:
            yield
        finally:
            fcntl.flock(file.fileno(), fcntl.LOCK_UN)


def wait_for_database(path: Path, day: str, deadline: float) -> None:
    waiting = False
    while True:
        check_wait(day, deadline)
        if not path.exists():
            return
        try:
            # Probe and close immediately: holding this connection while
            # launching a child would itself prevent the child from opening DB.
            duckdb.connect(str(path)).close()
            return
        except duckdb.IOException as exc:
            if "Could not set lock" not in str(exc):
                raise
            if not waiting:
                LOGGER.info("DuckDB 正被其他进程使用，等待其结束")
                waiting = True
            time.sleep(POLL_SECONDS)


def run_command(command: list[str], root: Path) -> int:
    child = subprocess.Popen(command, cwd=root)
    previous = signal.signal(signal.SIGTERM, lambda *_: child.terminate())
    try:
        returncode = child.wait()
        return returncode if returncode >= 0 else 128 - returncode
    except BaseException:
        child.terminate()
        try:
            child.wait(timeout=30)
        except subprocess.TimeoutExpired:
            child.kill()
            child.wait()
        raise
    finally:
        signal.signal(signal.SIGTERM, previous)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="仅在本地日历确认交易日后同步；两个任务互斥运行")
    parser.add_argument("group", choices=("stock", "finance"))
    parser.add_argument("--uv", default=shutil.which("uv"), help="uv 的绝对路径")
    parser.add_argument("--check-only", action="store_true", help="只检查日历及命令，不同步、不获取数据库锁")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
    try:
        day = today()
        # Keep execution day for wait expiry; only finance's calendar check and
        # data watermark refer to yesterday. Never backdate actual timestamps.
        reference_day = (
            (datetime.strptime(day, "%Y%m%d") - timedelta(days=1)).strftime("%Y%m%d")
            if args.group == "finance" else day
        )
        data_dir, db_path = configured_paths(ROOT)
        if not is_trading_day(data_dir, reference_day):
            LOGGER.info("执行日期=%s，判断日期=%s 非交易日，跳过 %s 同步", day, reference_day, args.group)
            return 0
        if not args.uv or not Path(args.uv).is_absolute() or not os.access(args.uv, os.X_OK):
            raise RuntimeError("uv 绝对路径不可执行，请重新安装 LaunchAgents")
        command = [args.uv, "run", "python", "main.py", "sync", f"--{args.group}"]
        if args.group == "finance":
            command.extend(["--end-date", reference_day])
        if args.check_only:
            LOGGER.info("检查通过：执行日期=%s；判断日期=%s 为交易日；目录=%s；命令=%s",
                        day, reference_day, ROOT, command)
            return 0
        deadline = time.monotonic() + WAIT_SECONDS
        lock_path = ROOT / "logs" / "launchd" / "sync.lock"
        with sync_lock(lock_path, day, deadline):
            wait_for_database(db_path, day, deadline)
            if not is_trading_day(data_dir, reference_day):
                LOGGER.info("等待后日历显示非交易日，跳过 %s", args.group)
                return 0
            LOGGER.info("执行日期=%s，交易日=%s，开始 %s 同步", day, reference_day, args.group)
            code = run_command(command, ROOT)
            LOGGER.log(logging.INFO if code == 0 else logging.ERROR,
                       "%s 同步结束，退出码=%s", args.group, code)
            return code
    except ExpiredTask as exc:
        LOGGER.warning("%s", exc)
        return 0
    except Exception as exc:
        LOGGER.error("定时同步失败：%s", exc)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
