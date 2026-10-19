from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
import time

import pyarrow.parquet as pq
from loguru import logger

from zer0share import dateutil
from zer0share.finance.catalog import FINANCE_SPECS, FinanceSpec
from zer0share.finance.fetcher import RateLimiter
from zer0share.finance.storage import FinanceStore
from zer0share.sync import SyncRuntime
from zer0share.sync._jobs import SyncJob, _format_duration, _estimate_eta


class FinanceSyncJob(SyncJob):
    supports_date_range = True

    def __init__(self, spec: FinanceSpec, fetch, data_dir: Path, max_workers: int = 1):
        self.table_name = spec.name
        self.spec = spec
        self._fetch = fetch
        self._data_dir = data_dir
        self._store = FinanceStore(data_dir, spec)
        self._max_workers = max_workers

    def run(self, rt: SyncRuntime, start_date=None, end_date=None) -> None:
        today = rt.calendar.today()
        end = end_date or today
        dateutil.parse_date(end)
        if start_date is not None:
            dateutil.parse_date(start_date)
            if start_date > end:
                raise ValueError("start_date must be on or before end_date")
        basic_path = self._data_dir / "stock" / "basic" / "data.parquet"
        if not basic_path.exists():
            raise RuntimeError("财务同步需要股票列表，请先运行 sync --table basic")
        basic = pq.ParquetFile(basic_path).read(columns=["ts_code"]).to_pandas()
        codes = sorted(basic.ts_code.dropna().unique())
        if not codes:
            raise RuntimeError("股票列表为空，请先运行 sync --table basic")
        for code in codes:
            self._store.path(code)  # Validate stock codes before making requests.

        refresh_start = f"{int(today[:4]) - 1}0101"
        # Precompute each stock's refresh window in the main thread: DuckDB
        # (MetaStore) is not thread-safe, so meta reads stay out of the workers.
        # A newly added stock or a lost partition needs full history.
        tasks = []
        for code in codes:
            key = f"finance:{self.table_name}:{code}"
            last = rt.meta.get_last_date(key)
            if not self._store.path(code).exists():
                last = None
            start = start_date or (
                min(refresh_start, dateutil.add_days(last, 1)) if last else self.spec.first_date
            )
            if start > end:
                continue
            tasks.append((code, start, last))

        started = time.monotonic()
        rows = 0
        logger.info(
            f"{self.table_name}: start stocks={len(codes)}, to_fetch={len(tasks)}, "
            f"date_column={self.spec.date_column}"
        )
        try:
            with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
                futures = [
                    pool.submit(self._fetch, self.table_name, code, start, end)
                    for code, start, _ in tasks
                ]
                for processed, (future, (code, start, last)) in enumerate(zip(futures, tasks), 1):
                    try:
                        df = future.result()
                    except Exception:
                        for pending in futures:
                            pending.cancel()
                        raise
                    self._store.replace_range(code, start, end, df)
                    rows += len(df)
                    key = f"finance:{self.table_name}:{code}"
                    # This key means continuous initial history has been downloaded.
                    # A narrow explicit backfill must not mark the stock initialized.
                    if start <= self.spec.first_date or (last is not None and start <= dateutil.add_days(last, 1)):
                        rt.meta.update_last_date(key, max(last or end, end))
                    if processed % 50 == 0 or processed == len(tasks):
                        elapsed = time.monotonic() - started
                        logger.info(
                            f"{self.table_name}: progress {processed}/{len(tasks)} stocks, rows={rows} "
                            f"elapsed={_format_duration(elapsed)} eta={_estimate_eta(elapsed, processed, len(tasks))}"
                        )
        except Exception as exc:
            logger.error(f"{self.table_name} {code}: 同步失败，已完成股票的数据已保留: {exc}")
            rt.notifier.send(f"{self.table_name} 同步失败\n股票：{code}｜{exc}")
            raise
        last_table = rt.meta.get_last_date(self.table_name)
        rt.meta.update_last_date(self.table_name, max(last_table or end, end))
        message = f"{self.table_name} 同步完成\n股票：{len(codes)}｜本次获取：{rows} 条"
        logger.info(message)
        rt.notifier.send(message)


def build_jobs(cfg, fetcher) -> list[SyncJob]:
    limiter = RateLimiter(cfg.finance.requests_per_minute)

    def fetch(table: str, ts_code: str, start: str, end: str):
        return fetcher.fetch_financial(table, ts_code, start, end, limiter=limiter)

    return [
        FinanceSyncJob(spec, fetch, cfg.data_dir, max_workers=cfg.finance.max_workers)
        for spec in FINANCE_SPECS.values()
    ]
