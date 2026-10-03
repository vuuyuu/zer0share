import time

import requests

from zer0share.sync._jobs import TickerSyncJob

from .models import FINANCIAL_TABLES, FinancialSeedRunResult, SeedTaskStatus
from .state import FinancialSeedState


def classify_error(error: Exception) -> str:
    message = str(error).lower()
    if "rate" in message or "limit" in message or "频率" in message or "429" in message:
        return "RATE_LIMIT"
    if isinstance(error, requests.exceptions.RequestException):
        return "NETWORK_ERROR"
    if "parquet" in message or "arrow" in message or "serial" in message:
        return "SERIALIZATION_ERROR"
    if isinstance(error, OSError):
        return "WRITE_ERROR"
    if isinstance(error, (KeyError, TypeError, ValueError)):
        return "SCHEMA_ERROR"
    if "api" in message or "tushare" in message:
        return "API_ERROR"
    return "UNKNOWN_ERROR"


class FinancialSeedRunner:
    def __init__(self, state: FinancialSeedState, jobs: dict[str, TickerSyncJob]):
        unsupported = set(jobs) - set(FINANCIAL_TABLES)
        if unsupported or set(jobs) != set(FINANCIAL_TABLES):
            raise ValueError("financial seed runner requires exactly the supported financial jobs")
        if not all(isinstance(job, TickerSyncJob) for job in jobs.values()):
            raise ValueError("financial seed runner requires TickerSyncJob instances")
        self.state = state
        self.jobs = jobs

    def run(
        self,
        run_id: str,
        table_name: str | None = None,
        limit: int | None = None,
        retry_failed: bool = False,
    ) -> FinancialSeedRunResult:
        self.state.run_metadata(run_id)
        self.state.reset_running(run_id)
        tasks = self.state.eligible_tasks(run_id, table_name, limit, retry_failed)
        result = FinancialSeedRunResult(0, 0, 0, 0)
        for task in tasks:
            job = self.jobs[task.table_name]
            self.state.mark_running(task)
            try:
                status = job._sync_ticker(task.ticker, task.start_date, task.end_date)
            except Exception as error:
                self.state.finish(task, SeedTaskStatus.FAILED, error_type=classify_error(error), error_message=str(error)[:1000])
                result = FinancialSeedRunResult(result.attempted + 1, result.success, result.valid_empty, result.failed + 1)
            else:
                if status == "FETCH_EMPTY":
                    self.state.finish(task, SeedTaskStatus.FAILED, error_type="FETCH_EMPTY", error_message="empty response is not durably verifiable")
                    result = FinancialSeedRunResult(result.attempted + 1, result.success, result.valid_empty, result.failed + 1)
                else:
                    self.state.finish(task, SeedTaskStatus.SUCCESS)
                    result = FinancialSeedRunResult(result.attempted + 1, result.success + 1, result.valid_empty, result.failed)
            finally:
                if job.ticker_sleep:
                    time.sleep(job.ticker_sleep)
        return result
