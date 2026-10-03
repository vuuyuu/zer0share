from dataclasses import dataclass
from enum import StrEnum


FINANCIAL_TABLES = (
    "income",
    "balancesheet",
    "cashflow",
    "fina_indicator",
    "dividend",
    "fina_audit",
)


class SeedTaskStatus(StrEnum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    VALID_EMPTY = "VALID_EMPTY"
    FAILED = "FAILED"


@dataclass(frozen=True)
class FinancialSeedManifest:
    path: str
    tickers: tuple[str, ...]
    sha256: str


@dataclass(frozen=True)
class FinancialSeedTask:
    run_id: str
    table_name: str
    ticker: str
    status: SeedTaskStatus
    start_date: str
    end_date: str
    attempt_count: int


@dataclass(frozen=True)
class FinancialSeedRunResult:
    attempted: int
    success: int
    valid_empty: int
    failed: int
