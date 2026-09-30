"""Announcement-range income history with report and update versions retained."""
from functools import partial

import pandas as pd

import zer0share.dateutil as dateutil
from zer0share.catalog import INCOME_SPEC
from zer0share.schema import INCOME_COLS
from zer0share.storage import TickerPartitionStore
from zer0share.sync._jobs import TickerSyncJob
from zer0share.sync.fina_audit import get_fina_audit_tickers

INCOME_KEY = [
    "ts_code", "end_date", "ann_date", "f_ann_date",
    "report_type", "comp_type", "update_flag",
]
INCOME_SORT = [
    "ts_code", "end_date", "f_ann_date", "ann_date",
    "report_type", "update_flag", "comp_type",
]


def get_income_effective_announcement_date(row: pd.Series) -> str | None:
    """Actual disclosure date is the knowledge date when available."""
    for column in ("f_ann_date", "ann_date"):
        value = row.get(column)
        if pd.notna(value) and value != "":
            return dateutil.date_str(value)
    return None


def get_income_last_date(existing: pd.DataFrame) -> str | None:
    if existing.empty:
        return None
    dates = existing.apply(get_income_effective_announcement_date, axis=1).dropna()
    return dates.max() if not dates.empty else None


def merge_income(existing: pd.DataFrame, fetched: pd.DataFrame) -> pd.DataFrame:
    """Distinct report versions survive; newly fetched complete keys win."""
    frames = [df.reindex(columns=INCOME_COLS) for df in (existing, fetched) if not df.empty]
    if not frames:
        return pd.DataFrame(columns=INCOME_COLS)
    result = pd.concat(frames, ignore_index=True)
    # Nullable strings make None, NaN, and pd.NA one stable missing identity.
    for column in INCOME_KEY:
        result[column] = result[column].astype("string")
    return (result.drop_duplicates(subset=INCOME_KEY, keep="last")
            .sort_values(INCOME_SORT, kind="stable")
            .reset_index(drop=True))


def build_jobs(cfg, fetcher) -> list[TickerSyncJob]:
    store = TickerPartitionStore(cfg.data_dir.joinpath(*INCOME_SPEC.path_parts))
    return [TickerSyncJob(
        table_name=INCOME_SPEC.name,
        get_tickers=partial(get_fina_audit_tickers, cfg.data_dir),
        fetch_range=fetcher.fetch_income,
        read_existing=store.read,
        write_ticker=store.write,
        merge=merge_income,
        first_date=INCOME_SPEC.first_date,
        get_last_date=get_income_last_date,
        overlap_days=1825,
    )]
