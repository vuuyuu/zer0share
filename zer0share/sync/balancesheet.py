"""Announcement-range balance-sheet history with report versions retained."""
from functools import partial

import pandas as pd

import zer0share.dateutil as dateutil
from zer0share.catalog import BALANCESHEET_SPEC
from zer0share.schema import BALANCESHEET_COLS
from zer0share.storage import TickerPartitionStore
from zer0share.sync._jobs import TickerSyncJob
from zer0share.sync.fina_audit import get_fina_audit_tickers

BALANCESHEET_KEY = [
    "ts_code", "end_date", "ann_date", "f_ann_date",
    "report_type", "comp_type", "update_flag",
]
BALANCESHEET_SORT = [
    "ts_code", "end_date", "f_ann_date", "ann_date",
    "report_type", "update_flag", "comp_type",
]


def get_balancesheet_effective_announcement_date(row: pd.Series) -> str | None:
    """Actual disclosure date is the knowledge date when available."""
    for column in ("f_ann_date", "ann_date"):
        value = row.get(column)
        if pd.notna(value) and value != "":
            return dateutil.date_str(value)
    return None


def get_balancesheet_last_date(existing: pd.DataFrame) -> str | None:
    if existing.empty:
        return None
    dates = existing.apply(get_balancesheet_effective_announcement_date, axis=1).dropna()
    return dates.max() if not dates.empty else None


def merge_balancesheet(existing: pd.DataFrame, fetched: pd.DataFrame) -> pd.DataFrame:
    """Distinct report versions survive; newly fetched complete keys win."""
    frames = [
        df.reindex(columns=BALANCESHEET_COLS)
        for df in (existing, fetched)
        if not df.empty
    ]
    if not frames:
        return pd.DataFrame(columns=BALANCESHEET_COLS)
    result = pd.concat(frames, ignore_index=True)
    # Nullable strings make None, NaN, and pd.NA one stable missing identity.
    for column in BALANCESHEET_KEY:
        result[column] = result[column].astype("string")
    return (result.drop_duplicates(subset=BALANCESHEET_KEY, keep="last")
            .sort_values(BALANCESHEET_SORT, kind="stable")
            .reset_index(drop=True))


def build_jobs(cfg, fetcher) -> list[TickerSyncJob]:
    store = TickerPartitionStore(cfg.data_dir.joinpath(*BALANCESHEET_SPEC.path_parts))
    return [TickerSyncJob(
        table_name=BALANCESHEET_SPEC.name,
        get_tickers=partial(get_fina_audit_tickers, cfg.data_dir),
        fetch_range=fetcher.fetch_balancesheet,
        read_existing=store.read,
        write_ticker=store.write,
        merge=merge_balancesheet,
        first_date=BALANCESHEET_SPEC.first_date,
        get_last_date=get_balancesheet_last_date,
        overlap_days=1825,
    )]
