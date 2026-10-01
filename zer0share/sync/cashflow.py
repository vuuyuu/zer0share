"""Announcement-range cashflow history with all report versions retained."""
from functools import partial

import pandas as pd

from zer0share.catalog import CASHFLOW_SPEC
from zer0share.schema import CASHFLOW_COLS
from zer0share.storage import TickerPartitionStore
from zer0share.sync._jobs import TickerSyncJob
from zer0share.sync.balancesheet import get_balancesheet_effective_announcement_date
from zer0share.sync.fina_audit import get_fina_audit_tickers

CASHFLOW_KEY = ["ts_code", "end_date", "ann_date", "f_ann_date", "report_type", "comp_type", "update_flag"]
CASHFLOW_SORT = ["ts_code", "end_date", "f_ann_date", "ann_date", "report_type", "update_flag"]


def get_cashflow_effective_announcement_date(row: pd.Series) -> str | None:
    return get_balancesheet_effective_announcement_date(row)


def get_cashflow_last_date(existing: pd.DataFrame) -> str | None:
    if existing.empty:
        return None
    dates = existing.apply(get_cashflow_effective_announcement_date, axis=1).dropna()
    return dates.max() if not dates.empty else None


def merge_cashflow(existing: pd.DataFrame, fetched: pd.DataFrame) -> pd.DataFrame:
    frames = [df.reindex(columns=CASHFLOW_COLS) for df in (existing, fetched) if not df.empty]
    if not frames:
        return pd.DataFrame(columns=CASHFLOW_COLS)
    result = pd.concat(frames, ignore_index=True)
    for column in CASHFLOW_KEY:
        result[column] = result[column].astype("string")
    return (result.drop_duplicates(subset=CASHFLOW_KEY, keep="last")
            .sort_values(CASHFLOW_SORT, kind="stable").reset_index(drop=True))


def build_jobs(cfg, fetcher) -> list[TickerSyncJob]:
    store = TickerPartitionStore(cfg.data_dir.joinpath(*CASHFLOW_SPEC.path_parts))
    return [TickerSyncJob(
        table_name=CASHFLOW_SPEC.name, get_tickers=partial(get_fina_audit_tickers, cfg.data_dir),
        fetch_range=fetcher.fetch_cashflow, read_existing=store.read, write_ticker=store.write,
        merge=merge_cashflow, first_date=CASHFLOW_SPEC.first_date,
        get_last_date=get_cashflow_last_date, overlap_days=1825,
    )]
