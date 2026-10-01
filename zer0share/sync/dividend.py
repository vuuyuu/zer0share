"""Per-ticker full-history dividend refresh; keep all proposal stages."""
from functools import partial

import pandas as pd

from zer0share.catalog import DIVIDEND_SPEC
from zer0share.schema import DIVIDEND_COLS
from zer0share.storage import TickerPartitionStore
from zer0share.sync._jobs import TickerSyncJob
from zer0share.sync.fina_audit import get_fina_audit_tickers

DIVIDEND_KEY = [
    "ts_code", "end_date", "ann_date", "div_proc", "record_date",
    "ex_date", "pay_date", "imp_ann_date",
]
DIVIDEND_SORT = [
    "ts_code", "end_date", "ann_date", "imp_ann_date", "ex_date",
    "pay_date", "record_date", "div_proc",
]


def merge_dividend(existing: pd.DataFrame, fetched: pd.DataFrame) -> pd.DataFrame:
    """Retain distinct stages/dates; newer content wins on an identical key."""
    frames = [df.reindex(columns=DIVIDEND_COLS) for df in (existing, fetched) if not df.empty]
    if not frames:
        return pd.DataFrame(columns=DIVIDEND_COLS)
    result = pd.concat(frames, ignore_index=True)
    # Nullable string keys treat None, NaN, and pd.NA as the same identity.
    for column in DIVIDEND_KEY:
        result[column] = result[column].astype("string")
    return (result.drop_duplicates(subset=DIVIDEND_KEY, keep="last")
            .sort_values(DIVIDEND_SORT, kind="stable")
            .reset_index(drop=True))


def get_dividend_last_date(existing: pd.DataFrame) -> None:
    """Full-history refresh has no incremental date anchor."""
    return None


def build_jobs(cfg, fetcher) -> list[TickerSyncJob]:
    store = TickerPartitionStore(cfg.data_dir.joinpath(*DIVIDEND_SPEC.path_parts))
    return [TickerSyncJob(
        table_name=DIVIDEND_SPEC.name,
        get_tickers=partial(get_fina_audit_tickers, cfg.data_dir),
        fetch_range=fetcher.fetch_dividend,
        read_existing=store.read,
        write_ticker=store.write,
        merge=merge_dividend,
        first_date=DIVIDEND_SPEC.first_date,
        get_last_date=get_dividend_last_date,
        overlap_days=0,
    )]
