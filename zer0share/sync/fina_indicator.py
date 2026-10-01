"""Report-period fetch anchor, announcement visibility, and retained versions.

Grade B PIT: update_flag has no reliable revision timestamp. A revised row
cannot be assumed visible on ann_date; no revision timestamp is fabricated.
"""
from functools import partial

import pandas as pd

from zer0share.catalog import FINA_INDICATOR_SPEC
from zer0share.schema import FINA_INDICATOR_COLS
from zer0share.storage import TickerPartitionStore
from zer0share.sync._jobs import TickerSyncJob
from zer0share.sync.fina_audit import get_fina_audit_tickers

FINA_INDICATOR_KEY = ["ts_code", "end_date", "ann_date", "update_flag"]


def merge_fina_indicator(existing: pd.DataFrame, fetched: pd.DataFrame) -> pd.DataFrame:
    """New wins on complete-key conflicts; normalize missing identity to pd.NA."""
    frames = [df.reindex(columns=FINA_INDICATOR_COLS) for df in (existing, fetched) if not df.empty]
    if not frames:
        return pd.DataFrame(columns=FINA_INDICATOR_COLS)
    result = pd.concat(frames, ignore_index=True)
    for column in FINA_INDICATOR_KEY:
        # Nullable strings unify None/NaN/pd.NA and numeric 0/1 flags.
        values = result[column]
        if column == "update_flag":
            values = values.map(lambda v: str(int(v)) if pd.notna(v) and isinstance(v, (int, float)) and v in (0, 1) else v)
        result[column] = values.astype("string")
    return (result.drop_duplicates(FINA_INDICATOR_KEY, keep="last")
            .sort_values(FINA_INDICATOR_KEY, kind="stable")
            .reset_index(drop=True))


def get_fina_indicator_last_date(existing: pd.DataFrame) -> str | None:
    if existing.empty:
        return None
    dates = existing["end_date"].dropna()
    return dates.max() if not dates.empty else None


def build_jobs(cfg, fetcher) -> list[TickerSyncJob]:
    store = TickerPartitionStore(cfg.data_dir.joinpath(*FINA_INDICATOR_SPEC.path_parts))
    return [TickerSyncJob(
        table_name=FINA_INDICATOR_SPEC.name,
        get_tickers=partial(get_fina_audit_tickers, cfg.data_dir),
        fetch_range=fetcher.fetch_fina_indicator,
        read_existing=store.read, write_ticker=store.write,
        merge=merge_fina_indicator, first_date=FINA_INDICATOR_SPEC.first_date,
        get_last_date=get_fina_indicator_last_date, overlap_days=1825,
    )]
