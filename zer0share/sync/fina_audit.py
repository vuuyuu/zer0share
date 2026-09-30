"""Audit histories: announcement-date overlap and per-ticker PIT retention."""

from functools import partial
from pathlib import Path

import pandas as pd

from zer0share.catalog import BASIC_SPEC, FINA_AUDIT_SPEC
from zer0share.schema import FINA_AUDIT_COLS
from zer0share.storage import SnapshotStore, TickerPartitionStore
from zer0share.sync._jobs import TickerSyncJob
from zer0share.universe import _is_a_share_common


FINA_AUDIT_KEY = [
    "ts_code", "end_date", "ann_date", "audit_result", "audit_agency", "audit_sign",
]


def merge_fina_audit(existing: pd.DataFrame, fetched: pd.DataFrame) -> pd.DataFrame:
    """Retain PIT versions; an identical key takes the fetched audit_fees value.

    pandas treats missing values in each dedup key column as equal, including
    None/NaN/pd.NA, so replaying an overlap window is idempotent.
    """
    frames = [df[FINA_AUDIT_COLS] for df in (existing, fetched) if not df.empty]
    if not frames:
        return pd.DataFrame(columns=FINA_AUDIT_COLS)
    return (
        pd.concat(frames, ignore_index=True)
        .drop_duplicates(subset=FINA_AUDIT_KEY, keep="last")
        .sort_values(["ts_code", "end_date", "ann_date"], kind="stable")
        .reset_index(drop=True)
    )


def get_fina_audit_last_date(existing: pd.DataFrame) -> str | None:
    if existing.empty:
        return None
    dates = existing["ann_date"].dropna()
    return dates.max() if not dates.empty else None


def get_fina_audit_tickers(data_dir: Path) -> list[str]:
    """Use the stock_basic snapshot, without ST/listing-age/status eligibility."""
    basic = SnapshotStore(data_dir.joinpath(*BASIC_SPEC.path_parts, "data.parquet")).read()
    if basic.empty:
        raise RuntimeError("fina_audit: A-share universe is empty; run sync --table basic first")
    stock_code = basic["ts_code"].str.fullmatch(
        r"(?:6\d{5}\.SH|(?:00|30)\d{4}\.SZ|[489]\d{5}\.BJ)", na=False,
    )
    tickers = sorted(basic.loc[_is_a_share_common(basic) & stock_code, "ts_code"].dropna().unique().tolist())
    if not tickers:
        raise RuntimeError("fina_audit: basic contains no A-share tickers; refresh sync --table basic")
    return tickers


def build_jobs(cfg, fetcher) -> list[TickerSyncJob]:
    store = TickerPartitionStore(cfg.data_dir.joinpath(*FINA_AUDIT_SPEC.path_parts))
    return [
        TickerSyncJob(
            table_name=FINA_AUDIT_SPEC.name,
            get_tickers=partial(get_fina_audit_tickers, cfg.data_dir),
            fetch_range=fetcher.fetch_fina_audit,
            read_existing=store.read,
            write_ticker=store.write,
            merge=merge_fina_audit,
            first_date=FINA_AUDIT_SPEC.first_date,
            get_last_date=get_fina_audit_last_date,
            overlap_days=120,
        ),
    ]
