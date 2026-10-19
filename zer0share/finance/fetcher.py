import time

import pandas as pd
import requests
from loguru import logger

from zer0share import dateutil
from zer0share.finance.catalog import FINANCE_SPECS


def fetch_financial(pro, table: str, ts_code: str, start: str, end: str) -> pd.DataFrame:
    """Download a complete stock/window; split capped responses, never truncate.

    Use ordinary per-stock endpoints. Do not assume limit/offset is supported
    where the official endpoint only documents date filters.
    """
    spec = FINANCE_SPECS[table]

    def request(**params):
        for attempt in range(3):
            try:
                try:
                    df = getattr(pro, table)(
                        ts_code=ts_code, fields=",".join(spec.columns), **params
                    )
                finally:
                    # Applies to EVERY request, including split windows/retries.
                    time.sleep(1.5)
                if not isinstance(df, pd.DataFrame):
                    raise ValueError(f"{table}: expected a DataFrame, got {type(df).__name__}")
                return df
            except (requests.RequestException, OSError) as exc:
                if attempt == 2:
                    raise
                delay = (5, 15)[attempt]
                logger.warning(f"{table} {ts_code}: request failed, retry in {delay}s: {exc}")
                time.sleep(delay)

    def window(left: str, right: str, **params) -> pd.DataFrame:
        df = request(start_date=left, end_date=right, **params)
        if len(df) < spec.row_limit:
            return df
        if left == right:
            raise RuntimeError(
                f"{table} {ts_code} {left}: response reached row cap; "
                "cannot guarantee completeness, local data was not replaced"
            )
        middle = dateutil.add_days(left, (dateutil.parse_date(right) - dateutil.parse_date(left)).days // 2)
        return pd.concat([
            window(left, middle, **params),
            window(dateutil.add_days(middle, 1), right, **params),
        ], ignore_index=True)

    if spec.supports_range:
        if table == "fina_mainbz":
            df = pd.concat([window(start, end, type=t) for t in ("P", "D", "I", "S")], ignore_index=True)
        else:
            df = window(start, end)
    else:
        df = request()
        if len(df) >= spec.row_limit:
            raise RuntimeError(f"{table} {ts_code}: response reached row cap; local data was not replaced")

    if df.empty:
        return pd.DataFrame(columns=spec.columns)
    if "ts_code" not in df or not df["ts_code"].eq(ts_code).all():
        raise ValueError(f"{table}: response contains missing/unexpected stock codes")
    if spec.date_column not in df or df[spec.date_column].isna().any():
        raise ValueError(f"{table}: response has missing {spec.date_column}")
    return df.loc[df[spec.date_column].between(start, end)].reset_index(drop=True)
