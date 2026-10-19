import os
import re
import tempfile
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from zer0share.finance.catalog import FinanceSpec
from zer0share.finance.schema import FIELD_TYPES


class FinanceStore:
    """One current source mirror per stock; atomic replacement of a date window."""

    def __init__(self, data_dir: Path, spec: FinanceSpec):
        self.table_dir = data_dir / "finance" / spec.name
        self.spec = spec

    def path(self, ts_code: str) -> Path:
        # stock_basic also includes historical codes such as T600018.SH.
        # Preserve the source identifier rather than stripping its prefix.
        if not re.fullmatch(r"T?\d{6}\.[A-Z]+", ts_code):
            raise ValueError(f"invalid stock code: {ts_code!r}")
        return self.table_dir / f"ts_code={ts_code}" / "data.parquet"

    def read(self, ts_code: str) -> pd.DataFrame:
        path = self.path(ts_code)
        if not path.exists():
            return pd.DataFrame(columns=self.spec.columns)
        # ParquetFile avoids inferring another ts_code column from the directory.
        return pq.ParquetFile(path).read().to_pandas()

    def replace_range(self, ts_code: str, start: str, end: str, df: pd.DataFrame) -> None:
        if not df.empty:
            if "ts_code" not in df or not df["ts_code"].eq(ts_code).all():
                raise ValueError("response stock code does not match partition")
            dates = df[self.spec.date_column]
            if dates.isna().any() or not dates.between(start, end).all():
                raise ValueError("response dates do not match refresh window")
        old = self.read(ts_code)
        outside = old.loc[~old[self.spec.date_column].between(start, end)]
        frames = [f for f in (outside, df) if not f.empty]
        merged = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=self.spec.columns)
        # Keep all returned columns, including future source extensions. Known
        # all-null columns have stable types across stock partitions.
        columns = dict.fromkeys([*self.spec.columns, *merged.columns])
        data = {}
        for col in columns:
            series = merged[col] if col in merged else pd.Series(None, index=merged.index, dtype=object)
            kind = FIELD_TYPES[self.spec.name].get(col)
            if kind == "str":
                series = series.astype("string")
            elif kind is not None:
                series = pd.to_numeric(series, errors="raise").astype("Int64" if kind == "int" else "float64")
            data[col] = series
        merged = pd.DataFrame(data)
        path = self.path(ts_code)
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(dir=path.parent, suffix=".tmp", delete=False) as tmp:
                tmp_path = Path(tmp.name)
            pq.write_table(pa.Table.from_pandas(merged, preserve_index=False), tmp_path)
            os.replace(tmp_path, path)
        finally:
            if tmp_path is not None:
                tmp_path.unlink(missing_ok=True)
