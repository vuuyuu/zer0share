import pyarrow.parquet as pq

from zer0share.finance.catalog import FINANCE_SPECS
from zer0share.finance.storage import FinanceStore
from zer0share.query.repository import (
    BaseParquetRepository, TableSpec, date_range_filters, in_filter, parse_date,
)


def query_financial(ctx, table, ts_code=None, ann_date=None, start_date=None,
                    end_date=None, period=None, fields=None, limit=None, offset=None,
                    **filters):
    spec = FINANCE_SPECS[table]
    if table == "fina_mainbz" and "type" in filters:
        filters["bz_code"] = filters.pop("type")
    store = FinanceStore(ctx.data_dir, spec)
    if not store.table_dir.exists():
        raise FileNotFoundError(f"{table} data not found; run `python main.py sync --table {table}` first")
    codes = ts_code.split(",") if isinstance(ts_code, str) else ts_code
    if codes is None:
        sources = sorted(store.table_dir.glob("ts_code=*/data.parquet"))
    else:
        sources = [store.path(code.strip()) for code in codes]
        sources = [p for p in sources if p.exists()]
    columns = list(spec.columns)
    for path in sources:
        columns.extend(col for col in pq.read_schema(path).names if col not in columns)
    if ann_date is not None:
        parse_date(ann_date)
        if spec.date_column == "ann_date" and (start_date is not None or end_date is not None):
            raise ValueError("ann_date cannot be combined with start_date or end_date")
        filters["ann_date"] = ann_date
    if period is not None:
        parse_date(period)
        filters["end_date"] = period
    if (limit is not None and limit < 0) or (offset is not None and offset < 0):
        raise ValueError("limit and offset must be non-negative")
    query_filters = date_range_filters(spec.date_column, None, start_date, end_date, columns)
    for key, value in filters.items():
        if value is not None:
            if key.endswith("date") and isinstance(value, str):
                parse_date(value)
            query_filters.append(in_filter(key, value, columns))
    table_spec = TableSpec(
        name=table, path_parts=("finance", table), columns=columns,
        parquet_pattern="ts_code=*/data.parquet", sync_table=table,
        order_by=f"ts_code, {spec.date_column}", union_by_name=True,
        first_date=spec.first_date,
    )
    return BaseParquetRepository(ctx, table_spec).query(
        fields=fields, filters=query_filters, limit=limit, offset=offset, source=sources,
    )
