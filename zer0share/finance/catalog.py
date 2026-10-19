from dataclasses import dataclass

from zer0share.finance.schema import FIELD_TYPES


@dataclass(frozen=True)
class FinanceSpec:
    name: str
    date_column: str = "ann_date"
    row_limit: int = 100
    supports_range: bool = True
    first_date: str = "20000101"

    @property
    def columns(self) -> list[str]:
        return list(FIELD_TYPES[self.name])


# Ordinary per-stock endpoints. A conservative cap is used where the official
# page gives no numeric cap: large responses are re-queried in smaller windows.
FINANCE_SPECS = {
    spec.name: spec for spec in (
        FinanceSpec("income"),
        FinanceSpec("balancesheet"),
        FinanceSpec("cashflow"),
        FinanceSpec("dividend", "end_date", 2000, False),
        FinanceSpec("forecast", row_limit=3500),
        FinanceSpec("express"),
        FinanceSpec("fina_indicator", "end_date"),
        FinanceSpec("fina_audit"),
        FinanceSpec("fina_mainbz", "end_date"),
        FinanceSpec("disclosure_date", "end_date", 6000, False),
    )
}
FINANCE_TABLES = list(FINANCE_SPECS)
