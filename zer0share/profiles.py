"""Sync profiles for specialized data subsets."""

IPO_SCORE_INIT_START = "20100101"

IPO_SCORE_TABLES = (
    "trade_cal",
    "basic",
    "daily_kline",
    "adj_factor",
    "daily_basic",
    "stock_st",
    "suspend_d",
    "stk_limit",
    "index_daily",
    "industry",
    "fina_audit",
)

IPO_SCORE_DATED_TABLES = frozenset(
    {
        "daily_kline",
        "adj_factor",
        "daily_basic",
        "stock_st",
        "suspend_d",
        "stk_limit",
        "index_daily",
    }
)
