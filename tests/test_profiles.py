from zer0share.profiles import IPO_SCORE_DATED_TABLES, IPO_SCORE_INIT_START, IPO_SCORE_TABLES


def test_ipo_score_profile():
    assert IPO_SCORE_INIT_START == "20100101"
    assert IPO_SCORE_TABLES == (
        "trade_cal", "basic", "daily_kline", "adj_factor", "daily_basic",
        "stock_st", "suspend_d", "stk_limit", "index_daily", "industry",
    )
    assert isinstance(IPO_SCORE_DATED_TABLES, frozenset)
    assert IPO_SCORE_DATED_TABLES == {
        "daily_kline", "adj_factor", "daily_basic", "stock_st",
        "suspend_d", "stk_limit", "index_daily",
    }
