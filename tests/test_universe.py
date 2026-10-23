from mexcmm.universe import candidates, pick


def t(symbol, bid, ask, qv):
    return {"symbol": symbol, "bidPrice": str(bid), "askPrice": str(ask), "quoteVolume": str(qv)}


def test_filters_and_groups():
    tickers = [
        t("TIAUSDT", 0.547, 0.548, 1_100_000),      # ref, 18 bps
        t("CTXUSDT", 0.000702, 0.000721, 4_500_000),  # solo, wide
        t("BTCUSDT", 81856.5, 81856.51, 3e8),        # too much volume
        t("USDCUSDT", 1.0001, 1.0012, 1_000_000),    # stablecoin
        t("TSLAONUSDT", 366.2, 366.9, 160_000),      # tokenized stock
        t("MONUSDT", 0.0220, 0.0221, 300_000),       # short base ending in ON stays
        t("DOTUSDT", 1.1369, 1.1370, 4_000_000),     # spread below minimum
        t("TIABTC", 1, 2, 1_000_000),                # not a USDT pair
        t("DEADUSDT", 0, 0, 500_000),                # empty book
    ]
    c = candidates(tickers, {"TIAUSDT", "DOTUSDT"}, min_volume=200_000,
                   max_volume=20_000_000, min_spread_bps=8)
    got = {x.symbol: x.group for x in c}
    assert got == {"TIAUSDT": "ref", "CTXUSDT": "solo", "MONUSDT": "solo"}


def test_pick_takes_highest_volume_per_group():
    tickers = [t(f"A{i}USDT", 1.0, 1.01, 300_000 + i) for i in range(5)]
    c = candidates(tickers, {"A0USDT", "A1USDT", "A2USDT"}, min_volume=0,
                   max_volume=1e9, min_spread_bps=0)
    assert [x.symbol for x in pick(c, 2, 1)] == ["A2USDT", "A1USDT", "A4USDT"]
