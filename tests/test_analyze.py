import numpy as np

from mexcmm.analyze import mid_at, spread_stats


def test_mid_at_uses_latest_value_at_or_before():
    t = np.array([1000, 2000, 3000])
    mid = np.array([1.0, 2.0, 3.0])
    out = mid_at(t, mid, np.array([999, 1000, 2500, 9999]))
    assert np.isnan(out[0])
    assert list(out[1:]) == [1.0, 2.0, 3.0]


def test_spread_stats_time_weighted():
    # 10 bps for 9 s, 100 bps for 1 s -> median 10 bps
    t = np.array([0, 9000, 10000])
    bid = np.array([0.9995, 0.995, 0.995])
    ask = np.array([1.0005, 1.005, 1.005])
    s = spread_stats(t, bid, ask)
    assert abs(s["median"] - 10) < 0.01
    assert abs(s["p90"] - 10) < 0.01 or abs(s["p90"] - 100) < 0.01
