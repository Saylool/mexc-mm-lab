from pathlib import Path

import pyarrow.parquet as pq

from mexcmm.book import Applied, OrderBook
from mexcmm.rebuild import _group_diffs, _group_snapshots, rebuild_symbol

FIX = Path(__file__).parent / "fixtures"


def seeded():
    b = OrderBook()
    b.apply_snapshot(100, [("1.00", "5"), ("0.99", "3")], [("1.01", "4"), ("1.02", "2")])
    return b


def test_stale_diff_is_ignored():
    b = seeded()
    assert b.apply_diff(90, 100, [("1.00", "0")], []) is Applied.STALE
    assert b.best_bid() == (1.0, 5.0)


def test_first_diff_may_straddle_snapshot_version():
    b = seeded()
    assert b.apply_diff(95, 105, [("1.00", "0")], [("1.005", "1")]) is Applied.OK
    assert b.best_bid() == (0.99, 3.0)
    assert b.best_ask() == (1.005, 1.0)
    assert b.version == 105


def test_first_diff_after_hole_is_a_gap():
    b = seeded()
    assert b.apply_diff(102, 110, [], []) is Applied.GAP
    assert b.apply_diff(111, 112, [], []) is Applied.NO_SNAPSHOT


def test_chain_break_after_sync_is_a_gap():
    b = seeded()
    assert b.apply_diff(101, 101, [], [("1.01", "0")]) is Applied.OK
    assert b.apply_diff(103, 104, [], []) is Applied.GAP


def test_quantities_are_absolute():
    b = seeded()
    b.apply_diff(101, 101, [("1.00", "7")], [])
    assert b.best_bid() == (1.0, 7.0)


def test_recorded_tiausdt_rebuilds_cleanly():
    """Two minutes of real TIAUSDT data: no gaps, never crossed, snapshots agree."""
    depth = pq.read_table(FIX / "tia_mexc_depth.parquet").sort_by(
        [("symbol", "ascending"), ("from_v", "ascending"), ("side", "ascending")])
    snaps = pq.read_table(FIX / "tia_mexc_snapshots.parquet")
    q = rebuild_symbol(_group_snapshots(snaps)["TIAUSDT"], _group_diffs(depth)["TIAUSDT"])
    assert q.diffs > 900
    assert q.gaps == 0 and q.unsynced == 0 and q.crossed == 0
    assert q.checks >= 1 and q.top_mismatch == 0
