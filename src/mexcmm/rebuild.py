"""Rebuild books from recorded snapshots and diffs, and check data quality.

For each symbol: start from the first snapshot, apply diffs in arrival
order, re-seed from the next snapshot whenever a version gap appears, and
compare every later snapshot with the rebuilt book at the same version.

Usage: python -m mexcmm.rebuild data/raw [--date YYYY-MM-DD]
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

import pyarrow.dataset as ds

from .book import Applied, OrderBook


@dataclass
class Quality:
    diffs: int = 0
    applied: int = 0
    stale: int = 0
    gaps: int = 0
    unsynced: int = 0  # diffs before the first snapshot or after a gap
    crossed: int = 0
    checks: int = 0  # later snapshots compared with the rebuilt book
    top_mismatch: int = 0
    tops: list[tuple[int, float, float]] = field(default_factory=list)


def _read(root: Path, stream: str, date: str | None):
    path = root / stream if date is None else root / stream / f"date={date}"
    files = sorted(str(p) for p in path.rglob("*.parquet"))
    if not files:
        return None
    return ds.dataset(files, format="parquet").to_table()


def _group_snapshots(snaps):
    """symbol -> list of (recv_ms, version, bids, asks), in arrival order."""
    out = defaultdict(dict)
    for sym, rv, ver, side, price, qty in zip(
            *(snaps[c].to_pylist() for c in ("symbol", "recv_ms", "version", "side", "price", "qty"))):
        key = (rv, ver)
        entry = out[sym].setdefault(key, ([], []))
        entry[side].append((price, qty))
    return {s: sorted((rv, ver, b, a) for (rv, ver), (b, a) in d.items()) for s, d in out.items()}


def _group_diffs(depth):
    """symbol -> list of (recv_ms, from_v, to_v, bids, asks), one per message."""
    out = defaultdict(list)
    cols = [depth[c].to_pylist() for c in ("symbol", "recv_ms", "from_v", "to_v", "side", "price", "qty")]
    current = {}
    for sym, rv, fv, tv, side, price, qty in zip(*cols):
        msg = current.get(sym)
        if msg is None or msg[1] != fv or msg[2] != tv:
            msg = [rv, fv, tv, [], []]
            current[sym] = msg
            out[sym].append(msg)
        if side >= 0:
            msg[3 + side].append((price, qty))
    return out


def rebuild_symbol(snaps, diffs, keep_tops: bool = False) -> Quality:
    q = Quality()
    book = OrderBook()
    si = 0
    # Seed from the first snapshot.
    pending = list(snaps)
    def seed():
        nonlocal si
        if si < len(pending):
            _, ver, b, a = pending[si]
            book.apply_snapshot(ver, b, a)
            si += 1
            return True
        return False

    seed()
    for rv, fv, tv, bids, asks in diffs:
        q.diffs += 1
        # Skip checkpoints the book has already passed.
        while si < len(pending) and book.version is not None and pending[si][1] < fv - 1:
            si += 1
        check_snap = None
        if book.synced and si < len(pending) and fv - 1 <= pending[si][1] <= tv:
            check_snap = pending[si]
            si += 1
            before = (book.best_bid(), book.best_ask())
        r = book.apply_diff(fv, tv, bids, asks)
        if r is Applied.OK:
            q.applied += 1
            if book.crossed():
                q.crossed += 1
            if keep_tops:
                q.tops.append((rv, book.best_bid()[0], book.best_ask()[0]))
            if check_snap is not None and check_snap[2] and check_snap[3]:
                # The snapshot sits between the states before and after this
                # aggregated diff; its top must match one of them.
                snap_top = (max(p for p, _ in check_snap[2]), min(p for p, _ in check_snap[3]))
                tops = {tuple(x[0] if x else None for x in before),
                        (book.best_bid()[0], book.best_ask()[0])}
                q.checks += 1
                if snap_top not in tops:
                    q.top_mismatch += 1
        elif r is Applied.STALE:
            q.stale += 1
        elif r is Applied.GAP:
            q.gaps += 1
            # Re-seed from the first snapshot that arrived after this diff.
            while si < len(pending) and pending[si][0] < rv:
                si += 1
            seed()
        else:
            q.unsynced += 1
            while si < len(pending) and pending[si][0] < rv:
                si += 1
            seed()
    return q


def check(root: Path, date: str | None = None) -> dict[str, Quality]:
    depth = _read(root, "mexc_depth", date)
    snaps = _read(root, "mexc_snapshots", date)
    if depth is None or snaps is None:
        return {}
    # Version order keeps the rows of one message together even when two
    # messages arrived in the same millisecond.
    depth = depth.sort_by([("symbol", "ascending"), ("from_v", "ascending"),
                           ("side", "ascending")])
    s = _group_snapshots(snaps)
    d = _group_diffs(depth)
    return {sym: rebuild_symbol(s.get(sym, []), d[sym]) for sym in sorted(d)}


def main() -> None:
    ap = argparse.ArgumentParser(description="rebuild books and report data quality")
    ap.add_argument("root", type=Path)
    ap.add_argument("--date")
    args = ap.parse_args()
    res = check(args.root, args.date)
    print(f"{'symbol':12s} {'diffs':>7s} {'applied':>7s} {'stale':>6s} {'gaps':>5s} "
          f"{'unsync':>6s} {'crossed':>7s} {'checks':>6s} {'mismatch':>8s}")
    for sym, q in res.items():
        print(f"{sym:12s} {q.diffs:7d} {q.applied:7d} {q.stale:6d} {q.gaps:5d} {q.unsynced:6d} "
              f"{q.crossed:7d} {q.checks:6d} {q.top_mismatch:8d}")


if __name__ == "__main__":
    main()
