"""First market-making analysis of a recording.

1. Rebuild every book (snapshot + diffs, file by file) into a top-of-book
   time series per symbol.
2. Spread: time-weighted distribution of the quoted spread.
3. Maker markouts: every trade has a resting maker on the other side. For a
   taker sell at price p the maker bought at p; after a horizon h its result
   is mid(t+h) - p (for a taker buy: p - mid(t+h)), in bps of p. With a 0 %
   maker fee this is the maker's net result per fill. Positive at long
   horizons means resting quotes earned money on average; the drop from
   h=0 to longer horizons is adverse selection.
4. Binance lead: correlation between Binance mid moves and later MEXC mid
   moves on a 1 s grid, for pairs listed on both.

Caveats: markouts describe fills that the makers present at the time got.
A newcomer joins the back of the queue and is filled more often when price
trades through, so its markouts are worse. Hidden self-trades (wash trades)
look like ordinary fills and inflate the sample.

Usage: python -m mexcmm.analyze data/raw [--out report.json]
"""

from __future__ import annotations

import argparse
import glob
import json
from collections import defaultdict
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq

from .book import Applied, OrderBook

HORIZONS_S = [0, 1, 10, 60, 300]
BOOK_LAG_MS = 150


def load_snapshots(root: Path):
    """symbol -> list of (recv_ms, version, bids, asks) sorted by arrival."""
    out: dict[str, dict] = defaultdict(dict)
    for f in sorted(glob.glob(str(root / "mexc_snapshots" / "*" / "*.parquet"))):
        t = pq.read_table(f, columns=["symbol", "recv_ms", "version", "side", "price", "qty"])
        for sym, rv, ver, side, p, q in zip(*(t[c].to_pylist() for c in t.column_names)):
            e = out[sym].setdefault((rv, ver), ([], []))
            e[side].append((p, q))
    return {s: sorted((rv, ver, b, a) for (rv, ver), (b, a) in d.items()) for s, d in out.items()}


class Tops:
    """Top-of-book series of one symbol, appended as the book is rebuilt."""

    def __init__(self) -> None:
        self.t: list[int] = []
        self.bid: list[float] = []
        self.ask: list[float] = []

    def arrays(self):
        return np.array(self.t, dtype=np.int64), np.array(self.bid), np.array(self.ask)


def rebuild_tops(root: Path, snaps):
    """Rebuild all books; returns symbol -> Tops and a quality summary."""
    books: dict[str, OrderBook] = {}
    snap_i: dict[str, int] = defaultdict(int)
    tops: dict[str, Tops] = defaultdict(Tops)
    quality: dict[str, dict] = defaultdict(lambda: defaultdict(int))

    def seed(sym: str, after_ms: int) -> None:
        lst = snaps.get(sym, [])
        i = snap_i[sym]
        while i < len(lst) and lst[i][0] < after_ms:
            i += 1
        if i < len(lst):
            _, ver, b, a = lst[i]
            books[sym].apply_snapshot(ver, b, a)
            i += 1
        snap_i[sym] = i

    files = sorted(glob.glob(str(root / "mexc_depth" / "*" / "*.parquet")))
    for f in files:
        t = pq.read_table(f).sort_by([("symbol", "ascending"), ("from_v", "ascending"),
                                      ("side", "ascending")])
        sym_col = t["symbol"].to_numpy(zero_copy_only=False)
        fv = t["from_v"].to_numpy()
        tv = t["to_v"].to_numpy()
        rv = t["recv_ms"].to_numpy()
        side = t["side"].to_numpy()
        price = t["price"].to_numpy()
        qty = t["qty"].to_numpy()
        n = len(fv)
        if n == 0:
            continue
        brk = np.flatnonzero((fv[1:] != fv[:-1]) | (sym_col[1:] != sym_col[:-1])) + 1
        starts = np.concatenate(([0], brk))
        ends = np.concatenate((brk, [n]))
        for s, e in zip(starts.tolist(), ends.tolist()):
            sym = sym_col[s]
            book = books.get(sym)
            if book is None:
                book = books[sym] = OrderBook()
                seed(sym, 0)
            q = quality[sym]
            q["diffs"] += 1
            bids = [(price[i], qty[i]) for i in range(s, e) if side[i] == 0]
            asks = [(price[i], qty[i]) for i in range(s, e) if side[i] == 1]
            r = book.apply_diff(int(fv[s]), int(tv[s]), bids, asks)
            if r is Applied.OK:
                bb, ba = book.best_bid(), book.best_ask()
                if bb and ba:
                    if bb[0] >= ba[0]:
                        q["crossed"] += 1
                    else:
                        tp = tops[sym]
                        tp.t.append(int(rv[s]))
                        tp.bid.append(bb[0])
                        tp.ask.append(ba[0])
            elif r is Applied.STALE:
                q["stale"] += 1
            else:
                q["gap" if r is Applied.GAP else "unsynced"] += 1
                seed(sym, int(rv[s]))
    return tops, {s: dict(v) for s, v in quality.items()}


def spread_stats(t, bid, ask):
    if len(t) < 2:
        return None
    mid = (bid + ask) / 2
    spread = (ask - bid) / mid * 1e4
    dur = np.diff(t).astype(float)
    dur = np.minimum(dur, 60_000)  # do not let an outage weigh as one long state
    s = spread[:-1]
    order = np.argsort(s)
    cum = np.cumsum(dur[order]) / dur.sum()
    pct = lambda p: float(s[order][np.searchsorted(cum, p)])
    return {"p10": pct(0.10), "median": pct(0.50), "p90": pct(0.90),
            "mean": float((s * dur).sum() / dur.sum()), "hours": float(dur.sum() / 3.6e6)}


def mid_at(t, mid, when):
    """Mid of the latest top at or before `when`; NaN before the series starts."""
    i = np.searchsorted(t, when, side="right") - 1
    out = mid[np.clip(i, 0, len(mid) - 1)].copy()
    out[i < 0] = np.nan
    return out


def markouts(root: Path, tops_by_sym):
    res = {}
    files = sorted(glob.glob(str(root / "mexc_trades" / "*" / "*.parquet")))
    tables = [pq.read_table(f, columns=["recv_ms", "symbol", "price", "qty", "side"]) for f in files]
    import pyarrow as pa
    trades = pa.concat_tables(tables)
    syms = trades["symbol"].to_numpy(zero_copy_only=False)
    for sym, tp in tops_by_sym.items():
        tt, bid, ask = tp.arrays()
        if len(tt) < 100:
            continue
        mid = (bid + ask) / 2
        m = syms == sym
        rv = trades["recv_ms"].to_numpy()[m]
        p = trades["price"].to_numpy()[m]
        qy = trades["qty"].to_numpy()[m]
        sd = trades["side"].to_numpy()[m]
        # Only trades inside the rebuilt window, with every horizon available.
        ok = (rv > tt[0]) & (rv + HORIZONS_S[-1] * 1000 < tt[-1])
        rv, p, qy, sd = rv[ok], p[ok], qy[ok], sd[ok]
        if len(rv) < 50:
            continue
        notional = p * qy
        # Maker sign: taker sell (2) -> maker bought (+1); taker buy (1) -> maker sold (-1).
        sign = np.where(sd == 2, 1.0, -1.0)
        # Book shortly before the trade arrived. Trades and depth come on
        # separate streams ~100 ms apart, so look BOOK_LAG_MS back.
        bb = mid_at(tt, bid, rv - BOOK_LAG_MS)
        ba = mid_at(tt, ask, rv - BOOK_LAG_MS)
        mid_before = (bb + ba) / 2
        # A trade strictly inside the visible spread hit an order that never
        # showed up in the 100 ms aggregated book (posted and filled within
        # one aggregation window). A slow quote at the touch cannot get those.
        inside = (p > bb * (1 + 1e-9)) & (p < ba * (1 - 1e-9))
        row = {"trades": int(len(rv)), "volume_usdt": float(notional.sum()),
               "median_trade_usdt": float(np.median(notional)),
               "inside_spread_volume_share": float(notional[inside].sum() / notional.sum())}
        for name, sel in (("all", np.ones_like(inside)), ("touch", ~inside), ("inside", inside)):
            if sel.sum() < 20:
                continue
            for h in HORIZONS_S:
                ref = mid_before if h == 0 else mid_at(tt, mid, rv + h * 1000)
                bps = sign * (ref - p) / p * 1e4
                g = sel & ~np.isnan(bps)
                row[f"{name}_mo_{h}s_bps"] = float(np.average(bps[g], weights=notional[g]))
                row[f"{name}_pnl_{h}s_usdt"] = float((bps[g] / 1e4 * notional[g]).sum())
            row[f"{name}_volume_usdt"] = float(notional[sel].sum())
            row[f"{name}_trades"] = int(sel.sum())
        res[sym] = row
    return res


def binance_lead(root: Path, tops_by_sym, ref_symbols, max_lag_s: int = 5):
    files = sorted(glob.glob(str(root / "binance_bbo" / "*" / "*.parquet")))
    if not files:
        return {}
    import pyarrow as pa
    bb = pa.concat_tables([pq.read_table(f) for f in files])
    syms = bb["symbol"].to_numpy(zero_copy_only=False)
    out = {}
    for sym in ref_symbols:
        tp = tops_by_sym.get(sym)
        m = syms == sym
        if tp is None or m.sum() < 1000:
            continue
        tt, bid, ask = tp.arrays()
        bt = bb["recv_ms"].to_numpy()[m]
        bmid = (bb["bid"].to_numpy()[m] + bb["ask"].to_numpy()[m]) / 2
        order = np.argsort(bt, kind="stable")
        bt, bmid = bt[order], bmid[order]
        start, end = max(tt[0], bt[0]), min(tt[-1], bt[-1])
        grid = np.arange(start, end, 1000)
        mm = mid_at(tt, (bid + ask) / 2, grid)
        bm = mid_at(bt, bmid, grid)
        rm, rb = np.diff(np.log(mm)), np.diff(np.log(bm))
        g = ~(np.isnan(rm) | np.isnan(rb))
        rm, rb = rm[g], rb[g]
        corr = {}
        for k in range(0, max_lag_s + 1):
            a, b = rb[:len(rb) - k], rm[k:]
            corr[k] = float(np.corrcoef(a, b)[0, 1]) if len(a) > 100 else None
        # Price level gap: MEXC mid versus Binance mid, in bps.
        diff_bps = (mm - bm) / bm * 1e4
        diff_bps = diff_bps[~np.isnan(diff_bps)]
        out[sym] = {"corr_binance_leads_mexc_by_s": corr,
                    "mexc_minus_binance_median_bps": float(np.median(diff_bps)),
                    "mexc_minus_binance_p95_abs_bps": float(np.quantile(np.abs(diff_bps), 0.95))}
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="first market-making analysis")
    ap.add_argument("root", type=Path)
    ap.add_argument("--universe", type=Path)
    ap.add_argument("--out", type=Path)
    args = ap.parse_args()
    uni_path = args.universe or args.root.parent / "universe.json"
    universe = json.loads(uni_path.read_text())["symbols"]
    ref = [s["symbol"] for s in universe if s["group"] == "ref"]

    snaps = load_snapshots(args.root)
    tops, quality = rebuild_tops(args.root, snaps)
    report = {"quality": quality,
              "spread": {s: spread_stats(*tp.arrays()) for s, tp in tops.items()},
              "markouts": markouts(args.root, tops),
              "binance_lead": binance_lead(args.root, tops, ref),
              "groups": {s["symbol"]: s["group"] for s in universe}}
    text = json.dumps(report, indent=1)
    if args.out:
        args.out.write_text(text)
    else:
        print(text)


if __name__ == "__main__":
    main()
