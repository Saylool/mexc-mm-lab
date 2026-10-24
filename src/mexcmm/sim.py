"""Strategy simulations on a recording, at several fee levels and latencies.

Two strategies, both replayed against the recorded MEXC trades and books:

1. maker ("what the fast bots appear to do"): keep a bid at fair*(1-d) and an
   ask at fair*(1+d), where fair is the Binance mid (pairs listed there) or
   the MEXC mid, as known `latency` ms ago, skewed against inventory. A
   recorded taker trade fills our quote when it prints at a price at least as
   good for us: strictly through it (conservative), or at it too
   (optimistic, assumes queue priority). Fill size is min(trade size, quote
   size); inventory is capped. Leftover inventory is closed at the end at
   the far side of the book (paying the spread). The maker fee applies to
   fills (0 on MEXC spot; negative = rebate).
2. taker: when the Binance mid moved more than `threshold` bps away from
   the MEXC mid, buy (sell) on MEXC at the ask (bid) `latency` ms later, hold
   `hold` seconds, close at the bid (ask). Taker fee on both legs.

Market impact is ignored (our orders do not change other traders'
behaviour), which flatters both strategies.

Usage: python -m mexcmm.sim data/raw [--cache reports/tops.npz]
"""

from __future__ import annotations

import argparse
import glob
import json
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from .analyze import load_snapshots, mid_at, rebuild_tops


def load_tops(root: Path, cache: Path | None):
    if cache and cache.exists():
        z = np.load(cache, allow_pickle=False)
        syms = sorted({k.rsplit("_", 1)[0] for k in z.files})
        return {s: (z[f"{s}_t"], z[f"{s}_bid"], z[f"{s}_ask"]) for s in syms}
    tops, _ = rebuild_tops(root, load_snapshots(root))
    out = {s: tp.arrays() for s, tp in tops.items()}
    if cache:
        cache.parent.mkdir(parents=True, exist_ok=True)
        np.savez_compressed(cache, **{f"{s}_{n}": a for s, arrs in out.items()
                                      for n, a in zip(("t", "bid", "ask"), arrs)})
    return out


def load_stream(root: Path, stream: str, cols):
    files = sorted(glob.glob(str(root / stream / "*" / "*.parquet")))
    return pa.concat_tables([pq.read_table(f, columns=cols) for f in files])


def binance_mids(bb, sym):
    m = bb["symbol"].to_numpy(zero_copy_only=False) == sym
    if m.sum() < 1000:
        return None
    t = bb["recv_ms"].to_numpy()[m]
    mid = (bb["bid"].to_numpy()[m] + bb["ask"].to_numpy()[m]) / 2
    o = np.argsort(t, kind="stable")
    return t[o], mid[o]


def sim_maker(trades, tops, fair_t, fair, *, d_bps, latency_ms, size_usdt, max_inv_usdt,
              maker_fee_bps, optimistic):
    """Returns dict with pnl and fill statistics for one symbol."""
    rv, price, qty, side = trades
    tt, bid, ask = tops
    f = mid_at(fair_t, fair, rv - latency_ms)
    inv = 0.0  # in coins
    cash = 0.0
    fills = 0
    volume = 0.0
    fees = 0.0
    for i in range(len(rv)):
        fv = f[i]
        if fv != fv:  # NaN: no fair price yet
            continue
        p = price[i]
        # Skew: shift both quotes against inventory, up to d at the cap.
        skew = -(inv * fv) / max_inv_usdt * d_bps
        b = fv * (1 + (-d_bps + skew) / 1e4)
        a = fv * (1 + (d_bps + skew) / 1e4)
        if side[i] == 2:  # taker sold: can fill our bid
            hit = p < b or (optimistic and p <= b)
            if hit and inv * fv < max_inv_usdt:
                q = min(qty[i], size_usdt / b, (max_inv_usdt - inv * fv) / b)
                inv += q
                cash -= q * b
                fee = q * b * maker_fee_bps / 1e4
        else:  # taker bought: can fill our ask
            hit = p > a or (optimistic and p >= a)
            if hit and -inv * fv < max_inv_usdt:
                q = min(qty[i], size_usdt / a, (max_inv_usdt + inv * fv) / a)
                inv -= q
                cash += q * a
                fee = q * a * maker_fee_bps / 1e4
        if hit and q > 0:
            fills += 1
            volume += q * (b if side[i] == 2 else a)
            cash -= fee
            fees += fee
    # Close leftover inventory crossing the spread at the last book.
    close = bid[-1] if inv > 0 else ask[-1]
    cash += inv * close
    return {"pnl_usdt": cash, "fills": fills, "volume_usdt": volume, "fees_usdt": fees,
            "end_inventory_usdt": inv * close}


def sim_taker(tops, fair_t, fair, *, threshold_bps, latency_ms, hold_s, taker_fee_bps,
              size_usdt, step_ms=250):
    tt, bid, ask = tops
    start, end = max(tt[0], fair_t[0]), min(tt[-1], fair_t[-1]) - hold_s * 1000 - latency_ms
    grid = np.arange(start, end, step_ms)
    mm = (mid_at(tt, bid, grid) + mid_at(tt, ask, grid)) / 2
    fm = mid_at(fair_t, fair, grid)
    dev = (fm - mm) / mm * 1e4
    pnl = 0.0
    trades = 0
    busy_until = 0
    for i in np.flatnonzero(np.abs(dev) > threshold_bps):
        t0 = grid[i]
        if t0 < busy_until:
            continue
        te, tx = np.array([t0 + latency_ms]), np.array([t0 + latency_ms + hold_s * 1000])
        if dev[i] > 0:  # Binance above MEXC: buy MEXC
            entry, exit_ = mid_at(tt, ask, te)[0], mid_at(tt, bid, tx)[0]
            r = (exit_ - entry) / entry
        else:
            entry, exit_ = mid_at(tt, bid, te)[0], mid_at(tt, ask, tx)[0]
            r = (entry - exit_) / entry
        pnl += size_usdt * (r - 2 * taker_fee_bps / 1e4)
        trades += 1
        busy_until = tx[0]
    return {"pnl_usdt": pnl, "trades": trades}


def main() -> None:
    ap = argparse.ArgumentParser(description="strategy simulations")
    ap.add_argument("root", type=Path)
    ap.add_argument("--cache", type=Path, default=Path("reports/tops.npz"))
    ap.add_argument("--out", type=Path)
    args = ap.parse_args()
    universe = json.loads((args.root.parent / "universe.json").read_text())["symbols"]
    groups = {s["symbol"]: s["group"] for s in universe}

    tops = load_tops(args.root, args.cache)
    tr = load_stream(args.root, "mexc_trades", ["recv_ms", "symbol", "price", "qty", "side"])
    tr = tr.sort_by([("recv_ms", "ascending")])
    tsym = tr["symbol"].to_numpy(zero_copy_only=False)
    bb = load_stream(args.root, "binance_bbo", ["recv_ms", "symbol", "bid", "ask"])

    hours = {s: (t[-1] - t[0]) / 3.6e6 for s, (t, _, _) in tops.items()}
    out = {"hours": hours, "maker": {}, "taker": {}}
    for sym in sorted(tops):
        tp = tops[sym]
        m = tsym == sym
        trades = tuple(tr[c].to_numpy()[m] for c in ("recv_ms", "price", "qty", "side"))
        bm = binance_mids(bb, sym) if groups.get(sym) == "ref" else None
        if bm is not None:
            fair_t, fair = bm
            fair_src = "binance"
        else:
            fair_t, fair = tp[0], (tp[1] + tp[2]) / 2
            fair_src = "mexc"
        res = {"fair": fair_src, "runs": []}
        for latency in (50, 300, 1000):
            for d in (5, 10, 20, 40):
                for fee in (0.0, -0.75):  # MEXC spot base; MM program top tier rebate
                    for optimistic in (False, True):
                        r = sim_maker(trades, tp, fair_t, fair, d_bps=d, latency_ms=latency,
                                      size_usdt=100, max_inv_usdt=500, maker_fee_bps=fee,
                                      optimistic=optimistic)
                        r.update(latency_ms=latency, d_bps=d, maker_fee_bps=fee,
                                 optimistic=optimistic)
                        res["runs"].append(r)
        out["maker"][sym] = res
        if bm is not None:
            runs = []
            for latency in (50, 300, 1000):
                for th in (5, 10, 20):
                    for fee in (5.0, 2.0, 0.0):
                        r = sim_taker(tp, fair_t, fair, threshold_bps=th, latency_ms=latency,
                                      hold_s=5, taker_fee_bps=fee, size_usdt=100)
                        r.update(latency_ms=latency, threshold_bps=th, taker_fee_bps=fee)
                        runs.append(r)
            out["taker"][sym] = runs
    text = json.dumps(out, indent=1)
    if args.out:
        args.out.write_text(text)
    else:
        print(text)


if __name__ == "__main__":
    main()
