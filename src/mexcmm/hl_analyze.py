"""Hyperliquid maker analysis, comparable with analyze.py for MEXC.

Book: every recorded l2Book push is a full top-20 book, so the top of the
book is level 0 of each push (levels[0] = bids, checked against recorded
data: bid < ask in every push). Trade side "B" means the taker bought
(87 % of recorded "B" trades print above the mid); `users` is
[buyer, seller] per the API docs, so the maker is the seller of a "B"
trade and the buyer of an "A" trade.

Per coin: time-weighted spread, the share of volume that traded strictly
inside the last pushed book, and maker markouts (as in analyze.py) for
fills at the touch and inside. Hyperliquid pushes books roughly once per
block, slower than MEXC's 100 ms diffs, so "inside" here also catches
quotes that lived for less than one push.

Per wallet: maker volume and markout PnL at 60 s across all coins, with the
base maker fee (0.015 %) subtracted, which upper-bounds what the same fills
would earn a newcomer with base fees. The largest makers show who provides
the liquidity and whether it pays.

Usage: python -m mexcmm.hl_analyze data/raw [--out report.json]
"""

from __future__ import annotations

import argparse
import glob
import json
from collections import defaultdict
from pathlib import Path

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .analyze import HORIZONS_S, mid_at, spread_stats

BASE_MAKER_FEE_BPS = 1.5


def read_stream(root: Path, stream: str, cols=None):
    files = sorted(glob.glob(str(root / stream / "*" / "*.parquet")))
    if not files:
        return None
    return pa.concat_tables([pq.read_table(f, columns=cols) for f in files])


def tops_by_coin(book):
    l0 = book.filter(pc.equal(book["level"], 0))
    out = {}
    for coin in pc.unique(l0["coin"]).to_pylist():
        x = l0.filter(pc.equal(l0["coin"], coin))
        bid = x.filter(pc.equal(x["side"], 0)).sort_by("recv_ms")
        ask = x.filter(pc.equal(x["side"], 1)).sort_by("recv_ms")
        # One bid row and one ask row per push; align on push time.
        bt, at = bid["time_ms"].to_numpy(), ask["time_ms"].to_numpy()
        common, bi, ai = np.intersect1d(bt, at, return_indices=True)
        t = bid["recv_ms"].to_numpy()[bi]
        o = np.argsort(t, kind="stable")
        out[coin] = (t[o], bid["price"].to_numpy()[bi][o], ask["price"].to_numpy()[ai][o])
    return out


def dedupe_trades(tr):
    # A resubscribe replays recent trades; (time, coin, tid) is unique per the docs.
    keys = pa.Table.from_arrays([tr["time_ms"], tr["coin"], tr["tid"]], names=["a", "b", "c"])
    idx = keys.append_column("i", pa.array(np.arange(tr.num_rows))) \
              .group_by(["a", "b", "c"]).aggregate([("i", "min")])["i_min"]
    return tr.take(pc.sort_indices(idx))


def analyze(root: Path):
    book = read_stream(root, "hl_book")
    tr = read_stream(root, "hl_trades")
    if book is None or tr is None:
        return {}
    tr = dedupe_trades(tr).sort_by("recv_ms")
    tops = tops_by_coin(book)
    report = {"spread": {}, "markouts": {}, "wallets": {}}
    wallet = defaultdict(lambda: {"fills": 0, "volume_usdt": 0.0, "pnl_60s_usdt": 0.0,
                                  "coins": set()})
    coins_col = tr["coin"].to_numpy(zero_copy_only=False)
    for coin, (tt, bid, ask) in sorted(tops.items()):
        if len(tt) < 100:
            continue
        report["spread"][coin] = spread_stats(tt, bid, ask)
        mid = (bid + ask) / 2
        m = coins_col == coin
        rv = tr["recv_ms"].to_numpy()[m]
        p = tr["price"].to_numpy()[m]
        q = tr["qty"].to_numpy()[m]
        sd = tr["side"].to_numpy()[m]
        buyer = tr["buyer"].to_numpy(zero_copy_only=False)[m]
        seller = tr["seller"].to_numpy(zero_copy_only=False)[m]
        ok = (rv > tt[0]) & (rv + HORIZONS_S[-1] * 1000 < tt[-1])
        rv, p, q, sd, buyer, seller = rv[ok], p[ok], q[ok], sd[ok], buyer[ok], seller[ok]
        if len(rv) < 50:
            continue
        notional = p * q
        sign = np.where(sd == 2, 1.0, -1.0)  # maker bought on a taker sell
        maker = np.where(sd == 2, buyer, seller)
        bb, ba = mid_at(tt, bid, rv - 1), mid_at(tt, ask, rv - 1)
        mid_before = (bb + ba) / 2
        inside = (p > bb * (1 + 1e-9)) & (p < ba * (1 - 1e-9))
        row = {"trades": int(len(rv)), "volume_usdt": float(notional.sum()),
               "median_trade_usdt": float(np.median(notional)),
               "inside_spread_volume_share": float(notional[inside].sum() / notional.sum()),
               "distinct_makers": int(len(set(maker)))}
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
        # Wallet-level maker results at 60 s, after the base maker fee.
        ref = mid_at(tt, mid, rv + 60_000)
        bps = sign * (ref - p) / p * 1e4 - BASE_MAKER_FEE_BPS
        pnl = bps / 1e4 * notional
        g = ~np.isnan(pnl)
        for w, n, pl in zip(maker[g], notional[g], pnl[g]):
            s = wallet[w]
            s["fills"] += 1
            s["volume_usdt"] += float(n)
            s["pnl_60s_usdt"] += float(pl)
            s["coins"].add(coin)
        # Concentration: share of maker volume taken by the five largest makers.
        vol_by = defaultdict(float)
        for w, n in zip(maker, notional):
            vol_by[w] += n
        top = sorted(vol_by.values(), reverse=True)
        row["top5_maker_volume_share"] = float(sum(top[:5]) / sum(top))
        report["markouts"][coin] = row
    ranked = sorted(wallet.items(), key=lambda kv: -kv[1]["volume_usdt"])
    total = sum(v["volume_usdt"] for _, v in ranked) or 1
    report["wallets"] = {
        "makers": len(ranked),
        "top": [{"wallet": w, **{k: v for k, v in s.items() if k != "coins"},
                 "coins": sorted(s["coins"]),
                 "volume_share": s["volume_usdt"] / total,
                 "pnl_bps": s["pnl_60s_usdt"] / s["volume_usdt"] * 1e4 if s["volume_usdt"] else 0}
                for w, s in ranked[:25]],
    }
    hours = {c: (t[-1] - t[0]) / 3.6e6 for c, (t, _, _) in tops.items()}
    report["hours"] = hours
    return report


def main() -> None:
    ap = argparse.ArgumentParser(description="Hyperliquid maker analysis")
    ap.add_argument("root", type=Path)
    ap.add_argument("--out", type=Path)
    args = ap.parse_args()
    text = json.dumps(analyze(args.root), indent=1)
    if args.out:
        args.out.write_text(text)
    else:
        print(text)


if __name__ == "__main__":
    main()
