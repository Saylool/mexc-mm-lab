"""Pick the symbols to record from public 24h tickers.

Market making earns the spread, so the interesting pairs have a spread wide
enough to pay for adverse moves and enough volume to get filled. Two groups:

- "ref": pairs also listed on Binance spot, so the Binance best bid/offer can
  be recorded as a reference price (MEXC prices of such coins mostly follow
  Binance; the lag between the two is a signal in its own right).
- "solo": MEXC-only pairs, where nobody else's book anchors the price.

Excluded: stablecoins and pegged tokens, and tokenized stocks (MEXC's
"...ON" pairs such as TSLAONUSDT), whose 24h volumes cluster suspiciously
around the same value and look incentivised rather than organic.
"""

from __future__ import annotations

import http.client
import json
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass

MEXC_TICKERS = "https://api.mexc.com/api/v3/ticker/24hr"
BINANCE_BOOK = "https://api.binance.com/api/v3/ticker/bookTicker"

STABLE_BASES = {
    "USDC", "USDE", "USD1", "FDUSD", "TUSD", "DAI", "USDD", "PYUSD", "USDP", "EUR",
    "EURC", "USDF", "USD", "BUSD", "RLUSD", "USDS", "USDTB", "XUSD", "LEE",
}


@dataclass
class Candidate:
    symbol: str
    group: str  # "ref" or "solo"
    spread_bps: float
    quote_volume: float
    mid: float


def get_json(url: str, timeout: float = 30, attempts: int = 4):
    req = urllib.request.Request(url, headers={"User-Agent": "mexc-mm-lab"})
    for i in range(attempts):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return json.load(r)
        except (http.client.IncompleteRead, urllib.error.URLError, TimeoutError):
            if i == attempts - 1:
                raise
            time.sleep(2 * (i + 1))


def is_tokenized_stock(base: str) -> bool:
    # Ondo tokenized equities are listed as <TICKER>ON (TSLAON, AAPLON). The
    # length check keeps short bases such as MON; a few real coins ending in
    # ON are lost, which is acceptable for a candidate filter.
    return len(base) >= 4 and base.endswith("ON")


def candidates(mexc_tickers, binance_symbols: set[str], *, min_volume: float,
               max_volume: float, min_spread_bps: float) -> list[Candidate]:
    out = []
    for t in mexc_tickers:
        s = t.get("symbol", "")
        if not s.endswith("USDT"):
            continue
        base = s[:-4]
        if base in STABLE_BASES or is_tokenized_stock(base) or "(" in base:
            continue
        try:
            bid, ask = float(t["bidPrice"]), float(t["askPrice"])
            qv = float(t["quoteVolume"] or 0)
        except (KeyError, TypeError, ValueError):
            continue
        if bid <= 0 or ask <= bid:
            continue
        mid = (bid + ask) / 2
        spread = (ask - bid) / mid * 1e4
        if not (min_volume <= qv <= max_volume) or spread < min_spread_bps:
            continue
        group = "ref" if s in binance_symbols else "solo"
        out.append(Candidate(s, group, spread, qv, mid))
    return out


def pick(cands: list[Candidate], n_ref: int, n_solo: int) -> list[Candidate]:
    """Highest-volume candidates of each group."""
    by_vol = sorted(cands, key=lambda c: -c.quote_volume)
    ref = [c for c in by_vol if c.group == "ref"][:n_ref]
    solo = [c for c in by_vol if c.group == "solo"][:n_solo]
    return ref + solo


def select(n_ref: int = 12, n_solo: int = 8, min_volume: float = 200_000,
           max_volume: float = 20_000_000, min_spread_bps: float = 8.0) -> list[dict]:
    mexc = get_json(MEXC_TICKERS)
    binance = {b["symbol"] for b in get_json(BINANCE_BOOK) if float(b.get("bidPrice") or 0) > 0}
    cands = candidates(mexc, binance, min_volume=min_volume, max_volume=max_volume,
                       min_spread_bps=min_spread_bps)
    return [asdict(c) for c in pick(cands, n_ref, n_solo)]


if __name__ == "__main__":
    for c in select():
        print(f"{c['symbol']:14s} {c['group']:4s} spread {c['spread_bps']:6.1f} bps  "
              f"vol ${c['quote_volume']:>12,.0f}")
