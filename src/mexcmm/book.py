"""Local order book built from a REST snapshot plus websocket diffs.

Follows MEXC's documented procedure ("How to properly maintain a local copy
of the order book", spot v3 docs):

1. Subscribe to spot@public.aggre.depth.v3.api.pb@100ms@SYMBOL.
2. Fetch a REST snapshot (/api/v3/depth?limit=1000); its lastUpdateId is the
   snapshot version.
3. Drop diffs whose toVersion is below the snapshot version.
4. If the first remaining diff's fromVersion is above the snapshot version,
   data is missing: take a new snapshot.
5. Afterwards every diff's fromVersion must equal the previous toVersion + 1.
6. Quantities are absolute sizes at the level; 0 removes the level.

The snapshot is depth-limited, so levels outside it that never change are
missing from the local book; the top of the book is exact.
"""

from __future__ import annotations

from enum import Enum


class Applied(Enum):
    OK = "ok"
    STALE = "stale"  # diff entirely older than the book; ignored
    GAP = "gap"  # versions skipped; book is invalid until a new snapshot
    NO_SNAPSHOT = "no_snapshot"


class OrderBook:
    def __init__(self) -> None:
        self.bids: dict[float, float] = {}
        self.asks: dict[float, float] = {}
        self.version: int | None = None
        self.synced = False  # True once a diff has been chained onto the snapshot

    def apply_snapshot(self, version: int, bids, asks) -> None:
        self.bids = {float(p): float(q) for p, q in bids if float(q) > 0}
        self.asks = {float(p): float(q) for p, q in asks if float(q) > 0}
        self.version = version
        self.synced = False

    def apply_diff(self, from_v: int, to_v: int, bids, asks) -> Applied:
        if self.version is None:
            return Applied.NO_SNAPSHOT
        if to_v <= self.version:
            return Applied.STALE
        if self.synced:
            if from_v != self.version + 1:
                self.version = None
                return Applied.GAP
        elif from_v > self.version + 1:
            # The first diff after the snapshot must cover version + 1.
            self.version = None
            return Applied.GAP
        for side, levels in ((self.bids, bids), (self.asks, asks)):
            for p, q in levels:
                p, q = float(p), float(q)
                if q == 0:
                    side.pop(p, None)
                else:
                    side[p] = q
        self.version = to_v
        self.synced = True
        return Applied.OK

    def best_bid(self) -> tuple[float, float] | None:
        if not self.bids:
            return None
        p = max(self.bids)
        return p, self.bids[p]

    def best_ask(self) -> tuple[float, float] | None:
        if not self.asks:
            return None
        p = min(self.asks)
        return p, self.asks[p]

    def crossed(self) -> bool:
        b, a = self.best_bid(), self.best_ask()
        return b is not None and a is not None and b[0] >= a[0]
