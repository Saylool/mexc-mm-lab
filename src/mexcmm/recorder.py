"""Record MEXC spot order books and trades, plus Binance best bid/offer.

Public data only: no API key, no account, no orders.

Per MEXC symbol: the 100 ms aggregated depth diffs and trade stream over
websocket, and a 1000-level REST snapshot at start, after every version gap
and every --snapshot-every seconds, so that the book can be rebuilt offline
from any point (see book.py). For symbols also listed on Binance, Binance's
bookTicker stream is recorded as the reference price.

Usage: python -m mexcmm.recorder --data data [--universe universe.json]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import time
from pathlib import Path

import websockets

from . import universe as uni
from .pb import DecodeError, decode_push
from .writer import Sink, now_ms, rss_mb

log = logging.getLogger("recorder")

MEXC_WS = "wss://wbs-api.mexc.com/ws"
MEXC_DEPTH = "https://api.mexc.com/api/v3/depth?symbol={}&limit=1000"
BINANCE_WS = "wss://stream.binance.com:9443/stream?streams="

# MEXC allows 30 subscriptions per connection; two per symbol (depth, deals).
SYMBOLS_PER_CONN = 12
# Connections live at most 24 h on MEXC; reconnect before that on our terms.
MAX_CONN_AGE_S = 23 * 3600
PING_EVERY_S = 20


class Stats:
    def __init__(self) -> None:
        self.msgs: dict[str, int] = {}
        self.gaps = 0
        self.snapshots = 0
        self.reconnects = 0

    def bump(self, k: str, n: int = 1) -> None:
        self.msgs[k] = self.msgs.get(k, 0) + n


class Recorder:
    def __init__(self, data: Path, symbols: list[dict], snapshot_every: float,
                 run_id: str, use_proxy: bool) -> None:
        self.sink = Sink(data / "raw", run_id)
        self.symbols = [s["symbol"] for s in symbols]
        self.ref = [s["symbol"] for s in symbols if s["group"] == "ref"]
        self.snapshot_every = snapshot_every
        self.proxy = True if use_proxy else None
        self.last_to: dict[str, int] = {}
        self.snapshot_queue: asyncio.Queue[str] = asyncio.Queue()
        self.queued: set[str] = set()
        self.stats = Stats()
        self.stop = asyncio.Event()

    def event(self, kind: str, symbol: str = "", detail: str = "") -> None:
        self.sink["events"].add(now_ms(), kind, symbol, detail)

    # --- snapshots -------------------------------------------------------

    def want_snapshot(self, symbol: str) -> None:
        if symbol not in self.queued:
            self.queued.add(symbol)
            self.snapshot_queue.put_nowait(symbol)

    async def snapshot_worker(self) -> None:
        while not self.stop.is_set():
            symbol = await self.snapshot_queue.get()
            self.queued.discard(symbol)
            try:
                snap = await asyncio.to_thread(uni.get_json, MEXC_DEPTH.format(symbol), 20)
            except Exception as e:  # network trouble; retry later
                log.warning("snapshot %s failed: %s", symbol, e)
                self.event("snapshot_failed", symbol, str(e)[:200])
                await asyncio.sleep(5)
                self.want_snapshot(symbol)
                continue
            recv = now_ms()
            w = self.sink["mexc_snapshots"]
            version = int(snap["lastUpdateId"])
            server = int(snap.get("timestamp") or 0)
            for side, key in ((0, "bids"), (1, "asks")):
                for i, (p, q) in enumerate(snap[key]):
                    w.add(recv, server, symbol, version, side, i, float(p), float(q))
            self.stats.snapshots += 1
            await asyncio.sleep(0.25)  # stay far below the REST limits

    async def snapshot_timer(self) -> None:
        while not self.stop.is_set():
            await asyncio.sleep(self.snapshot_every)
            for s in self.symbols:
                self.want_snapshot(s)

    # --- MEXC websocket --------------------------------------------------

    def on_mexc(self, frame: bytes, recv: int) -> None:
        try:
            p = decode_push(frame)
        except (DecodeError, ValueError, UnicodeDecodeError) as e:
            self.event("decode_error", "", str(e)[:200])
            return
        if p.depth is not None:
            d = p.depth
            prev = self.last_to.get(p.symbol)
            if prev is not None and d.from_version != prev + 1:
                self.stats.gaps += 1
                self.event("gap", p.symbol, f"{prev}->{d.from_version}")
                self.want_snapshot(p.symbol)
            self.last_to[p.symbol] = d.to_version
            w = self.sink["mexc_depth"]
            if not d.bids and not d.asks:
                # A version step without level changes still belongs to the
                # chain; without this row the rebuild would see a false gap.
                w.add(recv, p.send_time, p.symbol, d.from_version, d.to_version,
                      -1, float("nan"), float("nan"))
            for side, levels in ((0, d.bids), (1, d.asks)):
                for price, qty in levels:
                    w.add(recv, p.send_time, p.symbol, d.from_version, d.to_version,
                          side, float(price), float(qty))
            self.stats.bump("depth")
        elif p.deals is not None:
            w = self.sink["mexc_trades"]
            for t in p.deals:
                w.add(recv, t.time, p.symbol, float(t.price), float(t.quantity),
                      t.trade_type, t.trade_id)
            self.stats.bump("trades", len(p.deals))

    async def mexc_conn(self, symbols: list[str]) -> None:
        params = [f"spot@public.aggre.{ch}.v3.api.pb@100ms@{s}"
                  for s in symbols for ch in ("depth", "deals")]
        backoff = 1
        while not self.stop.is_set():
            try:
                async with websockets.connect(MEXC_WS, proxy=self.proxy, max_size=2**23,
                                              ping_interval=None) as ws:
                    await ws.send(json.dumps({"method": "SUBSCRIPTION", "params": params}))
                    self.event("connect", ",".join(symbols))
                    for s in symbols:
                        # The version chain restarts with a new connection.
                        self.last_to.pop(s, None)
                    # Snapshot once diffs are flowing, so its version falls inside
                    # the diff chain instead of before the first diff.
                    asyncio.get_running_loop().call_later(
                        2, lambda: [self.want_snapshot(s) for s in symbols])
                    opened = time.monotonic()
                    last_ping = opened
                    backoff = 1
                    while not self.stop.is_set():
                        if time.monotonic() - last_ping > PING_EVERY_S:
                            await ws.send(json.dumps({"method": "PING"}))
                            last_ping = time.monotonic()
                        if time.monotonic() - opened > MAX_CONN_AGE_S:
                            self.event("rotate", ",".join(symbols))
                            break
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=5)
                        except asyncio.TimeoutError:
                            continue
                        if isinstance(msg, bytes):
                            self.on_mexc(msg, now_ms())
                        elif '"code":0' not in msg and "PONG" not in msg:
                            self.event("server_msg", "", msg[:300])
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.stats.reconnects += 1
                log.warning("mexc connection %s… lost: %s", symbols[0], e)
                self.event("disconnect", ",".join(symbols), str(e)[:200])
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    # --- Binance reference -----------------------------------------------

    async def binance_conn(self) -> None:
        if not self.ref:
            return
        url = BINANCE_WS + "/".join(f"{s.lower()}@bookTicker" for s in self.ref)
        backoff = 1
        while not self.stop.is_set():
            try:
                async with websockets.connect(url, proxy=self.proxy, max_size=2**22) as ws:
                    self.event("binance_connect", ",".join(self.ref))
                    opened = time.monotonic()
                    backoff = 1
                    w = self.sink["binance_bbo"]
                    while not self.stop.is_set():
                        if time.monotonic() - opened > MAX_CONN_AGE_S:
                            break
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=5)
                        except asyncio.TimeoutError:
                            continue
                        d = json.loads(msg).get("data") or {}
                        if "u" not in d:
                            continue
                        w.add(now_ms(), d["s"], int(d["u"]), float(d["b"]), float(d["B"]),
                              float(d["a"]), float(d["A"]))
                        self.stats.bump("binance")
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.stats.reconnects += 1
                log.warning("binance connection lost: %s", e)
                self.event("binance_disconnect", "", str(e)[:200])
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    # --- housekeeping ----------------------------------------------------

    async def flusher(self, every: float = 30) -> None:
        last = dict(self.stats.msgs)
        t_last = time.monotonic()
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=every)
            except asyncio.TimeoutError:
                pass
            # Detach buffers on the event loop, write them in a thread.
            try:
                await asyncio.to_thread(self.sink.write, self.sink.take())
            except Exception as e:  # never let one bad write stop recording
                log.exception("write failed: %s", e)
                self.event("write_error", "", str(e)[:200])
            dt = time.monotonic() - t_last
            rates = {k: (v - last.get(k, 0)) / dt for k, v in self.stats.msgs.items()}
            last, t_last = dict(self.stats.msgs), time.monotonic()
            log.info("msgs/s %s | gaps %d snapshots %d reconnects %d | rss %d MB",
                     " ".join(f"{k}={v:.1f}" for k, v in sorted(rates.items())),
                     self.stats.gaps, self.stats.snapshots, self.stats.reconnects, rss_mb())

    async def run(self) -> None:
        self.event("start", ",".join(self.symbols))
        tasks = [asyncio.create_task(self.snapshot_worker()),
                 asyncio.create_task(self.snapshot_timer()),
                 asyncio.create_task(self.binance_conn()),
                 asyncio.create_task(self.flusher())]
        for i in range(0, len(self.symbols), SYMBOLS_PER_CONN):
            tasks.append(asyncio.create_task(self.mexc_conn(self.symbols[i:i + SYMBOLS_PER_CONN])))
        await self.stop.wait()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.event("stop")
        self.sink.close()


def load_universe(data: Path, path: Path | None, refresh: bool) -> list[dict]:
    target = path or data / "universe.json"
    if target.exists() and not refresh:
        return json.loads(target.read_text())["symbols"]
    symbols = uni.select()
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps({"selected_ms": now_ms(), "symbols": symbols}, indent=1))
    return symbols


def report_leftovers(data: Path) -> None:
    for p in (data / "raw").rglob("*.parquet.part"):
        log.warning("incomplete file from an earlier run (unreadable, not uploaded): %s", p)


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--data", type=Path, default=Path("data"))
    ap.add_argument("--universe", type=Path, help="JSON file with the symbols to record")
    ap.add_argument("--refresh-universe", action="store_true",
                    help="select symbols again from live tickers")
    ap.add_argument("--snapshot-every", type=float, default=600)
    ap.add_argument("--proxy", action="store_true",
                    help="route websockets through the environment's HTTP proxy")
    ap.add_argument("--duration", type=float, help="stop after this many seconds")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    report_leftovers(args.data)
    symbols = load_universe(args.data, args.universe, args.refresh_universe)
    log.info("recording %d symbols: %s", len(symbols), " ".join(s["symbol"] for s in symbols))
    # The venue prefix keeps file names apart when several recorders start in
    # the same second (they share the events stream directory).
    run_id = "mexc" + time.strftime("%Y%m%d%H%M%S", time.gmtime())
    rec = Recorder(args.data, symbols, args.snapshot_every, run_id, args.proxy)

    async def go() -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, rec.stop.set)
        if args.duration:
            loop.call_later(args.duration, rec.stop.set)
        await rec.run()

    asyncio.run(go())


if __name__ == "__main__":
    main()
