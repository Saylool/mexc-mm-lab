"""Record Hyperliquid perp order books and trades.

Public data only. Hyperliquid's websocket (wss://api.hyperliquid.xyz/ws)
pushes the full top-20 book of a coin on every change (`l2Book`) and every
trade with both wallet addresses (`trades`), so unlike MEXC there are no
diffs to chain and no snapshots to fetch; the book file simply holds every
pushed book. The addresses let us measure what real market makers earn.

Coins: the "ref" coins of the MEXC universe that also trade on Hyperliquid
(same coin on both venues, so bot density and maker markouts are comparable)
plus the coins in --extra.

Usage: python -m mexcmm.hl_recorder --data data [--extra HYPE]
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import signal
import time
import urllib.request
from pathlib import Path

import websockets

from .writer import Sink, now_ms

log = logging.getLogger("hl")

HL_WS = "wss://api.hyperliquid.xyz/ws"
HL_INFO = "https://api.hyperliquid.xyz/info"
PING_EVERY_S = 30
MAX_CONN_AGE_S = 23 * 3600


def post_json(url: str, body: dict, timeout: float = 30):
    req = urllib.request.Request(url, data=json.dumps(body).encode(),
                                 headers={"content-type": "application/json",
                                          "User-Agent": "mexc-mm-lab"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


def perp_coins() -> set[str]:
    meta = post_json(HL_INFO, {"type": "meta"})
    return {u["name"] for u in meta["universe"] if not u.get("isDelisted")}


class HLRecorder:
    def __init__(self, data: Path, coins: list[str], run_id: str, use_proxy: bool) -> None:
        self.sink = Sink(data / "raw", run_id)
        self.coins = coins
        self.proxy = True if use_proxy else None
        self.stop = asyncio.Event()
        self.msgs = {"book": 0, "trades": 0}
        self.reconnects = 0

    def event(self, kind: str, symbol: str = "", detail: str = "") -> None:
        self.sink["events"].add(now_ms(), kind, symbol, detail)

    def on_book(self, d: dict, recv: int) -> None:
        w = self.sink["hl_book"]
        coin, t = d["coin"], int(d["time"])
        for side, levels in enumerate(d["levels"]):  # 0 bids, 1 asks
            for i, lv in enumerate(levels):
                w.add(recv, t, coin, side, i, float(lv["px"]), float(lv["sz"]), int(lv["n"]))
        self.msgs["book"] += 1

    def on_trades(self, trades: list[dict], recv: int) -> None:
        w = self.sink["hl_trades"]
        for tr in trades:
            users = tr.get("users") or ["", ""]
            # HL side is the taker's side: "B" = taker bought from a resting ask.
            side = 1 if tr["side"] == "B" else 2
            w.add(recv, int(tr["time"]), tr["coin"], float(tr["px"]), float(tr["sz"]),
                  side, int(tr["tid"]), users[0], users[1])
        self.msgs["trades"] += len(trades)

    async def conn(self) -> None:
        backoff = 1
        while not self.stop.is_set():
            try:
                async with websockets.connect(HL_WS, proxy=self.proxy, max_size=2**23,
                                              ping_interval=None) as ws:
                    for c in self.coins:
                        for typ in ("l2Book", "trades"):
                            await ws.send(json.dumps({"method": "subscribe",
                                                      "subscription": {"type": typ, "coin": c}}))
                    self.event("hl_connect", ",".join(self.coins))
                    opened = last_ping = time.monotonic()
                    backoff = 1
                    while not self.stop.is_set():
                        now = time.monotonic()
                        if now - last_ping > PING_EVERY_S:
                            await ws.send(json.dumps({"method": "ping"}))
                            last_ping = now
                        if now - opened > MAX_CONN_AGE_S:
                            self.event("hl_rotate")
                            break
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=5)
                        except asyncio.TimeoutError:
                            continue
                        recv = now_ms()
                        m = json.loads(raw)
                        ch = m.get("channel")
                        if ch == "l2Book":
                            self.on_book(m["data"], recv)
                        elif ch == "trades":
                            self.on_trades(m["data"], recv)
                        elif ch not in ("subscriptionResponse", "pong"):
                            self.event("hl_server_msg", "", raw[:300])
            except asyncio.CancelledError:
                raise
            except Exception as e:
                self.reconnects += 1
                log.warning("hyperliquid connection lost: %s", e)
                self.event("hl_disconnect", "", str(e)[:200])
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def flusher(self, every: float = 30) -> None:
        last = dict(self.msgs)
        t_last = time.monotonic()
        while not self.stop.is_set():
            try:
                await asyncio.wait_for(self.stop.wait(), timeout=every)
            except asyncio.TimeoutError:
                pass
            try:
                await asyncio.to_thread(self.sink.write, self.sink.take())
            except Exception as e:  # never let one bad write stop recording
                log.exception("write failed: %s", e)
                self.event("write_error", "", str(e)[:200])
            dt = time.monotonic() - t_last
            rates = {k: (v - last[k]) / dt for k, v in self.msgs.items()}
            last, t_last = dict(self.msgs), time.monotonic()
            log.info("msgs/s book=%.1f trades=%.1f | reconnects %d",
                     rates["book"], rates["trades"], self.reconnects)

    async def run(self) -> None:
        self.event("hl_start", ",".join(self.coins))
        tasks = [asyncio.create_task(self.conn()), asyncio.create_task(self.flusher())]
        await self.stop.wait()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        self.event("hl_stop")
        self.sink.close()


def select_coins(data: Path, extra: list[str]) -> list[str]:
    uni = json.loads((data / "universe.json").read_text())["symbols"]
    ref = [s["symbol"][:-4] for s in uni if s["group"] == "ref"]
    perps = perp_coins()
    coins = [c for c in ref if c in perps]
    missing = [c for c in ref if c not in perps]
    if missing:
        log.info("not on Hyperliquid perps, skipped: %s", " ".join(missing))
    for c in extra:
        if c and c not in coins:
            coins.append(c)
    return coins


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--data", type=Path, default=Path("data"))
    ap.add_argument("--extra", default="HYPE", help="comma-separated extra coins")
    ap.add_argument("--proxy", action="store_true")
    ap.add_argument("--duration", type=float)
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    coins_file = args.data / "hl_coins.json"
    if coins_file.exists():
        coins = json.loads(coins_file.read_text())["coins"]
    else:
        coins = select_coins(args.data, args.extra.split(","))
        coins_file.write_text(json.dumps({"selected_ms": now_ms(), "coins": coins}, indent=1))
    log.info("recording %d Hyperliquid coins: %s", len(coins), " ".join(coins))
    rec = HLRecorder(args.data, coins, "hl" + time.strftime("%Y%m%d%H%M%S", time.gmtime()),
                     args.proxy)

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
