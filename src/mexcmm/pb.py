"""Minimal protobuf wire-format decoder for MEXC spot websocket pushes.

MEXC spot streams are protobuf since 2025. The message definitions are
published in https://github.com/mexcdevelop/websocket-proto; only the few
fields we record are decoded here, so no protoc step or generated code is
needed. Field numbers below are copied from those .proto files:

PushDataV3ApiWrapper: 1 channel, 3 symbol, 5 createTime, 6 sendTime,
    313 publicAggreDepths, 314 publicAggreDeals, 315 publicAggreBookTicker
PublicAggreDepthsV3Api: 1 asks, 2 bids, 3 eventType, 4 fromVersion,
    5 toVersion, 6 lastOrderCreateTime; item: 1 price, 2 quantity
PublicAggreDealsV3Api: 1 deals, 2 eventType;
    item: 1 price, 2 quantity, 3 tradeType (1 buy, 2 sell), 4 time, 5 tradeId
"""

from __future__ import annotations

from dataclasses import dataclass, field


class DecodeError(ValueError):
    pass


def _varint(buf: bytes, pos: int) -> tuple[int, int]:
    result = 0
    shift = 0
    while True:
        if pos >= len(buf):
            raise DecodeError("truncated varint")
        b = buf[pos]
        pos += 1
        result |= (b & 0x7F) << shift
        if not b & 0x80:
            return result, pos
        shift += 7
        if shift > 63:
            raise DecodeError("varint too long")


def fields(buf: bytes):
    """Yield (field_number, wire_type, value) for every field in buf.

    value is an int for varint/fixed types and bytes for length-delimited.
    """
    pos = 0
    n = len(buf)
    while pos < n:
        key, pos = _varint(buf, pos)
        num, wt = key >> 3, key & 7
        if wt == 0:
            val, pos = _varint(buf, pos)
        elif wt == 2:
            ln, pos = _varint(buf, pos)
            if pos + ln > n:
                raise DecodeError("truncated length-delimited field")
            val = buf[pos:pos + ln]
            pos += ln
        elif wt == 1:
            val = int.from_bytes(buf[pos:pos + 8], "little")
            pos += 8
        elif wt == 5:
            val = int.from_bytes(buf[pos:pos + 4], "little")
            pos += 4
        else:
            raise DecodeError(f"unsupported wire type {wt}")
        yield num, wt, val


@dataclass
class Depth:
    asks: list[tuple[str, str]] = field(default_factory=list)
    bids: list[tuple[str, str]] = field(default_factory=list)
    event_type: str = ""
    from_version: int = 0
    to_version: int = 0
    last_order_create_time: int = 0


@dataclass
class Deal:
    price: str = ""
    quantity: str = ""
    trade_type: int = 0
    time: int = 0
    trade_id: str = ""


@dataclass
class Push:
    channel: str = ""
    symbol: str = ""
    create_time: int = 0
    send_time: int = 0
    depth: Depth | None = None
    deals: list[Deal] | None = None


def _level(buf: bytes) -> tuple[str, str]:
    price = qty = ""
    for num, _, val in fields(buf):
        if num == 1:
            price = val.decode()
        elif num == 2:
            qty = val.decode()
    return price, qty


def _depth(buf: bytes) -> Depth:
    d = Depth()
    for num, _, val in fields(buf):
        if num == 1:
            d.asks.append(_level(val))
        elif num == 2:
            d.bids.append(_level(val))
        elif num == 3:
            d.event_type = val.decode()
        elif num == 4:
            d.from_version = int(val.decode())
        elif num == 5:
            d.to_version = int(val.decode())
        elif num == 6:
            d.last_order_create_time = val
    return d


def _deal(buf: bytes) -> Deal:
    t = Deal()
    for num, _, val in fields(buf):
        if num == 1:
            t.price = val.decode()
        elif num == 2:
            t.quantity = val.decode()
        elif num == 3:
            t.trade_type = val
        elif num == 4:
            t.time = val
        elif num == 5:
            t.trade_id = val.decode()
    return t


def decode_push(buf: bytes) -> Push:
    p = Push()
    for num, _, val in fields(buf):
        if num == 1:
            p.channel = val.decode()
        elif num == 3:
            p.symbol = val.decode()
        elif num == 5:
            p.create_time = val
        elif num == 6:
            p.send_time = val
        elif num == 313:
            p.depth = _depth(val)
        elif num == 314:
            p.deals = [_deal(v) for n, _, v in fields(val) if n == 1]
    return p
