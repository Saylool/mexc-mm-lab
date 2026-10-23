from pathlib import Path

import pytest

from mexcmm.pb import DecodeError, decode_push, fields

FIX = Path(__file__).parent / "fixtures"


def frames():
    """ws_tiausdt.bin: 15 s of real TIAUSDT pushes, each prefixed by a 4-byte length."""
    b = (FIX / "ws_tiausdt.bin").read_bytes()
    pos, out = 0, []
    while pos < len(b):
        n = int.from_bytes(b[pos:pos + 4], "big")
        out.append(b[pos + 4:pos + 4 + n])
        pos += 4 + n
    return out


def test_recorded_frames_decode():
    pushes = [decode_push(f) for f in frames()]
    depth = [p for p in pushes if p.depth is not None]
    deals = [p for p in pushes if p.deals is not None]
    assert len(pushes) == 213
    assert len(depth) == 62 and len(deals) == 3
    assert all(p.symbol == "TIAUSDT" for p in pushes)
    first = depth[0]
    assert (first.depth.from_version, first.depth.to_version) == (4467723941, 4467723952)
    assert first.depth.asks[0] == ("0.5481", "8172.77")
    t = deals[0].deals[0]
    assert (t.price, t.quantity, t.trade_type) == ("0.5472", "1159.81", 2)
    assert t.time == 1792764504735


def test_recorded_depth_versions_chain():
    depth = [p.depth for p in map(decode_push, frames()) if p.depth is not None]
    for a, b in zip(depth, depth[1:]):
        assert b.from_version == a.to_version + 1


def test_varint_and_length_fields():
    # field 1 (len) "ab", field 5 (varint) 300
    buf = bytes([0x0A, 2]) + b"ab" + bytes([0x28, 0xAC, 0x02])
    assert list(fields(buf)) == [(1, 2, b"ab"), (5, 0, 300)]


def test_truncated_input_raises():
    with pytest.raises(DecodeError):
        list(fields(bytes([0x0A, 5]) + b"ab"))
