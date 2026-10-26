"""Hourly Parquet files, one per stream, all symbols in one file.

Rows are buffered in memory and flushed as row groups. A file being written
ends in .part; it is renamed to .parquet when the hour closes, so the uploader
only ever sees complete files. A crash loses at most the open hour of one
stream (the .part file has no footer); recorder start-up logs such leftovers.

Layout: <root>/<stream>/date=YYYY-MM-DD/<stream>-YYYYMMDDTHH-<run>.parquet
"""

from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMAS: dict[str, pa.Schema] = {
    "mexc_depth": pa.schema([
        ("recv_ms", pa.int64()), ("send_ms", pa.int64()), ("symbol", pa.string()),
        ("from_v", pa.int64()), ("to_v", pa.int64()),
        ("side", pa.int8()),  # 0 bid, 1 ask, -1 version step without levels
        ("price", pa.float64()), ("qty", pa.float64()),
    ]),
    "mexc_trades": pa.schema([
        ("recv_ms", pa.int64()), ("time_ms", pa.int64()), ("symbol", pa.string()),
        ("price", pa.float64()), ("qty", pa.float64()),
        ("side", pa.int8()),  # 1 buy (taker bought), 2 sell
        ("trade_id", pa.string()),
    ]),
    "mexc_snapshots": pa.schema([
        ("recv_ms", pa.int64()), ("server_ms", pa.int64()), ("symbol", pa.string()),
        ("version", pa.int64()), ("side", pa.int8()), ("level", pa.int16()),
        ("price", pa.float64()), ("qty", pa.float64()),
    ]),
    "binance_bbo": pa.schema([
        ("recv_ms", pa.int64()), ("symbol", pa.string()), ("update_id", pa.int64()),
        ("bid", pa.float64()), ("bid_qty", pa.float64()),
        ("ask", pa.float64()), ("ask_qty", pa.float64()),
    ]),
    # Hyperliquid pushes the full top-20 book on every change (no diffs).
    "hl_book": pa.schema([
        ("recv_ms", pa.int64()), ("time_ms", pa.int64()), ("coin", pa.string()),
        ("side", pa.int8()), ("level", pa.int8()),
        ("price", pa.float64()), ("qty", pa.float64()), ("orders", pa.int32()),
    ]),
    "hl_trades": pa.schema([
        ("recv_ms", pa.int64()), ("time_ms", pa.int64()), ("coin", pa.string()),
        ("price", pa.float64()), ("qty", pa.float64()),
        ("side", pa.int8()),  # 1 taker bought, 2 taker sold (MEXC convention)
        ("tid", pa.int64()), ("buyer", pa.string()), ("seller", pa.string()),
    ]),
    "events": pa.schema([
        ("recv_ms", pa.int64()), ("kind", pa.string()), ("symbol", pa.string()),
        ("detail", pa.string()),
    ]),
}


def rss_mb() -> int:
    """Peak resident memory of this process (macOS reports bytes)."""
    import resource
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // (1024 * 1024)


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def hour_key(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y%m%dT%H")


class StreamWriter:
    def __init__(self, root: Path, stream: str, run_id: str) -> None:
        self.root = root
        self.stream = stream
        self.schema = SCHEMAS[stream]
        self.run_id = run_id
        self.cols: dict[str, list] = {n: [] for n in self.schema.names}
        self.rows = 0
        self.total_rows = 0
        self._writer: pq.ParquetWriter | None = None
        self._hour: str | None = None
        self._part: Path | None = None

    def add(self, *values) -> None:
        for name, v in zip(self.schema.names, values):
            self.cols[name].append(v)
        self.rows += 1

    def _path(self, hour: str) -> Path:
        day = f"{hour[:4]}-{hour[4:6]}-{hour[6:8]}"
        d = self.root / self.stream / f"date={day}"
        d.mkdir(parents=True, exist_ok=True)
        return d / f"{self.stream}-{hour}-{self.run_id}.parquet"

    def take(self) -> tuple[dict[str, list], int]:
        """Detach the buffered rows. Call from the thread that calls add()."""
        cols, rows = self.cols, self.rows
        self.cols = {n: [] for n in self.schema.names}
        self.rows = 0
        return cols, rows

    def write(self, cols: dict[str, list], rows: int, at_ms: int | None = None) -> None:
        """Write detached rows; rotate the file when the UTC hour changed.

        Safe to run in a worker thread while add() keeps filling new buffers,
        as long as write() calls themselves are not concurrent.
        """
        hour = hour_key(at_ms if at_ms is not None else now_ms())
        if rows:
            if self._hour is None:
                self._open(hour)
            self._writer.write_table(pa.Table.from_pydict(cols, schema=self.schema))
            self.total_rows += rows
        if self._hour is not None and hour != self._hour:
            self.close()

    def flush(self, at_ms: int | None = None) -> None:
        self.write(*self.take(), at_ms)

    def _open(self, hour: str) -> None:
        final = self._path(hour)
        self._part = final.with_suffix(".parquet.part")
        self._writer = pq.ParquetWriter(self._part, self.schema, compression="zstd")
        self._hour = hour

    def close(self) -> None:
        if self._writer is None:
            return
        self._writer.close()
        self._part.rename(self._part.with_suffix(""))
        self._writer = None
        self._hour = None
        self._part = None


class Sink:
    """All stream writers of one recorder run."""

    def __init__(self, root: Path, run_id: str) -> None:
        self.w = {s: StreamWriter(root, s, run_id) for s in SCHEMAS}

    def __getitem__(self, stream: str) -> StreamWriter:
        return self.w[stream]

    def take(self) -> dict[str, tuple[dict[str, list], int]]:
        return {s: w.take() for s, w in self.w.items()}

    def write(self, taken: dict[str, tuple[dict[str, list], int]], at_ms: int | None = None) -> None:
        for s, (cols, rows) in taken.items():
            self.w[s].write(cols, rows, at_ms)

    def flush(self, at_ms: int | None = None) -> None:
        self.write(self.take(), at_ms)

    def close(self) -> None:
        for w in self.w.values():
            w.flush()
            w.close()
