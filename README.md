# mexc-mm-lab

Measure before you trade: record MEXC spot order books and trades, then find
out offline whether market making on MEXC spot can pay.

MEXC spot charges **0 % maker / 0.05 % taker** through the API (checked
2026-10-23 via `exchangeInfo` and the fee page; API orders are excluded from
zero-fee campaigns, and API *futures* cost 0.06 % / 0.08 % since 2026-06-01).
A maker earns the spread, so pairs with a wide spread and real volume are the
candidates. The open questions are how often quotes get filled, how much the
price moves against a filled quote (adverse selection) and how much of the
volume is real. This repository collects the data to answer them.

**Public data only.** No API keys, no account, no order placement.

## What is recorded

For 20 USDT pairs chosen from live tickers (`src/mexcmm/universe.py`): 12
pairs also listed on Binance ("ref") and 8 MEXC-only pairs ("solo"), each
with 24 h volume between 200 k and 20 M USDT and a spread of at least 8 bps.
Stablecoins and tokenized stocks are excluded. The choice is written to
`data/universe.json` and kept until `--refresh-universe`.

| Stream | Source | Content |
|---|---|---|
| `mexc_depth` | websocket `spot@public.aggre.depth.v3.api.pb@100ms` | every book change: versions, side, price, absolute size |
| `mexc_trades` | websocket `spot@public.aggre.deals.v3.api.pb@100ms` | every trade: time, price, size, taker side |
| `mexc_snapshots` | REST `/api/v3/depth?limit=1000` | full book at start, after gaps and every 10 min |
| `binance_bbo` | Binance `<symbol>@bookTicker` | Binance best bid/ask for the "ref" pairs |
| `events` | recorder | connects, disconnects, version gaps, errors |

MEXC pushes protobuf; `src/mexcmm/pb.py` decodes the few fields needed,
using the field numbers from
[mexcdevelop/websocket-proto](https://github.com/mexcdevelop/websocket-proto).

Files are hourly Parquet (zstd), one per stream, all symbols in one file:
`data/raw/<stream>/date=YYYY-MM-DD/<stream>-YYYYMMDDTHH-<run>.parquet`.
A file being written ends in `.parquet.part`. Times are UTC milliseconds;
`recv_ms` is local arrival time. About 1.2 GB per day for 20 pairs.

## Rebuilding the book

`src/mexcmm/book.py` follows MEXC's documented procedure (snapshot, drop
stale diffs, check `fromVersion == previous toVersion + 1`). To check a
recording:

```bash
.venv/bin/python -m mexcmm.rebuild data/raw --date 2026-10-23
```

It reports per symbol the diffs applied, version gaps, crossed books, and
how many later snapshots agree with the rebuilt book. A two-minute trial on
2026-10-23 gave 0 gaps, 0 crossed books, and all 32 snapshot checks matching.

## Running

```bash
/opt/homebrew/bin/python3.12 -m venv .venv
.venv/bin/pip install -e '.[dev]'
.venv/bin/python -m pytest
scripts/run.sh          # recorder, restarts itself, keeps the Mac awake
scripts/upload.sh       # optional: hourly files to Google Drive
```

## Google Drive

`scripts/upload.sh` uses [rclone](https://rclone.org). Create a remote named
`gdrive` whose root is the Drive folder that should hold the data:

```bash
brew install rclone
rclone config create gdrive drive scope=drive root_folder_id=<folder id>
```

The folder id is the last part of the folder's Drive URL. Files go to
`ham-veri/` inside it; local copies are deleted 48 h after upload
(`MEXCMM_KEEP_HOURS`).

## Türkçe özet

MEXC spot'ta market making kâr eder mi, bunu para koymadan ölçmek için
yazıldı. 20 paritenin emir defteri, işlemleri ve Binance referans fiyatı
saat saat Parquet dosyalarına kaydedilir ve Google Drive'a yüklenir. Sadece
herkese açık veri kullanılır, hesap ve API anahtarı gerekmez, emir verilmez.

## License

MIT
