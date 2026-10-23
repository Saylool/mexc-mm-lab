from datetime import datetime, timezone

import pyarrow.parquet as pq

from mexcmm.writer import StreamWriter


def ms(h, m=0):
    return int(datetime(2026, 10, 23, h, m, tzinfo=timezone.utc).timestamp() * 1000)


def test_rotates_on_hour_and_renames_part(tmp_path):
    w = StreamWriter(tmp_path, "events", "run1")
    w.add(ms(14, 10), "start", "", "")
    w.flush(ms(14, 10))
    day = tmp_path / "events" / "date=2026-10-23"
    assert [p.name for p in day.iterdir()] == ["events-20261023T14-run1.parquet.part"]
    w.add(ms(14, 59), "gap", "TIAUSDT", "1->3")
    w.flush(ms(15, 0))  # hour changed: file closes
    assert [p.name for p in day.iterdir()] == ["events-20261023T14-run1.parquet"]
    t = pq.read_table(day / "events-20261023T14-run1.parquet")
    assert t.column("kind").to_pylist() == ["start", "gap"]


def test_rows_added_after_take_are_kept(tmp_path):
    w = StreamWriter(tmp_path, "events", "run1")
    w.add(ms(14), "a", "", "")
    taken = w.take()
    w.add(ms(14), "b", "", "")  # arrives while the worker thread writes
    w.write(*taken, ms(14))
    w.flush(ms(14))
    w.close()
    files = list((tmp_path / "events").rglob("*.parquet"))
    assert pq.read_table(files[0]).column("kind").to_pylist() == ["a", "b"]
