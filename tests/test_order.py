"""Тести порядку точок (перехід на зимовий час). Запуск: `python -m unittest discover -s tests` (або `pytest`)."""

from __future__ import annotations

import os
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from helpers import WialonMock  # noqa: E402
from test_core import FakeClient, cfg_for, make_position, wait_job  # noqa: E402

from m2m_replay.models import Point  # noqa: E402
from m2m_replay.order import (  # noqa: E402
    HOUR_MS, TimeSorter, export_rows, get_local_tz, is_ambiguous, rows_to_csv, scan_pages, segment_reason,
)
from m2m_replay.replay import TransferJob  # noqa: E402

TZ = get_local_tz()
FOLD_B = datetime(2026, 10, 25, 0, 0, 0, tzinfo=timezone.utc)   # 03:00 за літнім часом (EEST)
FOLD_A = datetime(2026, 10, 25, 1, 0, 0, tzinfo=timezone.utc)   # 03:00 за зимовим часом (EET)


def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000Z")


def zigzag_positions(n: int = 120) -> list[dict]:
    """Те, що віддав Prod у ніч переходу: точки двох годин перемішані B0 A0 B1 A1 ... (як за локальним часом)."""
    out = []
    for k in range(n):
        for base, lat0 in ((FOLD_B, 52.68), (FOLD_A, 53.06)):
            p = make_position(len(out), base)
            p["deviceTime"] = iso(base + timedelta(seconds=15 * k))
            p["latitude"], p["longitude"] = lat0 + 0.0032 * k, 20.4
            p["attributes"] = '{"pf_orig_lat": %f, "pf_orig_lon": 20.4}' % (lat0 + 0.0032 * k)
            out.append(p)
    return out


def sorted_positions(n: int = 120) -> list[dict]:
    return sorted(zigzag_positions(n), key=lambda p: p["deviceTime"])


def pages(items: list[dict], size: int = 50):
    for i in range(0, len(items), size):
        yield {"items": items[i:i + size]}


def pt(ts_s: int, tag: int = 0) -> Point:
    return Point(ts_ms=ts_s * 1000, lat=0.0, lon=0.0, attrs={"tag": tag})


class TimeSorterTests(unittest.TestCase):
    def test_sorts_zigzag_across_chunks(self):
        stream = [pt(s) for pair in zip(range(0, 600, 10), range(3600, 4200, 10)) for s in pair]
        sorter, out = TimeSorter(), []
        for i in range(0, len(stream), 7):          # дрібні шматки, як сторінки API
            out += sorter.push(stream[i:i + 7])
        out += sorter.flush()
        self.assertEqual([p.ts_ms for p in out], sorted(p.ts_ms for p in stream))
        self.assertGreater(sorter.reordered, 0)
        self.assertEqual(sorter.late, 0)

    def test_buffer_is_bounded_by_window(self):
        sorter = TimeSorter(window_ms=HOUR_MS)
        emitted = sorter.push([pt(s) for s in range(0, 20_000, 10)])   # ~5,5 год, уже впорядковано
        self.assertGreater(len(emitted), 1000)       # точки віддаються по ходу, а не лише у flush
        self.assertEqual(sorter.reordered, 0)

    def test_stable_for_equal_timestamps(self):
        sorter = TimeSorter()
        out = sorter.push([pt(5, 1), pt(5, 2), pt(5, 3)]) + sorter.flush()
        self.assertEqual([p.attrs["tag"] for p in out], [1, 2, 3])

    def test_late_beyond_window_is_counted(self):
        sorter = TimeSorter(window_ms=HOUR_MS)
        sorter.push([pt(s) for s in range(0, 7200, 10)])   # 2 год
        sorter.push([pt(0)])                               # відкат на 2 год > вікна 1 год
        self.assertEqual(sorter.late, 1)


class AmbiguityTests(unittest.TestCase):
    def test_repeated_hour(self):
        ms = lambda dt: int(dt.timestamp() * 1000)  # noqa: E731
        self.assertTrue(is_ambiguous(ms(FOLD_B + timedelta(minutes=30)), TZ))     # 03:30 EEST
        self.assertTrue(is_ambiguous(ms(FOLD_A + timedelta(minutes=30)), TZ))     # 03:30 EET
        self.assertFalse(is_ambiguous(ms(FOLD_A + timedelta(hours=1, minutes=30)), TZ))   # 04:30 EET
        self.assertFalse(is_ambiguous(ms(datetime(2026, 10, 24, 12, 0, tzinfo=timezone.utc)), TZ))


class ScanTests(unittest.TestCase):
    def test_detects_dst_zigzag(self):
        rep = scan_pages(pages(zigzag_positions()), TZ)
        self.assertEqual(rep.total, 240)
        self.assertEqual(rep.out_of_order, 119)
        self.assertEqual(len(rep.segments), 1)
        seg = rep.segments[0]
        self.assertTrue(seg.dst_fold)
        self.assertAlmostEqual(seg.max_back_ms / HOUR_MS, 1.0, delta=0.05)
        self.assertEqual(seg.jumps, 119)
        self.assertEqual(rep.jumps_other, 0)
        self.assertIn("Перехід на зимовий час", segment_reason(seg))

    def test_sorted_data_is_clean(self):
        rep = scan_pages(pages(sorted_positions()), TZ)
        self.assertEqual((rep.out_of_order, len(rep.segments), rep.jumps_other), (0, 0, 0))

    def test_zero_coords_and_unrelated_jump(self):
        items = sorted_positions(10)
        zero = dict(items[3], latitude=0.0, longitude=0.0)
        far = dict(items[8], latitude=49.0, longitude=30.0)     # стрибок на сотні км за 15 с, час іде вперед
        rep = scan_pages(pages(items[:3] + [zero] + items[4:8] + [far] + items[9:]), TZ)
        self.assertEqual(rep.zero_coords, 1)
        self.assertEqual(rep.out_of_order, 0)
        self.assertEqual(rep.jumps_other, 2)        # туди й назад
        self.assertFalse(rep.segments)

    def test_non_dst_back_step_has_other_reason(self):
        base = datetime(2026, 8, 10, 6, 0, 0, tzinfo=timezone.utc)
        items = [make_position(i, base) for i in range(30)]
        items[10], items[11] = items[11], items[10]
        rep = scan_pages(pages(items), TZ)
        self.assertEqual(len(rep.segments), 1)
        self.assertFalse(rep.segments[0].dst_fold)
        self.assertIn("Не перехід годин", segment_reason(rep.segments[0]))

    def test_export_rows_are_chronological(self):
        rep = scan_pages(pages(zigzag_positions()), TZ)
        rows = export_rows(rep, TZ)
        self.assertEqual(len(rows), 240)
        times = [r["Час UTC"] for r in rows]
        self.assertEqual(times, sorted(times))
        self.assertEqual([r["№ у правильному порядку"] for r in rows], list(range(1, 241)))
        self.assertNotEqual([r["№ у API"] for r in rows], list(range(1, 241)))
        self.assertTrue(rows[0]["Час (Київ)"].endswith("(+03)"))
        self.assertTrue(rows[-1]["Час (Київ)"].endswith("(+02)"))
        self.assertTrue(rows_to_csv(rows).startswith(b"\xef\xbb\xbf"))
        self.assertEqual(rows_to_csv([]), b"")


class TransferOrderTests(unittest.TestCase):
    """Наскрізно: на dev (мок Wialon IPS) точки приходять у хронологічному порядку."""

    @staticmethod
    def received_times(srv) -> list[datetime]:
        out = []
        for line in srv.data:
            f = line.rpartition(";")[0].split(";")
            out.append(datetime.strptime(f[0] + f[1], "%d%m%y%H%M%S"))
        return out

    def run_job(self, sort_by_time: bool):
        srv = WialonMock("2.0")
        self.addCleanup(srv.close)
        job = TransferJob(cfg_for(FakeClient(zigzag_positions()), srv.port, protocol="wialon",
                                  sort_by_time=sort_by_time, batch_size=50))
        job.start()
        wait_job(job)
        self.assertEqual(job.status, "done", job.message)
        self.assertEqual((job.ok, job.failed), (240, 0))
        return job, self.received_times(srv)

    def test_sorted_by_default(self):
        job, times = self.run_job(True)
        self.assertEqual(times, sorted(times))
        self.assertGreater(job.reordered, 0)
        self.assertEqual(job.late_points, 0)

    def test_can_be_disabled(self):
        job, times = self.run_job(False)
        self.assertNotEqual(times, sorted(times))      # як було: порядок Prod «як є»
        self.assertEqual(job.reordered, 0)


if __name__ == "__main__":
    unittest.main()
