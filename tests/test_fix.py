"""Тести виправлення порядку на dev: що зачіпається, перевірки до змін, автовідкат.

Dev тут: стан у пам'яті (FakeDev) + підроблений відправник, який «приймає» точки в цей стан.
"""

from __future__ import annotations

import gzip
import json
import os
import sys
import tempfile
import time
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from test_core import FakeClient, make_position  # noqa: E402
from test_order import FOLD_A, FOLD_B, iso, pages, zigzag_positions  # noqa: E402

from m2m_replay import fix as fixmod  # noqa: E402
from m2m_replay import replay  # noqa: E402
from m2m_replay.client import M2MClient  # noqa: E402
from m2m_replay.errors import ApiError, SendError  # noqa: E402
from m2m_replay.fix import FixError, FixJob, Interval, check_targets, plan_interval  # noqa: E402
from m2m_replay.models import parse_ts  # noqa: E402
from m2m_replay.order import get_local_tz, scan_pages  # noqa: E402

IMEI = "353742377269816"
DEV_ID, OTHER_ID = 5, 6


def ms_of(value) -> int:
    return int(parse_ts(value).timestamp() * 1000)


class FakeDev(M2MClient):
    """Платформа dev у пам'яті: точки кількох об'єктів, інтервальний запит, видалення пакетом, uniqueId."""

    def __init__(self, store: dict[int, list[dict]], unique: dict[int, str], ignore_filter: bool = False):
        super().__init__("https://dev.example")
        self.token = "t"
        self.store = {k: list(v) for k, v in store.items()}
        self.unique = unique
        self.ignore_filter = ignore_filter
        self.next_id = 1_000_000
        self.delete_calls: list[tuple[int, list[int]]] = []

    def inside(self, did, a, b):
        lo, hi = ms_of(a), ms_of(b)
        return [p for p in self.store[did] if self.ignore_filter or lo <= ms_of(p["deviceTime"]) <= hi]

    def positions_page(self, did, date_from, date_to, page=1, per_page=200, order="ASC"):
        items = self.inside(did, date_from, date_to)
        if order != "ASC":
            items = list(reversed(items))
        n = len(items)
        return {"items": items[(page - 1) * per_page: page * per_page],
                "pages": (n + per_page - 1) // per_page, "total": n}

    def get_device(self, did):
        return {"id": did, "uniqueId": self.unique[did]}

    def delete_positions(self, did, ids, chunk=5000):
        self.delete_calls.append((did, list(ids)))
        gone = set(ids)
        self.store[did] = [p for p in self.store[did] if p["id"] not in gone]
        return len(ids)

    def add_point(self, did, pt) -> None:
        self.next_id += 1
        self.store[did].append({"id": self.next_id, "deviceTime": iso(
            __import__("datetime").datetime.fromtimestamp(pt.ts_ms / 1000, tz=__import__("datetime").timezone.utc)),
            "latitude": pt.lat, "longitude": pt.lon, "attributes": json.dumps(pt.attrs)})


class FakeSender:
    """Підробка WialonSender: приймає точки в FakeDev. `mode` керує поведінкою для одного сеансу."""

    def __init__(self, dev: FakeDev, did: int, mode: str = "ok", fail_after: int = 100):
        self.dev, self.did, self.mode, self.fail_after = dev, did, mode, fail_after
        self.sock = object()
        self.unmapped: set = set()
        self.skipped_ids: set = set()
        self.last_error = ""
        self.sent = 0

    def connect(self):
        if self.mode == "no_connect":
            raise SendError("Не вдалося підключитись")

    def close(self):
        pass

    def send(self, pts):
        res = []
        for p in pts:
            self.sent += 1
            if self.mode == "reject_after" and self.sent > self.fail_after:
                res.append(False)
            elif self.mode == "silent_drop" and self.sent == 50:
                res.append(True)            # сервер «підтвердив», але точка не потрапила в базу
            else:
                self.dev.add_point(self.did, p)
                res.append(True)
        if not all(res):
            self.last_error = "сервер відповів #AD#-1"
        return res


def sender_factory(dev, did, *modes):
    """Сеанси по черзі: [перевірка з'єднання, відправка, відновлення]; решта — «ok».
    Режими задаються лише для відправки/відновлення — перевірка з'єднання завжди перший сеанс."""
    seq = list(modes)
    if not (seq and seq[0] == "no_connect"):
        seq.insert(0, "ok")

    def make(cfg):
        return FakeSender(dev, did, seq.pop(0) if seq else "ok")
    return make


def dev_item(i: int, base, k: int, did_tag: int = 0) -> dict:
    p = make_position(i, base)
    p["id"] = i
    p["deviceTime"] = iso(base + __import__("datetime").timedelta(seconds=15 * k))
    p["latitude"], p["longitude"] = 52.68 + 0.0032 * k, 20.4
    p["attributes"] = json.dumps({"pf_orig_lat": 52.68 + 0.0032 * k, "pf_orig_lon": 20.4, "tag": did_tag})
    return p


def build_world(imei_on_dev: str = IMEI, ignore_filter: bool = False):
    """Prod: зигзаг. Dev: ті самі точки + «чужі» (поза інтервалом і інший об'єкт у тому самому інтервалі)."""
    prod_items = zigzag_positions()
    prod = FakeClient(prod_items)
    prod.base_url = "https://prod.example"
    dev_items = []
    for n, p in enumerate(prod_items):
        q = dict(p)
        q["id"] = 100 + n
        dev_items.append(q)
    outside_before = dev_item(9001, FOLD_B - __import__("datetime").timedelta(hours=4), 0)
    outside_after = dev_item(9002, FOLD_A + __import__("datetime").timedelta(hours=4), 0)
    dev_items += [outside_before, outside_after]
    other = [dev_item(7000 + k, FOLD_B, k, did_tag=77) for k in range(30)]      # інший об'єкт у тому самому часі
    dev = FakeDev({DEV_ID: dev_items, OTHER_ID: other}, {DEV_ID: imei_on_dev, OTHER_ID: "OTHER"}, ignore_filter)
    rep = scan_pages(pages(prod_items), get_local_tz())
    iv = plan_interval(rep, "hull")
    return prod, dev, iv


def send_cfg() -> dict:
    return dict(host="127.0.0.1", port=1, imei=IMEI, protocol="wialon", batch_size=50)


def run_fix(prod, dev, iv, modes=("ok",), **kw) -> FixJob:
    tmp = tempfile.mkdtemp()
    job = FixJob(prod, 1, dev, DEV_ID, iv, send_cfg(), backup_path=os.path.join(tmp, "b.json.gz"),
                 verify_timeout=0.2, verify_poll=0.02, **kw)
    with mock.patch.object(replay, "make_sender", sender_factory(dev, DEV_ID, *modes)):
        job.start()
        t0 = time.time()
        while job.running and time.time() - t0 < 60:
            time.sleep(0.02)
        assert not job.running, "FixJob не завершився вчасно"
        job._thread.join(5)
    return job


def times(dev, did, iv) -> list[str]:
    return [p["deviceTime"] for p in dev.inside(did, iv.iso_from, iv.iso_to)]


class PlanTests(unittest.TestCase):
    def test_intervals(self):
        rep = scan_pages(pages(zigzag_positions()), get_local_tz())
        hull = plan_interval(rep, "hull")
        seg = rep.segments[0]
        self.assertEqual((hull.from_ms, hull.to_ms), (seg.lo_ms - 7_200_000, seg.hi_ms + 7_200_000))
        end = ms_of("2026-10-31T21:59:59Z")
        to_end = plan_interval(rep, "to_end", period_end_ms=end)
        self.assertEqual((to_end.from_ms, to_end.to_ms), (hull.from_ms, end))
        # формат як у запитах інтерфейсу: київський час зі зсувом (до секунди; початок вниз, кінець вгору)
        iv2 = Interval(ms_of("2026-10-24T23:56:00.400Z"), ms_of("2026-10-25T00:04:00.400Z"))
        self.assertEqual((iv2.iso_from, iv2.iso_to), ("2026-10-25T02:56:00+03:00", "2026-10-25T03:04:01+03:00"))
        # друга година 03:xx має зсув +02:00, тож момент однозначний
        self.assertEqual(Interval(ms_of("2026-10-25T01:04:00Z"), ms_of("2026-10-25T01:05:00Z")).iso_from,
                         "2026-10-25T03:04:00+02:00")
        with self.assertRaises(FixError):
            plan_interval(scan_pages(pages([make_position(i, FOLD_B) for i in range(5)]), get_local_tz()), "hull")


class GuardTests(unittest.TestCase):
    def test_prod_client_is_read_only(self):
        c = M2MClient("https://prod.example")
        c.read_only = True
        c.token = "t"
        with self.assertRaises(ApiError):
            c.delete_positions(1, [1, 2, 3])
        with self.assertRaises(ApiError):
            c.request("POST", "/api/anything")

    def test_same_host_refused(self):
        prod, dev, _ = build_world()
        dev.base_url = "https://PROD.example:443"
        with self.assertRaises(FixError):
            check_targets(prod, dev)

    def test_delete_is_batched(self):
        class Rec(M2MClient):
            calls: list = []

            def request(self, method, path, params=None, json_data=None, retries=3):
                self.calls.append((method, path, json_data))
                return {}
        c = Rec("https://x")
        self.assertEqual(c.delete_positions(5, list(range(12001)), chunk=5000), 12001)
        self.assertEqual([len(j["ids"]) for _, _, j in c.calls], [5000, 5000, 2001])
        self.assertTrue(all(m == "DELETE" and p == "/api/device/5/positions" for m, p, _ in c.calls))


class FixHappyPathTests(unittest.TestCase):
    def test_fixes_only_target_object_and_interval(self):
        prod, dev, iv = build_world()
        before_ids = {p["id"] for p in dev.inside(DEV_ID, iv.iso_from, iv.iso_to)}
        outside_before = [p for p in dev.store[DEV_ID] if p["id"] in (9001, 9002)]
        other_before = list(dev.store[OTHER_ID])
        self.assertNotEqual(times(dev, DEV_ID, iv), sorted(times(dev, DEV_ID, iv)))     # було: зигзаг

        job = run_fix(prod, dev, iv)

        self.assertEqual(job.status, "done", job.message)
        # видалення: лише цей об'єкт і лише id з інтервалу
        self.assertEqual({d for d, _ in dev.delete_calls}, {DEV_ID})
        deleted = {i for _, ids in dev.delete_calls for i in ids}
        self.assertEqual(deleted, before_ids)
        # чужі точки не зачеплено
        self.assertEqual([p for p in dev.store[DEV_ID] if p["id"] in (9001, 9002)], outside_before)
        self.assertEqual(dev.store[OTHER_ID], other_before)
        # результат: правильний порядок, рівно стільки ж точок, скільки на Prod
        t = times(dev, DEV_ID, iv)
        self.assertEqual(t, sorted(t))
        self.assertEqual(len(t), 240)
        self.assertEqual(job.verify_info["segments"], 0)
        # резервна копія зроблена повністю
        with gzip.open(job.backup_path, "rt", encoding="utf-8") as f:
            lines = f.read().splitlines()
        self.assertEqual(len(lines), 1 + len(before_ids))
        self.assertIn("meta", json.loads(lines[0]))


class FixRollbackTests(unittest.TestCase):
    def assert_restored(self, dev, iv, original_times, original_ids_outside):
        self.assertEqual(times(dev, DEV_ID, iv), original_times)       # той самий вміст і порядок, що були
        self.assertEqual([p["id"] for p in dev.store[DEV_ID] if p["id"] in (9001, 9002)], original_ids_outside)

    def test_rollback_when_sending_fails_midway(self):
        prod, dev, iv = build_world()
        original = times(dev, DEV_ID, iv)
        other_before = list(dev.store[OTHER_ID])
        job = run_fix(prod, dev, iv, modes=("reject_after", "ok"))
        self.assertEqual(job.status, "rolled_back", job.message)
        self.assert_restored(dev, iv, original, [9001, 9002])
        self.assertEqual(dev.store[OTHER_ID], other_before)
        self.assertIsNotNone(job.restore_job)
        self.assertEqual(job.restore_job.ok, len(original))

    def test_rollback_when_server_silently_loses_a_point(self):
        prod, dev, iv = build_world()
        original = times(dev, DEV_ID, iv)
        job = run_fix(prod, dev, iv, modes=("silent_drop", "ok"))
        self.assertEqual(job.status, "rolled_back", job.message)
        self.assertIn("має бути 240", job.message + " ".join(job.log))
        self.assert_restored(dev, iv, original, [9001, 9002])

    def test_rollback_when_stopped_by_user(self):
        prod, dev, iv = build_world()
        original = times(dev, DEV_ID, iv)

        class Slow(FakeSender):
            def send(self, pts):
                time.sleep(0.05)
                return super().send(pts)

        tmp = tempfile.mkdtemp()
        job = FixJob(prod, 1, dev, DEV_ID, iv, send_cfg(), backup_path=os.path.join(tmp, "b.json.gz"),
                     verify_timeout=0.2, verify_poll=0.02)
        made = []

        def make(cfg):
            made.append(1)
            return (Slow if len(made) == 2 else FakeSender)(dev, DEV_ID)
        with mock.patch.object(replay, "make_sender", make):
            job.start()
            t0 = time.time()
            while (job.send_job is None or not job.send_job.running) and time.time() - t0 < 10:
                time.sleep(0.01)
            job.stop()
            while job.running and time.time() - t0 < 60:
                time.sleep(0.02)
            job._thread.join(5)
        self.assertEqual(job.status, "rolled_back", job.message)
        self.assert_restored(dev, iv, original, [9001, 9002])

    def test_strict_mileage_triggers_rollback(self):
        prod, dev, iv = build_world()
        original = times(dev, DEV_ID, iv)
        # Додаємо totalDistance, який не збігається з довжиною треку, до кожної точки Prod
        for n, p in enumerate(prod.positions):
            a = json.loads(p["attributes"])
            a["totalDistance"] = n * 50_000.0     # +50 км на точку
            p["attributes"] = json.dumps(a)
        job = run_fix(prod, dev, iv, strict_mileage=True)
        # FakeSender зберігає attrs як є, тож розбіжність пробігу видно при перевірці
        self.assertEqual(job.status, "rolled_back", job.message)
        self.assertIn("пробіг", job.message + " ".join(job.log))
        self.assert_restored(dev, iv, original, [9001, 9002])


class FixPreflightTests(unittest.TestCase):
    """До першої зміни: будь-яка розбіжність → нічого не видалено, dev не змінено."""

    def assert_untouched(self, job, dev, snapshot_store):
        self.assertEqual(job.status, "error", job.message)
        self.assertEqual(dev.delete_calls, [])
        self.assertEqual(dev.store, snapshot_store)

    def test_wrong_object_identity(self):
        prod, dev, iv = build_world(imei_on_dev="999999999999999")
        snap = {k: list(v) for k, v in dev.store.items()}
        job = run_fix(prod, dev, iv)
        self.assert_untouched(job, dev, snap)
        self.assertIn("uniqueId", job.message)

    def test_dev_server_unreachable(self):
        prod, dev, iv = build_world()
        snap = {k: list(v) for k, v in dev.store.items()}
        job = run_fix(prod, dev, iv, modes=("no_connect",))
        self.assert_untouched(job, dev, snap)

    def test_count_changed_since_preview(self):
        prod, dev, iv = build_world()
        snap = {k: list(v) for k, v in dev.store.items()}
        job = run_fix(prod, dev, iv, expected_dev_count=239)
        self.assert_untouched(job, dev, snap)
        self.assertIn("змінилась", job.message)

    def test_api_ignores_interval_filter(self):
        prod, dev, iv = build_world(ignore_filter=True)
        snap = {k: list(v) for k, v in dev.store.items()}
        job = run_fix(prod, dev, iv)
        self.assert_untouched(job, dev, snap)
        self.assertIn("поза вибраним інтервалом", job.message)

    def test_same_domain_as_prod(self):
        prod, dev, iv = build_world()
        dev.base_url = prod.base_url
        snap = {k: list(v) for k, v in dev.store.items()}
        job = run_fix(prod, dev, iv)
        self.assert_untouched(job, dev, snap)

    def test_nothing_on_prod_for_interval(self):
        prod, dev, iv = build_world()
        prod.positions = []
        snap = {k: list(v) for k, v in dev.store.items()}
        job = run_fix(prod, dev, iv)
        self.assert_untouched(job, dev, snap)


class LocalSortDev(FakeDev):
    """dev, який при читанні сортує за локальним часом без зсуву (як у проблемі): порядок вставки нічого не змінює."""

    def inside(self, did, a, b):
        tz = get_local_tz()
        return sorted(super().inside(did, a, b),
                      key=lambda p: parse_ts(p["deviceTime"]).astimezone(tz).replace(tzinfo=None))


class SelfFixTests(unittest.TestCase):
    """Виправлення на місці: джерело правильних точок — резервна копія самого об'єкта, Prod не потрібен."""

    def run_self(self, dev, iv, modes=("ok",), **kw):
        tmp = tempfile.mkdtemp()
        job = FixJob(None, None, dev, DEV_ID, iv, send_cfg(), backup_path=os.path.join(tmp, "b.json.gz"),
                     verify_timeout=0.2, verify_poll=0.02, **kw)
        with mock.patch.object(replay, "make_sender", sender_factory(dev, DEV_ID, *modes)):
            job.start()
            t0 = time.time()
            while job.running and time.time() - t0 < 60:
                time.sleep(0.02)
            job._thread.join(5)
        return job

    def test_resorts_points_of_the_same_object(self):
        _, dev, iv = build_world()
        before = times(dev, DEV_ID, iv)
        other_before = list(dev.store[OTHER_ID])
        self.assertNotEqual(before, sorted(before))
        job = self.run_self(dev, iv)
        self.assertEqual(job.status, "done", job.message)
        t = times(dev, DEV_ID, iv)
        self.assertEqual(t, sorted(t))
        self.assertEqual(len(t), len(before))
        self.assertEqual({d for d, _ in dev.delete_calls}, {DEV_ID})
        self.assertEqual(dev.store[OTHER_ID], other_before)
        self.assertEqual([p["id"] for p in dev.store[DEV_ID] if p["id"] in (9001, 9002)], [9001, 9002])

    def test_rolls_back_when_read_order_ignores_insert_order(self):
        """Платформа сортує при читанні за локальним часом: перевставка не допомагає, перевірка має відкотити."""
        _, dev, iv = build_world()
        slow = LocalSortDev(dev.store, dev.unique)
        original_ids = {p["id"] for p in slow.inside(DEV_ID, iv.iso_from, iv.iso_to)}
        before = times(slow, DEV_ID, iv)
        job = self.run_self(slow, iv)
        self.assertEqual(job.status, "rolled_back", job.message)
        self.assertIn("порядок", job.message.lower())
        self.assertEqual(times(slow, DEV_ID, iv), before)
        self.assertEqual(len(original_ids), len(before))

    def test_unmapped_params_are_reported(self):
        _, dev, iv = build_world()
        job = self.run_self(dev, iv)
        self.assertEqual(job.status, "done", job.message)
        self.assertIsInstance(job.unmapped, list)


FOLD_START_UTC = ms_of("2026-10-24T23:56:00Z")
SECOND_HOUR_UTC = ms_of("2026-10-25T01:00:00Z")


def shifted_world(buggy_ingest: bool = False):
    """Як у реальних даних: друга година 03:xx збережена з міткою на 1 год раніше; годинник трекера (sns16486) каже правду.

    Платформа віддає точки за міткою часу, тож перша й друга години перемішані, а координати стрибають між двома місцями.
    """
    import datetime as dt
    items, n = [], 0
    for k in range(0, 4 * 60 * 2):                       # 4 год за хвилиною
        true_ms = ms_of("2026-10-24T23:30:00Z") + k * 30_000
        second = SECOND_HOUR_UTC <= true_ms < SECOND_HOUR_UTC + 3_600_000
        stamp = true_ms + 19_000 - (3_600_000 if second else 0)
        if not (ms_of("2026-10-24T23:30:00Z") <= true_ms < ms_of("2026-10-25T03:30:00Z")):
            continue
        n += 1
        lat = 53.34 + 0.001 * (k % 7) if second else 52.68
        items.append({"id": 1000 + n, "deviceTime": iso(dt.datetime.fromtimestamp(stamp / 1000, tz=dt.timezone.utc)),
                      "latitude": lat, "longitude": 20.4, "speed": 0,
                      "attributes": json.dumps({"sns16486": true_ms // 1000, "pf_orig_lat": lat, "pf_orig_lon": 20.4})})
    return items


class InstantSortDev(FakeDev):
    """Платформа: читає точки за міткою часу (стабільно), як це видно в API."""

    def inside(self, did, a, b):
        return sorted(super().inside(did, a, b), key=lambda p: ms_of(p["deviceTime"]))


class BuggyIngestDev(InstantSortDev):
    """Прийом знову «з'їдає» годину в другій годині переходу: нові точки зберігаються з міткою на 1 год раніше."""

    def add_point(self, did, pt) -> None:
        if SECOND_HOUR_UTC <= pt.ts_ms < SECOND_HOUR_UTC + 3_600_000:    # справжній час другої години «з'їдається»
            pt.ts_ms -= 3_600_000
        super().add_point(did, pt)


class ShiftedHourTests(unittest.TestCase):
    def make(self, cls):
        items = shifted_world()
        dev = cls({DEV_ID: [dict(p) for p in items]}, {DEV_ID: IMEI})
        return items, dev

    def test_scan_sees_the_fan_through_tracker_clock(self):
        items, dev = self.make(InstantSortDev)
        rep = scan_pages(pages(dev.inside(DEV_ID, "2026-10-01T00:00:00Z", "2026-11-01T00:00:00Z")), get_local_tz())
        self.assertEqual(len(rep.segments), 1)
        self.assertGreater(rep.time_fixed, 100)
        self.assertGreater(rep.out_of_order, 10)

    def test_fix_corrects_timestamps_and_track(self):
        items, dev = self.make(InstantSortDev)
        rep = scan_pages(pages(dev.inside(DEV_ID, "2026-10-01T00:00:00Z", "2026-11-01T00:00:00Z")), get_local_tz())
        iv = plan_interval(rep, "hull")
        job = SelfFixTests().run_self(dev, iv)
        self.assertEqual(job.status, "done", job.message)
        stored = sorted(dev.store[DEV_ID], key=lambda p: ms_of(p["deviceTime"]))
        # після виправлення порядок за міткою збігається з порядком за годинником трекера
        clocks = [json.loads(p["attributes"])["sns16486"] for p in stored]
        self.assertEqual(clocks, sorted(clocks))
        self.assertEqual(len(stored), len(items))
        rep2 = scan_pages(pages(dev.inside(DEV_ID, iv.iso_from, iv.iso_to)), get_local_tz())
        self.assertEqual((len(rep2.segments), rep2.time_fixed), (0, 0))

    def test_rolls_back_if_ingest_keeps_shifting_time(self):
        items, dev = self.make(BuggyIngestDev)
        rep = scan_pages(pages(dev.inside(DEV_ID, "2026-10-01T00:00:00Z", "2026-11-01T00:00:00Z")), get_local_tz())
        iv = plan_interval(rep, "hull")
        before = sorted((p["deviceTime"], p["attributes"]) for p in dev.store[DEV_ID])
        job = SelfFixTests().run_self(dev, iv)
        self.assertEqual(job.status, "rolled_back", job.message)
        after = sorted((p["deviceTime"], p["attributes"]) for p in dev.store[DEV_ID])
        self.assertEqual(after, before)


if __name__ == "__main__":
    unittest.main()
