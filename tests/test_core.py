"""Тести ядра. Запуск: `python -m unittest discover -s tests` (або `pytest`)."""

from __future__ import annotations

import json
import os
import sys
import time
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from helpers import TeltonikaMock, WialonMock, crc16, decode_teltonika  # noqa: E402

from m2m_replay import wialon  # noqa: E402
from m2m_replay.client import M2MClient  # noqa: E402
from m2m_replay.crc import crc16_arc  # noqa: E402
from m2m_replay.mapping import attrs_to_io, attrs_to_wialon_params  # noqa: E402
from m2m_replay.models import Point, parse_ts, position_to_point  # noqa: E402
from m2m_replay.replay import TransferConfig, TransferJob, build_preview, check_connection  # noqa: E402
from m2m_replay.teltonika import CODEC_8, CODEC_8E, build_packet, encode_io, encode_record  # noqa: E402

# Атрибути за структурою реальної точки з prod (значення синтетичні)
SAMPLE_ATTRS = {
    "lac": 38180, "mcc": 255, "mnc": 3, "sat": 0, "hdop": 0.0, "pdop": 0.0, "rssi": 3, "event": 637,
    "iccid": "8900000000000123456", "io_11": 8900000000000, "io_14": 123456, "io_24": 0, "io_68": 0,
    "io_69": 2, "io_80": 0, "power": 12.084, "cellId": 36043, "io_113": 100, "io_200": 0, "io_205": 36043,
    "io_206": 38180, "io_383": 0, "io_637": 1, "motion": False, "pf_lbs": True, "_MOTION": False,
    "battery": 0.8140000000000001, "m2m_odo": 17.23, "distance": 0.0, "ignition": False, "odometer": 29039,
    "operator": 25503, "priority": 1, "pf_reason": "INVALID_TRACKER_FLAG", "power_enb": 0, "pf_filtered": True,
    "pf_orig_lat": 46.4695666, "pf_orig_lon": 30.713775, "totalDistance": 62745332.95, "pf_lbs_cell_id": 679040,
    "power_filtered": 12.084,
}

OFFICIAL_CODEC8_SAMPLE = bytes.fromhex(
    "000000000000003608010000016B40D8EA30010000000000000000000000000000000105021503010101425E0F01F10000601A"
    "014E0000000000000000010000C7CF"
)


def make_position(i: int, base: datetime, step_s: int = 10, power: float | None = None, attrs: dict | None = None) -> dict:
    a = dict(SAMPLE_ATTRS)
    a["power"] = power if power is not None else 12.0 + (i % 50) / 100
    a["pf_orig_lat"] = 49.0 + i * 1e-5
    a["pf_orig_lon"] = 32.0 + i * 1e-5
    if attrs:
        a.update(attrs)
    t = base + timedelta(seconds=i * step_s)
    return {
        "id": i, "deviceTime": t.strftime("%Y-%m-%dT%H:%M:%S.000Z"), "latitude": 50.0, "longitude": 30.0,
        "altitude": 100 + i % 5, "speed": float(i % 60), "course": float(i % 360), "satellites": 7,
        "attributes": json.dumps(a),
    }


class FakeClient(M2MClient):
    """Той самий M2MClient (з реальним паралельним iter_positions), але сторінки віддає з пам'яті."""

    def __init__(self, positions: list[dict], delay: float = 0.0):
        super().__init__("https://prod.example")
        self.positions = positions
        self.delay = delay
        self.calls = 0

    def positions_page(self, did, date_from, date_to, page=1, per_page=200, order="ASC"):
        self.calls += 1
        if self.delay:
            time.sleep(self.delay)
        items = self.positions if order == "ASC" else list(reversed(self.positions))
        total = len(items)
        pages = (total + per_page - 1) // per_page
        return {"items": items[(page - 1) * per_page: page * per_page], "pages": pages, "total": total}


def wait_job(job: TransferJob, timeout: float = 30.0) -> None:
    t0 = time.time()
    while job.running and time.time() - t0 < timeout:
        time.sleep(0.02)
    assert not job.running, "завдання не завершилось вчасно"
    job._thread.join(5)


def cfg_for(client, port, protocol="teltonika8e", **kw) -> TransferConfig:
    base = dict(client=client, device_id=1, date_from="2026-10-20T00:00:00+02:00", date_to="2026-10-20T23:59:59+02:00",
                protocol=protocol, host="127.0.0.1", port=port, imei="353742377269816", shift=False, batch_size=100)
    base.update(kw)
    return TransferConfig(**base)


class CrcTests(unittest.TestCase):
    def test_standard_check_value(self):
        self.assertEqual(crc16_arc(b"123456789"), 0xBB3D)

    def test_official_teltonika_sample_crc(self):
        body = OFFICIAL_CODEC8_SAMPLE[8:-4]
        self.assertEqual(crc16_arc(body), 0xC7CF)
        self.assertEqual(crc16(body), 0xC7CF)


class TeltonikaEncodingTests(unittest.TestCase):
    def test_matches_official_codec8_sample_byte_for_byte(self):
        attrs = {"io_21": 3, "io_1": 1, "power": 24.079, "operator": 24602, "io_78": 0}
        p = Point(ts_ms=0x16B40D8EA30, lat=0.0, lon=0.0, priority=1, event_id=1, attrs=attrs)
        io, unknown = attrs_to_io(attrs)
        self.assertEqual(unknown, [])
        block, skipped = encode_io(io, p.event_id, CODEC_8)
        self.assertEqual(skipped, [])
        packet = build_packet([encode_record(p, block)], CODEC_8)
        self.assertEqual(packet, OFFICIAL_CODEC8_SAMPLE)

    def test_codec8e_roundtrip_with_big_ids_and_negative(self):
        p = Point(ts_ms=1_760_000_000_123, lat=49.438923, lon=-32.085565, alt=-12, course=359, sats=9, speed=88,
                  priority=1, event_id=637, attrs={})
        io = {66: 12084, 318: 1, 637: 1, 11: 8900000000000, 14: 123456, 68: -5, 24: 40000}
        block, skipped = encode_io(io, p.event_id, CODEC_8E)
        self.assertEqual(skipped, [])
        rec = decode_teltonika(build_packet([encode_record(p, block)] * 3, CODEC_8E))
        self.assertEqual(len(rec), 3)
        r = rec[0]
        self.assertEqual((r["ts"], r["prio"], r["event"], r["sat"], r["speed"], r["angle"], r["alt"]),
                         (1_760_000_000_123, 1, 637, 9, 88, 359, -12))
        self.assertEqual((r["lat"], r["lon"]), (494389230, -320855650))
        self.assertEqual(r["io"][66], 12084)
        self.assertEqual(r["io"][11], 8900000000000)
        self.assertEqual(r["io"][68], (1 << 16) - 5)   # -5 у два байти, доповняльний код
        self.assertEqual(r["io"][24], 40000)

    def test_codec8_drops_ids_above_255_and_zeroes_big_event(self):
        block, skipped = encode_io({66: 12000, 318: 1, 637: 1}, 637, CODEC_8)
        self.assertEqual(sorted(skipped), [318, 637])
        p = Point(ts_ms=1, lat=1, lon=1)
        rec = decode_teltonika(build_packet([encode_record(p, block)], CODEC_8))[0]
        self.assertEqual(rec["event"], 0)
        self.assertEqual(rec["io"], {66: 12000})

    def test_packet_limits(self):
        with self.assertRaises(ValueError):
            build_packet([], CODEC_8E)
        with self.assertRaises(ValueError):
            build_packet([b"x"] * 256, CODEC_8E)


class MappingTests(unittest.TestCase):
    def test_sample_row_maps_without_unknowns(self):
        io, unknown = attrs_to_io(SAMPLE_ATTRS)
        self.assertEqual(unknown, [])
        self.assertEqual(io[66], 12084)          # power
        self.assertEqual(io[67], 814)            # battery
        self.assertEqual(io[239], 0)             # ignition
        self.assertEqual(io[240], 0)             # motion
        self.assertEqual(io[21], 3)              # rssi
        self.assertEqual(io[16], 29039)          # odometer
        self.assertEqual(io[241], 25503)         # operator
        self.assertEqual(io[182], 0)
        self.assertEqual(io[205], 36043)         # з raw io_205, а не cellId
        for raw in (11, 14, 24, 68, 69, 80, 113, 200, 206, 383, 637):
            self.assertIn(raw, io)
        # службові параметри платформи й заголовок запису в IO не потрапляють
        for banned in (0, 1, 2):
            self.assertNotIn(banned, io)
        self.assertEqual(len(io), 21)

    def test_unknown_named_attribute_is_reported(self):
        io, unknown = attrs_to_io({"power": 12.0, "fuel_level_custom": 55.5})
        self.assertEqual(io, {66: 12000})
        self.assertEqual(unknown, ["fuel_level_custom"])

    def test_basic_only_keeps_voltage(self):
        io, unknown = attrs_to_io(SAMPLE_ATTRS, basic_only=True)
        self.assertEqual(io, {66: 12084})
        self.assertEqual(unknown, [])

    def test_wialon_params_exclude_platform_by_default(self):
        names = {n for n, _, _ in attrs_to_wialon_params(SAMPLE_ATTRS)}
        self.assertIn("power", names)
        self.assertIn("io_637", names)
        self.assertIn("iccid", names)
        for n in ("pf_lbs", "_MOTION", "power_enb", "power_filtered", "m2m_odo", "distance", "totalDistance"):
            self.assertNotIn(n, names)
        names_all = {n for n, _, _ in attrs_to_wialon_params(SAMPLE_ATTRS, include_platform=True)}
        for n in ("pf_lbs", "_MOTION", "power_enb", "power_filtered", "m2m_odo"):
            self.assertIn(n, names_all)

    def test_wialon_param_types(self):
        p = dict((n, (t, v)) for n, t, v in attrs_to_wialon_params(
            {"a": True, "b": 5, "c": 1.5, "d": "x y,z"}))
        self.assertEqual(p["a"], (1, "1"))
        self.assertEqual(p["b"], (1, "5"))
        self.assertEqual(p["c"], (2, "1.5"))
        self.assertEqual(p["d"], (3, "x_y_z"))


class ModelTests(unittest.TestCase):
    def test_parse_ts_variants(self):
        want = datetime(2026, 10, 26, 13, 0, 0, tzinfo=timezone.utc)
        for s in ("2026-10-26T13:00:00Z", "2026-10-26T15:00:00+02:00", "2026-10-26T13:00:00", "2026-10-26T13:00:00.000Z"):
            self.assertEqual(parse_ts(s), want, s)
        self.assertIsNone(parse_ts(None))
        self.assertIsNone(parse_ts("not a date"))

    def test_raw_coordinates_preferred(self):
        pos = make_position(0, datetime(2026, 10, 20, tzinfo=timezone.utc))
        pt = position_to_point(pos)
        self.assertAlmostEqual(pt.lat, 49.0)         # pf_orig_lat, а не latitude 50.0
        pos["attributes"] = json.dumps({"power": 12.0})
        pt = position_to_point(pos)
        self.assertEqual((pt.lat, pt.lon), (50.0, 30.0))  # без pf_orig — координати точки

    def test_attributes_as_dict_or_garbage(self):
        base = datetime(2026, 10, 20, tzinfo=timezone.utc)
        pos = make_position(0, base)
        pos["attributes"] = {"power": 12.5}
        self.assertEqual(position_to_point(pos).attrs, {"power": 12.5})
        pos["attributes"] = "{broken"
        self.assertEqual(position_to_point(pos).attrs, {})
        pos["deviceTime"] = None
        pos["fixTime"] = None
        pos["serverTime"] = None
        self.assertIsNone(position_to_point(pos))

    def test_delta_applied_to_ms(self):
        pos = make_position(0, datetime(2026, 10, 20, tzinfo=timezone.utc))
        a = position_to_point(pos, 0).ts_ms
        b = position_to_point(pos, 5000).ts_ms
        self.assertEqual(b - a, 5000)


class WialonFormatTests(unittest.TestCase):
    def test_coordinates(self):
        self.assertEqual(wialon.format_lat(49.438923), ("4926.3354", "N"))
        self.assertEqual(wialon.format_lon(32.085565), ("03205.1339", "E"))
        self.assertEqual(wialon.format_lat(-12.044573), ("1202.6744", "S"))
        self.assertEqual(wialon.format_lon(-77.042072), ("07702.5243", "W"))
        self.assertEqual(wialon.format_lat(0.0), ("0000.0000", "N"))
        self.assertEqual(wialon.format_lat(10.99999999), ("1100.0000", "N"))   # 59.99999994' округлюється в наступний градус

    def test_login_crc(self):
        line = wialon.build_login("353742377269816", "NA", "2.0").decode()
        self.assertTrue(line.startswith("#L#2.0;353742377269816;NA;") and line.endswith("\r\n"))
        payload = line[3:-2]
        body, _, crc = payload.rpartition(";")
        self.assertEqual(crc, f"{crc16((body + ';').encode()):04X}")
        self.assertEqual(wialon.build_login("353742377269816", "NA", "1.1"), b"#L#353742377269816;NA\r\n")

    def test_data_packet_fields(self):
        ts = int(datetime(2026, 10, 26, 13, 5, 9, tzinfo=timezone.utc).timestamp() * 1000)
        p = Point(ts_ms=ts, lat=49.438923, lon=32.085565, alt=120, course=45, sats=8, speed=60,
                  attrs={"power": 12.084, "hdop": 1.5, "ignition": True})
        line = wialon.build_data_packet(p, "1.1").decode()
        self.assertEqual(
            line,
            "#D#261026;130509;4926.3354;N;03205.1339;E;60;45;120;8;1.50;NA;NA;NA;NA;power:2:12.084,hdop:2:1.5,ignition:1:1\r\n",
        )


class EndToEndTeltonikaTests(unittest.TestCase):
    BASE = datetime(2026, 10, 20, 6, 0, 0, tzinfo=timezone.utc)

    def make(self, n=530):
        return [make_position(i, self.BASE) for i in range(n)]

    def test_full_transfer_codec8e(self):
        srv = TeltonikaMock()
        self.addCleanup(srv.close)
        client = FakeClient(self.make(530))
        job = TransferJob(cfg_for(client, srv.port, batch_size=100))
        job.start()
        wait_job(job)
        self.assertEqual(job.status, "done", job.message)
        self.assertEqual((job.total, job.ok, job.failed), (530, 530, 0))
        self.assertEqual(len(srv.records), 530)
        self.assertEqual(srv.imeis, ["353742377269816"])
        self.assertEqual(srv.connections, 1)                  # одне з'єднання на весь перенос
        self.assertEqual(srv.packets, 6)                      # 530 записів по 100 → 6 пакетів
        ts = [r["ts"] for r in srv.records]
        self.assertEqual(ts, sorted(ts))
        self.assertEqual(ts[0], int(self.BASE.timestamp() * 1000))
        self.assertEqual(ts[1] - ts[0], 10_000)
        r0 = srv.records[0]
        self.assertEqual(r0["io"][66], 12000)
        self.assertEqual(r0["io"][637], 1)                    # ID > 255 у Codec 8E передаються
        self.assertEqual(r0["event"], 637)
        self.assertAlmostEqual(r0["lat"] / 1e7, 49.0, places=6)   # сирі координати pf_orig_*
        self.assertNotIn(0, r0["io"])
        self.assertEqual(job.unmapped, set())

    def test_codec8_reports_skipped_ids(self):
        srv = TeltonikaMock()
        self.addCleanup(srv.close)
        job = TransferJob(cfg_for(FakeClient(self.make(20)), srv.port, protocol="teltonika8"))
        job.start()
        wait_job(job)
        self.assertEqual(job.status, "done", job.message)
        self.assertEqual(job.skipped_ids, {383, 637})
        self.assertEqual(srv.records[0]["event"], 0)

    def test_time_shift_moves_last_point_to_now_minus_n(self):
        srv = TeltonikaMock()
        self.addCleanup(srv.close)
        job = TransferJob(cfg_for(FakeClient(self.make(100)), srv.port, shift=True, shift_minutes=30))
        job.start()
        wait_job(job)
        self.assertEqual(job.status, "done", job.message)
        last = datetime.fromtimestamp(srv.records[-1]["ts"] / 1000, tz=timezone.utc)
        expected = datetime.now(timezone.utc) - timedelta(minutes=30)
        self.assertLess(abs((last - expected).total_seconds()), 10)
        self.assertEqual(srv.records[1]["ts"] - srv.records[0]["ts"], 10_000)   # інтервали збережено

    def test_basic_only_sends_just_voltage(self):
        srv = TeltonikaMock()
        self.addCleanup(srv.close)
        job = TransferJob(cfg_for(FakeClient(self.make(10)), srv.port, all_params=False))
        job.start()
        wait_job(job)
        self.assertEqual(list(srv.records[0]["io"]), [66])

    def test_test_run_limit(self):
        srv = TeltonikaMock()
        self.addCleanup(srv.close)
        job = TransferJob(cfg_for(FakeClient(self.make(500)), srv.port, limit=20))
        job.start()
        wait_job(job)
        self.assertEqual((job.total, job.ok, len(srv.records)), (20, 20, 20))

    def test_partial_ack_resends_only_the_rest_without_duplicates(self):
        srv = TeltonikaMock(ack_fn=lambda n, idx: n - 7 if idx == 1 else n)
        self.addCleanup(srv.close)
        job = TransferJob(cfg_for(FakeClient(self.make(250)), srv.port, batch_size=100))
        job.start()
        wait_job(job)
        self.assertEqual(job.status, "done", job.message)
        self.assertEqual((job.ok, job.failed), (250, 0))
        ts = [r["ts"] for r in srv.records]
        self.assertEqual(len(ts), 250)
        self.assertEqual(len(set(ts)), 250)                   # дублікатів немає

    def test_server_rejecting_everything_stops_job_with_clear_error(self):
        srv = TeltonikaMock(ack_fn=lambda n, idx: 0)
        self.addCleanup(srv.close)
        job = TransferJob(cfg_for(FakeClient(self.make(2000)), srv.port))
        job.start()
        wait_job(job)
        self.assertEqual(job.status, "error")
        self.assertIn("не приймає", job.message)
        self.assertEqual(job.ok, 0)

    def test_unknown_imei_is_reported(self):
        srv = TeltonikaMock(accept_imei=False)
        self.addCleanup(srv.close)
        job = TransferJob(cfg_for(FakeClient(self.make(10)), srv.port))
        job.start()
        wait_job(job)
        self.assertEqual(job.status, "error")
        self.assertIn("IMEI", job.message)

    def test_server_down_is_reported(self):
        import socket
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()                      # на цьому порту ніхто не слухає
        job = TransferJob(cfg_for(FakeClient(self.make(10)), port))
        job.start()
        wait_job(job)
        self.assertEqual(job.status, "error")
        self.assertIn("підключитись", job.message)

    def test_reconnects_after_connection_drop(self):
        srv = TeltonikaMock()
        self.addCleanup(srv.close)
        job = TransferJob(cfg_for(FakeClient(self.make(300)), srv.port, batch_size=100))
        orig = TransferJob._try_send
        state = {"n": 0}

        def flaky(self_, pts):
            state["n"] += 1
            if state["n"] == 2 and self_._sender.sock is not None:
                self_._sender.sock.close()          # обрив між пакетами
                self_._sender.sock = None
            return orig(self_, pts)

        TransferJob._try_send = flaky
        self.addCleanup(setattr, TransferJob, "_try_send", orig)
        job.start()
        wait_job(job)
        self.assertEqual(job.status, "done", job.message)
        self.assertEqual(len(srv.records), 300)
        self.assertEqual(srv.connections, 2)

    def test_stop_midway(self):
        srv = TeltonikaMock()
        self.addCleanup(srv.close)
        job = TransferJob(cfg_for(FakeClient(self.make(4000), delay=0.05), srv.port, batch_size=50))
        job.start()
        time.sleep(0.4)
        job.stop()
        wait_job(job)
        self.assertEqual(job.status, "stopped")
        self.assertLess(job.processed, 4000)
        self.assertGreater(job.processed, 0)

    def test_empty_period(self):
        srv = TeltonikaMock()
        self.addCleanup(srv.close)
        job = TransferJob(cfg_for(FakeClient([]), srv.port))
        job.start()
        wait_job(job)
        self.assertEqual((job.status, job.total), ("done", 0))
        self.assertIn("немає", job.message)

    def test_check_connection(self):
        srv = TeltonikaMock()
        self.addCleanup(srv.close)
        msg = check_connection(cfg_for(FakeClient([]), srv.port))
        self.assertIn("прийняв", msg)
        self.assertEqual(srv.packets, 0)

    def test_batch_size_is_capped_to_255(self):
        srv = TeltonikaMock()
        self.addCleanup(srv.close)
        job = TransferJob(cfg_for(FakeClient(self.make(300)), srv.port, batch_size=1000))
        job.start()
        wait_job(job)
        self.assertEqual(job.status, "done", job.message)
        self.assertEqual(srv.packets, 2)


class EndToEndWialonTests(unittest.TestCase):
    BASE = datetime(2026, 10, 20, 6, 0, 0, tzinfo=timezone.utc)

    def run_job(self, srv, version="2.0", n=130, **kw):
        positions = [make_position(i, self.BASE) for i in range(n)]
        job = TransferJob(cfg_for(FakeClient(positions), srv.port, protocol="wialon", wialon_version=version, **kw))
        job.start()
        wait_job(job)
        return job

    def test_full_transfer_v2(self):
        srv = WialonMock("2.0")
        self.addCleanup(srv.close)
        job = self.run_job(srv)
        self.assertEqual(job.status, "done", job.message)
        self.assertEqual((job.ok, job.failed), (130, 0))
        self.assertEqual(len(srv.data), 130)
        self.assertTrue(srv.logins[0].startswith("2.0;353742377269816;NA;"))
        first = srv.data[0].rpartition(";")[0].split(";")
        self.assertEqual(first[0:2], ["201026", "060000"])
        self.assertIn("power:2:12", first[15])
        self.assertNotIn("pf_", first[15])
        self.assertIn("io_637:1:1", first[15])

    def test_full_transfer_v1(self):
        srv = WialonMock("1.1")
        self.addCleanup(srv.close)
        job = self.run_job(srv, version="1.1", n=20)
        self.assertEqual(job.status, "done", job.message)
        self.assertEqual(srv.logins, ["353742377269816;NA"])
        self.assertEqual(len(srv.data), 20)

    def test_include_platform_params(self):
        srv = WialonMock("2.0")
        self.addCleanup(srv.close)
        job = self.run_job(srv, n=5, include_platform=True)
        self.assertEqual(job.status, "done", job.message)
        self.assertIn("power_enb:1:0", srv.data[0])
        self.assertIn("pf_orig_lat:2:", srv.data[0])

    def test_rejected_packets_are_collected_and_retried(self):
        rejected = {"on": True}
        # відхиляємо за вмістом (час 06:00:30 і 06:12:50 — це 4-та і 78-ма точки), тож повторна відправка теж відхиляється
        srv = WialonMock("2.0", reject=lambda idx, line: rejected["on"] and line.split(";")[1] in ("060030", "061250"))
        self.addCleanup(srv.close)
        job = self.run_job(srv, n=100)
        self.assertEqual(job.status, "done", job.message)
        self.assertEqual((job.ok, job.failed), (98, 2))
        self.assertTrue(all("-1" in e["error"] for e in job.errors))
        rejected["on"] = False
        job.retry_failed()
        wait_job(job)
        self.assertEqual((job.ok, job.failed, job.status), (100, 0, "done"))
        self.assertEqual(job.errors, [])

    def test_bad_login(self):
        srv = WialonMock("2.0")
        self.addCleanup(srv.close)
        # версія 1.1 на стороні клієнта проти сервера 2.0 → сервер відповідає #AL#0
        job = self.run_job(srv, version="1.1", n=5)
        self.assertEqual(job.status, "error")
        self.assertIn("не прийняв логін", job.message)


class PreviewTests(unittest.TestCase):
    def test_preview_counts_range_and_series(self):
        base = datetime(2026, 10, 20, 6, 0, 0, tzinfo=timezone.utc)
        client = FakeClient([make_position(i, base) for i in range(1234)])
        pv = build_preview(client, 1, "a", "b")
        self.assertEqual(pv["total"], 1234)
        self.assertEqual(pv["first"], base)
        self.assertEqual(pv["last"], base + timedelta(seconds=1233 * 10))
        self.assertGreater(len(pv["series"]), 100)
        self.assertLess(client.calls, 15)       # вибірка сторінок, а не весь період

    def test_preview_empty(self):
        pv = build_preview(FakeClient([]), 1, "a", "b")
        self.assertEqual((pv["total"], pv["first"], pv["series"]), (0, None, []))


if __name__ == "__main__":
    unittest.main()
