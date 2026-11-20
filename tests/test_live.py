import unittest
from unittest.mock import MagicMock, patch
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

from m2m_replay.live import MultiLiveRelayEngine, RouteItem, KYIV_TZ


class TestMultiLiveRelayEngine(unittest.TestCase):
    def setUp(self):
        self.engine = MultiLiveRelayEngine()
        self.engine.routes.clear()
        self.engine.dev_target_url = "http://95.217.119.27:5055"
        self.engine.tz_offset_sec = 0

    def test_add_and_toggle_route(self):
        self.engine.add_route(905, "DAF CA 1316 TT TACHO", "11861937062956496111")
        self.assertIn(905, self.engine.routes)
        r = self.engine.routes[905]
        self.assertEqual(r.device_id, 905)
        self.assertTrue(r.enabled)

        # Toggle route to paused
        self.engine.set_route_enabled(905, False)
        self.assertFalse(r.enabled)
        self.assertEqual(r.status, "PAUSED")

        # Toggle back
        self.engine.set_route_enabled(905, True)
        self.assertTrue(r.enabled)
        self.assertIsNone(r.last_seen_id)

    @patch("m2m_replay.live.requests.get")
    def test_forward_single_point_clean_columns_and_kyiv_time(self, mock_get):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_get.return_value = mock_resp

        r = RouteItem(device_id=905, source_name="DAF CA 1316 TT", target_unique_id="11861937062956496111")

        # Point at 17:14:22 UTC -> in Kyiv winter time (UTC+2) this MUST display as 19:14:22
        pos = {
            "id": 5001,
            "deviceTime": "2026-11-20T17:14:22.000Z",
            "latitude": 50.451725,
            "longitude": 23.437510,
            "speed": 86.0,
            "course": 180,
            "altitude": 220,
            "attributes": '{"in1":1,"ta":0,"fuel":235.0,"ignition":true}',
        }

        success = self.engine.forward_single_point(r, pos)
        self.assertTrue(success)
        self.assertEqual(r.packets_count, 1)
        self.assertEqual(r.speed_kmh, 86.0)
        self.assertEqual(r.coords_str, "50.451725, 23.437510")
        self.assertEqual(r.last_time, "19:14:22")

        # Check OsmAnd parameters sent to dev
        call_kwargs = mock_get.call_args[1]
        params = call_kwargs["params"]
        self.assertEqual(params["id"], "11861937062956496111")
        self.assertEqual(params["lat"], 50.451725)
        self.assertEqual(params["lon"], 23.437510)
        # raw timestamp is UTC
        dt = datetime(2026, 11, 20, 17, 14, 22, tzinfo=timezone.utc)
        self.assertEqual(params["timestamp"], int(dt.timestamp()))

        # Check packet log does not contain unwanted UI columns
        log_entry = r.packet_log[0]
        self.assertNotIn("Паливо", log_entry)
        self.assertNotIn("Запалювання", log_entry)
        self.assertNotIn("Картка водія", log_entry)
        self.assertIn("Координати", log_entry)
        self.assertIn("Швидкість", log_entry)
        self.assertIn("Час", log_entry)
        self.assertEqual(log_entry["Час"], "19:14:22")
        self.assertEqual(log_entry["Координати"], "50.451725, 23.437510")


if __name__ == "__main__":
    unittest.main()
