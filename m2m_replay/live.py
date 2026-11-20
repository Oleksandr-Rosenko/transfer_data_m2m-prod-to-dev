"""Модуль онлайн-ретрансляції телеметрії Prod -> Dev у реальному часі.

Підключається до WebSocket Prod у режимі суворого читання (READ-ONLY)
та дублює точки обраного об'єкта на Dev через OsmAnd HTTP-протокол (порт 5055).
"""

from __future__ import annotations

import json
import threading
import time
from collections import deque
from datetime import datetime
from typing import Any, Dict, List, Optional
import requests
import websocket


class LiveRelayEngine:
    """Потокобезпечний синглтон-рушій для онлайн-ретрансляції точок."""

    def __init__(self):
        self.is_running = False
        self.thread: Optional[threading.Thread] = None
        self.ws: Optional[websocket.WebSocketApp] = None

        # Конфігурація активного сеансу
        self.prod_ws_url = ""
        self.prod_token = ""
        self.dev_target_url = ""
        self.source_device_id: Optional[int] = None
        self.source_imei: str = ""
        self.source_name: str = ""
        self.target_unique_id: str = "dev-tacho-1"

        # Стан
        self.status = "STOPPED"  # STOPPED, CONNECTING, STREAMING, ERROR
        self.status_message = "Міст зупинено"
        self.prod_device_map: Dict[int, str] = {}  # {internal_id: uniqueId (IMEI)}

        # Метрики та логи
        self.stats = {
            "packets_in": 0,
            "packets_forwarded": 0,
            "errors": 0,
            "last_fuel": None,
            "last_speed": 0.0,
            "last_ignition": None,
            "last_driver": None,
            "last_time": None,
            "last_lat": None,
            "last_lon": None,
        }
        self.packet_log = deque(maxlen=50)
        self.fuel_history = deque(maxlen=200)
        self.lock = threading.Lock()

    def sync_devices(self, client) -> None:
        """Синхронізує карту internal_id -> uniqueId через існуючий M2MClient."""
        try:
            res = client.request("GET", "/api/devices")
            if isinstance(res, list):
                new_map = {}
                for d in res:
                    did = d.get("id")
                    uid = str(d.get("uniqueId") or "")
                    if did is not None:
                        new_map[did] = uid
                with self.lock:
                    self.prod_device_map = new_map
        except Exception:
            pass

    def forward_position(self, pos: Dict[str, Any]) -> None:
        """Обробляє та передає точку, якщо вона належить обраному авто."""
        dev_id = pos.get("deviceId")
        with self.lock:
            self.stats["packets_in"] += 1

        # Фільтр: перевіряємо збіг за внутрішнім ID або за IMEI
        is_match = False
        if self.source_device_id and dev_id == self.source_device_id:
            is_match = True
        elif self.source_imei:
            current_imei = self.prod_device_map.get(dev_id)
            if current_imei and current_imei == self.source_imei:
                is_match = True

        if not is_match:
            return

        try:
            # Час фіксації
            fix_time = pos.get("fixTime")
            if fix_time:
                try:
                    dt = datetime.fromisoformat(fix_time.replace("Z", "+00:00"))
                    ts = int(dt.timestamp())
                except Exception:
                    ts = int(time.time())
            else:
                ts = int(time.time())

            speed_raw = float(pos.get("speed", 0.0) or 0.0)
            speed_kmh = round(speed_raw * 1.852, 1)

            # Базовий запит для Traccar OsmAnd
            params: Dict[str, Any] = {
                "id": self.target_unique_id,
                "lat": pos.get("latitude", 0.0),
                "lon": pos.get("longitude", 0.0),
                "timestamp": ts,
                "speed": round(speed_raw * 0.514444, 2),  # м/с
                "bearing": pos.get("course", 0),
                "altitude": pos.get("altitude", 0),
            }

            # Витягуємо всі attributes (паливо, запалювання, tacho, CAN)
            attrs = pos.get("attributes", {})
            fuel = None
            ignition = None
            driver = None

            if isinstance(attrs, dict):
                for k, v in attrs.items():
                    if not isinstance(v, (dict, list)):
                        params[k] = v

                fuel = attrs.get("fuel") or attrs.get("can_fuel") or attrs.get("fuel1")
                ignition = attrs.get("ignition")
                driver = attrs.get("driverUniqueId") or attrs.get("driver")

            # ВІДПРАВКА: суворо на Dev
            resp = requests.get(self.dev_target_url, params=params, timeout=4)

            with self.lock:
                now_str = datetime.now().strftime("%H:%M:%S")
                if resp.status_code == 200:
                    self.stats["packets_forwarded"] += 1
                    self.stats["last_time"] = now_str
                    self.stats["last_speed"] = speed_kmh
                    self.stats["last_lat"] = params["lat"]
                    self.stats["last_lon"] = params["lon"]

                    if fuel is not None:
                        try:
                            f_num = float(fuel)
                            self.stats["last_fuel"] = f_num
                            self.fuel_history.append({"time": now_str, "fuel": f_num})
                        except (ValueError, TypeError):
                            pass

                    if ignition is not None:
                        self.stats["last_ignition"] = bool(ignition)
                    if driver:
                        self.stats["last_driver"] = str(driver)

                    self.packet_log.appendleft({
                        "Час": now_str,
                        "Джерело (Prod)": self.source_name,
                        "Ціль (Dev)": self.target_unique_id,
                        "Паливо (л)": f"{fuel} л" if fuel is not None else "—",
                        "Швидкість": f"{speed_kmh} км/г",
                        "Запалювання": "ВКЛ" if ignition else "ВИМК",
                        "Картка тахо": driver if driver else "—",
                        "Статус": "✅ 200 OK",
                    })
                else:
                    self.stats["errors"] += 1
                    self.packet_log.appendleft({
                        "Час": now_str,
                        "Джерело (Prod)": self.source_name,
                        "Ціль (Dev)": self.target_unique_id,
                        "Паливо (л)": "—",
                        "Швидкість": "—",
                        "Запалювання": "—",
                        "Картка тахо": "—",
                        "Статус": f"❌ HTTP {resp.status_code}",
                    })

        except Exception:
            with self.lock:
                self.stats["errors"] += 1

    def _worker_loop(self) -> None:
        headers = [f"Authorization: Bearer {self.prod_token}"]

        while self.is_running:
            try:
                self.status = "CONNECTING"
                self.status_message = f"Підключення до сокета Prod: {self.prod_ws_url}"

                def on_message(ws, msg):
                    if not self.is_running:
                        ws.close()
                        return
                    try:
                        data = json.loads(msg)
                        if "positions" in data:
                            for p in data["positions"]:
                                self.forward_position(p)
                    except Exception:
                        pass

                def on_open(ws):
                    self.status = "STREAMING"
                    self.status_message = f"🟢 Активна ретрансляція: [{self.source_name}] ➡️ [{self.target_unique_id}]"

                def on_close(ws, code, msg):
                    self.status = "CONNECTING"
                    self.status_message = "Перепідключення сокета..."

                self.ws = websocket.WebSocketApp(
                    self.prod_ws_url,
                    header=headers,
                    on_open=on_open,
                    on_message=on_message,
                    on_close=on_close,
                )
                self.ws.run_forever(ping_interval=20, ping_timeout=10)

            except Exception as e:
                self.status = "ERROR"
                self.status_message = f"Помилка з'єднання: {e}"

            if self.is_running:
                time.sleep(4)

        self.status = "STOPPED"
        self.status_message = "Міст зупинено"

    def start(
        self,
        base_url: str,
        token: str,
        dev_url: str,
        source_device_id: Optional[int],
        source_imei: str,
        source_name: str,
        target_unique_id: str,
    ) -> None:
        """Запуск ретрансляції у фоні."""
        if self.is_running:
            return

        clean_url = base_url.strip().rstrip("/")
        ws_scheme = "wss://" if clean_url.startswith("https://") else "ws://"
        host_part = clean_url.replace("https://", "").replace("http://", "")
        self.prod_ws_url = f"{ws_scheme}{host_part}/api/socket"
        self.prod_token = token
        self.dev_target_url = dev_url.strip()
        self.source_device_id = source_device_id
        self.source_imei = source_imei
        self.source_name = source_name
        self.target_unique_id = target_unique_id.strip()
        self.is_running = True

        self.thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        """Зупинка ретрансляції."""
        self.is_running = False
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
        self.status = "STOPPED"
        self.status_message = "Зупинено користувачем"


# Глобальний синглтон-інстанс
_live_engine: Optional[LiveRelayEngine] = None


def get_live_engine() -> LiveRelayEngine:
    global _live_engine
    if _live_engine is None:
        _live_engine = LiveRelayEngine()
    return _live_engine
