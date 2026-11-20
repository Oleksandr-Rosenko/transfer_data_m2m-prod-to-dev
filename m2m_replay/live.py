"""Модуль онлайн-ретрансляції телеметрії Prod -> Dev у реальному часі.

Отримує нові точки з Prod API (суворо в режимі читання) та миттєво
дублює їх на Dev-сервер через OsmAnd HTTP-протокол (порт 5055).
"""

from __future__ import annotations

import threading
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from .models import parse_attributes, parse_ts


class LiveRelayEngine:
    """Потокобезпечний рушій ретрансляції точок."""

    def __init__(self):
        self.is_running = False
        self.thread: Optional[threading.Thread] = None

        # Параметри
        self.client = None
        self.dev_target_url = ""
        self.source_device_id: Optional[int] = None
        self.source_name: str = ""
        self.target_unique_id: str = "dev-tacho-1"

        # Стан
        self.status = "STOPPED"  # STOPPED, STREAMING, ERROR
        self.status_message = "Міст зупинено"
        self.last_seen_pos_id: Optional[int] = None

        # Метрики та логи
        self.stats = {
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

    def forward_single_point(self, pos: Dict[str, Any]) -> bool:
        """Пересилає одну точку на Dev."""
        try:
            # Час точки
            dt = parse_ts(pos.get("deviceTime") or pos.get("fixTime"))
            ts = int(dt.timestamp()) if dt else int(time.time())

            speed_kmh = round(float(pos.get("speed", 0.0) or 0.0), 1)

            params: Dict[str, Any] = {
                "id": self.target_unique_id,
                "lat": pos.get("latitude", 0.0),
                "lon": pos.get("longitude", 0.0),
                "timestamp": ts,
                "speed": round(speed_kmh * 0.277778, 2),  # км/г -> м/с для OsmAnd
                "bearing": int(pos.get("course", 0) or 0),
                "altitude": int(pos.get("altitude", 0) or 0),
            }

            # Витягуємо attributes (паливо, запалювання, tacho, CAN)
            attrs = parse_attributes(pos.get("attributes"))
            fuel = None
            ignition = None
            driver = None

            if attrs:
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
                        "Картка тахо": str(driver) if driver else "—",
                        "Статус": "✅ 200 OK",
                    })
                    return True
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
                        "Статус": f"❌ Dev HTTP {resp.status_code}",
                    })
                    return False

        except Exception as e:
            with self.lock:
                self.stats["errors"] += 1
                self.packet_log.appendleft({
                    "Час": datetime.now().strftime("%H:%M:%S"),
                    "Джерело (Prod)": self.source_name,
                    "Ціль (Dev)": self.target_unique_id,
                    "Паливо (л)": "—",
                    "Швидкість": "—",
                    "Запалювання": "—",
                    "Картка тахо": "—",
                    "Статус": f"❌ Помилка: {str(e)[:40]}",
                })
            return False

    def _worker_loop(self) -> None:
        """Основний цикл онлайн-зчитування нових точок."""
        self.status = "STREAMING"
        self.status_message = f"🟢 Активна ретрансляція: [{self.source_name}] ➡️ [{self.target_unique_id}]"

        # 1. Початковий запит: беремо останню точку авто з Prod
        try:
            now = datetime.now(timezone.utc)
            t_from = (now - timedelta(minutes=10)).strftime("%Y-%m-%dT%H:%M:%SZ")
            t_to = (now + timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

            res = self.client.positions_page(
                did=self.source_device_id,
                date_from=t_from,
                date_to=t_to,
                page=1,
                per_page=5,
                order="DESC",
            )
            items = res.get("items", [])
            if items:
                # Беремо найсвіжішу точку, відправляємо на Dev і фіксуємо ID
                latest = items[0]
                self.last_seen_pos_id = latest.get("id")
                self.forward_single_point(latest)
        except Exception as ex:
            # Якщо за останні 10 хв точок не було, не критично
            pass

        # 2. Неперервний цикл зчитування онлайн-потоку що 2 секунди
        while self.is_running:
            try:
                now = datetime.now(timezone.utc)
                t_from = (now - timedelta(minutes=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
                t_to = (now + timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%SZ")

                res = self.client.positions_page(
                    did=self.source_device_id,
                    date_from=t_from,
                    date_to=t_to,
                    page=1,
                    per_page=20,
                    order="ASC",
                )
                items = res.get("items", [])

                for pos in items:
                    pos_id = pos.get("id")
                    if self.last_seen_pos_id is None or (pos_id and pos_id > self.last_seen_pos_id):
                        self.forward_single_point(pos)
                        if pos_id:
                            self.last_seen_pos_id = pos_id

                self.status = "STREAMING"
                self.status_message = f"🟢 Активна ретрансляція: [{self.source_name}] ➡️ [{self.target_unique_id}]"

            except Exception as e:
                self.status = "STREAMING"
                self.status_message = f"⚠️ Помилка читання з Prod (повтор...): {str(e)[:50]}"

            # Інтервал онлайн-перевірки
            for _ in range(20):
                if not self.is_running:
                    break
                time.sleep(0.1)

        self.status = "STOPPED"
        self.status_message = "Міст зупинено"

    def start(
        self,
        client,
        dev_url: str,
        source_device_id: int,
        source_name: str,
        target_unique_id: str,
    ) -> None:
        """Запуск онлайн-трансляції у фоні."""
        if self.is_running:
            return

        self.client = client
        self.dev_target_url = dev_url.strip()
        self.source_device_id = source_device_id
        self.source_name = source_name
        self.target_unique_id = target_unique_id.strip()
        self.last_seen_pos_id = None
        self.is_running = True

        self.thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        """Зупинка ретрансляції."""
        self.is_running = False
        self.status = "STOPPED"
        self.status_message = "Зупинено користувачем"


# Глобальний синглтон-інстанс
_live_engine: Optional[LiveRelayEngine] = None


def get_live_engine() -> LiveRelayEngine:
    global _live_engine
    if _live_engine is None:
        _live_engine = LiveRelayEngine()
    return _live_engine
