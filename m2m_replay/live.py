"""Модуль мульти-об'єктної онлайн-ретрансляції телеметрії Prod -> Dev у реальному часі.

Підтримує одночасну ретрансляцію до 10+ об'єктів.
Кожен об'єкт має свій мапінг: [Об'єкт Prod] -> [Dev Unique ID].

ОПТИМІЗАЦІЯ ТА НАДІЙНІСТЬ:
- Вузьке вікно 90 секунд та per_page=3 на кожен запит по B-Tree індексу.
- Потокобезпечний синглтон-воркер працює у фоні незалежно від інтерфейсу.
- Адаптивний смарт-таймінг без спаму та без навантаження на Prod.
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

import requests

from .models import parse_attributes, parse_ts


@dataclass
class RouteItem:
    """Маршрут ретрансляції одного автомобіля."""
    device_id: int
    source_name: str
    target_unique_id: str
    enabled: bool = True
    last_seen_id: Optional[int] = None
    packets_count: int = 0
    errors_count: int = 0
    last_time: str = "—"
    speed_kmh: float = 0.0
    fuel: Optional[float] = None
    ignition: Optional[bool] = None
    driver: Optional[str] = None
    status: str = "IDLE"  # IDLE, STREAMING, ERROR
    last_error: str = ""


class MultiLiveRelayEngine:
    """Двигун паралельної ретрансляції кількох об'єктів."""

    def __init__(self):
        self.is_running = False
        self.thread: Optional[threading.Thread] = None
        self.client = None
        self.dev_target_url = ""

        # Словник маршрутів {device_id: RouteItem}
        self.routes: Dict[int, RouteItem] = {}

        # Глобальні метрики
        self.total_packets_forwarded = 0
        self.total_errors = 0
        self.started_at: Optional[str] = None
        self.packet_log = deque(maxlen=60)
        self.lock = threading.Lock()

    def add_route(self, device_id: int, source_name: str, target_unique_id: str) -> None:
        """Додає або оновлює маршрут авто."""
        with self.lock:
            if device_id in self.routes:
                self.routes[device_id].target_unique_id = target_unique_id.strip()
                self.routes[device_id].source_name = source_name
            else:
                self.routes[device_id] = RouteItem(
                    device_id=device_id,
                    source_name=source_name,
                    target_unique_id=target_unique_id.strip(),
                )

    def remove_route(self, device_id: int) -> None:
        """Видаляє маршрут."""
        with self.lock:
            self.routes.pop(device_id, None)

    def get_routes(self) -> List[RouteItem]:
        """Повертає список копій поточних маршрутів."""
        with self.lock:
            return list(self.routes.values())

    def forward_single_point(self, route: RouteItem, pos: Dict[str, Any]) -> bool:
        """Пересилає точку конкретного авто на Dev."""
        try:
            dt = parse_ts(pos.get("deviceTime") or pos.get("fixTime"))
            ts = int(dt.timestamp()) if dt else int(time.time())

            speed_kmh = round(float(pos.get("speed", 0.0) or 0.0), 1)

            params: Dict[str, Any] = {
                "id": route.target_unique_id,
                "lat": pos.get("latitude", 0.0),
                "lon": pos.get("longitude", 0.0),
                "timestamp": ts,
                "speed": round(speed_kmh * 0.277778, 2),  # км/г -> м/с
                "bearing": int(pos.get("course", 0) or 0),
                "altitude": int(pos.get("altitude", 0) or 0),
            }

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
                    route.packets_count += 1
                    self.total_packets_forwarded += 1
                    route.last_time = now_str
                    route.speed_kmh = speed_kmh
                    route.status = "STREAMING"
                    route.last_error = ""

                    if fuel is not None:
                        try:
                            route.fuel = float(fuel)
                        except (ValueError, TypeError):
                            pass

                    if ignition is not None:
                        route.ignition = bool(ignition)
                    if driver:
                        route.driver = str(driver)

                    self.packet_log.appendleft({
                        "Час": now_str,
                        "Авто (Prod)": route.source_name,
                        "Ціль (Dev)": route.target_unique_id,
                        "Паливо": f"{fuel} л" if fuel is not None else "—",
                        "Швидкість": f"{speed_kmh} км/г",
                        "Запалювання": "ВКЛ" if ignition else "ВИМК",
                        "Водій": str(driver) if driver else "—",
                        "Статус": "✅ 200 OK",
                    })
                    return True
                else:
                    route.errors_count += 1
                    self.total_errors += 1
                    route.last_error = f"Dev HTTP {resp.status_code}"
                    self.packet_log.appendleft({
                        "Час": now_str,
                        "Авто (Prod)": route.source_name,
                        "Ціль (Dev)": route.target_unique_id,
                        "Паливо": "—",
                        "Швидкість": "—",
                        "Запалювання": "—",
                        "Водій": "—",
                        "Статус": f"❌ Dev {resp.status_code}",
                    })
                    return False

        except Exception as e:
            with self.lock:
                route.errors_count += 1
                self.total_errors += 1
                route.last_error = str(e)[:30]
            return False

    def _worker_loop(self) -> None:
        """Головний цикл багатопотокової ретрансляції."""
        # 1. Початковий якір для всіх зареєстрованих авто (миттєвий старт)
        with self.lock:
            active_routes = [r for r in self.routes.values() if r.enabled]

        now = datetime.now(timezone.utc)
        t_from_init = (now - timedelta(minutes=15)).strftime("%Y-%m-%dT%H:%M:%SZ")
        t_to_init = (now + timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

        for r in active_routes:
            try:
                res = self.client.positions_page(
                    did=r.device_id,
                    date_from=t_from_init,
                    date_to=t_to_init,
                    page=1,
                    per_page=3,
                    order="DESC",
                )
                items = res.get("items", [])
                if items:
                    latest = items[0]
                    r.last_seen_id = latest.get("id")
                    self.forward_single_point(r, latest)
            except Exception:
                pass

        # 2. Неперервний смарт-цикл опитування всіх авто
        while self.is_running:
            with self.lock:
                current_routes = [r for r in self.routes.values() if r.enabled]

            if not current_routes:
                time.sleep(1)
                continue

            now = datetime.now(timezone.utc)
            t_from = (now - timedelta(seconds=90)).strftime("%Y-%m-%dT%H:%M:%SZ")
            t_to = (now + timedelta(seconds=30)).strftime("%Y-%m-%dT%H:%M:%SZ")

            for r in current_routes:
                if not self.is_running:
                    break
                try:
                    res = self.client.positions_page(
                        did=r.device_id,
                        date_from=t_from,
                        date_to=t_to,
                        page=1,
                        per_page=3,
                        order="ASC",
                    )
                    items = res.get("items", [])
                    for idx, pos in enumerate(items):
                        pos_id = pos.get("id")
                        if r.last_seen_id is None or (pos_id and pos_id > r.last_seen_id):
                            if idx > 0:
                                time.sleep(0.2)
                            self.forward_single_point(r, pos)
                            if pos_id:
                                r.last_seen_id = pos_id
                except Exception as e:
                    r.last_error = str(e)[:30]

            # Пауза між колами опитування (2.5 секунди для свіжих даних)
            for _ in range(25):
                if not self.is_running:
                    break
                time.sleep(0.1)

        with self.lock:
            for r in self.routes.values():
                r.status = "IDLE"

    def start(self, client, dev_url: str) -> None:
        """Запуск ретрансляції всіх налаштованих авто."""
        if self.is_running:
            return

        self.client = client
        self.dev_target_url = dev_url.strip()
        self.is_running = True
        self.started_at = datetime.now().strftime("%H:%M:%S")

        self.thread = threading.Thread(target=self._worker_loop, daemon=True)
        self.thread.start()

    def stop(self) -> None:
        """Зупинка ретрансляції."""
        self.is_running = False
        with self.lock:
            for r in self.routes.values():
                r.status = "IDLE"


_multi_engine: Optional[MultiLiveRelayEngine] = None


def get_multi_live_engine() -> MultiLiveRelayEngine:
    global _multi_engine
    if _multi_engine is None:
        _multi_engine = MultiLiveRelayEngine()
    return _multi_engine
