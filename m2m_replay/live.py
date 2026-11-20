"""Модуль мульти-об'єктної онлайн-ретрансляції телеметрії Prod -> Dev у реальному часі.

Підтримує одночасну ретрансляцію до 10+ об'єктів.
Кожен об'єкт має свій мапінг: [Об'єкт Prod] -> [Dev Unique ID].

ОПТИМІЗАЦІЯ ТА НАДІЙНІСТЬ:
- Вузьке вікно 90 секунд та per_page=3 на кожен запит по B-Tree індексу.
- Потокобезпечний синглтон-воркер працює у фоні незалежно від інтерфейсу.
- Адаптивний смарт-таймінг без спаму та без навантаження на Prod.
"""

from __future__ import annotations

import json
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from zoneinfo import ZoneInfo

import requests

from .models import parse_attributes, parse_ts

try:
    KYIV_TZ = ZoneInfo("Europe/Kyiv")
except Exception:
    KYIV_TZ = ZoneInfo("Europe/Kiev")


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
    coords_str: str = "—"
    fuel: Optional[float] = None
    ignition: Optional[bool] = None
    driver: Optional[str] = None
    status: str = "IDLE"  # IDLE, STREAMING, WAITING, ERROR, PAUSED
    last_error: str = ""
    latest_params_raw: str = ""
    packet_log: deque = field(default_factory=lambda: deque(maxlen=40))


class MultiLiveRelayEngine:
    """Двигун паралельної ретрансляції кількох об'єктів."""

    def __init__(self):
        self.is_running = False
        self.thread: Optional[threading.Thread] = None
        self.client = None
        self.dev_target_url = ""
        self.tz_offset_sec = 0  # 0 = стандартний UTC timestamp (Dev сам додає локальну зону в інтерфейсі)

        # Словник маршрутів {device_id: RouteItem}
        self.routes: Dict[int, RouteItem] = {}

        # Глобальні метрики
        self.total_packets_forwarded = 0
        self.total_errors = 0
        self.started_at: Optional[str] = None
        self.packet_log = deque(maxlen=80)
        self.lock = threading.Lock()

        # Автоматичне відновлення збережених маршрутів
        self.load_cache()

    def _get_cache_path(self) -> Path:
        return Path(__file__).resolve().parent.parent / ".live_routes.json"

    def load_cache(self) -> None:
        """Завантажує збережені маршрути з диску."""
        try:
            fpath = self._get_cache_path()
            if fpath.exists():
                with open(fpath, "r", encoding="utf-8") as f:
                    data = json.load(f)
                with self.lock:
                    for it in data:
                        did = it.get("device_id")
                        if did:
                            self.routes[did] = RouteItem(
                                device_id=did,
                                source_name=it.get("source_name", f"ID {did}"),
                                target_unique_id=it.get("target_unique_id", ""),
                                enabled=it.get("enabled", True),
                            )
        except Exception:
            pass

    def save_cache(self) -> None:
        """Зберігає поточні маршрути на диск для відновлення після перезапуску."""
        try:
            fpath = self._get_cache_path()
            with self.lock:
                data = [
                    {
                        "device_id": r.device_id,
                        "source_name": r.source_name,
                        "target_unique_id": r.target_unique_id,
                        "enabled": r.enabled,
                    }
                    for r in self.routes.values()
                ]
            with open(fpath, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception:
            pass

    def add_route(self, device_id: int, source_name: str, target_unique_id: str) -> None:
        """Додає або оновлює маршрут авто."""
        with self.lock:
            if device_id in self.routes:
                self.routes[device_id].target_unique_id = target_unique_id.strip()
                self.routes[device_id].source_name = source_name
                self.routes[device_id].enabled = True
            else:
                self.routes[device_id] = RouteItem(
                    device_id=device_id,
                    source_name=source_name,
                    target_unique_id=target_unique_id.strip(),
                    enabled=True,
                )
        self.save_cache()

    def remove_route(self, device_id: int) -> None:
        """Видаляє маршрут."""
        with self.lock:
            self.routes.pop(device_id, None)
        self.save_cache()

    def set_route_enabled(self, device_id: int, enabled: bool) -> None:
        """Явно вмикає або вимикає ретрансляцію для конкретного авто."""
        with self.lock:
            if device_id in self.routes:
                r = self.routes[device_id]
                r.enabled = enabled
                if not r.enabled:
                    r.status = "PAUSED"
                elif self.is_running:
                    r.last_seen_id = None
                    r.status = "STREAMING" if r.packets_count > 0 else "WAITING"
        self.save_cache()

    def toggle_route(self, device_id: int) -> bool:
        """Вмикає або зупиняє стрім для конкретного авто."""
        with self.lock:
            if device_id in self.routes:
                r = self.routes[device_id]
                r.enabled = not r.enabled
                if not r.enabled:
                    r.status = "PAUSED"
                elif self.is_running:
                    r.last_seen_id = None
                    r.status = "STREAMING" if r.packets_count > 0 else "WAITING"
                res = r.enabled
            else:
                res = False
        self.save_cache()
        return res

    def get_routes(self) -> List[RouteItem]:
        """Повертає список копій поточних маршрутів."""
        with self.lock:
            return list(self.routes.values())

    def forward_single_point(self, route: RouteItem, pos: Dict[str, Any]) -> bool:
        """Пересилає точку конкретного авто на Dev."""
        try:
            dt = parse_ts(pos.get("deviceTime") or pos.get("fixTime") or pos.get("serverTime"))
            raw_ts = int(dt.timestamp()) if dt else int(time.time())
            offset = getattr(self, "tz_offset_sec", 0)
            ts = raw_ts + offset

            speed_kmh = round(float(pos.get("speed", 0.0) or 0.0), 1)
            lat = float(pos.get("latitude", 0.0) or 0.0)
            lon = float(pos.get("longitude", 0.0) or 0.0)
            coords_str = f"{lat:.6f}, {lon:.6f}"

            params: Dict[str, Any] = {
                "id": route.target_unique_id,
                "lat": lat,
                "lon": lon,
                "timestamp": ts,
                "speed": round(speed_kmh * 0.277778, 2),  # км/г -> м/с
                "bearing": int(pos.get("course", 0) or 0),
                "altitude": int(pos.get("altitude", 0) or 0),
            }

            attrs = parse_attributes(pos.get("attributes"))
            raw_items = []
            if attrs:
                for k, v in attrs.items():
                    if not isinstance(v, (dict, list)):
                        params[k] = v
                        raw_items.append(f"{k}={v}")

            if not raw_items:
                raw_params_str = f"lat={lat}, lon={lon}, speed={speed_kmh}"
            else:
                raw_params_str = ", ".join(raw_items)

            # ВІДПРАВКА: суворо на Dev
            resp = requests.get(self.dev_target_url, params=params, timeout=4)

            # Відображення часу суворо за київським часом (Europe/Kyiv)
            if dt:
                kyiv_dt = dt.astimezone(KYIV_TZ)
                display_time = kyiv_dt.strftime("%H:%M:%S")
            else:
                display_time = datetime.now(KYIV_TZ).strftime("%H:%M:%S")

            with self.lock:
                if resp.status_code == 200:
                    route.packets_count += 1
                    self.total_packets_forwarded += 1
                    route.last_time = display_time
                    route.speed_kmh = speed_kmh
                    route.coords_str = coords_str
                    route.status = "STREAMING"
                    route.last_error = ""
                    route.latest_params_raw = raw_params_str

                    log_entry = {
                        "device_id": route.device_id,
                        "Час": display_time,
                        "Авто (Prod)": route.source_name,
                        "ID (Dev)": route.target_unique_id,
                        "Координати": coords_str,
                        "Швидкість": f"{speed_kmh} км/г",
                        "Статус": "200 OK",
                        "Сирі параметри (Telemetry)": raw_params_str,
                    }
                    route.packet_log.appendleft(log_entry)
                    self.packet_log.appendleft(log_entry)
                    return True
                else:
                    route.errors_count += 1
                    self.total_errors += 1
                    route.last_error = f"Dev HTTP {resp.status_code}"
                    route.status = "ERROR"

                    log_entry = {
                        "device_id": route.device_id,
                        "Час": display_time,
                        "Авто (Prod)": route.source_name,
                        "ID (Dev)": route.target_unique_id,
                        "Координати": coords_str,
                        "Швидкість": f"{speed_kmh} км/г",
                        "Статус": f"Dev {resp.status_code}",
                        "Сирі параметри (Telemetry)": raw_params_str,
                    }
                    route.packet_log.appendleft(log_entry)
                    self.packet_log.appendleft(log_entry)
                    return False

        except Exception as e:
            with self.lock:
                route.errors_count += 1
                self.total_errors += 1
                route.last_error = str(e)[:30]
            return False

    def _worker_loop(self) -> None:
        """Головний цикл багатопотокової ретрансляції."""
        while self.is_running:
            with self.lock:
                current_routes = [r for r in self.routes.values() if r.enabled]

            if not current_routes:
                time.sleep(1)
                continue

            now_kyiv = datetime.now(KYIV_TZ)
            t_from = (now_kyiv - timedelta(minutes=30)).isoformat()
            t_to = (now_kyiv + timedelta(minutes=5)).isoformat()

            for r in current_routes:
                if not self.is_running:
                    break
                try:
                    # Запитуємо останні точки у зворотному порядку (найновіші перші)
                    res = self.client.positions_page(
                        did=r.device_id,
                        date_from=t_from,
                        date_to=t_to,
                        page=1,
                        per_page=15,
                        order="DESC",
                    )
                    items = res.get("items", [])
                    if not items:
                        continue

                    # Якщо це перший запуск для цього авто — пересилаємо найостаннішу точку
                    if r.last_seen_id is None:
                        latest = items[0]
                        r.last_seen_id = latest.get("id")
                        self.forward_single_point(r, latest)
                        continue

                    # Відбираємо всі нові точки, які ще не пересилались
                    candidates = []
                    for pos in items:
                        pos_id = pos.get("id")
                        if pos_id is not None and pos_id > r.last_seen_id:
                            candidates.append(pos)

                    # Сортуємо нові точки за порядком виникнення (ASC) для послідовної передачі
                    if candidates:
                        candidates.sort(key=lambda x: x.get("id", 0))
                        for idx, pos in enumerate(candidates):
                            if idx > 0:
                                time.sleep(0.05)
                            self.forward_single_point(r, pos)
                            r.last_seen_id = pos.get("id")

                except Exception as e:
                    r.last_error = str(e)[:30]

            # Пауза між колами опитування (1.5 секунди для реактивності)
            for _ in range(15):
                if not self.is_running:
                    break
                time.sleep(0.1)

        with self.lock:
            for r in self.routes.values():
                r.status = "IDLE"

    def start(self, client, dev_url: str, tz_offset_sec: int = 0) -> None:
        """Запуск ретрансляції всіх налаштованих авто."""
        self.client = client
        self.dev_target_url = dev_url.strip()
        self.tz_offset_sec = tz_offset_sec

        with self.lock:
            # При запуску активуємо всі зареєстровані авто та скидаємо якір на найсвіжішу точку
            for r in self.routes.values():
                r.enabled = True
                r.last_seen_id = None
                if r.status in ("IDLE", "PAUSED"):
                    r.status = "WAITING" if r.packets_count == 0 else "STREAMING"

        self.save_cache()

        if self.is_running:
            return

        self.is_running = True
        self.started_at = datetime.now(KYIV_TZ).strftime("%H:%M:%S")

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
