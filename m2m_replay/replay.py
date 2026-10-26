"""Перенос: завантаження точок з Prod, зсув часу, відправка на Dev пачками, прогрес. Працює у фоновому потоці."""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from .client import PER_PAGE, M2MClient
from .errors import FatalTransferError, SendError
from .models import Point, parse_attributes, parse_ts, position_time, position_to_point
from .teltonika import CODEC_8, CODEC_8E, MAX_RECORDS_PER_PACKET, TeltonikaSender
from .wialon import WialonSender

PROTOCOLS = {
    "teltonika8e": "Teltonika Codec 8E",
    "teltonika8": "Teltonika Codec 8",
    "wialon": "Wialon IPS",
}
MAX_ERRORS_KEPT = 1000


@dataclass
class TransferConfig:
    client: M2MClient
    device_id: int
    date_from: str                    # ISO із зсувом часової зони, як у запитах інтерфейсу платформи
    date_to: str
    protocol: str                     # ключ з PROTOCOLS
    host: str
    port: int
    imei: str
    shift: bool = True                # зсунути час: кінець періоду = зараз мінус shift_minutes
    shift_minutes: int = 30
    batch_size: int = 100
    all_params: bool = True           # False: лише напруга (IO 66)
    include_platform: bool = False    # лише Wialon: передати й службові параметри платформи
    wialon_version: str = "2.0"
    limit: int | None = None          # пробний запуск: лише перші N повідомлень
    source_name: str = ""


def make_sender(cfg: TransferConfig):
    basic = not cfg.all_params
    if cfg.protocol == "wialon":
        return WialonSender(cfg.host, cfg.port, cfg.imei, version=cfg.wialon_version,
                            include_platform=cfg.include_platform, basic_only=basic)
    codec = CODEC_8 if cfg.protocol == "teltonika8" else CODEC_8E
    return TeltonikaSender(cfg.host, cfg.port, cfg.imei, codec=codec, basic_only=basic)


def check_connection(cfg: TransferConfig) -> str:
    """Перевірка: з'єднання + handshake за IMEI/логіном. Дані не надсилаються."""
    sender = make_sender(cfg)
    try:
        sender.connect()
    finally:
        sender.close()
    return f"{PROTOCOLS[cfg.protocol]}: {cfg.host}:{cfg.port} прийняв {cfg.imei}"


class TransferJob:
    """Фонове завдання. Усі поля для інтерфейсу читаються з будь-якого потоку."""

    def __init__(self, cfg: TransferConfig):
        self.cfg = cfg
        self.status = "idle"            # idle | running | done | stopped | error
        self.message = ""               # поточний етап (поки виконується) або причина помилки
        self.note = ""                  # зауваження до завершеного переносу (порожній період, неповна обробка)
        self.total = 0
        self.ok = 0
        self.failed_points: list[Point] = []
        self.errors: list[dict] = []
        self.started_at: float | None = None
        self.finished_at: float | None = None
        self.delta_ms = 0
        self.first_ts_ms: int | None = None   # діапазон часу на dev (після зсуву)
        self.last_ts_ms: int | None = None
        self.unmapped: set[str] = set()
        self.skipped_ids: set[int] = set()
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._batch_no = 0
        self._conn_fail = 0
        self._reject_streak = 0
        self._sender = None

    # ---- стан для інтерфейсу ----
    @property
    def processed(self) -> int:
        return self.ok + len(self.failed_points)

    @property
    def failed(self) -> int:
        return len(self.failed_points)

    @property
    def percent(self) -> float:
        return 100.0 * self.processed / self.total if self.total else 0.0

    @property
    def elapsed(self) -> float:
        if self.started_at is None:
            return 0.0
        return (self.finished_at or time.time()) - self.started_at

    @property
    def speed(self) -> float:
        e = self.elapsed
        return self.processed / e if e > 0.5 else 0.0

    @property
    def eta(self) -> float | None:
        s = self.speed
        return (self.total - self.processed) / s if s > 0 and self.status == "running" else None

    @property
    def running(self) -> bool:
        return self.status == "running"

    # ---- керування ----
    def start(self) -> None:
        self.status = "running"
        self.started_at = time.time()
        self._thread = threading.Thread(target=self._guard, args=(self._run_full,), daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()

    def retry_failed(self) -> None:
        if self.running or not self.failed_points:
            return
        pts, self.failed_points, self.errors = self.failed_points, [], []
        self._stop.clear()
        self._conn_fail = self._reject_streak = 0
        self.status = "running"
        self.note = ""
        self.message = "Повторна відправка невдалих повідомлень"
        self.started_at = time.time() - self.elapsed
        self.finished_at = None
        self._thread = threading.Thread(target=self._guard, args=(lambda: self._run_points(pts),), daemon=True)
        self._thread.start()

    # ---- виконання ----
    def _guard(self, fn) -> None:
        try:
            fn()
        except FatalTransferError as e:
            self.status, self.message = "error", str(e)
        except Exception as e:  # noqa: BLE001 — будь-яка помилка має дійти до користувача, а не загубитись у потоці
            self.status, self.message = "error", f"{type(e).__name__}: {e}"
        finally:
            self.finished_at = time.time()
            if self.status == "running":
                self.status = "stopped" if self._stop.is_set() else "done"
            self._collect_sender_info()

    def _collect_sender_info(self) -> None:
        s = getattr(self, "_sender", None)
        if s is not None:
            self.unmapped |= s.unmapped
            self.skipped_ids |= s.skipped_ids
            s.close()

    def _run_full(self) -> None:
        cfg = self.cfg
        c = cfg.client
        per_page = min(PER_PAGE, cfg.limit) if cfg.limit else PER_PAGE
        self.message = "Читаю повідомлення з Prod"
        first = c.positions_page(cfg.device_id, cfg.date_from, cfg.date_to, 1, per_page)
        total = int(first.get("total") or len(first.get("items") or []))
        if cfg.limit:
            total = min(total, cfg.limit)
        self.total = total
        if total == 0:
            self.note = "У вибраному періоді повідомлень немає."
            return
        if cfg.shift:
            last = c.positions_page(cfg.device_id, cfg.date_from, cfg.date_to, 1, 1, "DESC")["items"]
            last_ts = position_time(last[0]) if last else None
            if last_ts is None:
                raise FatalTransferError("Не вдалося визначити час останнього повідомлення для зсуву")
            target_end = datetime.now(timezone.utc) - timedelta(minutes=cfg.shift_minutes)
            self.delta_ms = int((target_end - last_ts).total_seconds() * 1000)

        self._sender = make_sender(cfg)
        self._sender.connect()
        self.message = "Переношу повідомлення"
        remaining = cfg.limit
        for page in c.iter_positions(cfg.device_id, cfg.date_from, cfg.date_to, per_page, first_page=first,
                                     should_stop=self._stop.is_set):
            if self._stop.is_set():
                return
            items = page.get("items") or []
            if remaining is not None:
                items = items[:remaining]
                remaining -= len(items)
            pts = [pt for pt in (position_to_point(p, self.delta_ms) for p in items) if pt is not None]
            self._send_points(pts)
            if remaining is not None and remaining <= 0:
                break
        if self.processed < self.total and not self._stop.is_set():
            self.note = (f"Оброблено {self.processed} із {self.total}: решта повідомлень без часу, "
                         "або Prod віддав менше, ніж обіцяв.")

    def _run_points(self, pts: list[Point]) -> None:
        self._sender = make_sender(self.cfg)
        self._sender.connect()
        self._send_points(pts)

    def _send_points(self, pts: list[Point]) -> None:
        size = max(1, min(int(self.cfg.batch_size), MAX_RECORDS_PER_PACKET))
        for i in range(0, len(pts), size):
            if self._stop.is_set():
                return
            batch = pts[i:i + size]
            self._batch_no += 1
            before = self.ok
            self._send_batch(batch, self._batch_no, allow_split=self.ok > 0 or self._reject_streak < 2)
            if self.ok == before:
                self._reject_streak += 1
                if self.ok == 0 and self._reject_streak >= 3:
                    raise FatalTransferError(
                        "Сервер не приймає повідомлення: перші 3 пакети відхилено. "
                        + (self._sender.last_error or "Перевірте протокол, порт та IMEI.")
                    )
            else:
                self._reject_streak = 0
            self._track_range(batch)

    def _track_range(self, batch: list[Point]) -> None:
        lo, hi = batch[0].ts_ms, batch[-1].ts_ms
        self.first_ts_ms = lo if self.first_ts_ms is None else min(self.first_ts_ms, lo)
        self.last_ts_ms = hi if self.last_ts_ms is None else max(self.last_ts_ms, hi)

    def _send_batch(self, pts: list[Point], no: int, allow_split: bool, depth: int = 0) -> None:
        sender = self._sender
        try:
            res = self._try_send(pts)
        except SendError as e:
            self._fail(pts, no, str(e))
            self._conn_fail += 1
            if self._conn_fail >= 3:
                raise FatalTransferError(f"Сервер недоступний: {e}") from e
            return
        self._conn_fail = 0
        good = sum(1 for r in res if r)
        self.ok += good
        bad = [p for p, r in zip(pts, res) if not r]
        if not bad:
            return
        if len(bad) == 1 or depth >= 8 or not allow_split:
            self._fail(bad, no, sender.last_error or "сервер не підтвердив прийом")
            return
        mid = len(bad) // 2  # одна «погана» точка не має ховати решту: ділимо навпіл
        self._send_batch(bad[:mid], no, allow_split, depth + 1)
        self._send_batch(bad[mid:], no, allow_split, depth + 1)

    def _try_send(self, pts: list[Point]) -> list[bool]:
        err: SendError | None = None
        for attempt in range(3):
            try:
                if self._sender.sock is None:
                    self._sender.connect()
                return self._sender.send(pts)
            except SendError as e:
                err = e
                time.sleep(0.5 * (attempt + 1))
        assert err is not None
        raise err

    def _fail(self, pts: list[Point], no: int, error: str) -> None:
        self.failed_points.extend(pts)
        if len(self.errors) < MAX_ERRORS_KEPT:
            self.errors.append({
                "batch": no,
                "count": len(pts),
                "from_ms": pts[0].ts_ms,
                "to_ms": pts[-1].ts_ms,
                "error": error,
            })


# ---------------------------------------------------------------------------
# Попередній перегляд
# ---------------------------------------------------------------------------
def build_preview(client: M2MClient, device_id: int, date_from: str, date_to: str, sample_pages: int = 8) -> dict:
    """Кількість повідомлень, перше/останнє та вибірка напруги для графіка (кілька сторінок рівномірно по періоду)."""
    first = client.positions_page(device_id, date_from, date_to, 1, PER_PAGE)
    total = int(first.get("total") or len(first.get("items") or []))
    pages = int(first.get("pages") or (1 if total else 0))
    out = {"total": total, "first": None, "last": None, "series": []}
    if not total:
        return out
    items = list(first.get("items") or [])
    out["first"] = position_time(items[0]) if items else None
    last_items = client.positions_page(device_id, date_from, date_to, 1, 1, "DESC")["items"]
    out["last"] = position_time(last_items[0]) if last_items else None
    wanted = sorted({max(1, round(1 + i * (pages - 1) / max(sample_pages - 1, 1))) for i in range(sample_pages)}) if pages > 1 else [1]
    series = []
    for pg in wanted:
        page_items = items if pg == 1 else client.positions_page(device_id, date_from, date_to, pg, PER_PAGE)["items"]
        for p in page_items:
            attrs = parse_attributes(p.get("attributes"))
            v = attrs.get("power", attrs.get("battery"))
            t = position_time(p)
            if t is not None and isinstance(v, (int, float)) and not isinstance(v, bool):
                series.append((t, float(v)))
    out["series"] = series
    return out
