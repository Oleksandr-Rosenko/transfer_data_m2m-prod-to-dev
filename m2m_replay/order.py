"""Порядок точок: діагностика й виправлення.

Типова проблема: у ніч переходу на зимовий час година 03:00–03:59 за київським часом настає двічі. Якщо платформа
віддає точки впорядкованими за локальним часом, точки двох різних годин (різниця рівно 1 година в UTC) перемішуються,
і трек «зигзагом» стрибає між двома місцями.

Тут два інструменти, обидва працюють з UTC-часом точок (`position_time`), який API віддає однозначно:

* `TimeSorter` — впорядкування потоку точок за UTC із обмеженим буфером (для переносу на dev);
* `scan_pages` — діагностика: де порядок порушено, чи це схоже на перехід годин, скільки «стрибків» координат.
"""

from __future__ import annotations

import csv
import heapq
import io
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Iterable
from zoneinfo import ZoneInfo

from .models import Point, position_time

HOUR_MS = 3_600_000
DEFAULT_WINDOW_MS = 2 * HOUR_MS     # перемішування при переході годин — до 1 год; беремо із запасом
SEGMENT_GAP_MS = 3 * HOUR_MS        # порушення ближче за це — один фрагмент
JUMP_KM = 5.0                       # «стрибок» координат між сусідніми точками
JUMP_KMH = 250.0                    # …якщо при цьому швидкість між ними нереальна (або час іде назад)


def get_local_tz() -> ZoneInfo:
    try:
        return ZoneInfo("Europe/Kyiv")
    except Exception:  # noqa: BLE001 — старіша база часових зон
        return ZoneInfo("Europe/Kiev")


# ---------------------------------------------------------------------------
# Впорядкування потоку для переносу
# ---------------------------------------------------------------------------
class TimeSorter:
    """Стабільне впорядкування за `ts_ms` із буфером `window_ms`.

    Точка віддається, коли в потоці вже з'явилась точка, новіша за неї більше ніж на `window_ms`. Тож пам'ять
    обмежена вікном (кілька тисяч точок), а не всім періодом. Порушення, більші за вікно, не виправляються,
    а лише рахуються в `late`.
    """

    def __init__(self, window_ms: int = DEFAULT_WINDOW_MS):
        self.window_ms = int(window_ms)
        self.reordered = 0      # скільки точок вийшли пізніше за точки, що в потоці йшли після них
        self.late = 0           # точок, які прийшли запізно навіть для вікна (порядок лишився порушеним)
        self._heap: list[tuple[int, int, Point]] = []
        self._seq = 0
        self._newest: int | None = None
        self._last_out: int | None = None
        self._max_seq_out = -1

    def push(self, pts: Iterable[Point]) -> list[Point]:
        for p in pts:
            if self._last_out is not None and p.ts_ms < self._last_out:
                self.late += 1
            heapq.heappush(self._heap, (p.ts_ms, self._seq, p))
            self._seq += 1
            if self._newest is None or p.ts_ms > self._newest:
                self._newest = p.ts_ms
        return self._drain(lambda ts: ts < (self._newest or 0) - self.window_ms)

    def flush(self) -> list[Point]:
        return self._drain(lambda ts: True)

    def _drain(self, ready: Callable[[int], bool]) -> list[Point]:
        out: list[Point] = []
        while self._heap and ready(self._heap[0][0]):
            ts, seq, p = heapq.heappop(self._heap)
            if seq < self._max_seq_out:
                self.reordered += 1
            else:
                self._max_seq_out = seq
            self._last_out = ts if self._last_out is None else max(self._last_out, ts)
            out.append(p)
        return out


# ---------------------------------------------------------------------------
# Діагностика
# ---------------------------------------------------------------------------
def is_ambiguous(ts_ms: int, tz: ZoneInfo) -> bool:
    """Чи припадає момент на годину, що повторюється (локальний час 03:xx в ніч переходу на зимовий)."""
    local = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(tz)
    return local.replace(fold=0).utcoffset() != local.replace(fold=1).utcoffset()


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return 12742.0 * math.asin(math.sqrt(a))


@dataclass
class OrderSegment:
    """Фрагмент, де порядок порушено: від найранішої до найпізнішої точки, задіяних у порушеннях."""

    lo_ms: int
    hi_ms: int
    events: int = 0              # скільки разів час «йде назад»
    max_back_ms: int = 0         # найбільший відкат часу між сусідніми точками
    jumps: int = 0               # стрибків координат у цьому ж місці
    dst_fold: bool = False       # порушення припадають на годину, що повторюється
    last_event_ms: int = 0


@dataclass
class OrderReport:
    total: int = 0
    no_time: int = 0
    zero_coords: int = 0
    out_of_order: int = 0
    jumps_other: int = 0                         # стрибки координат, не пов'язані з порядком часу
    jump_samples: list[tuple[int, float]] = field(default_factory=list)   # (ts_ms, км), перші 30
    segments: list[OrderSegment] = field(default_factory=list)
    # (№ у відповіді API, ts_ms, lat, lon, швидкість) — для експорту
    points: list[tuple[int, int, float | None, float | None, float | None]] = field(default_factory=list)


def _f(x) -> float | None:
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    return v if v == v else None


def scan_pages(pages: Iterable[dict], tz: ZoneInfo | None = None,
               on_progress: Callable[[int], None] | None = None) -> OrderReport:
    """Проходить сторінки точок у порядку відповіді API й шукає місця, де UTC-час іде назад."""
    tz = tz or get_local_tz()
    rep = OrderReport()
    prev_ts: int | None = None
    prev_ll: tuple[float, float] | None = None
    idx = 0
    for page in pages:
        for p in page.get("items") or []:
            idx += 1
            t = position_time(p)
            if t is None:
                rep.no_time += 1
                continue
            ts = int(round(t.timestamp() * 1000))
            lat, lon = _f(p.get("latitude")), _f(p.get("longitude"))
            rep.points.append((idx, ts, lat, lon, _f(p.get("speed"))))
            has_ll = lat is not None and lon is not None
            if has_ll and lat == 0 and lon == 0:
                rep.zero_coords += 1
                has_ll = False
            back = prev_ts is not None and ts < prev_ts
            jump_km = 0.0
            if has_ll and prev_ll is not None and prev_ts is not None:
                jump_km = haversine_km(prev_ll[0], prev_ll[1], lat, lon)
            is_jump = False
            if jump_km > JUMP_KM and prev_ts is not None:
                dt_h = abs(ts - prev_ts) / HOUR_MS
                is_jump = back or dt_h == 0 or jump_km / dt_h > JUMP_KMH
            if back:
                rep.out_of_order += 1
                _add_event(rep, prev_ts, ts, is_jump, tz)
            elif is_jump:
                rep.jumps_other += 1
                if len(rep.jump_samples) < 30:
                    rep.jump_samples.append((ts, jump_km))
            prev_ts = ts
            if has_ll:
                prev_ll = (lat, lon)
        if on_progress:
            on_progress(idx)
    rep.total = idx
    return rep


def _add_event(rep: OrderReport, prev_ts: int, ts: int, is_jump: bool, tz: ZoneInfo) -> None:
    seg = rep.segments[-1] if rep.segments else None
    if seg is None or ts - seg.last_event_ms > SEGMENT_GAP_MS:
        seg = OrderSegment(lo_ms=ts, hi_ms=prev_ts)
        rep.segments.append(seg)
    seg.events += 1
    seg.lo_ms = min(seg.lo_ms, ts)
    seg.hi_ms = max(seg.hi_ms, prev_ts)
    seg.max_back_ms = max(seg.max_back_ms, prev_ts - ts)
    seg.last_event_ms = max(seg.last_event_ms, ts)
    if is_jump:
        seg.jumps += 1
    if not seg.dst_fold and (is_ambiguous(ts, tz) or is_ambiguous(prev_ts, tz)):
        seg.dst_fold = True


def segment_reason(seg: OrderSegment) -> str:
    if seg.dst_fold and abs(seg.max_back_ms - HOUR_MS) <= 10 * 60_000:
        return "Перехід на зимовий час: година повторюється, точки двох годин перемішані"
    if seg.dst_fold:
        return "Збіг із годиною, що повторюється, але відкат не дорівнює 1 год. Перевірте вручну"
    return "Не перехід годин (буферизовані точки, збій часу трекера?). Перевірте вручну"


def fmt_local(ts_ms: int, tz: ZoneInfo, with_offset: bool = False) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(tz)
    return dt.strftime("%d.%m.%Y %H:%M:%S") + (f" ({dt.strftime('%z')[:3]})" if with_offset else "")


def export_rows(rep: OrderReport, tz: ZoneInfo | None = None, margin_ms: int = DEFAULT_WINDOW_MS) -> list[dict]:
    """Точки проблемних фрагментів (± запас) у правильному порядку; `№ у API` показує, де вони стояли в API."""
    tz = tz or get_local_tz()
    if not rep.segments:
        return []
    order = sorted(rep.points, key=lambda r: (r[1], r[0]))
    rank = {r[0]: n for n, r in enumerate(order, 1)}
    spans = [(s.lo_ms - margin_ms, s.hi_ms + margin_ms) for s in rep.segments]
    rows = []
    for idx, ts, lat, lon, spd in order:
        if any(a <= ts <= b for a, b in spans):
            rows.append({
                "№ у правильному порядку": rank[idx],
                "№ у API": idx,
                "Час UTC": datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z",
                "Час (Київ)": fmt_local(ts, tz, with_offset=True),
                "Широта": lat, "Довгота": lon, "Швидкість": spd,
            })
    return rows


def rows_to_csv(rows: list[dict]) -> bytes:
    if not rows:
        return b""
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=list(rows[0]))
    w.writeheader()
    w.writerows(rows)
    return buf.getvalue().encode("utf-8-sig")   # BOM: Excel коректно відкриває кирилицю
