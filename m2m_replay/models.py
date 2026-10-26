from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone

from dateutil import parser as dtparser


@dataclass
class Point:
    """Одне повідомлення трекера, готове до відправки (час уже зсунутий, якщо потрібно)."""

    ts_ms: int                 # UTC, мілісекунди
    lat: float
    lon: float
    alt: int = 0
    course: int = 0
    sats: int = 0
    speed: int = 0
    priority: int = 0
    event_id: int = 0
    attrs: dict = field(default_factory=dict)


def parse_ts(value) -> datetime | None:
    """ISO-рядок з API → datetime у UTC. Рядок без часової зони вважаємо UTC."""
    if not value:
        return None
    try:
        dt = dtparser.isoparse(str(value))
    except (ValueError, OverflowError):
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def parse_attributes(raw) -> dict:
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str) and raw.strip():
        try:
            d = json.loads(raw)
        except ValueError:
            return {}
        return d if isinstance(d, dict) else {}
    return {}


def _num(x, default=0.0) -> float:
    try:
        f = float(x)
    except (TypeError, ValueError):
        return default
    return f if f == f else default  # відсікаємо NaN


def _int(x, default=0) -> int:
    return int(round(_num(x, default)))


def position_time(p: dict) -> datetime | None:
    return parse_ts(p.get("deviceTime") or p.get("fixTime") or p.get("serverTime"))


def position_to_point(p: dict, delta_ms: int = 0) -> Point | None:
    """Точка з відповіді API → Point. None, якщо в точці немає часу.

    Координати беремо «сирі», від трекера: pf_orig_lat/lon (платформа підміняє їх на LBS/фільтр),
    а якщо їх немає — latitude/longitude точки.
    """
    ts = position_time(p)
    if ts is None:
        return None
    attrs = parse_attributes(p.get("attributes"))
    lat = _num(attrs.get("pf_orig_lat", p.get("latitude")))
    lon = _num(attrs.get("pf_orig_lon", p.get("longitude")))
    sats = attrs.get("sat") if attrs.get("sat") is not None else p.get("satellites")
    return Point(
        ts_ms=int(round(ts.timestamp() * 1000)) + int(delta_ms),
        lat=lat,
        lon=lon,
        alt=_int(p.get("altitude")),
        course=_int(p.get("course")),
        sats=_int(sats),
        speed=_int(p.get("speed")),
        priority=_int(attrs.get("priority")),
        event_id=_int(attrs.get("event")),
        attrs=attrs,
    )
