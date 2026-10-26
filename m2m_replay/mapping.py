"""Відповідність параметрів платформи (поле `attributes` точки) і даних, які реально шле трекер.

Платформа показує в точці дві групи параметрів:
  1. «сирі» дані трекера: `io_<ID>` (невідомі платформі IO) та іменовані (`power`, `ignition`, ...),
     які платформа вже перетворила з IO (наприклад, power = IO 66 у мВ / 1000);
  2. службові, які платформа рахує сама (`pf_*`, `power_enb`, `power_filtered`, `m2m_*`, `distance`, ...).

Через протокол Teltonika можна передати лише першу групу, у вигляді IO-елементів. Службові параметри
dev-сервер порахує сам із тих самих вхідних даних. Wialon IPS приймає довільні параметри, тож там можна
передати і їх (за вибором).

Таблиця NAMED_TO_IO складена за документацією Teltonika (серія FMC/FMB). Її ЗВІРЕНО лише з даними з CSV
(значення сходяться: power 12.084 В ↔ 12084 мВ, rssi 0..5 тощо), але не з конфігурацією платформи.
Якщо на dev параметр з'являється під іншою назвою, виправте тут ID.
"""

from __future__ import annotations

import re

# назва параметра платформи -> (AVL IO ID, множник «значення платформи × множник = значення в IO»)
NAMED_TO_IO: dict[str, tuple[int, int]] = {
    "power": (66, 1000),     # External Voltage, мВ
    "battery": (67, 1000),   # Battery Voltage, мВ
    "rssi": (21, 1),         # GSM Signal, 0..5
    "ignition": (239, 1),    # Ignition
    "motion": (240, 1),      # Movement
    "odometer": (16, 1),     # Total Odometer, м
    "operator": (241, 1),    # Active GSM Operator
    "hdop": (182, 10),       # GNSS HDOP ×10
    "pdop": (181, 10),       # GNSS PDOP ×10
    "cellId": (205, 1),      # GSM Cell ID (використовується, лише якщо немає io_205)
    "lac": (206, 1),         # GSM Area Code (лише якщо немає io_206)
}

# Поля заголовка AVL-запису, а не IO
HEADER_KEYS = {"sat", "event", "priority"}

# Платформа збирає їх із IO 11/14 та 241 сама
SERVICE_FROM_IO = {"iccid", "mcc", "mnc"}

# Те, що платформа рахує сама з вхідних даних
PLATFORM_PREFIXES = ("pf_", "m2m_", "_")
PLATFORM_EXACT = {"distance", "totalDistance"}
PLATFORM_SUFFIXES = ("_enb", "_filtered")

# Режим «лише напруга»: IO 66
BASIC_IO = {66}

_IO_KEY = re.compile(r"^io_(\d+)$")
_BAD_NAME = re.compile(r"[:;,\s|]")


def is_platform_computed(key: str) -> bool:
    return key in PLATFORM_EXACT or key.startswith(PLATFORM_PREFIXES) or key.endswith(PLATFORM_SUFFIXES)


def _to_int(v, scale: int = 1):
    if isinstance(v, bool):
        return int(v)
    if isinstance(v, int):
        return v * scale
    if isinstance(v, float) and v == v and abs(v) != float("inf"):
        return int(round(v * scale))
    return None


def attrs_to_io(attrs: dict, basic_only: bool = False) -> tuple[dict[int, int], list[str]]:
    """Атрибути точки → ({IO ID: ціле значення}, [ключі, які не вдалось перетворити]).

    Службові параметри платформи тихо пропускаються (dev порахує їх сам) і в список невдач не потрапляють.
    """
    io: dict[int, int] = {}
    unknown: list[str] = []
    raw_ids = {int(m.group(1)) for k in attrs if (m := _IO_KEY.match(k))}
    for k, v in attrs.items():
        m = _IO_KEY.match(k)
        if m:
            iv = _to_int(v)
            if iv is None:
                unknown.append(k)
            else:
                io[int(m.group(1))] = iv
        elif k in NAMED_TO_IO:
            io_id, scale = NAMED_TO_IO[k]
            if io_id in raw_ids:
                continue
            iv = _to_int(v, scale)
            if iv is None:
                unknown.append(k)
            else:
                io.setdefault(io_id, iv)
        elif k in HEADER_KEYS or k in SERVICE_FROM_IO or is_platform_computed(k):
            continue
        else:
            unknown.append(k)
    if basic_only:
        io = {i: v for i, v in io.items() if i in BASIC_IO}
        unknown = []
    return io, unknown


def attrs_to_wialon_params(attrs: dict, include_platform: bool = False, basic_only: bool = False) -> list[tuple[str, int, str]]:
    """Атрибути → параметри Wialon IPS (ім'я, тип 1=ціле 2=дробове 3=рядок, значення). Імена лишаються як на проді."""
    out: list[tuple[str, int, str]] = []
    for k, v in attrs.items():
        if _BAD_NAME.search(k):
            continue
        if not include_platform and is_platform_computed(k):
            continue
        if basic_only and k not in ("power",):
            continue
        if v is None:
            continue
        if isinstance(v, bool):
            out.append((k, 1, "1" if v else "0"))
        elif isinstance(v, int):
            out.append((k, 1, str(v)))
        elif isinstance(v, float):
            if v != v or abs(v) == float("inf"):
                continue
            out.append((k, 2, format(v, ".10g")))
        else:
            s = str(v)
            if _BAD_NAME.search(s) or not s:
                s = _BAD_NAME.sub("_", s)
            if s:
                out.append((k, 3, s))
    return out
