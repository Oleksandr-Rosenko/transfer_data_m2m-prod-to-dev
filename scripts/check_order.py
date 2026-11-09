#!/usr/bin/env python3
"""Перевірка порядку точок об'єкта на Prod (лише читання). Те саме, що вкладка «Діагностика порядку точок».

Приклад:
    M2M_PASSWORD='...' python scripts/check_order.py --email me@company.eu --device-id 7994 \\
        --from "2026-10-25 02:00" --to "2026-10-25 06:00" --csv order_7994.csv

Дати без зсуву вважаються київським часом. Пароль береться зі змінної M2M_PASSWORD або запитується.
Код виходу: 0 — порядок правильний, 1 — знайдено порушення, 2 — помилка.
"""

from __future__ import annotations

import argparse
import getpass
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from m2m_replay.client import M2MClient  # noqa: E402
from m2m_replay.errors import ApiError  # noqa: E402
from m2m_replay.order import export_rows, fmt_local, get_local_tz, rows_to_csv, scan_pages, segment_reason  # noqa: E402

TZ = get_local_tz()


def parse_when(value: str, end: bool = False) -> str:
    dt = datetime.fromisoformat(value.strip().replace(" ", "T"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=TZ)
    if end and dt.second == 0 and len(value.strip()) <= 16:
        dt = dt.replace(second=59)
    return dt.isoformat()


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--url", default="https://my.m2m.eu", help="домен Prod (за замовчуванням https://my.m2m.eu)")
    ap.add_argument("--email", required=True)
    ap.add_argument("--device-id", type=int, required=True, help="ID об'єкта на платформі")
    ap.add_argument("--from", dest="date_from", required=True, help='початок, напр. "2026-10-25 02:00"')
    ap.add_argument("--to", dest="date_to", required=True, help="кінець")
    ap.add_argument("--csv", help="зберегти проблемні відрізки в CSV (у правильному порядку, ±2 год)")
    a = ap.parse_args(argv)

    password = os.environ.get("M2M_PASSWORD") or getpass.getpass("Пароль: ")
    client = M2MClient(a.url if "//" in a.url else f"https://{a.url}")
    try:
        client.login(a.email, password)
        date_from, date_to = parse_when(a.date_from), parse_when(a.date_to, end=True)
        rep = scan_pages(client.iter_positions(a.device_id, date_from, date_to), tz=TZ)
    except (ApiError, ValueError) as e:
        print(f"Помилка: {e}", file=sys.stderr)
        return 2

    print(f"Об'єкт {a.device_id}: {date_from[:16]} → {date_to[:16]}")
    print(f"Точок: {rep.total}; час іде назад: {rep.out_of_order}; фрагментів: {len(rep.segments)}; "
          f"координати 0,0: {rep.zero_coords}; інші стрибки координат: {rep.jumps_other}")
    for s in rep.segments:
        print(f"  {fmt_local(s.lo_ms, TZ, True)} → {fmt_local(s.hi_ms, TZ, True)}: порушень {s.events}, "
              f"макс. відкат {round(s.max_back_ms / 60000)} хв, стрибків {s.jumps}. {segment_reason(s)}")
    if a.csv and rep.segments:
        with open(a.csv, "wb") as f:
            f.write(rows_to_csv(export_rows(rep, TZ)))
        print(f"CSV збережено: {a.csv}")
    return 1 if rep.segments else 0


if __name__ == "__main__":
    sys.exit(main())
