"""Виправлення порядку точок на dev: видалити повідомлення об'єкта за інтервалом і відправити їх заново з Prod,
уже впорядкованими за UTC. Prod тільки читається. Видалення можливе лише через окремий dev-клієнт.

Гарантії (їх перевіряють тести):
* до першої зміни на dev перевіряється: домен dev ≠ Prod; `uniqueId` обраного об'єкта dev збігається з IMEI, на який
  підуть дані; dev приймає з'єднання; на Prod за інтервал є точки; усі точки dev, повернуті API, лежать в інтервалі;
  їхня кількість збігається з переглядом. Будь-яка розбіжність — нічого не змінюється;
* перед видаленням повні точки dev за інтервалом зберігаються в резервну копію (gzip, JSON-рядки);
* видаляються лише id цього об'єкта за цим інтервалом, зібрані наперед;
* після відправки перевіряється: усі точки прийняті, на dev рівно стільки ж точок, скільки на Prod, порядок
  правильний (немає «часу назад»). Якщо ні, а також при будь-якій помилці або зупинці, інтервал на dev очищається
  й відновлюється з резервної копії в тому ж порядку, в якому точки були.

Відновлення створює ті самі повідомлення (час, координати, параметри), але з новими id, а розрахункові поля
(пробіг тощо) платформа порахує заново.
"""

from __future__ import annotations

import gzip
import json
import os
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urlparse

from . import replay
from .client import M2MClient
from .models import position_time, position_to_point
from .order import DEFAULT_WINDOW_MS, OrderReport, get_local_tz, mileage_mismatch, scan_pages
from .replay import TransferConfig, TransferJob

MODES = {
    "hull": "Лише проблемні фрагменти (від першого до останнього, ±2 год)",
    "to_end": "Від першого проблемного фрагмента до кінця періоду (щоб пробіг далі порахувався заново)",
}


class FixError(Exception):
    pass


@dataclass(frozen=True)
class Interval:
    from_ms: int
    to_ms: int

    @staticmethod
    def _iso(ms: int) -> str:
        dt = datetime.fromtimestamp(ms / 1000, tz=timezone.utc)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.") + f"{dt.microsecond // 1000:03d}Z"

    @property
    def iso_from(self) -> str:
        return self._iso(self.from_ms)

    @property
    def iso_to(self) -> str:
        return self._iso(self.to_ms)


@dataclass
class FixPreview:
    interval: Interval
    dev_count: int      # скільки повідомлень на dev буде видалено
    prod_count: int     # скільки повідомлень з Prod буде відправлено


def host_of(url: str) -> str:
    return (urlparse(url if "//" in url else f"//{url}").hostname or "").lower()


def check_targets(prod: M2MClient, dev: M2MClient) -> None:
    """Видалення дозволене лише на іншому домені, ніж Prod, і лише для авторизованого dev-клієнта."""
    if not dev.token:
        raise FixError("Спершу увійдіть у dev (бічна панель)")
    if host_of(prod.base_url) == host_of(dev.base_url):
        raise FixError("Домен dev збігається з доменом Prod. Видалення дозволене лише на dev")


def plan_interval(rep: OrderReport, mode: str, period_end_ms: int | None = None,
                  margin_ms: int = DEFAULT_WINDOW_MS) -> Interval:
    if not rep.segments:
        raise FixError("Проблемних фрагментів немає, виправляти нічого")
    lo = min(s.lo_ms for s in rep.segments) - margin_ms
    hi_hull = max(s.hi_ms for s in rep.segments) + margin_ms
    if mode == "to_end":
        end = period_end_ms if period_end_ms else max(r[1] for r in rep.points)
        return Interval(lo, max(end, hi_hull))
    return Interval(lo, hi_hull)


def count_positions(client: M2MClient, device_id: int, iv: Interval) -> int:
    page = client.positions_page(device_id, iv.iso_from, iv.iso_to, 1, 1)
    return int(page.get("total") or len(page.get("items") or []))


def collect_ids(client: M2MClient, device_id: int, iv: Interval) -> list[int]:
    ids: list[int] = []
    for page in client.iter_positions(device_id, iv.iso_from, iv.iso_to):
        ids += [int(p["id"]) for p in page.get("items") or [] if p.get("id") is not None]
    return ids


def preview(prod: M2MClient, prod_id: int, dev: M2MClient, dev_id: int, iv: Interval) -> FixPreview:
    check_targets(prod, dev)
    return FixPreview(iv, count_positions(dev, dev_id, iv), count_positions(prod, prod_id, iv))


def snapshot(client: M2MClient, device_id: int, iv: Interval, path: str) -> list[int]:
    """Резервна копія: повні точки об'єкта за інтервалом у gzip (перший рядок — метадані). Повертає id точок.

    Перевіряє, що кожна точка має id та час, лежить в інтервалі (API міг проігнорувати фільтр) і що id унікальні.
    """
    ids: list[int] = []
    seen: set[int] = set()
    lo, hi = iv.from_ms - 1000, iv.to_ms + 1000
    with gzip.open(path, "wt", encoding="utf-8") as f:
        f.write(json.dumps({"meta": {"device_id": device_id, "from": iv.iso_from, "to": iv.iso_to,
                                     "created": datetime.now(timezone.utc).isoformat()}}) + "\n")
        for page in client.iter_positions(device_id, iv.iso_from, iv.iso_to):
            for p in page.get("items") or []:
                t = position_time(p)
                pid = p.get("id")
                if pid is None or t is None:
                    raise FixError("Точка без id або часу: безпечну резервну копію зробити неможливо")
                if not lo <= t.timestamp() * 1000 <= hi:
                    raise FixError("API dev повернуло точку поза вибраним інтервалом: нічого не видаляю")
                if int(pid) in seen:
                    raise FixError(f"Дублікат id {pid} у відповіді API dev: нічого не видаляю")
                seen.add(int(pid))
                ids.append(int(pid))
                f.write(json.dumps(p, ensure_ascii=False) + "\n")
    return ids


def restore_chunks(path: str, size: int = 2000):
    """Точки з резервної копії у тому порядку, в якому вони були (без впорядкування), шматками по `size`."""
    chunk: list = []
    with gzip.open(path, "rt", encoding="utf-8") as f:
        next(f)  # метадані
        for line in f:
            pt = position_to_point(json.loads(line))
            if pt is not None:
                chunk.append(pt)
            if len(chunk) >= size:
                yield chunk
                chunk = []
    if chunk:
        yield chunk


def sorted_chunks(path: str, size: int = 2000):
    """Точки з резервної копії, впорядковані за UTC-часом (стабільно), шматками по `size`."""
    pts = [pt for chunk in restore_chunks(path, size) for pt in chunk]
    pts.sort(key=lambda p: p.ts_ms)
    for i in range(0, len(pts), size):
        yield pts[i:i + size]


def preview_self(dev: M2MClient, dev_id: int, iv: Interval) -> FixPreview:
    """Виправлення на місці: скільки повідомлень буде видалено, стільки ж буде відправлено назад."""
    if not dev.token:
        raise FixError("Спершу увійдіть у систему")
    n = count_positions(dev, dev_id, iv)
    return FixPreview(iv, n, n)


def backup_dir() -> str:
    d = os.path.join(tempfile.gettempdir(), "m2m_fix_backups")
    os.makedirs(d, exist_ok=True)
    return d


class FixJob:
    """Фонове виправлення: перевірки → резервна копія → видалення → відправка → перевірка → (за потреби) відкат.

    Джерело правильних точок: Prod (`prod` задано) або сам цей об'єкт (`prod=None`): тоді точки беруться з резервної
    копії інтервалу й відправляються назад у порядку UTC-часу.
    """

    def __init__(self, prod: M2MClient | None, prod_id: int | None, dev: M2MClient, dev_id: int, iv: Interval,
                 send_cfg: dict,
                 expected_dev_count: int | None = None, source_name: str = "", strict_mileage: bool = False,
                 backup_path: str | None = None, verify_timeout: float = 60.0, verify_poll: float = 2.0):
        self.prod, self.prod_id, self.dev, self.dev_id, self.iv = prod, prod_id, dev, dev_id, iv
        self.send_cfg = dict(send_cfg)
        self.expected_dev_count = expected_dev_count
        self.source_name = source_name
        self.strict_mileage = strict_mileage
        self.verify_timeout, self.verify_poll = verify_timeout, verify_poll
        stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        self.backup_path = backup_path or os.path.join(backup_dir(), f"dev_{dev_id}_{stamp}.json.gz")
        self.status = "idle"        # idle | running | done | error | rolled_back | rollback_failed
        self.stage = ""
        self.message = ""
        self.log: list[str] = []
        self.backup_ids: list[int] = []
        self.deleted = 0
        self.prod_count = 0
        self.send_job: TransferJob | None = None
        self.restore_job: TransferJob | None = None
        self.verify_info: dict = {}
        self.unmapped: list[str] = []
        self.started_at: float | None = None
        self.finished_at: float | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # ---- керування ----
    @property
    def running(self) -> bool:
        return self.status == "running"

    @property
    def has_backup(self) -> bool:
        return bool(self.backup_ids) and os.path.exists(self.backup_path)

    def start(self) -> None:
        self.status, self.started_at = "running", time.time()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        """Зупинка до завершення відправки скасовує виправлення: інтервал буде повернуто як було."""
        self._stop.set()
        job = self.send_job
        if job is not None and job.running:
            job.stop()

    def _say(self, stage: str, text: str) -> None:
        self.stage, self.message = stage, text
        self.log.append(f"{datetime.now().strftime('%H:%M:%S')}  {text}")

    def _cfg(self, restore: bool = False) -> TransferConfig:
        if restore or self.prod is None:
            return TransferConfig(client=self.prod or self.dev, device_id=0, date_from="", date_to="", shift=False,
                                  sort_by_time=False, **self.send_cfg)
        return TransferConfig(client=self.prod, device_id=self.prod_id, date_from=self.iv.iso_from,
                              date_to=self.iv.iso_to, shift=False, sort_by_time=True,
                              source_name=self.source_name, **self.send_cfg)

    # ---- основний хід ----
    def _run(self) -> None:
        changed = False
        try:
            self._preflight()
            changed = True
            self._delete()
            self._send()
            self._verify()
            self.status = "done"
            self._say("done", "Порядок виправлено, результат перевірено")
        except Exception as e:  # noqa: BLE001 — будь-яка помилка має дійти до користувача, а не загубитись у потоці
            reason = str(e) if isinstance(e, FixError) else f"{type(e).__name__}: {e}"
            if not changed:
                self.status = "error"
                self._say("error", f"Нічого не змінено на dev: {reason}")
            else:
                self._rollback(reason)
        finally:
            self.finished_at = time.time()

    def _preflight(self) -> None:
        """Усе, що можна перевірити, не змінюючи dev."""
        self._say("check", "Перевіряю цілі")
        if self.prod is not None:
            check_targets(self.prod, self.dev)
        elif not self.dev.token:
            raise FixError("Спершу увійдіть у систему")
        imei = str(self.send_cfg.get("imei") or "")
        if not imei:
            raise FixError("Не вказано IMEI/ID об'єкта на dev")
        uid = str(self.dev.get_device(self.dev_id).get("uniqueId") or "")
        if uid != imei:
            raise FixError(f"Об'єкт dev ID {self.dev_id} має uniqueId «{uid}», а відправка йде на «{imei}»: "
                           "видалення й вставка зачепили б різні об'єкти")
        self._say("check", "Перевіряю з'єднання з dev-сервером")
        replay.check_connection(self._cfg())
        if self.prod is not None:
            self.prod_count = count_positions(self.prod, self.prod_id, self.iv)
            if self.prod_count == 0:
                raise FixError("На Prod за цей інтервал немає повідомлень: видаляти нічого")
        self._say("backup", "Роблю резервну копію повідомлень dev за інтервалом")
        self.backup_ids = snapshot(self.dev, self.dev_id, self.iv, self.backup_path)
        if self.expected_dev_count is not None and len(self.backup_ids) != self.expected_dev_count:
            raise FixError(f"Кількість повідомлень на dev змінилась ({self.expected_dev_count} → "
                           f"{len(self.backup_ids)}) після перегляду. Оновіть перегляд")
        if self.prod is None:
            self.prod_count = len(self.backup_ids)
            if self.prod_count == 0:
                raise FixError("У цьому інтервалі немає повідомлень: видаляти нічого")
        self._say("backup", f"Резервна копія: {len(self.backup_ids)} повідомлень → {self.backup_path}")

    def _wait_count(self, expected: int) -> int:
        t0 = time.time()
        while True:
            n = count_positions(self.dev, self.dev_id, self.iv)
            if n == expected or time.time() - t0 > self.verify_timeout:
                return n
            time.sleep(self.verify_poll)

    def _delete(self) -> None:
        self._say("delete", f"Видаляю на dev {len(self.backup_ids)} повідомлень об'єкта {self.dev_id}")
        self.deleted = self.dev.delete_positions(self.dev_id, self.backup_ids) if self.backup_ids else 0
        left = self._wait_count(0)
        if left:
            raise FixError(f"Після видалення на dev лишилось {left} повідомлень")

    def _wait_job(self, job: TransferJob) -> None:
        while job.running:
            time.sleep(0.1)
        if job._thread is not None:
            job._thread.join(10)

    def _send(self) -> None:
        job = TransferJob(self._cfg())
        self.send_job = job
        if self.prod is None:
            self._say("send", f"Відправляю {self.prod_count} повідомлень у порядку UTC-часу")
            job.start_stream(sorted_chunks(self.backup_path), self.prod_count)
        else:
            self._say("send", f"Відправляю {self.prod_count} повідомлень з Prod у правильному порядку")
            job.start()
        self._wait_job(job)
        if self._stop.is_set():
            raise FixError("Зупинено користувачем")
        self.unmapped = sorted(job.unmapped)
        if job.status != "done" or job.failed or job.ok != self.prod_count:
            raise FixError(f"Відправка не завершена: прийнято {job.ok} із {self.prod_count}, помилок {job.failed}. "
                           f"{job.message}".strip())

    def _verify(self) -> None:
        self._say("verify", "Перевіряю результат на dev")
        n = self._wait_count(self.prod_count)
        if n != self.prod_count:
            raise FixError(f"На dev {n} повідомлень за інтервал, а має бути {self.prod_count}")
        rep = scan_pages(self.dev.iter_positions(self.dev_id, self.iv.iso_from, self.iv.iso_to), get_local_tz())
        self.verify_info = {
            "count": n, "segments": len(rep.segments), "attr_km": rep.attr_km, "path_km": rep.path_km,
            "td_decreases": rep.td_decreases, "mileage_mismatch": mileage_mismatch(rep),
        }
        if rep.segments:
            raise FixError(f"Після виправлення порядок усе ще порушений ({len(rep.segments)} фрагм.)")
        if self.strict_mileage and self.verify_info["mileage_mismatch"]:
            raise FixError("Порядок виправлено, але пробіг за totalDistance не збігається з довжиною треку")

    # ---- відкат ----
    def _rollback(self, reason: str) -> None:
        self._say("rollback", f"Помилка: {reason}. Повертаю як було")
        try:
            current = collect_ids(self.dev, self.dev_id, self.iv)
            if set(current) == set(self.backup_ids):
                self.status = "rolled_back"
                self._say("rollback", f"Скасовано: {reason}. На dev нічого не змінилось")
                return
            if current:
                self.dev.delete_positions(self.dev_id, current)
            if self._wait_count(0):
                raise FixError("не вдалося очистити інтервал перед відновленням")
            job = TransferJob(self._cfg(restore=True))
            self.restore_job = job
            job.start_stream(restore_chunks(self.backup_path), len(self.backup_ids))
            self._wait_job(job)
            n = self._wait_count(len(self.backup_ids))
            if job.status != "done" or job.failed or n != len(self.backup_ids):
                raise FixError(f"відновлено {job.ok} із {len(self.backup_ids)}, на dev зараз {n}")
            self.status = "rolled_back"
            self._say("rollback", f"Скасовано: {reason}. Відновлено {n} повідомлень як було")
        except Exception as e:  # noqa: BLE001
            self.status = "rollback_failed"
            self._say("rollback", f"УВАГА: відновлення не вдалось ({e}). Резервна копія: {self.backup_path}")
