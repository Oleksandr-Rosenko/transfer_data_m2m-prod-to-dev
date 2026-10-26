"""Клієнт API платформи M2M (Prod): вхід, пошук об'єктів, точки за період. Потокобезпечний."""

from __future__ import annotations

import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor

import requests

from .errors import ApiError

PER_PAGE = 200  # як в аналізаторі генераторів; більше значення сервер може відхиляти


class M2MClient:
    """Окрема requests.Session на потік, автооновлення токена при 401."""

    def __init__(self, base_url: str):
        self.base_url = base_url.strip().rstrip("/")
        self.token: str | None = None
        self.refresh_token: str | None = None
        self.email: str | None = None
        self._password: str | None = None  # лише в пам'яті сесії — для повторного входу, ніде не зберігається
        self._lock = threading.Lock()
        self._local = threading.local()

    def _session(self) -> requests.Session:
        s = getattr(self._local, "s", None)
        if s is None:
            s = requests.Session()
            self._local.s = s
        return s

    def _send(self, method, path, params=None, json_data=None, token=None):
        headers = {"Accept": "application/json", "Cache-Control": "no-cache"}
        if token:
            headers["Authorization"] = f"Bearer {token}"
        return self._session().request(
            method, f"{self.base_url}{path}", headers=headers, params=params, json=json_data, timeout=90
        )

    def login(self, email: str, password: str) -> None:
        r = self._send("POST", "/api/login", json_data={"email": email, "password": password})
        if r.status_code >= 400:
            raise ApiError(f"Вхід не вдався: HTTP {r.status_code} {r.text[:200]}", r.status_code)
        d = r.json() if r.content else {}
        self.token = d.get("token") or d.get("accessToken")
        self.refresh_token = d.get("refreshToken")
        self.email, self._password = email, password
        if not self.token:
            raise ApiError("Сервер не повернув токен")

    def _refresh(self, used_token) -> bool:
        with self._lock:
            if self.token != used_token:
                return True  # інший потік уже оновив
            attempts = []
            if self.refresh_token:
                attempts.append(("/api/token/refresh", {"refreshToken": self.refresh_token}))
            if self.email and self._password:
                attempts.append(("/api/login", {"email": self.email, "password": self._password}))
            for path, body in attempts:
                try:
                    r = self._send("POST", path, json_data=body)
                    if r.status_code < 400 and r.content:
                        d = r.json()
                        t = d.get("token") or d.get("accessToken")
                        if t:
                            self.token = t
                            self.refresh_token = d.get("refreshToken") or self.refresh_token
                            return True
                except (requests.RequestException, ValueError):
                    pass
            return False

    def request(self, method, path, params=None, json_data=None, retries=3):
        last_exc = None
        for attempt in range(retries + 1):
            tok = self.token
            try:
                r = self._send(method, path, params=params, json_data=json_data, token=tok)
            except requests.RequestException as e:
                last_exc = e
                if attempt < retries:
                    time.sleep(1.5 * (attempt + 1))
                    continue
                raise ApiError(f"Помилка з'єднання: {e}") from e
            if r.status_code == 401 and attempt < retries and self._refresh(tok):
                continue
            if r.status_code in (429, 500, 502, 503, 504) and attempt < retries:
                time.sleep(1.5 * (attempt + 1))
                continue
            if r.status_code >= 400:
                raise ApiError(f"HTTP {r.status_code} {method} {path}: {r.text[:300]}", r.status_code)
            if not r.content:
                return {}
            try:
                return r.json()
            except ValueError:
                return {}
        raise ApiError(f"Не вдалося виконати запит: {last_exc}")

    # ---------------- Об'єкти ----------------
    def search_devices(self, query: str = "", limit: int = 50) -> list[dict]:
        """Об'єкти за назвою або IMEI (uniqueId): [{id, name}]. Порожній запит — перші `limit` за назвою."""
        query = (query or "").strip()
        base = {"page": 1, "per_page": limit, "order": "ASC", "orderBy": "DEVICE_NAME"}
        modes = ["DEVICE_NAME", "UNIQUE_ID"] if query else [None]
        found: dict[int, dict] = {}
        for mode in modes:
            params = dict(base)
            if mode:
                params.update({"search": query, "searchBy": mode})
            d = self.request("GET", "/api/devices/lightweight", params=params)
            items = d.get("items") if isinstance(d, dict) else d
            for it in items or []:
                if it.get("id") is not None and it["id"] not in found:
                    found[it["id"]] = {"id": it["id"], "name": it.get("name") or str(it["id"])}
        return list(found.values())[:limit]

    def get_device(self, did: int) -> dict:
        return self.request("GET", f"/api/device/{did}")

    # ---------------- Точки ----------------
    def positions_page(self, did: int, date_from: str, date_to: str, page: int = 1,
                       per_page: int = PER_PAGE, order: str = "ASC") -> dict:
        """Одна сторінка точок, нормалізована до {items, pages, total}."""
        d = self.request(
            "GET",
            f"/api/device/{did}/positions/interval",
            params={"dateFrom": date_from, "dateTo": date_to, "page": page, "per_page": per_page, "order": order},
        )
        if isinstance(d, list):
            return {"items": d, "pages": 0, "total": len(d)}
        items = d.get("items") or d.get("content") or d.get("data") or []
        return {"items": items, "pages": int(d.get("pages") or 0), "total": d.get("total"), "page": d.get("page")}

    def iter_positions(self, did: int, date_from: str, date_to: str, per_page: int = PER_PAGE,
                       first_page: dict | None = None, workers: int = 4, should_stop=None):
        """Сторінки по порядку (ASC). Наступні сторінки вантажаться паралельно наперед."""
        first = first_page if first_page is not None else self.positions_page(did, date_from, date_to, 1, per_page)
        yield first
        pages = int(first.get("pages") or 0)
        if not pages:  # сервер не віддав кількість сторінок — йдемо, поки сторінки повні
            page, last = 2, first
            while len(last.get("items") or []) >= per_page:
                if should_stop and should_stop():
                    return
                last = self.positions_page(did, date_from, date_to, page, per_page)
                if not last.get("items"):
                    return
                yield last
                page += 1
            return
        ex = ThreadPoolExecutor(max_workers=workers)
        window: deque = deque()
        nxt = 2
        try:
            while nxt <= pages or window:
                while nxt <= pages and len(window) < workers * 2:
                    window.append(ex.submit(self.positions_page, did, date_from, date_to, nxt, per_page))
                    nxt += 1
                if should_stop and should_stop():
                    return
                yield window.popleft().result()
        finally:
            ex.shutdown(wait=False, cancel_futures=True)
