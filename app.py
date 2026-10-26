"""Streamlit-застосунок «Перенос повідомлень Prod → Dev»."""

from __future__ import annotations

import re
from datetime import datetime, time as dtime, timedelta, timezone
from urllib.parse import urlparse
from zoneinfo import ZoneInfo

import pandas as pd
import streamlit as st

from m2m_replay.client import M2MClient
from m2m_replay.errors import ApiError, SendError
from m2m_replay.replay import PROTOCOLS, TransferConfig, TransferJob, build_preview, check_connection

try:
    LOCAL_TZ = ZoneInfo("Europe/Kyiv")
except Exception:  # noqa: BLE001 — старіша база часових зон
    LOCAL_TZ = ZoneInfo("Europe/Kiev")

DEFAULT_PORTS = {"teltonika8e": "5027", "teltonika8": "5027", "wialon": "5039"}

st.set_page_config(page_title="Перенос повідомлень Prod → Dev", layout="centered")
S = st.session_state
S.setdefault("client", None)
S.setdefault("stage", "setup")      # setup | progress
S.setdefault("job", None)
S.setdefault("preview", None)
S.setdefault("search_cache", {})


# ---------------------------------------------------------------- допоміжне
def secret(key: str, default: str = "") -> str:
    try:
        return str(st.secrets.get(key, default))
    except Exception:  # noqa: BLE001 — secrets.toml може бути відсутнім
        return default


def fmt_n(n) -> str:
    return f"{int(n):,}".replace(",", " ")


def fmt_dt(dt: datetime | None) -> str:
    return dt.astimezone(LOCAL_TZ).strftime("%d.%m.%Y %H:%M:%S") if dt else "—"


def fmt_ms(ms: int | None) -> str:
    return fmt_dt(datetime.fromtimestamp(ms / 1000, tz=timezone.utc)) if ms else "—"


def fmt_dur(sec: float | None) -> str:
    if sec is None:
        return "—"
    sec = int(sec)
    h, rem = divmod(sec, 3600)
    m, s = divmod(rem, 60)
    return f"{h}:{m:02d}:{s:02d}" if h else f"{m:02d}:{s:02d}"


def to_iso(d, t: dtime, end: bool = False) -> str:
    """Дата й час за київським часом → ISO зі зсувом, як у запитах інтерфейсу платформи."""
    return datetime.combine(d, t.replace(second=59 if end else 0, microsecond=0), tzinfo=LOCAL_TZ).isoformat()


def host_label(url: str) -> str:
    return urlparse(url if "//" in url else f"//{url}").hostname or url


def search_devices(client: M2MClient, q: str) -> list[dict]:
    key = (client.base_url, q.strip())
    if key not in S.search_cache:
        S.search_cache[key] = client.search_devices(q.strip())
    return S.search_cache[key]


# ---------------------------------------------------------------- бічна панель: вхід на Prod
def sidebar_login() -> None:
    with st.sidebar:
        st.markdown("### Підключення до Prod")
        client: M2MClient | None = S.client
        if client and client.token:
            st.success(f"Підключено до {host_label(client.base_url)}\n\n{client.email}")
            if st.button("Вийти", disabled=bool(S.job and S.job.running)):
                S.client, S.preview, S.search_cache = None, None, {}
                st.rerun()
        else:
            with st.form("login_form", border=False):
                url = st.text_input("Домен", value=secret("PROD_URL", "https://my.m2m.eu"))
                email = st.text_input("Логін")
                password = st.text_input("Пароль", type="password")
                submitted = st.form_submit_button("Увійти")
            if submitted:
                if not (url.strip() and email.strip() and password):
                    st.error("Заповніть домен, логін і пароль")
                else:
                    cl = M2MClient(url if "//" in url else f"https://{url.strip()}")
                    try:
                        with st.spinner("Входжу..."):
                            cl.login(email.strip(), password)
                    except ApiError as e:
                        st.error(str(e))
                    else:
                        S.client = cl
                        st.rerun()
        st.caption("Логін і пароль живуть лише в поточній сесії та ніде не зберігаються.")


# ---------------------------------------------------------------- екран 1: налаштування
def setup_page() -> None:
    client: M2MClient | None = S.client
    connected = bool(client and client.token)
    st.title("Перенос повідомлень Prod → Dev")
    st.caption("Скопіюйте повідомлення об'єкта з бойового сервера на dev, щоб розробники працювали з реальними даними.")
    if not connected:
        st.info("Увійдіть у Prod у бічній панелі ліворуч.")

    # 1. Джерело
    with st.container(border=True):
        st.subheader("1. Об'єкт-джерело (Prod)")
        q = st.text_input("Пошук об'єкта", placeholder="Назва або IMEI", key="search_q", disabled=not connected)
        devices: list[dict] = []
        if connected:
            try:
                devices = search_devices(client, q)
            except ApiError as e:
                st.error(f"Не вдалося отримати список об'єктів: {e}")
        names = {d["id"]: d["name"] for d in devices}
        device_id = st.selectbox(
            "Об'єкт", options=list(names), format_func=lambda i: f"{names[i]} (ID {i})",
            disabled=not devices, key="src_device",
        )
        if connected:
            more = " Показано перші 50, уточніть пошук." if len(devices) >= 50 else ""
            st.caption(f"Знайдено об'єктів: {len(devices)}.{more}")

    # 2. Призначення
    with st.container(border=True):
        st.subheader("2. Куди переносимо (Dev)")
        proto = st.selectbox("Протокол", list(PROTOCOLS), format_func=PROTOCOLS.get, key="proto")
        c1, c2, c3 = st.columns([2, 1, 2])
        host = c1.text_input("Домен або IP dev-сервера", value=secret("DEV_HOST"), key="dev_host")
        port_key = "PORT_WIALON" if proto == "wialon" else "PORT_TELTONIKA"
        port_s = c2.text_input("Порт", value=secret(port_key, DEFAULT_PORTS[proto]), key=f"port_{proto}")
        imei_label = "ID об'єкта на dev" if proto == "wialon" else "IMEI об'єкта на dev"
        imei = c3.text_input(imei_label, key="dev_imei").strip()
        st.caption("Об'єкт з цим IMEI має бути заздалегідь створений на dev.")
        imei_ok = bool(re.fullmatch(r"[A-Za-z0-9_-]{3,32}" if proto == "wialon" else r"\d{15}", imei))
        if imei and not imei_ok:
            st.warning("IMEI має складатися з 15 цифр." if proto != "wialon" else "ID: 3-32 символи (літери, цифри, - _).")
        port_ok = port_s.strip().isdigit() and 0 < int(port_s) < 65536

    # 3. Період
    with st.container(border=True):
        st.subheader("3. Період")
        today = datetime.now(LOCAL_TZ).date()
        a, b = st.columns(2)
        d_from = a.date_input("Від", value=today, key="d_from")
        t_from = a.time_input("Година", value=dtime(0, 0), key="t_from", step=60)
        d_to = b.date_input("До", value=today, key="d_to")
        t_to = b.time_input("Година ", value=dtime(23, 59), key="t_to", step=60)
        period_ok = datetime.combine(d_from, t_from) < datetime.combine(d_to, t_to)
        if not period_ok:
            st.error("Початок періоду має бути раніше за кінець.")
        shift_mode = st.radio(
            "Часові мітки на dev", ["shift", "orig"], key="tmode",
            format_func=lambda x: "Зсунути так, щоб кінець періоду був «зараз мінус N хв»" if x == "shift" else "Залишити оригінальні дати",
        )
        shift_min = st.number_input("N, хвилин", 0, 100000, 30, step=5, disabled=shift_mode != "shift", key="shift_min")

    date_from, date_to = to_iso(d_from, t_from), to_iso(d_to, t_to, end=True)
    pv_key = (device_id, date_from, date_to)

    # 4. Попередній перегляд
    with st.container(border=True):
        h1, h2 = st.columns([3, 1])
        h1.subheader("4. Попередній перегляд")
        if h2.button("Оновити перегляд", disabled=not (connected and device_id and period_ok)):
            try:
                with st.spinner("Читаю повідомлення з Prod..."):
                    S.preview = {"key": pv_key, **build_preview(client, device_id, date_from, date_to)}
            except ApiError as e:
                st.error(f"Не вдалося отримати повідомлення: {e}")
        pv = S.preview if S.preview and S.preview.get("key") == pv_key else None
        if pv is None:
            st.caption("Натисніть «Оновити перегляд», щоб побачити кількість повідомлень і графік напруги.")
        elif pv["total"] == 0:
            st.warning("У вибраному періоді повідомлень немає.")
        else:
            m1, m2, m3 = st.columns(3)
            m1.metric("Повідомлень знайдено", fmt_n(pv["total"]))
            m2.metric("Перше повідомлення", fmt_dt(pv["first"]))
            m3.metric("Останнє повідомлення", fmt_dt(pv["last"]))
            if pv["series"]:
                ser = sorted(pv["series"])
                idx = pd.DatetimeIndex([t for t, _ in ser]).tz_convert(LOCAL_TZ).tz_localize(None)
                st.caption("Напруга, В (вибірка по всьому періоду)")
                st.line_chart(pd.DataFrame({"Напруга, В": [v for _, v in ser]}, index=idx), height=220)
            if shift_mode == "shift" and pv["first"] and pv["last"]:
                end = datetime.now(timezone.utc) - timedelta(minutes=int(shift_min))
                start = end - (pv["last"] - pv["first"])
                st.caption(f"Інтервал на dev буде: {fmt_dt(start)} → {fmt_dt(end)}")
        all_params = st.checkbox("Переносити всі параметри, а не лише напругу", value=True, key="all_params")
        o1, o2 = st.columns(2)
        batch = o1.number_input("Записів у пакеті", 1, 255, 100, key="batch")
        test_run = o2.checkbox("Пробний запуск: лише перші 20 повідомлень", key="test_run")
        include_platform, w_version = False, "2.0"
        if proto == "wialon":
            with st.expander("Додатково (Wialon IPS)"):
                w_version = st.selectbox("Версія протоколу", ["2.0", "1.1"], key="w_ver")
                include_platform = st.checkbox(
                    "Передавати й службові параметри платформи (pf_*, power_enb, power_filtered, ...)", key="incl_platform",
                    help="За замовчуванням dev рахує їх сам із вхідних даних.",
                )

    # Дії
    def build_cfg() -> TransferConfig:
        return TransferConfig(
            client=client, device_id=int(device_id or 0), date_from=date_from, date_to=date_to, protocol=proto,
            host=host.strip(), port=int(port_s) if port_ok else 0, imei=imei, shift=shift_mode == "shift",
            shift_minutes=int(shift_min), batch_size=int(batch), all_params=all_params,
            include_platform=include_platform, wialon_version=w_version, limit=20 if test_run else None,
            source_name=names.get(device_id, ""),
        )

    dest_ok = bool(host.strip()) and port_ok and imei_ok
    if st.button("Перевірити з'єднання з dev", disabled=not dest_ok):
        try:
            with st.spinner("Підключаюсь..."):
                st.success(check_connection(build_cfg()))
        except SendError as e:
            st.error(str(e))

    ready = connected and bool(device_id) and dest_ok and period_ok
    if st.button("Почати перенос", type="primary", disabled=not ready):
        job = TransferJob(build_cfg())
        job.start()
        S.job, S.stage = job, "progress"
        st.rerun()
    if pv and pv["total"]:
        n = min(pv["total"], 20) if test_run else pv["total"]
        st.caption(f"Буде надіслано {fmt_n(n)} повідомлень на {host.strip() or '…'}:{port_s} в об'єкт {imei or '…'}.")
    else:
        st.caption("Кількість повідомлень буде визначено на старті.")
    st.caption("Якщо перенести той самий період двічі, на dev можуть з'явитись дублікати.")


# ---------------------------------------------------------------- екран 2: перенос іде
def progress_page() -> None:
    job: TransferJob = S.job
    cfg = job.cfg
    st.title("Перенос повідомлень Prod → Dev")

    with st.container(border=True):
        st.subheader("Параметри переносу")
        s1, s2 = st.columns(2)
        s1.markdown(f"**Джерело (Prod)**  \n{cfg.source_name} · ID {cfg.device_id}")
        s2.markdown(f"**Призначення (Dev)**  \n{cfg.host}:{cfg.port} · {PROTOCOLS[cfg.protocol]} · {cfg.imei}")
        s3, s4 = st.columns(2)
        s3.markdown(f"**Період на Prod**  \n{cfg.date_from[:16].replace('T', ' ')} → {cfg.date_to[:16].replace('T', ' ')}")
        s4.markdown("**Часові мітки на dev**  \n" + (f"кінець = зараз − {cfg.shift_minutes} хв" if cfg.shift else "оригінальні"))

    run_every = 1 if job.running else None

    @st.fragment(run_every=run_every)
    def live() -> None:
        with st.container(border=True):
            st.subheader("Прогрес")
            p1, p2 = st.columns([1, 3])
            p1.markdown(f"## {job.percent:.0f}%")
            p2.markdown(f"### {fmt_n(job.processed)} / {fmt_n(job.total)} повідомлень")
            st.progress(min(1.0, job.percent / 100))
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Швидкість", f"{fmt_n(job.speed)} повід./с")
            m2.metric("Минуло", fmt_dur(job.elapsed))
            m3.metric("Залишилось ≈", fmt_dur(job.eta))
            m4.metric("Помилок", fmt_n(job.failed))
            if job.status == "running":
                st.info(job.message or "Виконується")
            elif job.status == "done":
                st.success(f"Готово: передано {fmt_n(job.ok)} із {fmt_n(job.total)}. " + (job.message or ""))
                if job.first_ts_ms:
                    st.write(f"Дивіться на dev за інтервал: {fmt_ms(job.first_ts_ms)} → {fmt_ms(job.last_ts_ms)}")
            elif job.status == "stopped":
                st.warning(f"Зупинено користувачем. Передано {fmt_n(job.ok)} із {fmt_n(job.total)}.")
            elif job.status == "error":
                st.error(job.message)
            b1, b2, b3 = st.columns(3)
            if b1.button("Зупинити", disabled=not job.running):
                job.stop()
            if b2.button(f"Повторити невдалі ({job.failed})", disabled=job.running or job.failed == 0):
                job.retry_failed()
                st.rerun()
            if b3.button("← Назад до налаштувань", disabled=job.running):
                S.stage = "setup"
                st.rerun()
        if job.unmapped or job.skipped_ids:
            with st.expander("Параметри, які не передано"):
                if job.unmapped:
                    st.write("Немає відповідності IO (додайте в `m2m_replay/mapping.py`): " + ", ".join(sorted(job.unmapped)))
                if job.skipped_ids:
                    st.write("Не вмістились у протокол (Codec 8 не передає IO з ID > 255, оберіть Codec 8E): "
                             + ", ".join(str(i) for i in sorted(job.skipped_ids)))
        with st.expander(f"Журнал помилок ({len(job.errors)})", expanded=bool(job.errors)):
            if job.errors:
                st.dataframe(pd.DataFrame([{
                    "Пакет": e["batch"], "Повідомлень": e["count"], "Від": fmt_ms(e["from_ms"]),
                    "До": fmt_ms(e["to_ms"]), "Помилка": e["error"],
                } for e in job.errors]), hide_index=True)
            else:
                st.caption("Помилок немає.")
        if run_every and not job.running:
            st.rerun()  # завдання завершилось: перемальовуємо всю сторінку, щоб оновити кнопки

    live()


# ---------------------------------------------------------------- запуск
sidebar_login()
if S.stage == "progress" and S.job is not None:
    progress_page()
else:
    setup_page()
