"""Wialon IPS (1.1 та 2.0) поверх TCP: пакети #D# з параметрами, відправляються пачками без очікування ACK на кожен."""

from __future__ import annotations

import socket
from datetime import datetime, timezone

from .crc import crc16_arc
from .errors import SendError
from .mapping import attrs_to_wialon_params
from .models import Point

NA = "NA"


def _fmt_coord(value: float, deg_width: int, pos: str, neg: str) -> tuple[str, str]:
    hemi = pos if value >= 0 else neg
    v = abs(value)
    deg = int(v)
    minutes = round((v - deg) * 60, 4)
    if minutes >= 60:
        deg += 1
        minutes = 0.0
    return f"{deg:0{deg_width}d}{minutes:07.4f}", hemi


def format_lat(lat: float) -> tuple[str, str]:
    return _fmt_coord(max(-90.0, min(90.0, lat)), 2, "N", "S")


def format_lon(lon: float) -> tuple[str, str]:
    return _fmt_coord(max(-180.0, min(180.0, lon)), 3, "E", "W")


def _crc_hex(text: str) -> str:
    return f"{crc16_arc(text.encode('utf-8')):04X}"


def build_login(imei: str, password: str = NA, version: str = "2.0") -> bytes:
    if version == "2.0":
        body = f"2.0;{imei};{password};"
        return f"#L#{body}{_crc_hex(body)}\r\n".encode("utf-8")
    return f"#L#{imei};{password}\r\n".encode("utf-8")


def build_data_packet(p: Point, version: str = "2.0", include_platform: bool = False, basic_only: bool = False) -> bytes:
    dt = datetime.fromtimestamp(p.ts_ms / 1000.0, tz=timezone.utc)
    lat1, lat2 = format_lat(p.lat)
    lon1, lon2 = format_lon(p.lon)
    hdop = p.attrs.get("hdop")
    hdop_s = format(float(hdop), ".2f") if isinstance(hdop, (int, float)) and not isinstance(hdop, bool) else NA
    params = attrs_to_wialon_params(p.attrs, include_platform, basic_only)
    params_s = ",".join(f"{n}:{t}:{v}" for n, t, v in params) or NA
    body = ";".join([
        dt.strftime("%d%m%y"), dt.strftime("%H%M%S"),
        lat1, lat2, lon1, lon2,
        str(max(0, p.speed)), str(max(0, min(360, p.course))), str(p.alt), str(max(0, p.sats)),
        hdop_s, NA, NA, NA, NA, params_s,
    ])
    if version == "2.0":
        body += ";"
        return f"#D#{body}{_crc_hex(body)}\r\n".encode("utf-8")
    return f"#D#{body}\r\n".encode("utf-8")


class WialonSender:
    def __init__(self, host: str, port: int, imei: str, version: str = "2.0", password: str = NA,
                 timeout: float = 20.0, include_platform: bool = False, basic_only: bool = False):
        self.host, self.port, self.imei = host, int(port), imei
        self.version, self.password = version, password
        self.timeout = timeout
        self.include_platform = include_platform
        self.basic_only = basic_only
        self.sock: socket.socket | None = None
        self.last_error = ""
        self.unmapped: set[str] = set()
        self.skipped_ids: set[int] = set()
        self._buf = b""

    def _read_lines(self, n: int) -> list[bytes]:
        lines: list[bytes] = []
        while len(lines) < n:
            while b"\r\n" in self._buf and len(lines) < n:
                line, self._buf = self._buf.split(b"\r\n", 1)
                lines.append(line)
            if len(lines) >= n:
                break
            chunk = self.sock.recv(65536)  # type: ignore[union-attr]
            if not chunk:
                raise SendError("Сервер закрив з'єднання")
            self._buf += chunk
        return lines

    def connect(self) -> None:
        self.close()
        self._buf = b""
        try:
            s = socket.create_connection((self.host, self.port), timeout=self.timeout)
            s.settimeout(self.timeout)
            self.sock = s
            s.sendall(build_login(self.imei, self.password, self.version))
            line = self._read_lines(1)[0]
        except (OSError, SendError) as e:
            self.close()
            raise SendError(f"Не вдалося підключитись до {self.host}:{self.port}: {e}") from e
        if line.strip() != b"#AL#1":
            self.close()
            raise SendError(f"Сервер не прийняв логін {self.imei} (відповідь {line[:40]!r}). Перевірте, чи створено об'єкт на dev.")

    def close(self) -> None:
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

    def send(self, points: list[Point]) -> list[bool]:
        """Надсилає всі пакети підряд, потім читає стільки ж відповідей #AD#. Повертає підтвердження по кожній точці."""
        if self.sock is None:
            raise SendError("Немає з'єднання")
        data = b"".join(
            build_data_packet(p, self.version, self.include_platform, self.basic_only) for p in points
        )
        try:
            self.sock.sendall(data)
            lines = self._read_lines(len(points))
        except (OSError, SendError) as e:
            self.close()
            raise SendError(f"Обрив з'єднання: {e}") from e
        result: list[bool] = []
        bad_codes: list[str] = []
        for line in lines:
            ok = line.strip() == b"#AD#1"
            result.append(ok)
            if not ok:
                bad_codes.append(line.decode("utf-8", "replace")[:40])
        self.last_error = "" if not bad_codes else f"сервер відповів {bad_codes[0]}"
        return result
