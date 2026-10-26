"""Teltonika Codec 8 / 8E поверх TCP: кодування AVL-пакетів та відправник одним з'єднанням."""

from __future__ import annotations

import socket
import struct

from .crc import crc16_arc
from .errors import SendError
from .mapping import attrs_to_io
from .models import Point

CODEC_8 = 0x08
CODEC_8E = 0x8E

# Ширина IO-значення (байт) за ID, як у реальних трекерів Teltonika. Якщо значення не влізло — береться ширша.
KNOWN_WIDTH = {
    11: 8, 14: 8, 16: 4, 21: 1, 24: 2, 66: 2, 67: 2, 68: 2, 69: 1, 80: 1, 113: 1,
    78: 8, 181: 2, 182: 2, 200: 1, 205: 2, 206: 2, 207: 8, 239: 1, 240: 1, 241: 4, 318: 1,
}
MAX_RECORDS_PER_PACKET = 255


def _fit_width(v: int, preferred: int | None) -> int | None:
    signed = v < 0
    for w in (1, 2, 4, 8):
        if preferred and w < preferred:
            continue
        if signed:
            lo, hi = -(1 << (8 * w - 1)), (1 << (8 * w - 1)) - 1
        else:
            lo, hi = 0, (1 << (8 * w)) - 1
        if lo <= v <= hi:
            return w
    return None


def encode_io(io: dict[int, int], event_id: int, codec: int) -> tuple[bytes, list[int]]:
    """IO-блок запису. Повертає (байти, ID, які не вдалось закодувати)."""
    groups: dict[int, list[tuple[int, int]]] = {1: [], 2: [], 4: [], 8: []}
    skipped: list[int] = []
    max_id = 255 if codec == CODEC_8 else 65535
    for io_id, v in io.items():  # порядок як у вхідних даних (серверу він не важливий, але так збігається з прикладами Teltonika)
        if io_id < 0 or io_id > max_id:
            skipped.append(io_id)
            continue
        w = _fit_width(v, KNOWN_WIDTH.get(io_id))
        if w is None:
            skipped.append(io_id)
            continue
        groups[w].append((io_id, v))
    total = sum(len(g) for g in groups.values())
    out = bytearray()
    if codec == CODEC_8:
        out += bytes([event_id if 0 <= event_id <= 255 else 0, total])
        for w in (1, 2, 4, 8):
            out.append(len(groups[w]))
            for io_id, v in groups[w]:
                out.append(io_id)
                out += v.to_bytes(w, "big", signed=v < 0)
    else:
        out += struct.pack(">HH", event_id if 0 <= event_id <= 65535 else 0, total)
        for w in (1, 2, 4, 8):
            out += struct.pack(">H", len(groups[w]))
            for io_id, v in groups[w]:
                out += struct.pack(">H", io_id)
                out += v.to_bytes(w, "big", signed=v < 0)
        out += struct.pack(">H", 0)  # NX: IO змінної довжини не використовуємо
    return bytes(out), skipped


def encode_record(p: Point, io_block: bytes) -> bytes:
    lon = max(-1_800_000_000, min(1_800_000_000, int(round(p.lon * 1e7))))
    lat = max(-900_000_000, min(900_000_000, int(round(p.lat * 1e7))))
    gps = struct.pack(
        ">iihHBH",
        lon,
        lat,
        max(-32768, min(32767, p.alt)),
        max(0, min(360, p.course)),
        max(0, min(255, p.sats)),
        max(0, min(65535, p.speed)),
    )
    return struct.pack(">QB", p.ts_ms, max(0, min(2, p.priority))) + gps + io_block


def build_packet(records: list[bytes], codec: int) -> bytes:
    if not 1 <= len(records) <= MAX_RECORDS_PER_PACKET:
        raise ValueError(f"У пакеті має бути 1..{MAX_RECORDS_PER_PACKET} записів, а не {len(records)}")
    n = len(records)
    body = bytes([codec, n]) + b"".join(records) + bytes([n])
    return b"\x00\x00\x00\x00" + struct.pack(">I", len(body)) + body + struct.pack(">I", crc16_arc(body))


def _recv_exact(sock: socket.socket, n: int) -> bytes:
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise SendError("Сервер закрив з'єднання")
        buf += chunk
    return buf


class TeltonikaSender:
    """Одне TCP-з'єднання на весь перенос: handshake за IMEI, далі пакети по багато записів."""

    def __init__(self, host: str, port: int, imei: str, codec: int = CODEC_8E, timeout: float = 15.0,
                 basic_only: bool = False):
        self.host, self.port, self.imei = host, int(port), imei
        self.codec = codec
        self.timeout = timeout
        self.basic_only = basic_only
        self.sock: socket.socket | None = None
        self.last_error = ""
        self.unmapped: set[str] = set()      # параметри, які не вдалось перетворити на IO
        self.skipped_ids: set[int] = set()   # IO, які не вдалось закодувати (наприклад, ID > 255 у Codec 8)

    def connect(self) -> None:
        self.close()
        try:
            s = socket.create_connection((self.host, self.port), timeout=self.timeout)
            s.settimeout(self.timeout)
            imei_b = self.imei.encode("ascii")
            s.sendall(struct.pack(">H", len(imei_b)) + imei_b)
            resp = _recv_exact(s, 1)
        except (OSError, SendError) as e:
            self.close()
            raise SendError(f"Не вдалося підключитись до {self.host}:{self.port}: {e}") from e
        if resp != b"\x01":
            s.close()
            raise SendError(f"Сервер не прийняв IMEI {self.imei} (відповідь {resp.hex()}). Перевірте, чи створено об'єкт на dev.")
        self.sock = s

    def close(self) -> None:
        if self.sock is not None:
            try:
                self.sock.close()
            except OSError:
                pass
            self.sock = None

    def send(self, points: list[Point]) -> list[bool]:
        """Надсилає один пакет. Повертає для кожної точки, чи підтвердив її сервер (ACK = кількість прийнятих записів)."""
        if self.sock is None:
            raise SendError("Немає з'єднання")
        records = []
        for p in points:
            io, unknown = attrs_to_io(p.attrs, self.basic_only)
            self.unmapped.update(unknown)
            block, skipped = encode_io(io, p.event_id, self.codec)
            self.skipped_ids.update(skipped)
            records.append(encode_record(p, block))
        packet = build_packet(records, self.codec)
        try:
            self.sock.sendall(packet)
            ack = struct.unpack(">I", _recv_exact(self.sock, 4))[0]
        except (OSError, SendError) as e:
            self.close()
            raise SendError(f"Обрив з'єднання: {e}") from e
        self.last_error = "" if ack >= len(points) else f"сервер підтвердив {ack} із {len(points)} записів"
        return [i < ack for i in range(len(points))]
