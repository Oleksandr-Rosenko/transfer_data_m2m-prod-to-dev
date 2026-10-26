"""Незалежні від коду застосунку декодери та мок-сервери для тестів.

Декодери написані за специфікаціями (Teltonika Codec 8/8E, Wialon IPS), а не викликають код кодувальників.
"""

from __future__ import annotations

import socket
import struct
import threading


def crc16(data: bytes) -> int:
    crc = 0
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if crc & 1 else crc >> 1
    return crc


# ---------------------------------------------------------------- Teltonika
def decode_teltonika(packet: bytes) -> list[dict]:
    """Розбирає пакет Codec 8/8E, перевіряє преамбулу, довжину, CRC і лічильники. Повертає записи."""
    assert packet[:4] == b"\x00\x00\x00\x00", "преамбула"
    (length,) = struct.unpack(">I", packet[4:8])
    body = packet[8:8 + length]
    assert len(body) == length, "довжина даних"
    (crc,) = struct.unpack(">I", packet[8 + length:12 + length])
    assert len(packet) == 12 + length, "зайві байти"
    assert crc == crc16(body), "CRC"
    codec, n = body[0], body[1]
    assert codec in (0x08, 0x8E)
    assert body[-1] == n, "кількість записів у кінці"
    ext = codec == 0x8E
    pos = 2
    records = []

    def rd(size):
        nonlocal pos
        v = body[pos:pos + size]
        assert len(v) == size, "вихід за межі пакета"
        pos += size
        return v

    idw = 2 if ext else 1   # ширина ID та лічильників
    for _ in range(n):
        (ts,) = struct.unpack(">Q", rd(8))
        prio = rd(1)[0]
        lon, lat, alt, ang, sat, spd = struct.unpack(">iihHBH", rd(15))
        event = int.from_bytes(rd(idw), "big")
        total = int.from_bytes(rd(idw), "big")
        io = {}
        for w in (1, 2, 4, 8):
            cnt = int.from_bytes(rd(idw), "big")
            for _i in range(cnt):
                io_id = int.from_bytes(rd(idw), "big")
                io[io_id] = int.from_bytes(rd(w), "big")
        if ext:
            nx = int.from_bytes(rd(2), "big")
            assert nx == 0
        assert total == len(io), "загальна кількість IO"
        records.append({"ts": ts, "prio": prio, "lon": lon, "lat": lat, "alt": alt, "angle": ang,
                        "sat": sat, "speed": spd, "event": event, "io": io})
    assert pos == len(body) - 1, "зайві байти в записах"
    return records


class TeltonikaMock:
    """TCP-сервер: handshake за IMEI, пакети, ACK = кількість записів (або за сценарієм)."""

    def __init__(self, accept_imei=True, ack_fn=None):
        self.accept_imei = accept_imei
        self.ack_fn = ack_fn            # (n_records, packet_index) -> ack
        self.records: list[dict] = []
        self.imeis: list[str] = []
        self.packets = 0
        self.connections = 0
        self._srv = socket.socket()
        self._srv.bind(("127.0.0.1", 0))
        self._srv.listen(5)
        self.port = self._srv.getsockname()[1]
        self._t = threading.Thread(target=self._serve, daemon=True)
        self._t.start()

    def _serve(self):
        while True:
            try:
                c, _ = self._srv.accept()
            except OSError:
                return
            self.connections += 1
            threading.Thread(target=self._client, args=(c,), daemon=True).start()

    @staticmethod
    def _recv(c, n):
        buf = b""
        while len(buf) < n:
            ch = c.recv(n - len(buf))
            if not ch:
                raise EOFError
            buf += ch
        return buf

    def _client(self, c):
        try:
            (ln,) = struct.unpack(">H", self._recv(c, 2))
            self.imeis.append(self._recv(c, ln).decode())
            c.sendall(b"\x01" if self.accept_imei else b"\x00")
            if not self.accept_imei:
                return
            while True:
                head = self._recv(c, 8)
                (length,) = struct.unpack(">I", head[4:8])
                packet = head + self._recv(c, length + 4)
                recs = decode_teltonika(packet)
                self.packets += 1
                ack = self.ack_fn(len(recs), self.packets) if self.ack_fn else len(recs)
                if ack:
                    self.records.extend(recs[:ack])
                c.sendall(struct.pack(">I", ack))
        except (EOFError, OSError):
            pass
        finally:
            c.close()

    def close(self):
        self._srv.close()


# ---------------------------------------------------------------- Wialon
class WialonMock:
    """TCP-сервер Wialon IPS: #L#, #D# з перевіркою CRC (v2.0), відповіді #AL#1 / #AD#1."""

    def __init__(self, version="2.0", reject=None):
        self.version = version
        self.reject = reject or (lambda idx, line: False)
        self.logins: list[str] = []
        self.data: list[str] = []
        self._srv = socket.socket()
        self._srv.bind(("127.0.0.1", 0))
        self._srv.listen(5)
        self.port = self._srv.getsockname()[1]
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self):
        while True:
            try:
                c, _ = self._srv.accept()
            except OSError:
                return
            threading.Thread(target=self._client, args=(c,), daemon=True).start()

    def _check_crc(self, payload: str) -> bool:
        if self.version != "2.0":
            return True
        body, _, crc = payload.rpartition(";")
        return f"{crc16((body + ';').encode()):04X}" == crc

    def _client(self, c):
        buf = b""
        try:
            while True:
                ch = c.recv(65536)
                if not ch:
                    return
                buf += ch
                while b"\r\n" in buf:
                    line, buf = buf.split(b"\r\n", 1)
                    text = line.decode()
                    if text.startswith("#L#"):
                        payload = text[3:]
                        ok = self._check_crc(payload)
                        self.logins.append(payload)
                        c.sendall(b"#AL#1\r\n" if ok else b"#AL#0\r\n")
                    elif text.startswith("#D#"):
                        payload = text[3:]
                        idx = len(self.data)
                        self.data.append(payload)
                        ok = self._check_crc(payload) and not self.reject(idx, payload)
                        c.sendall(b"#AD#1\r\n" if ok else b"#AD#-1\r\n")
        except OSError:
            pass
        finally:
            c.close()

    def close(self):
        self._srv.close()
