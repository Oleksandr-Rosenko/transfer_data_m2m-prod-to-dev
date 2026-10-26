"""CRC-16/ARC (поліном 0xA001, початкове значення 0) — для Teltonika AVL-пакетів та Wialon IPS 2.0."""

_TABLE = []


def _build_table():
    for i in range(256):
        crc = i
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if crc & 1 else crc >> 1
        _TABLE.append(crc)


_build_table()


def crc16_arc(data: bytes, crc: int = 0) -> int:
    for b in data:
        crc = (crc >> 8) ^ _TABLE[(crc ^ b) & 0xFF]
    return crc
