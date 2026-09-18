"""
utils.py
--------
DualDesk PC için ortak yardımcı fonksiyonlar:
- Benzersiz 9 haneli ID ve 4 haneli dinamik şifre üretimi
- TCP soketleri üzerinden uzunluk-öncelikli (length-prefixed) mesaj/kare
  gönderme ve alma. Bu sayede JSON kontrol komutları ile JPEG/PCM ikili
  veriler aynı soket üzerinden güvenli biçimde ayrıştırılabilir.
"""

import json
import random
import socket
import string
import struct


def generate_id() -> str:
    """9 haneli sayısal bir istemci ID'si üretir."""
    return "".join(random.choices(string.digits, k=9))


def generate_password() -> str:
    """4 haneli tek kullanımlık/dinamik bir şifre üretir."""
    return "".join(random.choices(string.digits, k=4))


def recv_exact(sock: socket.socket, n: int) -> bytes:
    """Soketten tam olarak n byte okunana kadar bekler."""
    buf = bytearray()
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Bağlantı karşı taraf tarafından kapatıldı.")
        buf.extend(chunk)
    return bytes(buf)


def send_frame(sock: socket.socket, payload: bytes) -> None:
    """4 byte'lık büyük-endian uzunluk başlığı + ham veri gönderir.
    Görüntü karesi (JPEG), ses bloğu (PCM) veya JSON encode edilmiş
    kontrol komutları için kullanılabilir."""
    header = struct.pack(">I", len(payload))
    sock.sendall(header + payload)


def recv_frame(sock: socket.socket) -> bytes:
    """send_frame ile gönderilmiş bir veri bloğunu okur."""
    header = recv_exact(sock, 4)
    (length,) = struct.unpack(">I", header)
    return recv_exact(sock, length)


def send_json(sock: socket.socket, data: dict) -> None:
    """Bir sözlüğü JSON'a çevirip length-prefixed olarak gönderir."""
    send_frame(sock, json.dumps(data).encode("utf-8"))


def recv_json(sock: socket.socket) -> dict:
    """send_json ile gönderilmiş bir JSON mesajını okur."""
    payload = recv_frame(sock)
    return json.loads(payload.decode("utf-8"))
