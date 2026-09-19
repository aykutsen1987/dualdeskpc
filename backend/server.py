"""
server.py
---------
DualDesk PC Merkezi Sinyalleşme ve Doğrulama Sunucusu.
(Render.com gibi bulut platformlarına deploy edilmek üzere hazırlanmıştır.)

Görevleri:
  1. Her bağlanan istemciye benzersiz 9 haneli ID ve 4 haneli dinamik
     şifre atamak.
  2. İki istemci arasında (Host <-> Guest) bağlantı taleplerini yönetmek:
     ID + Şifre doğrulaması yapmak, hedefe "Kabul Et / Reddet" bildirimi
     iletmek.
  3. Bağlantı kabul edildiğinde, iki istemcinin doğrudan P2P/soket
     bağlantısı kurabilmesi için IP ve port bilgisini karşılıklı iletmek
     (broker rolü — gerçek medya/ekran verisi bu sunucudan GEÇMEZ).

Render.com notları:
  - Render, uygulamaya $PORT ortam değişkeni ile dinlenecek portu bildirir;
    bu script bunu otomatik okur (aşağıdaki PORT satırına bakın).
  - Render'ın "Web Service" sağlık kontrolü (health check) düz bir HTTP
    isteği gönderir; aşağıdaki `process_request` fonksiyonu WebSocket
    olmayan (düz HTTP) isteklere "200 OK" döndürerek bu kontrolü geçer.
  - Render TLS'i kendisi sonlandırır: dışarıya karşı adresiniz otomatik
    olarak "wss://uygulama-adiniz.onrender.com" şeklinde HTTPS/WSS olur.

NOT (Genel Sınırlama):
  Bu sunucu yalnızca eşleştirme/broker görevi görür; istemciler arası
  doğrudan bağlantı için istemcilerin birbirine (veya en azından
  "aranan" tarafın) ağ üzerinden erişilebilir olması gerekir. Farklı
  NAT'lar arkasındaki bilgisayarlar arasında güvenilir çalışma için
  STUN/TURN (örn. aiortc + coturn) entegrasyonu önerilir (bkz. ana
  README.md).
"""

import asyncio
import json
import logging
import os
from http import HTTPStatus

import websockets

from utils import generate_id, generate_password

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(message)s")
logger = logging.getLogger("DualDeskServer")

HOST = "0.0.0.0"
PORT = int(os.environ.get("PORT", 8765))  # Render bu değeri otomatik atar

# client_id -> {"password": str, "ws": WebSocketServerProtocol, "ip": str}
CLIENTS: dict[str, dict] = {}

# request_id -> {"from_id": str, "target_id": str, "mode": "screen"|"call"}
PENDING_REQUESTS: dict[str, dict] = {}


async def process_request(connection, request):
    """WebSocket el sıkışması olmayan düz HTTP isteklerine (örn. Render'ın
    sağlık kontrolü veya tarayıcıdan '/' ziyareti) 200 OK döndürür.
    WebSocket upgrade isteği ise None döndürülerek normal akışa bırakılır.

    Ayrıca, Render gibi ters proxy kullanan platformlar TLS'i kendi
    kenarında sonlandırdığı için connection.remote_address, istemcinin
    gerçek genel IP'si DEĞİL, platformun iç proxy IP'sidir. Gerçek istemci
    IP'si bu platformlarca 'X-Forwarded-For' header'ında iletilir (ilk
    değer). Burada, henüz orijinal HTTP isteğine erişimimiz varken bu
    değeri okuyup connection nesnesine ekliyoruz; handler() içinde bu
    header'a artık erişimimiz olmuyor, bu yüzden burada yakalamak gerekiyor.
    """
    forwarded = request.headers.get("X-Forwarded-For")
    connection.real_client_ip = forwarded.split(",")[0].strip() if forwarded else None

    if request.headers.get("Upgrade", "").lower() != "websocket":
        return connection.respond(
            HTTPStatus.OK,
            "DualDesk PC sinyalleşme sunucusu çalışıyor.\n"
        )
    return None


def _extract_client_ip(websocket) -> str:
    """process_request'te yakalanan gerçek istemci IP'sini (varsa) döndürür,
    yoksa (yerel/proxy'siz çalışırken) remote_address'e düşer."""
    real_ip = getattr(websocket, "real_client_ip", None)
    if real_ip:
        return real_ip
    return websocket.remote_address[0] if websocket.remote_address else "0.0.0.0"


async def handler(websocket):
    client_id = generate_id()
    while client_id in CLIENTS:
        client_id = generate_id()

    password = generate_password()
    ip = _extract_client_ip(websocket)

    CLIENTS[client_id] = {"password": password, "ws": websocket, "ip": ip}
    logger.info(f"Yeni istemci bağlandı: {client_id} ({ip})")

    await websocket.send(json.dumps({
        "type": "registered",
        "id": client_id,
        "password": password,
    }))

    try:
        async for raw in websocket:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            await route_message(client_id, msg)
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        CLIENTS.pop(client_id, None)
        logger.info(f"İstemci ayrıldı: {client_id}")


async def route_message(client_id: str, msg: dict) -> None:
    mtype = msg.get("type")
    requester = CLIENTS.get(client_id)
    if requester is None:
        return

    if mtype == "refresh_password":
        requester["password"] = generate_password()
        await requester["ws"].send(json.dumps({
            "type": "password_updated",
            "password": requester["password"],
        }))

    elif mtype == "connect_request":
        await handle_connect_request(client_id, requester, msg)

    elif mtype == "connect_response":
        await handle_connect_response(msg)


async def handle_connect_request(client_id: str, requester: dict, msg: dict) -> None:
    target_id = msg.get("target_id")
    target_password = msg.get("target_password")
    mode = msg.get("mode", "screen")  # "screen" ya da "call"
    # DualDesk Mobil entegrasyonu: "CONTROL" | "VIEW_ONLY". Bu sunucu yalnızca
    # broker'dır - değeri doğrulamaz/değiştirmez, olduğu gibi hedefe iletir.
    # Asıl doğrulama HOST tarafında (screen_share.py yaması) yapılır; bu alan
    # sadece iki istemcinin AYNI yetkiyi görmesini garanti eder.
    permission = msg.get("permission", "CONTROL")

    target = CLIENTS.get(target_id)

    if not target:
        await requester["ws"].send(json.dumps({
            "type": "connect_error", "reason": "Belirtilen ID bulunamadı."
        }))
        return

    if target["password"] != target_password:
        await requester["ws"].send(json.dumps({
            "type": "connect_error", "reason": "Şifre hatalı."
        }))
        return

    if target_id == client_id:
        await requester["ws"].send(json.dumps({
            "type": "connect_error", "reason": "Kendi ID'nize bağlanamazsınız."
        }))
        return

    request_id = f"{client_id}-{target_id}-{mode}-{generate_password()}"
    PENDING_REQUESTS[request_id] = {
        "from_id": client_id,
        "target_id": target_id,
        "mode": mode,
        "permission": permission,
    }

    logger.info(f"Bağlantı isteği: {client_id} -> {target_id} ({mode}, {permission})")

    await target["ws"].send(json.dumps({
        "type": "incoming_request",
        "from_id": client_id,
        "mode": mode,
        "permission": permission,
        "request_id": request_id,
    }))


async def handle_connect_response(msg: dict) -> None:
    request_id = msg.get("request_id")
    accepted = bool(msg.get("accepted"))

    req = PENDING_REQUESTS.pop(request_id, None)
    if not req:
        return

    requester = CLIENTS.get(req["from_id"])
    target = CLIENTS.get(req["target_id"])
    if not requester or not target:
        return

    if not accepted:
        logger.info(f"Bağlantı reddedildi: {req}")
        await requester["ws"].send(json.dumps({
            "type": "connect_rejected", "mode": req["mode"]
        }))
        return

    # Basit demo port ataması. Üretimde eşzamanlı oturumlar için
    # gerçek bir boş-port havuzu/ayırıcısı kullanılmalıdır. Not: Render
    # gibi platformlarda istemciler arası P2P bağlantı doğrudan
    # istemcilerin genel IP'leri üzerinden kurulur; sunucu yalnızca
    # bilgi alışverişini sağlar.
    port = 6000 + (abs(hash(request_id)) % 2000)

    logger.info(f"Bağlantı kabul edildi: {req} -> port {port}")

    permission = req.get("permission", "CONTROL")

    await target["ws"].send(json.dumps({
        "type": "start_listener",
        "port": port,
        "mode": req["mode"],
        "permission": permission,
    }))

    await requester["ws"].send(json.dumps({
        "type": "connect_accepted",
        "target_ip": target["ip"],
        "target_local_ip": msg.get("local_ip"),
        "port": port,
        "mode": req["mode"],
        "permission": permission,
    }))


async def main():
    logger.info(f"DualDesk PC Sinyalleşme Sunucusu {HOST}:{PORT} üzerinde başlatılıyor...")
    async with websockets.serve(
        handler, HOST, PORT, max_size=None, process_request=process_request
    ):
        await asyncio.Future()  # sonsuza kadar çalış


if __name__ == "__main__":
    asyncio.run(main())
