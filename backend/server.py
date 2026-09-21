"""
server.py
---------
DualDesk PC Merkezi Sinyalleşme + Röle (Relay) Sunucusu.
(Render.com gibi bulut platformlarına deploy edilmek üzere hazırlanmıştır.)

DEĞİŞİKLİK (v2 - farklı ağ / mobil veri desteği):
  Eski sürümde bu sunucu yalnızca "broker" idi: iki istemciye birbirinin
  IP/port bilgisini iletir, gerçek görüntü/ses verisi istemciler arasında
  DOĞRUDAN bir TCP soketi üzerinden akardı. Bu, aynı Wi-Fi ağında ya da
  port yönlendirmesi olan istemciler için çalışıyordu; ancak istemcilerden
  biri (özellikle mobil veri / CGNAT arkasındaki bir telefon) dışarıdan
  ERİŞİLEMEZ olduğunda bağlantı hiç kurulamıyordu.

  Artık bu sunucu tüm görüntü/ses/ekran verisini de KENDİSİ RÖLELİYOR:
  istemciler yalnızca bu sunucuya (zaten açık olan) WebSocket bağlantısı
  üzerinden GİDEN veri gönderir; sunucu bu veriyi aynı oturumdaki diğer
  üyelere iletir. İki istemcinin birbirine ulaşabilmesi hiç gerekmez,
  yalnızca ikisinin de bu sunucuya ulaşabilmesi yeterlidir - bu da aynı
  Wi-Fi, farklı Wi-Fi ya da mobil veri fark etmeksizin her zaman çalışır.

Sinyalleşme (JSON, metin WebSocket çerçevesi):
  1. registered / refresh_password / password_updated  (değişmedi)
  2. connect_request / incoming_request / connect_response  (1:1 arama,
     kişisel ID + şifre ile) -> kabul edilirse iki taraf da "session_start"
     alır (session_id ile).
  3. create_room / room_created  -> "grup görüşmesi" için KİŞİSEL ID/şifreden
     BAĞIMSIZ, ayrı bir salon ID'si (9 hane) + salon şifresi (4 hane) üretir.
  4. join_room / peer_joined / connect_error -> başka istemciler aynı salon
     ID + şifre ile katılır (en fazla MAX_ROOM_MEMBERS kişi).
  5. leave_session / peer_left -> bir üye ayrılınca diğerlerine bildirilir.

Veri kanalı (İKİLİ/binary WebSocket çerçevesi - JSON DEĞİL):
  İstemciden sunucuya:  [session_id: 9 byte ascii][marker: 1 byte 'V'/'A'][payload]
  Sunucudan istemciye:  [session_id: 9 byte ascii][sender_id: 9 byte ascii][marker: 1 byte][payload]
  'V' -> JPEG video karesi ya da ekran karesi, 'A' -> ham PCM16 ses bloğu.
  Sunucu bu veriyi ayrıştırmaz/işlemez, yalnızca oturumdaki DİĞER üyelere
  aynen iletir (JSON parse maliyeti yok, düşük gecikme).

Render.com notları:
  - Render, uygulamaya $PORT ortam değişkeni ile dinlenecek portu bildirir;
    bu script bunu otomatik okur.
  - Render'ın health check'i düz HTTP isteği gönderir; process_request bu
    isteklere "200 OK" döner.
  - Render TLS'i kendisi sonlandırır: dışa açık adres "wss://...":dir.

NOT (Genel Sınırlama):
  Bu sunucu artık yalnızca eşleştirme değil, aynı zamanda medya röle
  sunucusudur; bu yüzden tüm görüntü/ses trafiği bu sunucudan geçer.
  Küçük gruplar (<=4 kişi, JPEG/PCM tabanlı düşük bit hızlı akış) için bu
  yeterlidir; çok sayıda eşzamanlı kullanıcı için sunucu bant genişliği/CPU
  kapasitesi ölçeklenmelidir.
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

MAX_ROOM_MEMBERS = 4
ID_BYTES = 9  # generate_id() her zaman 9 haneli döner - binary framing bu sabite dayanır.

# client_id -> {"password": str, "ws": WebSocketServerProtocol, "ip": str}
CLIENTS: dict[str, dict] = {}

# request_id -> {"from_id": str, "target_id": str, "mode": "screen"|"call"}
PENDING_REQUESTS: dict[str, dict] = {}

# room_id (kişisel ID'den bağımsız, ayrı 9 haneli salon kimliği) -> {"password", "mode", "owner"}
ROOMS: dict[str, dict] = {}

# session_id (1:1 çağrılarda request'e özel üretilen id, grup çağrılarında room_id
# ile AYNI değer) -> set(client_id). Röle bu tabloya göre yapılır.
SESSION_MEMBERS: dict[str, set] = {}

# client_id -> içinde bulunduğu tek session_id (bir istemci aynı anda tek oturumda olabilir)
CLIENT_SESSION: dict[str, str] = {}


async def process_request(connection, request):
    """WebSocket el sıkışması olmayan düz HTTP isteklerine (örn. Render'ın
    sağlık kontrolü veya tarayıcıdan '/' ziyareti) 200 OK döndürür."""
    if request.headers.get("Upgrade", "").lower() != "websocket":
        return connection.respond(
            HTTPStatus.OK,
            "DualDesk PC sinyalleşme + röle sunucusu çalışıyor.\n"
        )
    return None


async def handler(websocket):
    client_id = generate_id()
    while client_id in CLIENTS:
        client_id = generate_id()

    password = generate_password()
    ip = websocket.remote_address[0] if websocket.remote_address else "0.0.0.0"

    CLIENTS[client_id] = {"password": password, "ws": websocket, "ip": ip}
    logger.info(f"Yeni istemci bağlandı: {client_id} ({ip})")

    await websocket.send(json.dumps({
        "type": "registered",
        "id": client_id,
        "password": password,
    }))

    try:
        async for raw in websocket:
            if isinstance(raw, (bytes, bytearray)):
                await relay_binary(client_id, raw)
                continue
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            await route_message(client_id, msg)
    except websockets.exceptions.ConnectionClosed:
        pass
    finally:
        await leave_session(client_id)
        CLIENTS.pop(client_id, None)
        logger.info(f"İstemci ayrıldı: {client_id}")


# ------------------------------------------------------------ İkili röle
async def relay_binary(sender_id: str, raw: bytes) -> None:
    """Video/ses/ekran karesini, gönderenin bulunduğu oturumdaki diğer
    tüm üyelere aynen iletir. Sunucu bu içeriği hiç ayrıştırmaz."""
    if len(raw) <= ID_BYTES:
        return
    session_id = raw[:ID_BYTES].decode("ascii", "ignore")
    members = SESSION_MEMBERS.get(session_id)
    if not members or sender_id not in members:
        return  # bilinmeyen/eski oturum - sessizce yok say

    sender_tag = sender_id.encode("ascii")
    out_frame = raw[:ID_BYTES] + sender_tag + raw[ID_BYTES:]

    for member_id in members:
        if member_id == sender_id:
            continue
        peer = CLIENTS.get(member_id)
        if not peer:
            continue
        try:
            await peer["ws"].send(out_frame)
        except Exception:
            pass  # bir üyeye ulaşılamaması diğerlerini etkilemesin


# ------------------------------------------------------------ JSON yönlendirme
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

    elif mtype == "create_room":
        await handle_create_room(client_id, requester, msg)

    elif mtype == "join_room":
        await handle_join_room(client_id, requester, msg)

    elif mtype == "leave_session":
        await leave_session(client_id)


async def handle_connect_request(client_id: str, requester: dict, msg: dict) -> None:
    target_id = msg.get("target_id")
    target_password = msg.get("target_password")
    mode = msg.get("mode", "screen")  # "screen" ya da "call"

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
    }

    logger.info(f"Bağlantı isteği: {client_id} -> {target_id} ({mode})")

    await target["ws"].send(json.dumps({
        "type": "incoming_request",
        "from_id": client_id,
        "mode": mode,
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

    session_id = generate_id()
    while session_id in SESSION_MEMBERS:
        session_id = generate_id()

    join_session(session_id, req["from_id"])
    join_session(session_id, req["target_id"])

    logger.info(f"Oturum başladı: {req} -> session {session_id}")

    payload = json.dumps({
        "type": "session_start",
        "session_id": session_id,
        "mode": req["mode"],
        "members": [req["from_id"], req["target_id"]],
    })
    await requester["ws"].send(payload)
    await target["ws"].send(payload)


# ------------------------------------------------------------ Salon (grup görüşmesi)
async def handle_create_room(client_id: str, requester: dict, msg: dict) -> None:
    """Kişisel ID/şifreden TAMAMEN BAĞIMSIZ, yalnızca bu salona özel bir
    ID + şifre üretir (kullanıcı isteği: 'toplu görüntüleme şifresi ve ID
    olsun'). Oda sahibi ilk üye olarak otomatik katılır."""
    mode = msg.get("mode", "call")

    room_id = generate_id()
    while room_id in ROOMS or room_id in SESSION_MEMBERS:
        room_id = generate_id()
    room_password = generate_password()

    ROOMS[room_id] = {"password": room_password, "mode": mode, "owner": client_id}
    join_session(room_id, client_id)

    logger.info(f"Salon oluşturuldu: {room_id} (sahip {client_id})")

    await requester["ws"].send(json.dumps({
        "type": "room_created",
        "room_id": room_id,
        "room_password": room_password,
        "session_id": room_id,
        "mode": mode,
        "members": [client_id],
    }))


async def handle_join_room(client_id: str, requester: dict, msg: dict) -> None:
    room_id = msg.get("room_id")
    room_password = msg.get("room_password")

    room = ROOMS.get(room_id)
    if not room:
        await requester["ws"].send(json.dumps({
            "type": "connect_error", "reason": "Salon bulunamadı."
        }))
        return
    if room["password"] != room_password:
        await requester["ws"].send(json.dumps({
            "type": "connect_error", "reason": "Salon şifresi hatalı."
        }))
        return

    members = SESSION_MEMBERS.get(room_id, set())
    if client_id in members:
        await requester["ws"].send(json.dumps({
            "type": "connect_error", "reason": "Zaten bu salondasınız."
        }))
        return
    if len(members) >= MAX_ROOM_MEMBERS:
        await requester["ws"].send(json.dumps({
            "type": "connect_error", "reason": f"Salon dolu (maksimum {MAX_ROOM_MEMBERS} kişi)."
        }))
        return

    join_session(room_id, client_id)
    new_members = list(SESSION_MEMBERS[room_id])

    logger.info(f"{client_id} salona katıldı: {room_id} (üye sayısı {len(new_members)})")

    await requester["ws"].send(json.dumps({
        "type": "session_start",
        "session_id": room_id,
        "mode": room["mode"],
        "members": new_members,
    }))

    for member_id in new_members:
        if member_id == client_id:
            continue
        peer = CLIENTS.get(member_id)
        if peer:
            await peer["ws"].send(json.dumps({
                "type": "peer_joined",
                "session_id": room_id,
                "peer_id": client_id,
                "members": new_members,
            }))


# ------------------------------------------------------------ Ortak oturum yardımcıları
def join_session(session_id: str, client_id: str) -> None:
    SESSION_MEMBERS.setdefault(session_id, set()).add(client_id)
    CLIENT_SESSION[client_id] = session_id


async def leave_session(client_id: str) -> None:
    session_id = CLIENT_SESSION.pop(client_id, None)
    if not session_id:
        return
    members = SESSION_MEMBERS.get(session_id)
    if members is None:
        return
    members.discard(client_id)

    for member_id in list(members):
        peer = CLIENTS.get(member_id)
        if peer:
            try:
                await peer["ws"].send(json.dumps({
                    "type": "peer_left",
                    "session_id": session_id,
                    "peer_id": client_id,
                    "members": list(members),
                }))
            except Exception:
                pass

    if not members:
        SESSION_MEMBERS.pop(session_id, None)
        ROOMS.pop(session_id, None)


async def main():
    logger.info(f"DualDesk PC Sinyalleşme + Röle Sunucusu {HOST}:{PORT} üzerinde başlatılıyor...")
    async with websockets.serve(
        handler, HOST, PORT, max_size=None, process_request=process_request
    ):
        await asyncio.Future()  # sonsuza kadar çalış


if __name__ == "__main__":
    asyncio.run(main())
