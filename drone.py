import os, json, time, uuid, random, threading, logging
import socket, struct
from datetime import datetime

# ── Configuração ──────────────────────────────────────────────
DRONE_ID    = os.environ.get("DRONE_ID",    f"drone-{uuid.uuid4().hex[:6]}")
BASE_SECTOR = os.environ.get("BASE_SECTOR", "Base-Alpha")

# ── Multicast ─────────────────────────────────────────────────
MC_GROUP = "224.1.1.1"
MC_PORT  = 5007

# ── Tópicos ───────────────────────────────────────────────────
T_DRONE_STATUS = "ormuz/drones/status"
T_DRONE_CMD    = "ormuz/drones/cmd"

logging.basicConfig(
    level=logging.INFO,
    format=f"[%(asctime)s] [{DRONE_ID}] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ── Estado do drone ───────────────────────────────────────────
drone_state = {
    "drone_id":   DRONE_ID,
    "status":     "idle",
    "sector":     BASE_SECTOR,
    "mission_id": None,
    "lat":        round(random.uniform(26.0, 27.0), 4),
    "lon":        round(random.uniform(56.5, 58.5), 4),
}

lock = threading.Lock()

def now_iso():
    return datetime.utcnow().isoformat() + "Z"


# ── Transporte UDP Multicast ───────────────────────────────────

# Socket de envio
_sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
_sender.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, struct.pack("b", 2))

# Socket de recepção
_receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
_receiver.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
try:
    _receiver.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
except AttributeError:
    pass
_receiver.bind(("", MC_PORT))
_mreq = struct.pack("4sL", socket.inet_aton(MC_GROUP), socket.INADDR_ANY)
_receiver.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, _mreq)

def pub(topic, payload):
    try:
        msg = json.dumps({"topic": topic, "payload": payload}).encode()
        _sender.sendto(msg, (MC_GROUP, MC_PORT))
    except Exception as e:
        log.error(f"Erro ao publicar {topic}: {e}")

def _recv_loop():
    while True:
        try:
            data, _ = _receiver.recvfrom(65535)
            msg     = json.loads(data.decode())
            topic   = msg.get("topic", "")
            payload = msg.get("payload", {})
            threading.Thread(target=on_message, args=(topic, payload), daemon=True).start()
        except Exception as e:
            log.error(f"Erro ao receber mensagem: {e}")

# ── Publicação de status ───────────────────────────────────────

def pub_status():
    with lock:
        p = dict(drone_state)
    p["ts"] = now_iso()
    pub(T_DRONE_STATUS, p)

# ── Simulação de missão ───────────────────────────────────────

def simulate_mission(request_id, sector, event_type, priority, severity):
    sev_label = {3:"🔴 GRAVE", 2:"🟡 MODERADO", 1:"🟢 LEVE"}.get(severity, "?")

    # Fase 1: deslocamento
    lo, hi = {3:(2,5), 2:(3,7), 1:(4,9)}.get(severity, (3,8))
    travel_time = random.uniform(lo, hi)
    with lock:
        drone_state["status"]     = "dispatched"
        drone_state["sector"]     = sector
        drone_state["mission_id"] = request_id
    pub_status()
    log.info(f"🚁 Voando → {sector} [{sev_label}] {event_type} | {int(travel_time)}s")
    time.sleep(travel_time)

    # Fase 2: missão
    lo, hi = {3:(12,20), 2:(7,13), 1:(4,8)}.get(severity, (5,12))
    mission_time = random.uniform(lo, hi)
    with lock:
        drone_state["status"] = "on_mission"
        drone_state["lat"]    = round(random.uniform(25.5, 27.5), 4)
        drone_state["lon"]    = round(random.uniform(56.0, 59.0), 4)
    pub_status()
    log.info(f"🔍 Em missão: {event_type} | duração {int(mission_time)}s")
    elapsed = 0
    while elapsed < mission_time:
        step = min(3, mission_time - elapsed)
        time.sleep(step)
        elapsed += step
        pub_status()

    # Fase 3: retorno
    with lock:
        drone_state["status"] = "returning"
    pub_status()
    log.info("↩️  Retornando à base...")
    time.sleep(random.uniform(2, 5))

    with lock:
        drone_state["status"]     = "idle"
        drone_state["sector"]     = BASE_SECTOR
        drone_state["mission_id"] = None
        drone_state["lat"]        = round(random.uniform(26.0, 27.0), 4)
        drone_state["lon"]        = round(random.uniform(56.5, 58.5), 4)
    pub_status()
    log.info(f"Missão {request_id} concluída.")

# ── Callback de mensagens ─────────────────────────────────────

def on_message(topic, payload):
    if topic != T_DRONE_CMD:
        return
    if payload.get("drone_id") != DRONE_ID:
        return
    if payload.get("cmd") != "DISPATCH":
        return

    with lock:
        if drone_state["status"] != "idle":
            log.warning(f"Recebi DISPATCH mas estou {drone_state['status']}. Ignorando.")
            return
        drone_state["status"] = "dispatched"

    req_id     = payload["request_id"]
    sector     = payload["sector"]
    event_type = payload.get("event_type", "MONITORAMENTO")
    priority   = payload.get("priority",   "MEDIA")
    severity   = payload.get("severity",   1)

    log.info(f"📨 Despacho recebido → missão {req_id} em {sector}")
    threading.Thread(
        target=simulate_mission,
        args=(req_id, sector, event_type, priority, severity),
        daemon=True
    ).start()

# ── Heartbeat de status ───────────────────────────────────────

def status_loop():
    while True:
        time.sleep(5)
        pub_status()

# ── Main ──────────────────────────────────────────────────────

def main():
    log.info(f"🚁 Drone {DRONE_ID} | Base: {BASE_SECTOR} | Transporte: UDP Multicast")

    threading.Thread(target=_recv_loop, daemon=True).start()
    time.sleep(1)
    pub_status()

    threading.Thread(target=status_loop, daemon=True).start()

    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
