import os, json, time, uuid, random, threading, logging
import socket, struct
from datetime import datetime
from RicartAgrawala import RicartAgrawala

# ── Configuração ───────────────────────────────────────────────────────────────
BROKER_ID       = os.environ.get("BROKER_ID",       f"broker-{uuid.uuid4().hex[:6]}")
SECTOR_NAME     = os.environ.get("SECTOR_NAME",     f"Setor-{random.randint(1,99)}")
SENSOR_INTERVAL = float(os.environ.get("SENSOR_INTERVAL", "10"))

# ── Multicast ──────────────────────────────────────────────────────────────────
MC_GROUP = "224.1.1.1"
MC_PORT  = 5007

# ── Tópicos ────────────────────────────────────────────────────────────────────
T_REQUESTS        = "ormuz/requests"
T_REQUEST_ACK     = "ormuz/requests/ack"
T_DRONE_STATUS    = "ormuz/drones/status"
T_DRONE_CMD       = "ormuz/drones/cmd"
T_LISTA_GERAL     = "ormuz/lista_geral"
T_DASHBOARD       = "ormuz/dashboard"
T_HEARTBEAT       = "ormuz/heartbeat"
T_SENSOR_DATA     = "ormuz/sensors"
T_RA_ELECTION_REQ = "ormuz/ra/election_req"
T_RA_ELECTION_REP = "ormuz/ra/election_rep"
T_COORDINATOR     = "ormuz/coordinator"
T_BROKER_OFFLINE  = "ormuz/broker/offline"
T_FILA_SYNC_REQ   = "ormuz/fila/sync_req"
T_FILA_SYNC_RES   = "ormuz/fila/sync_res"

logging.basicConfig(
    level=logging.INFO,
    format=f"[%(asctime)s] [{BROKER_ID}] %(levelname)s: %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger(__name__)

# ── Estado global ──────────────────────────────────────────────────────────────
state = {
    "broker_id":       BROKER_ID,
    "sector":          SECTOR_NAME,
    "is_coordinator":  False,
    "coordinator_id":  None,
    "known_brokers":   {},
    "queue":           {},
    "drones":          {},
    "active_missions": {},
    "active_requests": {},
    "seen_requests":   set(),
    "total_received":  0,
    "total_dispatched":0,
}

lock = threading.Lock()
ra   = RicartAgrawala(BROKER_ID)

def now_iso():
    return datetime.utcnow().isoformat() + "Z"


# ── Transporte UDP Multicast ───────────────────────────────────────────────────

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

def pub(topic, payload, retain=False):
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



def mostrar_fila(evento="ATUALIZAÇÃO"):
    with lock:
        fila = sorted(state["queue"].values(),
                      key=lambda r: (-r.get("severity", 1), r.get("enqueued_at", 0)))
    print()
    print(f"  ┌{'─'*66}┐")
    print(f"  │ {'FILA LOCAL — ' + SECTOR_NAME:^64} │")
    print(f"  │ evento : {evento:<55} │")
    print(f"  │ broker : {BROKER_ID:<55} │")
    print(f"  ├{'─'*4}┬{'─'*28}┬{'─'*10}┬{'─'*20}┤")
    print(f"  │ {'#':^2} │ {'OCORRÊNCIA':^26} │ {'GRAV':^8} │ {'SETOR':^18} │")
    print(f"  ├{'─'*4}┼{'─'*28}┼{'─'*10}┼{'─'*20}┤")
    if not fila:
        print(f"  │ {'FILA VAZIA':^66} │")
    else:
        SEV_LABEL = {3: "GRAVE", 2: "MOD", 1: "LEVE"}
        for i, r in enumerate(fila, 1):
            sev = SEV_LABEL.get(r.get("severity", 1), "?")
            ev  = r.get("event_type", "?")[:24]
            sec = r.get("sector", "?")[:18]
            print(f"  │ {i:^2} │ {ev:^26} │ {sev:^8} │ {sec:^18} │")
    print(f"  └{'─'*4}┴{'─'*28}┴{'─'*10}┴{'─'*20}┘")
    print()

def mostrar_broker_offline(broker_id):
    print()
    print(f"  ╔{'═'*56}╗")
    print(f"  ║ {'⚠  BROKER OFFLINE DETECTADO':^54} ║")
    print(f"  ╠{'═'*56}╣")
    print(f"  ║  broker_id : {broker_id:<40} ║")
    print(f"  ╚{'═'*56}╝")
    print()

def mostrar_eleicao(resultado):
    print()
    print(f"  ╔{'═'*56}╗")
    print(f"  ║ {'🗳  ELEIÇÃO RICART-AGRAWALA':^54} ║")
    print(f"  ╠{'═'*56}╣")
    print(f"  ║  {resultado:<54} ║")
    print(f"  ╚{'═'*56}╝")
    print()


# ── Ricart-Agrawala ────────────────────────────────────────────────────────────

def _send_ra_request(clock):
    pub(T_RA_ELECTION_REQ, {
        "from": BROKER_ID, "sector": SECTOR_NAME,
        "clock": clock, "ts": now_iso()
    })

def _send_ra_reply(peer_id, clock):
    pub(T_RA_ELECTION_REP, {
        "to": peer_id, "from": BROKER_ID,
        "clock": clock, "ts": now_iso()
    })

def ra_request_cs():
    with lock:
        peers = set(state["known_brokers"].keys()) - {BROKER_ID}
    log.info(f"RA: pedindo CS (peers={len(peers)})")
    granted = ra.request_cs(peers, _send_ra_request)
    if granted:
        _enter_coordinator()
    else:
        log.warning("RA: timeout — eleição cancelada")
        mostrar_eleicao("TIMEOUT — não eleito")

def _enter_coordinator():
    with lock:
        state["is_coordinator"] = True
        state["coordinator_id"] = BROKER_ID
    ra.release_cs(_send_ra_reply)
    mostrar_eleicao(f"ELEITO — {BROKER_ID} é o COORDENADOR")
    log.info("Sou o COORDENADOR — assumindo despacho de drones")
    pub(T_COORDINATOR, {
        "coordinator_id": BROKER_ID,
        "sector": SECTOR_NAME,
        "ts": now_iso()
    })
    threading.Thread(target=try_dispatch, daemon=True).start()


# ── Fila replicada ─────────────────────────────────────────────────────────────

PROBLEM_SEVERITY = {
    "COLISAO_IMINENTE":        3,
    "BLOQUEIO_ROTA":           3,
    "DERRAMAMENTO_OLEO":       3,
    "EMBARCACAO_DERIVA":       2,
    "FALHA_SINALIZACAO":       2,
    "RISCO_AMBIENTAL":         2,
    "OBJETO_NAO_IDENTIFICADO": 2,
    "CONGESTIONAMENTO":        1,
    "INSPECAO_VISUAL":         1,
    "REPLANEJAMENTO_ROTA":     1,
}

def _enqueue(payload):
    rid = payload.get("request_id")
    if not rid or rid in state["queue"] or rid in state["seen_requests"]:
        return False
    if rid in state["active_missions"]:
        return False
    event = payload.get("event_type", "INSPECAO_VISUAL")
    sev   = PROBLEM_SEVERITY.get(event, payload.get("severity", 1))
    payload = {**payload,
               "severity":          sev,
               "original_severity": sev,
               "enqueued_at":       payload.get("enqueued_at") or time.time()}
    state["queue"][rid] = payload
    state["seen_requests"].add(rid)
    state["total_received"] += 1
    return True

def _sorted_queue():
    return sorted(
        state["queue"].values(),
        key=lambda r: (-r.get("severity", 1), r.get("enqueued_at", 0))
    )


# ── Snapshot ───────────────────────────────────────────────────────────────────

def publish_snapshot():
    with lock:
        q_list = _sorted_queue()
        payload = {
            "ts":              now_iso(),
            "queue":           q_list,
            "queue_size":      len(q_list),
            "active_missions": dict(state["active_missions"]),
            "active_requests": dict(state["active_requests"]),
            "drones":          dict(state["drones"]),
            "brokers":         list(state["known_brokers"].keys()),
            "total_received":  state["total_received"],
            "total_dispatched":state["total_dispatched"],
        }
    pub(T_LISTA_GERAL, payload)
    pub(T_DASHBOARD,   payload)

# ── Despacho ───────────────────────────────────────────────────────────────────

def try_dispatch():
    with lock:
        if not state["is_coordinator"]:
            return
        now      = time.time()
        q_sorted = _sorted_queue()
        for req in q_sorted:
            rid = req["request_id"]
            if rid in state["active_missions"]:
                continue
            drone_id = next(
                (did for did, d in state["drones"].items()
                 if d.get("status") == "idle"
                 and (now - d.get("last_seen", 0)) < 25),
                None
            )
            if drone_id is None:
                log.warning("⏳ Sem drone disponível. Aguardando.")
                break
            state["drones"][drone_id]["status"] = "dispatched"
            state["active_missions"][rid]       = drone_id
            state["active_requests"][rid]       = {**req, "drone_id": drone_id, "status": "active"}
            state["total_dispatched"] += 1
            sev       = req.get("severity", 1)
            sev_label = {3:"🔴 GRAVE", 2:"🟡 MODERADO", 1:"🟢 LEVE"}.get(sev, "?")
            log.info(f"🚁 DESPACHANDO {drone_id} → {rid} [{sev_label}] {req.get('event_type','?')} | {req.get('sector','?')}")
            pub(T_DRONE_CMD, {
                "cmd": "DISPATCH", "drone_id": drone_id, "request_id": rid,
                "sector": req["sector"], "event_type": req.get("event_type", "MONITORAMENTO"),
                "priority": req.get("priority", "MEDIA"), "severity": sev, "ts": now_iso()
            })
            pub(T_REQUEST_ACK, {
                "request_id": rid, "drone_id": drone_id, "status": "DISPATCHED",
                "sector": req["sector"], "severity": sev,
                "event_type": req.get("event_type"), "ts": now_iso()
            })
            state["queue"].pop(rid, None)
    publish_snapshot()


# ── Escalação de severidade ────────────────────────────────────────────────────

def escalation_loop():
    while True:
        time.sleep(5)
        now     = time.time()
        changed = False
        with lock:
            for req in state["queue"].values():
                age     = now - req.get("enqueued_at", now)
                orig    = req.get("original_severity", req.get("severity", 1))
                old_sev = req["severity"]
                if age >= 40:   new_sev = 3
                elif age >= 20: new_sev = max(orig + 1, 2)
                else:           new_sev = orig
                new_sev = min(new_sev, 3)
                if new_sev != old_sev:
                    req["severity"] = new_sev
                    lbl = {3:"🔴 GRAVE", 2:"🟡 MODERADO"}.get(new_sev, "?")
                    log.info(f"⬆️  Escalando {req.get('event_type','?')} ({req.get('sector','?')}) sev {old_sev}→{new_sev} [{lbl}] (na fila há {int(age)}s)")
                    changed = True
        if changed:
            threading.Thread(target=try_dispatch, daemon=True).start()


# ── Sensor ─────────────────────────────────────────────────────────────────────

EVENTS = [
    ("COLISAO_IMINENTE","CRITICA",3), ("BLOQUEIO_ROTA","CRITICA",3),
    ("DERRAMAMENTO_OLEO","CRITICA",3), ("EMBARCACAO_DERIVA","ALTA",2),
    ("FALHA_SINALIZACAO","ALTA",2), ("RISCO_AMBIENTAL","ALTA",2),
    ("OBJETO_NAO_IDENTIFICADO","MEDIA",2), ("CONGESTIONAMENTO","MEDIA",1),
    ("INSPECAO_VISUAL","BAIXA",1), ("REPLANEJAMENTO_ROTA","BAIXA",1),
]

def sensor_loop():
    time.sleep(random.uniform(3, 7))
    log.info(f"📡 Sensor do {SECTOR_NAME} ativo (intervalo≈{SENSOR_INTERVAL}s)")
    while True:
        time.sleep(SENSOR_INTERVAL + random.uniform(-3, 3))
        event_type, priority, severity = random.choice(EVENTS)
        req_id  = f"req-{uuid.uuid4().hex[:8]}"
        payload = {
            "request_id": req_id, "sector": SECTOR_NAME, "broker_id": BROKER_ID,
            "event_type": event_type, "priority": priority, "severity": severity,
            "lat": round(random.uniform(25.5, 27.5), 4),
            "lon": round(random.uniform(56.0, 59.0), 4),
            "ts": now_iso()
        }
        sev_label = {3:"🔴 GRAVE", 2:"🟡 MODERADO", 1:"🟢 LEVE"}.get(severity, "?")
        log.info(f"📡 [{sev_label}] {event_type} gerado (req={req_id})")
        pub(T_SENSOR_DATA, {"broker_id": BROKER_ID, "sector": SECTOR_NAME,
                            "event_type": event_type, "priority": priority,
                            "severity": severity, "ts": now_iso()})
        pub(T_REQUESTS, payload)


# ── Heartbeat e watchdog ───────────────────────────────────────────────────────

def heartbeat_loop():
    while True:
        time.sleep(5)
        with lock:
            is_coord = state["is_coordinator"]
            coord_id = state["coordinator_id"]
            known    = dict(state["known_brokers"])
        pub(T_HEARTBEAT, {
            "broker_id": BROKER_ID, "sector": SECTOR_NAME,
            "is_coordinator": is_coord, "ra_clock": ra.clock, "ts": now_iso()
        })
        if coord_id and coord_id != BROKER_ID:
            last = known.get(coord_id, 0)
            if time.time() - last > 15:
                log.warning(f"⚠️  Coordenador {coord_id} silencioso — iniciando eleição RA...")
                with lock:
                    state["coordinator_id"] = None
                    state["is_coordinator"] = False
                ra.remove_peer(coord_id)
                _remove_broker(coord_id)
                threading.Thread(target=ra_request_cs, daemon=True).start()
        now  = time.time()
        dead = [bid for bid, ts in known.items() if bid != BROKER_ID and now - ts > 20]
        for bid in dead:
            _remove_broker(bid)

def _remove_broker(broker_id):
    with lock:
        if broker_id not in state["known_brokers"]:
            return
        del state["known_brokers"][broker_id]

        # Requisições ainda na fila
        rids_fila = [
            rid for rid, req in state["queue"].items()
            if req.get("broker_id") == broker_id
        ]
        for rid in rids_fila:
            del state["queue"][rid]
            state["seen_requests"].discard(rid)

        rids_missoes = [
            rid for rid, req in state["active_requests"].items()
            if req.get("broker_id") == broker_id
        ]
        for rid in rids_missoes:
            drone_id = state["active_missions"].pop(rid, None)
            state["active_requests"].pop(rid, None)
            state["seen_requests"].discard(rid)
            if drone_id and drone_id in state["drones"]:
                log.warning(f"🔄 Drone {drone_id} liberado (missão {rid} do broker offline {broker_id})")
                state["drones"][drone_id]["status"] = "idle"

        rids_removidos = rids_fila + rids_missoes

        if rids_removidos:
            log.warning(
                f"🗑️  {len(rids_fila)} req(s) da fila e "
                f"{len(rids_missoes)} missão(ões) ativa(s) do broker {broker_id} removidas"
            )

    ra.remove_peer(broker_id)
    pub(T_BROKER_OFFLINE, {"broker_id": broker_id, "reported_by": BROKER_ID, "ts": now_iso()})
    mostrar_broker_offline(broker_id)
    log.warning(f"🔴 Broker {broker_id} removido (offline)")
    if rids_removidos:
        mostrar_fila(f"BROKER OFFLINE — {broker_id[:12]}")

    with lock:
        is_coord = state["is_coordinator"]
    if is_coord and rids_missoes:
        threading.Thread(target=try_dispatch, daemon=True).start()


# ── Manutenção ─────────────────────────────────────────────────────────────────

def maintenance_loop():
    while True:
        time.sleep(10)
        with lock:
            if not state["is_coordinator"]:
                continue
            now = time.time()
            dead_drones = [did for did, d in state["drones"].items()
                           if (now - d.get("last_seen", 0)) > 12]
            for did in dead_drones:
                log.warning(f"🔴 Drone {did} removido (timeout)")
                del state["drones"][did]
                for rid in [r for r, d in state["active_missions"].items() if d == did]:
                    log.warning(f"⚠️  Missão {rid} devolvida à fila (drone offline)")
                    req = state["active_requests"].pop(rid, None)
                    del state["active_missions"][rid]
                    if req:
                        req.pop("drone_id", None)
                        req["status"] = "pending"
                        req["enqueued_at"] = req.get("enqueued_at") or time.time()
                        state["seen_requests"].discard(rid)
                        state["queue"][rid] = req
        threading.Thread(target=try_dispatch, daemon=True).start()


# ── Sync de fila ───────────────────────────────────────────────────────────────

def sincronizacao_inicial():
    log.info("Aguardando coordenador existente (5s)...")
    time.sleep(5)
    with lock:
        coord = state["coordinator_id"]
    if coord:
        log.info(f"Coordenador existente detectado: {coord} — pulando eleição inicial")
    else:
        log.info("Nenhum coordenador detectado — iniciando eleição")
        threading.Thread(target=ra_request_cs, daemon=True).start()

    for i in range(3):
        time.sleep(3)
        log.info(f"Sync inicial {i+1}/3...")
        pub(T_FILA_SYNC_REQ, {"from": BROKER_ID, "sector": SECTOR_NAME, "ts": now_iso()})
    log.info("Sync inicial concluída.")

# ── Callback de mensagens ──────────────────────────────────────────────────────

def on_message(topic, payload):
    if topic == T_REQUESTS:
        rid = payload.get("request_id")
        if not rid: return
        inserted = False
        with lock:
            inserted = _enqueue(payload)
        if inserted:
            sev       = PROBLEM_SEVERITY.get(payload.get("event_type",""), payload.get("severity",1))
            sev_label = {3:"🔴 GRAVE", 2:"🟡 MODERADO", 1:"🟢 LEVE"}.get(sev, "?")
            log.info(f"[{sev_label}] {payload.get('event_type','?')} do setor {payload.get('sector','?')} | req={rid}")
            mostrar_fila(f"NOVA — {payload.get('event_type','?')}")
            with lock:
                is_coord = state["is_coordinator"]
            if is_coord:
                threading.Thread(target=try_dispatch, daemon=True).start()

    elif topic == T_REQUEST_ACK:
        rid = payload.get("request_id")
        if rid:
            with lock:
                state["queue"].pop(rid, None)
            mostrar_fila(f"DESPACHADO — req {(rid or '')[:12]}...")

    elif topic == T_DRONE_STATUS:
        did = payload.get("drone_id")
        if not did: return
        now = time.time()
        with lock:
            prev_status  = state["drones"].get(did, {}).get("status")
            state["drones"][did] = {
                "status": payload.get("status", "idle"), "sector": payload.get("sector", "?"),
                "lat": payload.get("lat"), "lon": payload.get("lon"),
                "last_seen": now, "drone_id": did,
            }
            new_status   = payload.get("status")
            is_coord     = state["is_coordinator"]
            is_new_drone = prev_status is None

            if is_coord and new_status == "idle" and not is_new_drone and prev_status != "idle":
                freed = [r for r, d in state["active_missions"].items() if d == did]
                for rid in freed:
                    log.info(f"Drone {did} concluiu missão {rid} — liberado")
                    del state["active_missions"][rid]
                    state["active_requests"].pop(rid, None)

        if payload.get("status") == "idle":
            with lock:
                is_coord = state["is_coordinator"]
            if is_coord:
                threading.Thread(target=try_dispatch, daemon=True).start()
            publish_snapshot()

    elif topic == T_RA_ELECTION_REQ:
        sender = payload.get("from")
        clk    = payload.get("clock", 0)
        ra.tick(clk)
        if sender and sender != BROKER_ID:
            with lock:
                state["known_brokers"][sender] = time.time()
            ra.on_request(sender, clk, _send_ra_reply)

    elif topic == T_RA_ELECTION_REP:
        if payload.get("to") == BROKER_ID:
            ra.tick(payload.get("clock", 0))
            ra.on_reply(payload.get("from"))

    elif topic == T_COORDINATOR:
        new_coord = payload.get("coordinator_id")
        with lock:
            if new_coord != BROKER_ID:
                state["is_coordinator"] = False
            state["coordinator_id"] = new_coord
        log.info(f"Coordenador: {new_coord}")

    elif topic == T_HEARTBEAT:
        bid = payload.get("broker_id")
        if bid and bid != BROKER_ID:
            with lock:
                state["known_brokers"][bid] = time.time()
            ra.tick(payload.get("ra_clock", 0))

    elif topic == T_BROKER_OFFLINE:
        bid = payload.get("broker_id")
        if bid and bid != BROKER_ID:
            with lock:
                state["known_brokers"].pop(bid, None)

                rids_fila = [
                    rid for rid, req in state["queue"].items()
                    if req.get("broker_id") == bid
                ]
                for rid in rids_fila:
                    del state["queue"][rid]
                    state["seen_requests"].discard(rid)

                rids_missoes = [
                    rid for rid, req in state["active_requests"].items()
                    if req.get("broker_id") == bid
                ]
                for rid in rids_missoes:
                    drone_id = state["active_missions"].pop(rid, None)
                    state["active_requests"].pop(rid, None)
                    state["seen_requests"].discard(rid)
                    if drone_id and drone_id in state["drones"]:
                        state["drones"][drone_id]["status"] = "idle"

                rids_removidos = rids_fila + rids_missoes
                is_coord = state["is_coordinator"]

            ra.remove_peer(bid)
            log.warning(f"Broker {bid} offline (broadcast recebido)")
            if rids_removidos:
                log.warning(f"🗑️  {len(rids_removidos)} item(ns) do broker {bid} removidos")
                mostrar_fila(f"BROKER OFFLINE — {bid[:12]}")
            mostrar_broker_offline(bid)

            if is_coord and rids_missoes:
                threading.Thread(target=try_dispatch, daemon=True).start()

    elif topic == T_FILA_SYNC_REQ:
        requester = payload.get("from")
        if requester and requester != BROKER_ID:
            with lock:
                fila = list(state["queue"].values())
                am   = dict(state["active_missions"])
                ar   = dict(state["active_requests"])
            pub(T_FILA_SYNC_RES, {
                "from": BROKER_ID, "to": requester,
                "fila": fila,
                "active_missions": am,
                "active_requests": ar,
                "ts": now_iso()
            })

    elif topic == T_FILA_SYNC_RES:
        if payload.get("to") != BROKER_ID: return
        novos = 0
        with lock:
            for req in payload.get("fila", []):
                if _enqueue(req):
                    novos += 1

            for rid, did in payload.get("active_missions", {}).items():
                if rid in state["active_missions"]:
                    continue
                drone = state["drones"].get(did)
                if drone and drone.get("status") not in ("idle", None):
                    state["active_missions"][rid] = did
                    ar = payload.get("active_requests", {}).get(rid)
                    if ar:
                        state["active_requests"][rid] = ar
                    state["seen_requests"].add(rid)
                    state["queue"].pop(rid, None)

        if novos:
            log.info(f"Sync de {payload.get('from')}: +{novos} itens")
            mostrar_fila("SYNC RECEBIDA")
            with lock:
                is_coord = state["is_coordinator"]
            if is_coord:
                threading.Thread(target=try_dispatch, daemon=True).start()


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    print()
    print(f"  ┌{'─'*54}┐")
    print(f"  │ {'BROKER INICIADO — ESTREITO DE ORMUZ':^52} │")
    print(f"  ├{'─'*54}┤")
    print(f"  │  broker_id   : {BROKER_ID:<37} │")
    print(f"  │  sector      : {SECTOR_NAME:<37} │")
    print(f"  │  transporte  : {'UDP Multicast ' + MC_GROUP + ':' + str(MC_PORT):<37} │")
    print(f"  │  sensor_int  : {str(SENSOR_INTERVAL) + 's':<37} │")
    print(f"  └{'─'*54}┘")
    print()

    with lock:
        state["known_brokers"][BROKER_ID] = time.time()

    # Inicia recepção de mensagens
    threading.Thread(target=_recv_loop, daemon=True).start()
    time.sleep(1)

    threading.Thread(target=sincronizacao_inicial, daemon=True).start()
    threading.Thread(target=heartbeat_loop,        daemon=True).start()
    threading.Thread(target=sensor_loop,           daemon=True).start()
    threading.Thread(target=escalation_loop,       daemon=True).start()
    threading.Thread(target=maintenance_loop,      daemon=True).start()

    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
