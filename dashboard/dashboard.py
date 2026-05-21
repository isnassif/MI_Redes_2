import json, time, curses, threading, logging
import socket, struct
from collections import deque
from datetime import datetime

# ── Multicast ─────────────────────────────────────────────────
MC_GROUP = "224.1.1.1"
MC_PORT  = 5007

TOPICS_INTEREST = {
    "ormuz/lista_geral", "ormuz/dashboard", "ormuz/drones/status",
    "ormuz/requests",    "ormuz/requests/ack", "ormuz/heartbeat",
    "ormuz/coordinator", "ormuz/sensors",
}

state = {
    "requests": {}, "drones": {}, "brokers": {}, "log": deque(maxlen=200),
    "total_received": 0, "total_dispatched": 0, "queue_size": 0, "active_count": 0,
    "coordinator": "—",
}
state_lock = threading.Lock()
SEVERITY_LABEL = {3: "GRAVE", 2: "MODERADO", 1: "LEVE"}

def now_str(): return datetime.now().strftime("%H:%M:%S")
def add_log(msg):
    with state_lock: state["log"].appendleft(f"[{now_str()}] {msg}")


# ── Transporte UDP Multicast ───────────────────────────────────

_receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
_receiver.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
try:
    _receiver.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
except AttributeError:
    pass
_receiver.bind(("", MC_PORT))
_mreq = struct.pack("4sL", socket.inet_aton(MC_GROUP), socket.INADDR_ANY)
_receiver.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, _mreq)

def _recv_loop():
    while True:
        try:
            data, _ = _receiver.recvfrom(65535)
            msg     = json.loads(data.decode())
            topic   = msg.get("topic", "")
            payload = msg.get("payload", {})
            if topic in TOPICS_INTEREST:
                threading.Thread(target=on_message, args=(topic, payload), daemon=True).start()
        except Exception:
            pass


# ── Processamento de mensagens ─────────────────────────────────

def on_message(topic, payload):
    now = time.time()
    with state_lock:
        if topic in ("ormuz/lista_geral", "ormuz/dashboard"):
            state["total_received"]   = payload.get("total_received",  state["total_received"])
            state["total_dispatched"] = payload.get("total_dispatched",state["total_dispatched"])
            state["queue_size"]       = payload.get("queue_size", 0)
            active_missions           = payload.get("active_missions", {})
            state["active_count"]     = len(active_missions)

            queue_ids  = {r["request_id"] for r in payload.get("queue", [])}
            active_ids = set(active_missions.keys())
            known_ids  = queue_ids | active_ids
            stale = [rid for rid in state["requests"] if rid not in known_ids]
            for rid in stale:
                del state["requests"][rid]

            for req in payload.get("queue", []):
                rid      = req["request_id"]
                existing = state["requests"].get(rid, {})
                state["requests"][rid] = {
                    **req, "status": "pending",
                    "enqueued_at": existing.get("enqueued_at") or req.get("enqueued_at") or now,
                }

            active_requests = payload.get("active_requests", {})
            for rid, did in active_missions.items():
                existing = state["requests"].get(rid, {})
                details  = active_requests.get(rid, {})
                state["requests"][rid] = {
                    **details,
                    **{k: v for k, v in existing.items() if v is not None},
                    "request_id": rid, "drone_id": did, "status": "active",
                }

            state["drones"] = {
                did: {**d, "drone_id": did, "last_seen": now}
                for did, d in payload.get("drones", {}).items()
            }
            for bid in payload.get("brokers", []):
                if bid not in state["brokers"]:
                    state["brokers"][bid] = {"id": bid, "sector": "?", "coord": False, "last_seen": now}

        elif topic == "ormuz/drones/status":
            did = payload.get("drone_id")
            if did: state["drones"][did] = {**payload, "last_seen": now}

        elif topic == "ormuz/requests":
            rid = payload.get("request_id")
            ev  = payload.get("event_type", "?")
            sev = payload.get("severity", 1)
            state["log"].appendleft(f"[{now_str()}] ↓ [{SEVERITY_LABEL.get(sev,'?')}] {ev} | {payload.get('sector','?')}")
            if rid and rid not in state["requests"]:
                state["requests"][rid] = {**payload, "status": "pending"}

        elif topic == "ormuz/requests/ack":
            rid = payload.get("request_id")
            did = payload.get("drone_id")
            if rid and rid in state["requests"]:
                state["requests"][rid]["status"]   = "dispatched"
                state["requests"][rid]["drone_id"] = did
            state["log"].appendleft(f"[{now_str()}] ↑ DESPACHO ..{str(rid)[-6:]} → ..{str(did)[-6:]}")

        elif topic == "ormuz/heartbeat":
            bid  = payload.get("broker_id")
            sect = payload.get("sector", "?")
            if bid:
                state["brokers"][bid] = {
                    "id": bid, "sector": sect,
                    "coord": payload.get("is_coordinator", False), "last_seen": now,
                }

        elif topic == "ormuz/coordinator":
            cid  = payload.get("coordinator_id", "?")
            sect = payload.get("sector", "?")
            state["coordinator"] = f"{cid[-8:]} ({sect})"
            state["log"].appendleft(f"[{now_str()}] 👑 Coord: {cid[-8:]} ({sect})")

        elif topic == "ormuz/sensors":
            ev  = payload.get("event_type", "?")
            sev = payload.get("severity", 1)
            state["log"].appendleft(f"[{now_str()}] 📡 [{SEVERITY_LABEL.get(sev,'?')}] {ev} | {payload.get('sector','?')}")


# ── TUI ────────────────────────────────────────────────────────

def run_tui(stdscr):
    curses.curs_set(0)
    stdscr.nodelay(True)
    stdscr.timeout(200)
    curses.start_color()
    curses.use_default_colors()
    curses.init_pair(1, curses.COLOR_CYAN,    -1)
    curses.init_pair(2, curses.COLOR_GREEN,   -1)
    curses.init_pair(3, curses.COLOR_YELLOW,  -1)
    curses.init_pair(4, curses.COLOR_RED,     -1)
    curses.init_pair(5, curses.COLOR_WHITE,   -1)
    curses.init_pair(6, curses.COLOR_BLACK,   curses.COLOR_CYAN)
    curses.init_pair(7, curses.COLOR_MAGENTA, -1)

    CA = curses.color_pair(1) | curses.A_BOLD
    CG = curses.color_pair(2)
    CW = curses.color_pair(3)
    CR = curses.color_pair(4)
    CN = curses.color_pair(5)
    CH = curses.color_pair(6) | curses.A_BOLD
    CD = curses.A_DIM
    CM = curses.color_pair(7)
    CB = curses.A_BOLD
    SC = {3: CR, 2: CW, 1: CG}

    def put(y, x, txt, at=0):
        H, C = stdscr.getmaxyx()
        if y < 0 or y >= H or x < 0 or x >= C: return
        allowed = C - x - 1
        if allowed <= 0: return
        try: stdscr.addstr(y, x, str(txt)[:allowed], at)
        except: pass

    def vline(x, y_start, y_end):
        H, C = stdscr.getmaxyx()
        for y in range(y_start, min(y_end, H - 1)):
            try: stdscr.addch(y, x, curses.ACS_VLINE, CD)
            except: pass

    while True:
        key = stdscr.getch()
        if key == ord('q'): break
        if key == curses.KEY_RESIZE:
            curses.update_lines_cols()
            stdscr.clear()

        H, C = stdscr.getmaxyx()
        stdscr.erase()
        now = time.time()

        with state_lock:
            t_rx    = state["total_received"]
            t_tx    = state["total_dispatched"]
            q_size  = state["queue_size"]
            a_count = state["active_count"]
            coord   = state["coordinator"]
            reqs    = dict(state["requests"])
            drones  = dict(state["drones"])
            brokers = dict(state["brokers"])

        hdr = " ORMUZ — DASHBOARD TERMINAL "
        try:
            stdscr.addstr(0, 0, " " * max(0, C - 1), CH)
            stdscr.addstr(0, max(0, (C - len(hdr)) // 2), hdr, CH)
            cs = "● UDP MULTICAST"
            stdscr.addstr(0, max(0, C - len(cs) - 2), cs, CG)
        except: pass

        stats = f" Rx:{t_rx}  Tx:{t_tx}  Fila:{q_size}  Ativas:{a_count}  Coord:{coord}  [q]=sair"
        put(1, 0, stats, CD)
        try: stdscr.addstr(2, 0, "─" * max(0, C - 1), CD)
        except: pass

        LW = 28
        MW = max(10, C - LW)
        TY = 3
        vline(LW - 1, TY - 1, H - 1)

        put(TY, 1, "[ DRONES ]", CB)
        dlist = sorted(drones.values(), key=lambda d: str(d.get("drone_id", "")))
        max_drone_rows = (H - TY - 2) // 2
        for i, d in enumerate(dlist[:max_drone_rows]):
            y = TY + 1 + i
            if y >= H - 1: break
            did = str(d.get("drone_id", "?"))[-8:]
            st  = d.get("status", "?")
            age = now - d.get("last_seen", now)
            sc2 = CG if st == "idle" else (CW if st in ("dispatched","returning") else (CR if st == "offline" else CM))
            ss  = {"idle":"LIVRE","dispatched":"DESP","on_mission":"MISS","returning":"RET","offline":"OFF"}.get(st, st[:4].upper())
            put(y, 1,      did, CD if age > 20 else CN)
            put(y, LW - 7, ss,  sc2)
        if not dlist:
            put(TY + 1, 1, "sem drones", CD)

        broker_y = TY + max_drone_rows + 2
        if broker_y < H - 2:
            try: stdscr.addstr(broker_y - 1, 0, "─" * (LW - 1), CD)
            except: pass
            put(broker_y - 1, 1, "[ SETORES ]", CB)
            blist = sorted(brokers.values(), key=lambda b: b.get("sector", ""))
            for i, b in enumerate(blist):
                y = broker_y + i
                if y >= H - 1: break
                sect = b.get("sector", "?")
                is_c = b.get("coord", False)
                age  = now - b.get("last_seen", now)
                col  = CA if is_c else (CD if age > 15 else CN)
                put(y, 1, f"{sect[:LW-5]}{'  *' if is_c else ''}", col)
            if not blist:
                put(broker_y, 1, "sem setores", CD)

        put(TY,     LW + 1, "[ FILA DE REQUISICOES ]", CB)
        put(TY + 1, LW + 1, f"{'EVENTO':<20} {'SETOR':<13} {'GRAV':<6} STATUS", CB | CD)
        try: stdscr.addstr(TY + 2, LW, "─" * (MW - 1), CD)
        except: pass

        def rk(r):
            return ({"pending": 0, "active": 1, "dispatched": 2}.get(r.get("status","pending"), 0), -r.get("severity", 1))
        rl = sorted(reqs.values(), key=rk)

        CE = min(20, MW // 3)
        CS = min(13, MW // 4)
        CV = 6

        for i, req in enumerate(rl):
            y = TY + 3 + i
            if y >= H - 1: break
            ev          = req.get("event_type", "?")[:CE]
            sec         = req.get("sector",     "?")[:CS]
            sev         = req.get("severity", 1)
            st          = req.get("status",   "pending")
            sl2         = {3:"GRAVE", 2:"MOD", 1:"LEVE"}.get(sev, "?")
            drone_id    = req.get("drone_id", "")
            drone_short = str(drone_id)[-8:] if drone_id else ""
            eq_at       = req.get("enqueued_at")
            if st == "pending" and eq_at:
                age_s   = int(now - eq_at)
                age_str = f"{age_s}s" if age_s < 60 else f"{age_s//60}m{age_s%60:02d}s"
            else:
                age_str = ""
            stl     = {"pending":"NA FILA","active":"EM MISS","dispatched":"DESP"}.get(st, st)
            mk      = {3:"!!!",2:"!! ",1:"!  "}.get(sev,"   ")
            col     = CD if st == "dispatched" else SC.get(sev, CN)
            stc     = CW if st == "active" else (CD if st == "dispatched" else CG)
            age_col = CR if (eq_at and (now-eq_at) >= 40) else (CW if (eq_at and (now-eq_at) >= 20) else CD)

            put(y, LW + 1, mk, col)
            put(y, LW + 4, f"{ev:<{CE}} {sec:<{CS}}", CD if st == "dispatched" else CN)
            put(y, LW + 4 + CE + CS + 2,            f"{sl2:<{CV}}", col)
            put(y, LW + 4 + CE + CS + 2 + CV + 1,   stl,            stc)
            if drone_short:
                put(y, LW + 4 + CE + CS + 2 + CV + 9, f"<- {drone_short}", CA if st == "active" else CD)
            elif age_str:
                put(y, LW + 4 + CE + CS + 2 + CV + 9, f"[{age_str}]", age_col)

        if not rl:
            put(TY + 4, LW + (MW // 2) - 11, "Aguardando requisicoes...", CD)

        footer = f" ORMUZ TUI | {now_str()} | {H}x{C} | q=sair "
        try: stdscr.addstr(H - 1, 0, footer.ljust(C - 1), CD)
        except: pass

        stdscr.refresh()
        time.sleep(0.1)


# ── Main ───────────────────────────────────────────────────────

def main():
    logging.disable(logging.CRITICAL)
    threading.Thread(target=_recv_loop, daemon=True).start()
    time.sleep(0.5)
    curses.wrapper(run_tui)

if __name__ == "__main__":
    main()
