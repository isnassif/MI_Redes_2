import time
import threading
import unittest
from unittest.mock import MagicMock, patch, call
from copy import deepcopy

from RicartAgrawala import RicartAgrawala, RELEASED, WANTED, HELD


def make_state(broker_id="broker-test", sector="Setor-Test"):
    return {
        "broker_id":        broker_id,
        "sector":           sector,
        "is_coordinator":   False,
        "coordinator_id":   None,
        "known_brokers":    {},
        "queue":            {},
        "drones":           {},
        "active_missions":  {},
        "active_requests":  {},
        "seen_requests":    set(),
        "total_received":   0,
        "total_dispatched": 0,
    }


def make_request(rid="req-001", sector="Setor-A", broker_id="broker-x",
                 event_type="INSPECAO_VISUAL", severity=1, enqueued_at=None):
    return {
        "request_id":        rid,
        "sector":            sector,
        "broker_id":         broker_id,
        "event_type":        event_type,
        "priority":          "MEDIA",
        "severity":          severity,
        "original_severity": severity,
        "enqueued_at":       enqueued_at or time.time(),
        "lat":               26.0,
        "lon":               57.0,
        "ts":                "2024-01-01T00:00:00Z",
    }


def make_drone(drone_id="drone-001", status="idle", last_seen=None):
    return {
        "drone_id":  drone_id,
        "status":    status,
        "sector":    "Base-Alpha",
        "lat":       26.5,
        "lon":       57.5,
        "last_seen": last_seen or time.time(),
    }


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


def enqueue(state, payload):
    rid = payload.get("request_id")
    if not rid:
        return False
    if rid in state["queue"] or rid in state["seen_requests"]:
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


def sorted_queue(state):
    return sorted(
        state["queue"].values(),
        key=lambda r: (-r.get("severity", 1), r.get("enqueued_at", 0))
    )


def try_dispatch(state, published_cmds):
    if not state["is_coordinator"]:
        return
    now      = time.time()
    q_sorted = sorted_queue(state)
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
            break
        state["drones"][drone_id]["status"] = "dispatched"
        state["active_missions"][rid]       = drone_id
        state["active_requests"][rid]       = {**req, "drone_id": drone_id, "status": "active"}
        state["total_dispatched"] += 1
        state["queue"].pop(rid, None)
        published_cmds.append({"drone_id": drone_id, "request_id": rid})


def remove_broker(state, broker_id):
    if broker_id not in state["known_brokers"]:
        return [], []
    del state["known_brokers"][broker_id]

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
            state["drones"][drone_id]["status"] = "idle"

    return rids_fila, rids_missoes


def apply_escalation(state, now=None):
    now     = now or time.time()
    changed = False
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
            changed = True
    return changed


# ══════════════════════════════════════════════════════════════════════════════
# BLOCO 1 — Testes Unitários: RicartAgrawala
# ══════════════════════════════════════════════════════════════════════════════

class TestRicartAgrawalaRelogio(unittest.TestCase):

    def test_clock_inicia_zero(self):
        ra = RicartAgrawala("n1")
        self.assertEqual(ra.clock, 0)

    def test_tick_sem_incoming_incrementa(self):
        ra = RicartAgrawala("n1")
        val = ra.tick()
        self.assertEqual(val, 1)
        self.assertEqual(ra.clock, 1)

    def test_tick_com_incoming_maior(self):
        ra = RicartAgrawala("n1")
        val = ra.tick(10)
        self.assertEqual(val, 11)   # max(0,10)+1

    def test_tick_com_incoming_menor(self):
        ra = RicartAgrawala("n1")
        ra.tick(5)          # clock = 6
        val = ra.tick(2)    # max(6,2)+1 = 7
        self.assertEqual(val, 7)

    def test_tick_acumulado(self):
        ra = RicartAgrawala("n1")
        ra.tick()   # 1
        ra.tick()   # 2
        ra.tick()   # 3
        self.assertEqual(ra.clock, 3)


class TestRicartAgrawalaSeçãoCrítica(unittest.TestCase):

    def test_sozinho_entra_imediatamente(self):
        ra   = RicartAgrawala("n1")
        sent = []
        ok   = ra.request_cs(set(), lambda clk: sent.append(clk))
        self.assertTrue(ok)
        self.assertEqual(ra._state, HELD)
        self.assertEqual(sent, [])

    def test_estado_initial_e_released(self):
        ra = RicartAgrawala("n1")
        self.assertEqual(ra._state, RELEASED)

    def test_request_quando_ja_held_retorna_false(self):
        ra = RicartAgrawala("n1")
        ra.request_cs(set(), lambda c: None)
        ok = ra.request_cs(set(), lambda c: None)
        self.assertFalse(ok)

    def test_release_envia_diferidos(self):
        ra      = RicartAgrawala("n1")
        replies = []

        ra._state = HELD
        ra.on_request("n2", 5, lambda pid, clk: replies.append((pid, clk)))
        self.assertIn(("n2",), ra._deferred)

        ra.release_cs(lambda pid, clk: replies.append((pid, clk)))
        self.assertEqual(ra._state, RELEASED)
        self.assertTrue(any(r[0] == "n2" for r in replies))
        self.assertEqual(ra._deferred, [])

    def test_peers_com_reply_de_todos_libera(self):
        ra   = RicartAgrawala("n1")
        sent = []

        def fake_send(clk):
            # simula os outros respondendo imediatamente
            threading.Timer(0.05, lambda: ra.on_reply("n2")).start()
            threading.Timer(0.05, lambda: ra.on_reply("n3")).start()
            sent.append(clk)

        ok = ra.request_cs({"n2", "n3"}, fake_send)
        self.assertTrue(ok)
        self.assertEqual(ra._state, HELD)

    def test_timeout_sem_replies_retorna_false(self):
        """Sem ninguém responder, deve dar timeout (5s — mockado para 0.1s)."""
        ra = RicartAgrawala("n1")
        with patch.object(ra._granted, "wait", return_value=False):
            ok = ra.request_cs({"n2"}, lambda c: None)
        self.assertFalse(ok)
        self.assertEqual(ra._state, RELEASED)


class TestRicartAgrawalaOnRequest(unittest.TestCase):
 
    def test_released_responde_imediatamente(self):
        ra      = RicartAgrawala("n1")
        replies = []
        ra.on_request("n2", 3, lambda pid, clk: replies.append(pid))
        self.assertIn("n2", replies)
        self.assertEqual(ra._deferred, [])

    def test_held_defere(self):
        ra = RicartAgrawala("n1")
        ra._state = HELD
        replies   = []
        ra.on_request("n2", 3, lambda pid, clk: replies.append(pid))
        self.assertEqual(replies, [])
        self.assertIn(("n2",), ra._deferred)

    def test_wanted_clock_menor_defere(self):
        ra = RicartAgrawala("n1")
        ra._state     = WANTED
        ra._req_clock = 2
        replies       = []
        ra.on_request("n2", 5, lambda pid, clk: replies.append(pid))
        self.assertEqual(replies, [])
        self.assertIn(("n2",), ra._deferred)

    def test_wanted_clock_maior_responde(self):
        ra = RicartAgrawala("n1")
        ra._state     = WANTED
        ra._req_clock = 10
        replies       = []
        ra.on_request("n2", 3, lambda pid, clk: replies.append(pid))
        self.assertIn("n2", replies)

    def test_wanted_clock_igual_desempate_por_id(self):
        ra = RicartAgrawala("n1")
        ra._state     = WANTED
        ra._req_clock = 5
        replies       = []
        ra.on_request("n2", 5, lambda pid, clk: replies.append(pid))
        self.assertEqual(replies, [])   # n1 < n2, defere


class TestRicartAgrawalaRemovePeer(unittest.TestCase):
 
    def test_remove_peer_libera_pending(self):
        ra = RicartAgrawala("n1")
        ra._state           = WANTED
        ra._pending_replies = {"n2", "n3"}
        ra._granted.clear()

        ra.remove_peer("n2")
        self.assertNotIn("n2", ra._pending_replies)

    def test_remove_ultimo_peer_libera_granted(self):
        ra = RicartAgrawala("n1")
        ra._state           = WANTED
        ra._pending_replies = {"n2"}
        ra._granted.clear()

        ra.remove_peer("n2")
        self.assertTrue(ra._granted.is_set())

    def test_remove_peer_limpa_deferred(self):
        ra = RicartAgrawala("n1")
        ra._deferred = [("n2",), ("n3",)]
        ra.remove_peer("n2")
        self.assertNotIn(("n2",), ra._deferred)
        self.assertIn(("n3",), ra._deferred)


# ══════════════════════════════════════════════════════════════════════════════
# BLOCO 2 — Testes Unitários: Fila do Broker
# ══════════════════════════════════════════════════════════════════════════════

class TestFila(unittest.TestCase):

    def test_enqueue_simples(self):
        state = make_state()
        req   = make_request("req-001")
        ok    = enqueue(state, req)
        self.assertTrue(ok)
        self.assertIn("req-001", state["queue"])
        self.assertEqual(state["total_received"], 1)

    def test_enqueue_duplicado_ignorado(self):
        state = make_state()
        req   = make_request("req-001")
        enqueue(state, req)
        ok = enqueue(state, req)
        self.assertFalse(ok)
        self.assertEqual(len(state["queue"]), 1)
        self.assertEqual(state["total_received"], 1)

    def test_enqueue_sem_request_id(self):
        state = make_state()
        ok    = enqueue(state, {"event_type": "INSPECAO_VISUAL"})
        self.assertFalse(ok)
        self.assertEqual(len(state["queue"]), 0)

    def test_enqueue_seen_request_ignorado(self):
        state = make_state()
        state["seen_requests"].add("req-001")
        ok = enqueue(state, make_request("req-001"))
        self.assertFalse(ok)

    def test_enqueue_missao_ativa_ignorado(self):
        state = make_state()
        state["active_missions"]["req-001"] = "drone-001"
        ok = enqueue(state, make_request("req-001"))
        self.assertFalse(ok)

    def test_severidade_corrigida_pelo_event_type(self):
        state = make_state()
        req   = make_request("req-001", event_type="COLISAO_IMINENTE", severity=1)
        enqueue(state, req)
        self.assertEqual(state["queue"]["req-001"]["severity"], 3)

    def test_severidade_evento_desconhecido_usa_payload(self):
        state = make_state()
        req   = make_request("req-001", event_type="EVENTO_DESCONHECIDO", severity=2)
        enqueue(state, req)
        self.assertEqual(state["queue"]["req-001"]["severity"], 2)

    def test_ordenacao_por_severidade(self):
        state = make_state()
        t0    = time.time()
        enqueue(state, make_request("req-low",  event_type="INSPECAO_VISUAL",  severity=1, enqueued_at=t0))
        enqueue(state, make_request("req-high", event_type="COLISAO_IMINENTE", severity=3, enqueued_at=t0+1))
        enqueue(state, make_request("req-mid",  event_type="EMBARCACAO_DERIVA",severity=2, enqueued_at=t0+2))

        q = sorted_queue(state)
        self.assertEqual(q[0]["request_id"], "req-high")
        self.assertEqual(q[1]["request_id"], "req-mid")
        self.assertEqual(q[2]["request_id"], "req-low")

    def test_ordenacao_mesma_severidade_por_tempo(self):
        state = make_state()
        t0    = time.time()
        enqueue(state, make_request("req-novo",   severity=1, enqueued_at=t0+10))
        enqueue(state, make_request("req-antigo", severity=1, enqueued_at=t0))

        q = sorted_queue(state)
        self.assertEqual(q[0]["request_id"], "req-antigo")

    def test_seen_requests_atualizado(self):
        state = make_state()
        enqueue(state, make_request("req-001"))
        self.assertIn("req-001", state["seen_requests"])


# ══════════════════════════════════════════════════════════════════════════════
# BLOCO 3 — Testes Unitários: Escalação de Severidade
# ══════════════════════════════════════════════════════════════════════════════

class TestEscalacao(unittest.TestCase):

    def _req_com_idade(self, age_s, severity=1, event_type="INSPECAO_VISUAL"):
        req = make_request(event_type=event_type, severity=severity)
        req["original_severity"] = severity
        req["enqueued_at"]       = time.time() - age_s
        return req

    def test_sem_escalacao_abaixo_20s(self):
        state = make_state()
        req   = self._req_com_idade(10, severity=1)
        state["queue"]["r1"] = req
        changed = apply_escalation(state)
        self.assertFalse(changed)
        self.assertEqual(state["queue"]["r1"]["severity"], 1)

    def test_escalacao_para_moderado_aos_20s(self):
        state = make_state()
        req   = self._req_com_idade(25, severity=1)
        state["queue"]["r1"] = req
        changed = apply_escalation(state)
        self.assertTrue(changed)
        self.assertEqual(state["queue"]["r1"]["severity"], 2)

    def test_escalacao_para_grave_aos_40s(self):
        state = make_state()
        req   = self._req_com_idade(45, severity=1)
        state["queue"]["r1"] = req
        changed = apply_escalation(state)
        self.assertTrue(changed)
        self.assertEqual(state["queue"]["r1"]["severity"], 3)

    def test_nao_ultrapassa_3(self):
        state = make_state()
        req   = self._req_com_idade(60, severity=3)
        req["original_severity"] = 3
        state["queue"]["r1"] = req
        apply_escalation(state)
        self.assertEqual(state["queue"]["r1"]["severity"], 3)

    def test_ja_moderado_nao_regride(self):
        """Evento que já era sev=2 não volta para 1 mesmo novo na fila."""
        state = make_state()
        req   = self._req_com_idade(5, severity=2)
        req["original_severity"] = 2
        state["queue"]["r1"] = req
        changed = apply_escalation(state)
        self.assertFalse(changed)
        self.assertEqual(state["queue"]["r1"]["severity"], 2)

    def test_multiplos_itens_na_fila(self):
        state = make_state()
        state["queue"]["r1"] = self._req_com_idade(50, severity=1)   # → 3
        state["queue"]["r2"] = self._req_com_idade(25, severity=1)   # → 2
        state["queue"]["r3"] = self._req_com_idade(5,  severity=1)   # → 1
        apply_escalation(state)
        self.assertEqual(state["queue"]["r1"]["severity"], 3)
        self.assertEqual(state["queue"]["r2"]["severity"], 2)
        self.assertEqual(state["queue"]["r3"]["severity"], 1)


# ══════════════════════════════════════════════════════════════════════════════
# BLOCO 4 — Testes Unitários: Despacho de Drones
# ══════════════════════════════════════════════════════════════════════════════

class TestDespacho(unittest.TestCase):

    def test_nao_coordenador_nao_despacha(self):
        state = make_state()
        enqueue(state, make_request("req-001"))
        state["drones"]["d1"] = make_drone("d1", "idle")
        cmds = []
        try_dispatch(state, cmds)
        self.assertEqual(cmds, [])

    def test_coordenador_sem_drone_nao_despacha(self):
        state = make_state()
        state["is_coordinator"] = True
        enqueue(state, make_request("req-001"))
        cmds = []
        try_dispatch(state, cmds)
        self.assertEqual(cmds, [])

    def test_coordenador_com_drone_despacha(self):
        state = make_state()
        state["is_coordinator"] = True
        state["drones"]["d1"] = make_drone("d1", "idle")
        enqueue(state, make_request("req-001"))
        cmds = []
        try_dispatch(state, cmds)
        self.assertEqual(len(cmds), 1)
        self.assertEqual(cmds[0]["drone_id"], "d1")
        self.assertEqual(cmds[0]["request_id"], "req-001")

    def test_drone_fica_dispatched_apos_despacho(self):
        state = make_state()
        state["is_coordinator"] = True
        state["drones"]["d1"] = make_drone("d1", "idle")
        enqueue(state, make_request("req-001"))
        try_dispatch(state, [])
        self.assertEqual(state["drones"]["d1"]["status"], "dispatched")

    def test_req_sai_da_fila_apos_despacho(self):
        state = make_state()
        state["is_coordinator"] = True
        state["drones"]["d1"] = make_drone("d1", "idle")
        enqueue(state, make_request("req-001"))
        try_dispatch(state, [])
        self.assertNotIn("req-001", state["queue"])

    def test_req_entra_em_active_missions(self):
        state = make_state()
        state["is_coordinator"] = True
        state["drones"]["d1"] = make_drone("d1", "idle")
        enqueue(state, make_request("req-001"))
        try_dispatch(state, [])
        self.assertIn("req-001", state["active_missions"])
        self.assertEqual(state["active_missions"]["req-001"], "d1")

    def test_prioridade_grave_despachada_primeiro(self):
        state = make_state()
        state["is_coordinator"] = True
        state["drones"]["d1"] = make_drone("d1", "idle")
        t0 = time.time()
        enqueue(state, make_request("req-leve",  event_type="INSPECAO_VISUAL",  enqueued_at=t0))
        enqueue(state, make_request("req-grave", event_type="COLISAO_IMINENTE", enqueued_at=t0+1))
        cmds = []
        try_dispatch(state, cmds)
        self.assertEqual(cmds[0]["request_id"], "req-grave")

    def test_drone_stale_nao_usado(self):
        state = make_state()
        state["is_coordinator"] = True
        state["drones"]["d1"] = make_drone("d1", "idle", last_seen=time.time() - 30)
        enqueue(state, make_request("req-001"))
        cmds = []
        try_dispatch(state, cmds)
        self.assertEqual(cmds, [])

    def test_total_dispatched_incrementa(self):
        state = make_state()
        state["is_coordinator"] = True
        state["drones"]["d1"] = make_drone("d1", "idle")
        enqueue(state, make_request("req-001"))
        try_dispatch(state, [])
        self.assertEqual(state["total_dispatched"], 1)

    def test_dois_drones_dois_requests(self):
        state = make_state()
        state["is_coordinator"] = True
        state["drones"]["d1"] = make_drone("d1", "idle")
        state["drones"]["d2"] = make_drone("d2", "idle")
        enqueue(state, make_request("req-001"))
        enqueue(state, make_request("req-002"))
        cmds = []
        try_dispatch(state, cmds)
        self.assertEqual(len(cmds), 2)
        self.assertEqual(state["total_dispatched"], 2)


# ══════════════════════════════════════════════════════════════════════════════
# BLOCO 5 — Testes Unitários: Remoção de Broker Offline
# ══════════════════════════════════════════════════════════════════════════════

class TestBrokerOffline(unittest.TestCase):

    def _state_com_broker_e_req(self, bid="broker-x"):
        state = make_state()
        state["known_brokers"][bid] = time.time()
        req = make_request("req-001", broker_id=bid)
        enqueue(state, req)
        return state

    def test_remove_broker_da_known_brokers(self):
        state = self._state_com_broker_e_req("broker-x")
        remove_broker(state, "broker-x")
        self.assertNotIn("broker-x", state["known_brokers"])

    def test_remove_requests_da_fila(self):
        state = self._state_com_broker_e_req("broker-x")
        rids_fila, _ = remove_broker(state, "broker-x")
        self.assertNotIn("req-001", state["queue"])
        self.assertIn("req-001", rids_fila)

    def test_seen_requests_limpo(self):
        state = self._state_com_broker_e_req("broker-x")
        remove_broker(state, "broker-x")
        self.assertNotIn("req-001", state["seen_requests"])

    def test_broker_inexistente_nao_da_erro(self):
        state = make_state()
        try:
            remove_broker(state, "broker-fantasma")
        except Exception as e:
            self.fail(f"remove_broker lançou exceção inesperada: {e}")

    def test_missao_ativa_removida_e_drone_liberado(self):
        state = make_state()
        state["known_brokers"]["broker-x"] = time.time()
        state["drones"]["d1"] = make_drone("d1", "dispatched")

        req = make_request("req-001", broker_id="broker-x")
        state["active_missions"]["req-001"]  = "d1"
        state["active_requests"]["req-001"]  = {**req, "drone_id": "d1"}
        state["seen_requests"].add("req-001")

        _, rids_missoes = remove_broker(state, "broker-x")

        self.assertNotIn("req-001", state["active_missions"])
        self.assertNotIn("req-001", state["active_requests"])
        self.assertNotIn("req-001", state["seen_requests"])
        self.assertEqual(state["drones"]["d1"]["status"], "idle")
        self.assertIn("req-001", rids_missoes)

    def test_requests_de_outro_broker_preservados(self):
        state = make_state()
        state["known_brokers"]["broker-x"] = time.time()
        enqueue(state, make_request("req-x", broker_id="broker-x"))
        enqueue(state, make_request("req-y", broker_id="broker-y"))

        remove_broker(state, "broker-x")

        self.assertNotIn("req-x", state["queue"])
        self.assertIn("req-y", state["queue"])


# ══════════════════════════════════════════════════════════════════════════════
# BLOCO 6 — Testes Unitários: Sincronização de Fila
# ══════════════════════════════════════════════════════════════════════════════

class TestSyncFila(unittest.TestCase):

    def _apply_sync(self, state, fila=None, active_missions=None, active_requests=None):
        novos = 0
        for req in (fila or []):
            if enqueue(state, req):
                novos += 1

        for rid, did in (active_missions or {}).items():
            if rid in state["active_missions"]:
                continue
            drone = state["drones"].get(did)
            if drone and drone.get("status") not in ("idle", None):
                state["active_missions"][rid] = did
                ar = (active_requests or {}).get(rid)
                if ar:
                    state["active_requests"][rid] = ar
                state["seen_requests"].add(rid)
                state["queue"].pop(rid, None)
        return novos

    def test_sync_adiciona_novos_itens(self):
        state = make_state()
        fila  = [make_request("req-001"), make_request("req-002")]
        novos = self._apply_sync(state, fila=fila)
        self.assertEqual(novos, 2)
        self.assertIn("req-001", state["queue"])
        self.assertIn("req-002", state["queue"])

    def test_sync_nao_duplica_existentes(self):
        state = make_state()
        enqueue(state, make_request("req-001"))
        fila  = [make_request("req-001"), make_request("req-002")]
        novos = self._apply_sync(state, fila=fila)
        self.assertEqual(novos, 1)

    def test_sync_absorve_missao_ativa_com_drone_ocupado(self):
        state = make_state()
        state["drones"]["d1"] = make_drone("d1", "dispatched")
        req = make_request("req-001")

        novos = self._apply_sync(
            state,
            fila=[],
            active_missions={"req-001": "d1"},
            active_requests={"req-001": req}
        )
        self.assertIn("req-001", state["active_missions"])
        self.assertIn("req-001", state["seen_requests"])

    def test_sync_nao_absorve_missao_com_drone_idle(self):
        state = make_state()
        state["drones"]["d1"] = make_drone("d1", "idle")
        req = make_request("req-001")

        self._apply_sync(
            state,
            fila=[],
            active_missions={"req-001": "d1"},
            active_requests={"req-001": req}
        )
        self.assertNotIn("req-001", state["active_missions"])

    def test_sync_remove_da_fila_se_missao_absorvida(self):
        state = make_state()
        state["drones"]["d1"] = make_drone("d1", "dispatched")
        req = make_request("req-001")
        enqueue(state, req)   # estava na fila
        state["seen_requests"].discard("req-001")   # limpa para permitir absorção

        self._apply_sync(
            state,
            fila=[],
            active_missions={"req-001": "d1"},
            active_requests={"req-001": req}
        )
        self.assertNotIn("req-001", state["queue"])


# ══════════════════════════════════════════════════════════════════════════════
# BLOCO 7 — Casos de Uso (Integração)
# ══════════════════════════════════════════════════════════════════════════════

class TestCasoDeUso(unittest.TestCase):
    def test_cu01_sensor_gera_req_coordenador_despacha(self):
        state = make_state("broker-coord", "Setor-Norte")
        state["is_coordinator"] = True
        state["drones"]["d1"]   = make_drone("d1", "idle")

        req = make_request("req-sensor-001", event_type="COLISAO_IMINENTE",
                           broker_id="broker-coord", sector="Setor-Norte")
        enqueue(state, req)
        self.assertIn("req-sensor-001", state["queue"])
        self.assertEqual(state["queue"]["req-sensor-001"]["severity"], 3)

        cmds = []
        try_dispatch(state, cmds)

        self.assertEqual(len(cmds), 1)
        self.assertEqual(cmds[0]["request_id"], "req-sensor-001")
        self.assertNotIn("req-sensor-001", state["queue"])
        self.assertIn("req-sensor-001", state["active_missions"])

    def test_cu02_broker_nao_coordenador_enfileira_sem_despachar(self):
        state = make_state("broker-follow", "Setor-Sul")
        state["is_coordinator"] = False
        state["drones"]["d1"]   = make_drone("d1", "idle")

        enqueue(state, make_request("req-001"))
        cmds = []
        try_dispatch(state, cmds)

        self.assertIn("req-001", state["queue"])
        self.assertEqual(cmds, [])

    # ── CU-03 ──────────────────────────────────────────────────────────────────
    def test_cu03_escalacao_leva_a_redespacho(self):
        state = make_state()
        state["is_coordinator"] = True
        state["drones"]["d1"]   = make_drone("d1", "idle")

        req = make_request("req-001", event_type="INSPECAO_VISUAL", severity=1,
                           enqueued_at=time.time() - 45)
        enqueue(state, req)
        self.assertEqual(state["queue"]["req-001"]["severity"], 1)

        apply_escalation(state)
        self.assertEqual(state["queue"]["req-001"]["severity"], 3)

        cmds = []
        try_dispatch(state, cmds)
        self.assertEqual(len(cmds), 1)

    def test_cu04_drone_conclui_missao_volta_idle(self):
        state = make_state()
        state["is_coordinator"] = True
        state["drones"]["d1"]   = make_drone("d1", "dispatched")
        state["active_missions"]["req-001"] = "d1"
        req = make_request("req-001")
        state["active_requests"]["req-001"] = {**req, "drone_id": "d1", "status": "active"}

        prev_status = state["drones"]["d1"]["status"]
        state["drones"]["d1"]["status"] = "idle"
        new_status  = "idle"

        if state["is_coordinator"] and new_status == "idle" and prev_status != "idle":
            freed = [r for r, d in state["active_missions"].items() if d == "d1"]
            for rid in freed:
                del state["active_missions"][rid]
                state["active_requests"].pop(rid, None)

        self.assertEqual(state["drones"]["d1"]["status"], "idle")
        self.assertNotIn("req-001", state["active_missions"])
        self.assertNotIn("req-001", state["active_requests"])

        enqueue(state, make_request("req-002"))
        cmds = []
        try_dispatch(state, cmds)
        self.assertEqual(len(cmds), 1)
        self.assertEqual(cmds[0]["request_id"], "req-002")

    def test_cu05_broker_offline_libera_drone_e_redespacha(self):
        state = make_state("broker-coord")
        state["is_coordinator"] = True
        state["known_brokers"]["broker-caiu"] = time.time()
        state["drones"]["d1"] = make_drone("d1", "dispatched")

        req = make_request("req-001", broker_id="broker-caiu")
        state["active_missions"]["req-001"]  = "d1"
        state["active_requests"]["req-001"]  = {**req, "drone_id": "d1"}
        state["seen_requests"].add("req-001")

        enqueue(state, make_request("req-002", broker_id="broker-coord"))

        remove_broker(state, "broker-caiu")

        self.assertEqual(state["drones"]["d1"]["status"], "idle")
        self.assertNotIn("req-001", state["active_missions"])

        cmds = []
        try_dispatch(state, cmds)
        self.assertEqual(len(cmds), 1)
        self.assertEqual(cmds[0]["request_id"], "req-002")

    def test_cu06_eleicao_ra_no_caso_de_coordenador_ausente(self):
        ra   = RicartAgrawala("broker-novo")
        sent = []
        ok   = ra.request_cs(set(), lambda c: sent.append(c))

        self.assertTrue(ok)
        self.assertEqual(ra._state, HELD)
        self.assertEqual(sent, [])

    def test_cu07_sync_inicial_absorve_fila_do_coordenador(self):
        state_coord = make_state("broker-coord")
        state_coord["is_coordinator"] = True
        enqueue(state_coord, make_request("req-001"))
        enqueue(state_coord, make_request("req-002"))

        state_novo = make_state("broker-novo")
        fila_sync  = list(state_coord["queue"].values())

        novos = 0
        for req in fila_sync:
            if enqueue(state_novo, req):
                novos += 1

        self.assertEqual(novos, 2)
        self.assertEqual(len(state_novo["queue"]), 2)

        # Segunda sync não deve duplicar
        for req in fila_sync:
            enqueue(state_novo, req)
        self.assertEqual(len(state_novo["queue"]), 2)

    def test_cu08_multiplos_sensores_fila_ordenada(self):
        state = make_state("broker-coord")
        state["is_coordinator"] = True
        state["drones"]["d1"]   = make_drone("d1", "idle")

        t0 = time.time()
        enqueue(state, make_request("r1", event_type="CONGESTIONAMENTO",   enqueued_at=t0,   broker_id="b1"))
        enqueue(state, make_request("r2", event_type="COLISAO_IMINENTE",   enqueued_at=t0+1, broker_id="b2"))
        enqueue(state, make_request("r3", event_type="EMBARCACAO_DERIVA",  enqueued_at=t0+2, broker_id="b3"))

        q = sorted_queue(state)
        self.assertEqual(q[0]["request_id"], "r2")   # GRAVE
        self.assertEqual(q[1]["request_id"], "r3")   # MOD
        self.assertEqual(q[2]["request_id"], "r1")   # LEVE

        cmds = []
        try_dispatch(state, cmds)
        self.assertEqual(cmds[0]["request_id"], "r2")

    def test_cu09_ra_dois_nos_desempate_por_id(self):
        ra_a = RicartAgrawala("broker-aaa")   # menor ID
        ra_b = RicartAgrawala("broker-zzz")   # maior ID

        ra_a._state     = WANTED
        ra_a._req_clock = 1
        ra_b._state     = WANTED
        ra_b._req_clock = 1

        replies_a = []
        replies_b = []

        ra_a.on_request("broker-zzz", 1, lambda pid, clk: replies_a.append(pid))
        ra_b.on_request("broker-aaa", 1, lambda pid, clk: replies_b.append(pid))

        self.assertEqual(replies_a, [],           "broker-aaa deveria deferir broker-zzz")
        self.assertIn("broker-aaa", replies_b,    "broker-zzz deveria responder broker-aaa")

    def test_cu10_drone_timeout_missao_devolvida_fila(self):
        state = make_state()
        state["is_coordinator"] = True
        state["drones"]["d1"]   = make_drone("d1", "dispatched",
                                              last_seen=time.time() - 30)
        req = make_request("req-001")
        state["active_missions"]["req-001"]  = "d1"
        state["active_requests"]["req-001"]  = {**req, "drone_id": "d1"}
        state["seen_requests"].add("req-001")

        now    = time.time()
        dead   = [did for did, d in state["drones"].items()
                  if (now - d.get("last_seen", 0)) > 12]
        for did in dead:
            del state["drones"][did]
            for rid in [r for r, d in state["active_missions"].items() if d == did]:
                req_data = state["active_requests"].pop(rid, None)
                del state["active_missions"][rid]
                if req_data:
                    req_data.pop("drone_id", None)
                    req_data["status"] = "pending"
                    state["seen_requests"].discard(rid)
                    state["queue"][rid] = req_data

        self.assertNotIn("d1", state["drones"])
        self.assertIn("req-001", state["queue"])
        self.assertNotIn("req-001", state["active_missions"])


# ══════════════════════════════════════════════════════════════════════════════
# Entrada
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    unittest.main(verbosity=2)
