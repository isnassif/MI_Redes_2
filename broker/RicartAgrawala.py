#!/usr/bin/env python3
"""
RicartAgrawala.py — Algoritmo de Exclusão Mútua Distribuída
============================================================
Classe reutilizável de Ricart-Agrawala com relógio lógico de Lamport.
Portada e adaptada do projeto Redes2 para uso via MQTT no Ormuz.

Estados:
  RELEASED → não quer a seção crítica
  WANTED   → pediu, aguardando replies
  HELD     → dentro da seção crítica
"""

import threading
import logging

log = logging.getLogger(__name__)

RELEASED = 0
WANTED   = 1
HELD     = 2


class RicartAgrawala:
    def __init__(self, my_id: str):
        self.my_id     = my_id
        self._lock     = threading.Lock()
        self._granted  = threading.Event()

        self._clock          = 0        # Relógio de Lamport
        self._state          = RELEASED
        self._req_clock      = 0        # clock do nosso pedido atual
        self._deferred       = []       # (peer_id, reply_fn) diferidos
        self._pending_replies = set()   # peers dos quais ainda aguardamos reply

    # ── Relógio de Lamport ─────────────────────────────────────────────────────

    def tick(self, incoming: int = 0) -> int:
        """Avança o relógio local (regra de Lamport) e retorna o novo valor."""
        with self._lock:
            self._clock = max(self._clock, incoming) + 1
            return self._clock

    @property
    def clock(self) -> int:
        with self._lock:
            return self._clock

    # ── API pública ────────────────────────────────────────────────────────────

    def request_cs(self, peers: set, send_request_fn) -> bool:
        with self._lock:
            if self._state != RELEASED:
                return False
            self._clock += 1
            self._req_clock = self._clock
            self._state = WANTED
            self._pending_replies = set(p for p in peers if p != self.my_id)
            self._granted.clear()

        req_clock = self._req_clock
        log.debug(f"RA [{self.my_id}]: REQUEST clock={req_clock}, aguardando {len(self._pending_replies)} replies")

        if not self._pending_replies:
            # Sozinho no sistema → entra imediatamente
            with self._lock:
                self._state = HELD
            return True

        send_request_fn(req_clock)

        granted = self._granted.wait(timeout=5.0)
        if granted:
            with self._lock:
                self._state = HELD
        else:
            log.warning(f"RA [{self.my_id}]: timeout aguardando replies — cancelando pedido")
            with self._lock:
                self._state = RELEASED
        return granted

    def release_cs(self, send_reply_fn):
        with self._lock:
            self._state = RELEASED
            deferred = list(self._deferred)
            self._deferred.clear()

        for (peer_id,) in deferred:
            with self._lock:
                self._clock += 1
                clock = self._clock
            log.debug(f"RA [{self.my_id}]: enviando reply diferido → {peer_id}")
            send_reply_fn(peer_id, clock)

    def on_request(self, sender: str, their_clock: int, send_reply_fn):
        self.tick(their_clock)

        with self._lock:
            state = self._state
            my_clock = self._req_clock
       should_defer = (
            state == HELD or (
                state == WANTED and (
                    my_clock < their_clock or
                    (my_clock == their_clock and self.my_id < sender)
                )
            )
        )

        if should_defer:
            log.debug(f"RA [{self.my_id}]: deferindo reply para {sender}")
            with self._lock:
                self._deferred.append((sender,))
        else:
            log.debug(f"RA [{self.my_id}]: reply imediato para {sender}")
            with self._lock:
                clock = self._clock + 1
                self._clock = clock
            send_reply_fn(sender, clock)

    def on_reply(self, sender: str):
        with self._lock:
            if sender:
                self._pending_replies.discard(sender)
            elif self._pending_replies:
                self._pending_replies.pop()

            if not self._pending_replies and self._state == WANTED:
                self._granted.set()

    def remove_peer(self, peer_id: str):

        with self._lock:
            self._pending_replies.discard(peer_id)
            self._deferred = [(p,) for (p,) in self._deferred if p != peer_id]
            if not self._pending_replies and self._state == WANTED:
                self._granted.set()
        log.info(f"RA [{self.my_id}]: peer {peer_id} removido dos pendentes")
