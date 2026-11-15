#!/usr/bin/env python3
"""Nodo coordinador real del laboratorio distribuido (ZeroMQ).

Protocolo (canal de datos, ROUTER en puerto 5555):
  worker -> coord : REGISTER {"identity", "status_endpoint"}
  coord  -> worker: REGISTER_ACK
  coord  -> worker: TASK {"type": "TASK", task_id, cpu_work,
                          payload_bytes, created_at}
  worker -> coord : RESULT {task_id, worker, started_at, finished_at}
  coord  -> worker: STOP            (cierre limpio al terminar el lote)

Canal de control (puerto 5556):
  El worker atiende con REP; el coordinador consulta con REQ:
  "STATUS" -> {"queue": q, "running": r}   (carga = cola + ejecución)

Métricas calculadas al finalizar el lote:
  makespan_s, throughput_tps, mean_wait_s, load_imbalance_cv, control_messages
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import statistics
import sys
import time

import zmq

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import strategies  # noqa: E402
import workload    # noqa: E402

PORT_TASKS = 5555   # ROUTER: tareas y resultados
PORT_STATUS = 5556  # consultas de estado de control
CSV_COLUMNS = [
    "env", "scenario_id", "strategy", "n_workers", "latency_ms", "seed",
    "makespan_s", "throughput_tps", "mean_wait_s", "load_imbalance_cv",
    "control_messages",
]


class ControlPlane:
    """Consultas de carga por REQ a cada worker (puerto 5556).

    Implementa el callback `get_loads(ids)` que inyecta `strategies`:
    devuelve {worker_id: cola + en ejecución}. Ante timeout recrea el
    socket REQ (los REQ fallidos quedan en estado inválido) y usa la
    última carga conocida como fallback.
    """

    def __init__(self, endpoints: dict, timeout_ms: int = 150):
        self.ctx = zmq.Context.instance()
        self.timeout_ms = timeout_ms
        self.endpoints = dict(endpoints)  # worker_id -> tcp://host:5556
        self.socks = {w: self._conectar(ep) for w, ep in endpoints.items()}
        self.last_known = {}

    def _conectar(self, endpoint):
        s = self.ctx.socket(zmq.REQ)
        s.setsockopt(zmq.LINGER, 0)
        s.connect(endpoint)
        return s

    def _reset(self, wid):
        self.socks[wid].close(0)
        self.socks[wid] = self._conectar(self.endpoints[wid])

    def get_loads(self, ids):
        """Callback de la estrategia: consulta la carga de cada worker pedido."""
        return {wid: self._consultar(wid) for wid in ids}

    def _consultar(self, wid):
        sock = self.socks.get(wid)
        if sock is None:
            return self.last_known.get(wid, 0)
        poller = zmq.Poller()
        poller.register(sock, zmq.POLLIN)
        try:
            sock.send_json({"type": "STATUS"})
            if poller.poll(self.timeout_ms):
                rep = sock.recv_json()
                carga = int(rep.get("queue", 0)) + int(rep.get("running", 0))
                self.last_known[wid] = carga
                return carga
        except zmq.ZMQError:
            pass
        self._reset(wid)  # recupera el REQ tras un fallo/timeout
        return self.last_known.get(wid, 0)

    def close(self):
        for s in self.socks.values():
            s.close(0)


def parse_args():
    p = argparse.ArgumentParser(description="Coordinador real ZeroMQ del laboratorio")
    p.add_argument("--strategy", required=True, choices=strategies.STRATEGIES)
    p.add_argument("--workers", type=int, required=True, help="workers esperados")
    p.add_argument("--latency", type=float, default=0.0,
                   help="etiqueta numérica de latencia (ms) para el log")
    p.add_argument("--seed", type=int, default=workload.DEFAULT_SEED)
    p.add_argument("--tasks", type=int, default=workload.DEFAULT_TASKS)
    p.add_argument("--output-csv", default="resultados_reales.csv")
    p.add_argument("--scenario-id", default="batch_agro")
    p.add_argument("--reg-timeout", type=float, default=60.0,
                   help="segundos de espera máxima por los registros")
    return p.parse_args()


def recv_worker(router):
    """Recibe de un DEALER: normaliza el delimitador vacío si existe
    (DEALER no usa frame vacío; REQ sí) y devuelve (ident, payload)."""
    parts = router.recv_multipart()
    if len(parts) >= 3 and parts[1] == b"":
        return parts[0], parts[2]
    return parts[0], parts[1]


def esperar_registro(router, n_esperados, timeout_s):
    """Espera REGISTER de los workers; devuelve {worker_id: status_endpoint}."""
    poller = zmq.Poller()
    poller.register(router, zmq.POLLIN)
    endpoints, deadline = {}, time.time() + timeout_s
    while len(endpoints) < n_esperados:
        restante = deadline - time.time()
        if restante <= 0:
            raise TimeoutError(
                f"Solo {len(endpoints)}/{n_esperados} workers registrados en {timeout_s}s")
        if not poller.poll(restante * 1000):
            continue
        ident, payload = recv_worker(router)
        msg = json.loads(payload)
        if msg.get("type") != "REGISTER":
            continue  # ignora mensajes previos al registro completo
        wid = ident.decode()
        es_nuevo = wid not in endpoints  # re-REGISTROS en cola: solo 1 print
        endpoints[wid] = msg.get("status_endpoint")  # p. ej. tcp://host:5556
        # ACK de registro (cuenta como mensaje de control). A DEALER se le
        # envía [ident, payload] SIN frame delimitador vacío.
        router.send_multipart([ident, json.dumps({"type": "REGISTER_ACK"}).encode()])
        if es_nuevo:
            print(f"[REG] {wid} (control: {endpoints[wid] or 'n/d'})")
    return endpoints


def append_csv(path, fila):
    """Agrega una fila al CSV, escribiendo el encabezado si es archivo nuevo."""
    nuevo = not os.path.exists(path)
    with open(path, "a", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
        if nuevo:
            w.writeheader()
        w.writerow(fila)


def main():
    args = parse_args()
    ctx = zmq.Context.instance()
    router = ctx.socket(zmq.ROUTER)
    router.setsockopt(zmq.LINGER, 1000)
    router.bind(f"tcp://*:{PORT_TASKS}")

    try:
        # 1) Registro inicial de los --workers esperados
        endpoints = esperar_registro(router, args.workers, args.reg_timeout)
        worker_ids = list(endpoints)

        # 2) Estrategia con consulta de estado activa (canal de control 5556)
        cp = ControlPlane(endpoints)
        bal = strategies.create(args.strategy, worker_ids,
                                get_loads=cp.get_loads, seed=args.seed)

        # 3) Generación del lote (misma semilla => lote reproducible).
        # created_at es lógico; t0 lo convierte a tiempo absoluto del nodo.
        t0 = time.time()
        tasks = workload.generate_batch(args.tasks, args.seed)
        created = {t["task_id"]: t0 + t["created_at"] for t in tasks}

        # 4) Despacho del lote: entre tarea y tarea se drenan RESULT
        # pendientes (no bloqueante), así la vista de carga no queda rancia.
        poller = zmq.Poller()
        poller.register(router, zmq.POLLIN)
        estado = {"t_first": None, "t_last": None, "completadas": 0}
        waits = []                              # started_at - created_at
        active = {w: 0.0 for w in worker_ids}   # tiempo activo por worker

        def procesar_resultado(ident, payload):
            msg = json.loads(payload)
            if msg.get("type") != "RESULT":
                return
            estado["t_last"] = time.time()
            estado["completadas"] += 1
            waits.append(msg["started_at"] - created[msg["task_id"]])
            wid = ident.decode()
            active[wid] = active.get(wid, 0.0) + (msg["finished_at"] - msg["started_at"])

        for t in tasks:
            while poller.poll(0):
                ident, payload = recv_worker(router)
                procesar_resultado(ident, payload)
            wid = bal.select(t["task_id"])
            now = time.time()
            if estado["t_first"] is None:
                estado["t_first"] = now
            # Mensaje de tarea etiquetado con su tipo para el protocolo
            router.send_multipart(
                [wid.encode(), json.dumps({"type": "TASK", **t}).encode()])

        # 5) Recepción de los resultados restantes
        while estado["completadas"] < len(tasks):
            if not poller.poll(5000):
                print("[WARN] esperando resultados...", file=sys.stderr)
                continue
            ident, payload = recv_worker(router)
            procesar_resultado(ident, payload)

        t_first, t_last = estado["t_first"], estado["t_last"]
        completadas = estado["completadas"]

        # 6) Cierre limpio: señal STOP a todos los workers
        for wid in worker_ids:
            router.send_multipart([wid.encode(),
                                   json.dumps({"type": "STOP"}).encode()])

        # 7) Métricas del experimento
        makespan = (t_last - t_first) if (t_first is not None and t_last is not None) else 0.0
        throughput = completadas / makespan if makespan > 0 else 0.0
        mean_wait = statistics.fmean(waits) if waits else 0.0
        vals = list(active.values())
        media = statistics.fmean(vals)
        cv = (statistics.pstdev(vals) / media) if media > 0 else 0.0
        # Control: consultas de la estrategia (1 por worker consultado por
        # decisión) + 2 por worker (REGISTER + REGISTER_ACK).
        control_total = bal.control_messages + 2 * len(worker_ids)

        fila = {
            "env": "real",
            "scenario_id": args.scenario_id,
            "strategy": args.strategy,
            "n_workers": len(worker_ids),
            "latency_ms": args.latency,
            "seed": args.seed,
            "makespan_s": round(makespan, 6),
            "throughput_tps": round(throughput, 6),
            "mean_wait_s": round(mean_wait, 6),
            "load_imbalance_cv": round(cv, 6),
            "control_messages": control_total,
        }
        append_csv(args.output_csv, fila)
        cp.close()
        print(f"[OK] {completadas} tareas | makespan={fila['makespan_s']}s "
              f"tps={fila['throughput_tps']} wait={fila['mean_wait_s']}s "
              f"cv={fila['load_imbalance_cv']} control={control_total} "
              f"-> {args.output_csv}")
    finally:
        router.close(1000)
        ctx.term()


if __name__ == "__main__":
    main()
