#!/usr/bin/env python3
"""Nodo worker real del laboratorio distribuido (ZeroMQ).

Protocolo (canal de datos, DEALER -> ROUTER del coordinador, puerto 5555):
  worker -> coord : REGISTER {"worker_id", "status_endpoint"}
  coord  -> worker: REGISTER_ACK
  coord  -> worker: TASK {"type": "TASK", task_id, cpu_work,
                          payload_bytes, created_at}
  worker -> coord : RESULT {type, task_id, worker, started_at, finished_at}
  coord  -> worker: STOP            (cierre limpio al terminar el lote)
  worker -> coord : FINAL_REPORT {"worker", "tasks_completed",
                                  "active_cpu_s"}   (tras STOP)

Canal de control (REP bind en :5556 del worker):
  El coordinador consulta con REQ al endpoint anunciado en REGISTER:
  "STATUS" -> {"queue": q, "running": r}   (carga = cola + ejecución)

Modo benchmark (--benchmark):
  No conecta al coordinador. Ejecuta 10 veces una tarea de referencia
  fija de 1_000_000 unidades de CPU e imprime media y desviación
  estándar en segundos, para calibrar la velocidad de la vCPU de la VM
  en SimGrid (flops/s = 1_000_000 / tiempo_medio).

El hilo receptor alimenta una queue.Queue local; un único hilo
trabajador procesa las tareas secuencialmente (1 núcleo dedicado por
proceso worker).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import queue
import socket
import statistics
import sys
import threading
import time
from concurrent.futures import ProcessPoolExecutor

import zmq

PORT_TASKS = 5555   # DEALER -> ROUTER del coordinador
PORT_STATUS = 5556  # REP local: consultas de estado del coordinador

# Calibración del cómputo: cada iteración hace un sha256 sobre un bloque
# de HASH_BLOCK bytes y consume UNITS_PER_ITER unidades de trabajo.
# Bajar este valor hace las tareas más largas (mejor resolución del
# benchmark y más contención de cola en el clúster real).
UNITS_PER_ITER = 10         # unidades de CPU por iteración
HASH_BLOCK = 1024           # bytes por bloque hasheado (1 KB)
BENCHMARK_UNITS = 1_000_000
BENCHMARK_RUNS = 10


# ----------------------------------------------------------------------
# Carga CPU puramente determinista
# ----------------------------------------------------------------------
def execute_cpu_work(units: int) -> int:
    """Quema `units` unidades de CPU con cómputo determinista.

    Encadena hashlib.sha256 sobre un buffer proporcional a `units`.
    El digest de cada bloque alimenta al siguiente, así el intérprete
    no puede plegar ni memoizar el bucle: es cómputo real por iteración
    que ocupa efectivamente la vCPU asignada por VirtualBox.

    :return: cantidad de iteraciones ejecutadas (traza de depuración).
    """
    iteraciones = max(1, units // UNITS_PER_ITER)
    # Semilla determinista ligada a la cantidad de trabajo pedida
    bloque = (b"cluster-sim-cpu-work:" + units.to_bytes(8, "big")) * (HASH_BLOCK // 24 + 1)
    bloque = bloque[:HASH_BLOCK]
    digest = hashlib.sha256(bloque).digest()
    for i in range(iteraciones):
        # dependencia de datos entre iteraciones => no optimizable
        digest = hashlib.sha256(digest + i.to_bytes(8, "big")).digest()
    return iteraciones


def get_local_ip(dst_ip: str) -> str:
    """IP local de la interfaz que alcanza a `dst_ip` (trick UDP)."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        s.connect((dst_ip, 9))  # puerto arbitrario; no envía nada
        return s.getsockname()[0]
    except OSError:
        try:
            return socket.gethostbyname(socket.gethostname())
        except OSError:
            return "127.0.0.1"
    finally:
        s.close()


# ----------------------------------------------------------------------
# Estado compartido entre hilos
# ----------------------------------------------------------------------
class WorkerState:
    """Cola de tareas y contadores compartidos receptor/worker/control."""

    def __init__(self):
        self.tareas: queue.Queue = queue.Queue()
        self.slots_busy = 0           # nº de slots de ejecución ocupados
        self.slots_lock = threading.Lock()
        self.active_cpu_s = 0.0       # tiempo total activo acumulado
        self.tasks_completed = 0
        self.n_slots = 1              # nº de hilos consumidores (STOP: 1 centinela c/u)
        self.stop_event = threading.Event()
        self.registrado = threading.Event()   # ACK del coordinador recibido

    def load(self) -> int:
        """Carga vista por el canal de control: cola + slots ocupados."""
        return self.tareas.qsize() + self.slots_busy


# ----------------------------------------------------------------------
# Hilos de servicio
# ----------------------------------------------------------------------
def hilo_receptor(dealer, estado: WorkerState, worker_id: str):
    """DEALER -> cola local: TASK encola, STOP activa el cierre limpio."""
    poller = zmq.Poller()
    poller.register(dealer, zmq.POLLIN)
    while not estado.stop_event.is_set():
        if not poller.poll(100):
            continue
        try:
            partes = dealer.recv_multipart(flags=zmq.NOBLOCK)
        except zmq.ZMQError:
            continue
        payload = partes[-1]  # DEALER: sin frame delimitador vacío
        try:
            msg = json.loads(payload.decode())
        except (ValueError, UnicodeDecodeError):
            continue
        tipo = msg.get("type")
        if tipo == "TASK":
            estado.tareas.put(msg)
        elif tipo == "REGISTER_ACK":
            estado.registrado.set()
            print(f"[REG] ACK recibido por el coordinador ({worker_id})")
        elif tipo == "STOP":
            # centinela: UNO por hilo consumidor; cada hilo drena la cola
            # hasta encontrar el suyo (las tareas encoladas sí se ejecutan)
            for _ in range(estado.n_slots):
                estado.tareas.put(None)
            estado.stop_event.set()


def hilo_control(rep, estado: WorkerState, worker_id: str):
    """REP :5556: responde STATUS al instante sin bloquear el trabajo."""
    poller = zmq.Poller()
    poller.register(rep, zmq.POLLIN)
    # Encuesta corto: el coordinador usa timeout de 150 ms por consulta,
    # así que la respuesta debe salir en <100 ms para no perderla.
    while not estado.stop_event.is_set():
        if not poller.poll(50):
            continue
        try:
            msg = rep.recv_json(flags=zmq.NOBLOCK)
        except zmq.ZMQError:
            continue
        if msg.get("type") == "STATUS":
            # carga = tareas pendientes en cola + slots ocupados
            rep.send_json({"worker_id": worker_id,
                           "queue": estado.tareas.qsize(),
                           "running": estado.slots_busy})
        else:
            rep.send_json({"error": "mensaje no reconocido"})


def _tarea_cpu(units: int) -> float:
    """Ejecuta cómputo en un PROCESO del pool (evita el GIL: los threads de
    Python no paralelizan el hashing). Devuelve el tiempo activo real."""
    t0 = time.perf_counter()
    execute_cpu_work(units)
    return time.perf_counter() - t0


def hilo_trabajador(dealer, estado: WorkerState, worker_id: str,
                    executor, send_lock):
    """Consume la cola local. Con --threads 1 ejecuta inline (1 núcleo);
    con N>1, cada hilo despacha su tarea al pool de procesos y bloquea
    esperándola (N procesos = N núcleos reales)."""
    while True:
        tarea = estado.tareas.get()
        if tarea is None:               # centinela de STOP
            break
        with estado.slots_lock:
            estado.slots_busy += 1
        started_at = time.time()        # epoch: el coordinador mide waits
        units = int(tarea.get("cpu_work", 0))
        if executor is None:
            t0 = time.perf_counter()
            execute_cpu_work(units)
            elapsed = time.perf_counter() - t0
        else:
            elapsed = executor.submit(_tarea_cpu, units).result()
        finished_at = time.time()
        with estado.slots_lock:
            estado.slots_busy -= 1

        estado.active_cpu_s += elapsed  # tiempo activo real del cómputo
        estado.tasks_completed += 1
        # ZMQ: los sockets NO son thread-safe -> los envíos se serializan
        with send_lock:
            dealer.send_json({
                "type": "RESULT",
                "task_id": tarea.get("task_id"),
                "worker": worker_id,
                "started_at": started_at,
                "finished_at": finished_at,
            })


# ----------------------------------------------------------------------
# Modo benchmark (calibración SimGrid)
# ----------------------------------------------------------------------
def run_benchmark(n_slots: int = 1):
    """Mide la capacidad del nodo EN SU CONFIGURACIÓN REAL: `n_slots` debe
    coincidir con los --threads del servicio (p. ej. worker-1 = 2), y para
    nodos con cuota cgroup (worker-3) debe lanzarse DENTRO de ese cgroup
    (ver README §5.1) para que el techo se aplique.

    Con n_slots>1 la métrica relevante es la velocidad AGREGADA (los slots
    corren en paralelo vía procesos): es lo que el experimento experimenta
    y lo que SimGrid debe modelar como speed del host."""
    print(f"[BENCH] tarea de referencia: {BENCHMARK_UNITS} unidades CPU x "
          f"{BENCHMARK_RUNS} corridas x {n_slots} slot(s)")
    executor = None
    if n_slots > 1:
        executor = ProcessPoolExecutor(max_workers=n_slots)
        # pre-calentar: el forkserver de Python 3.14 tarda ~0.3 s en crear
        # cada proceso; sin esto la corrida 1 saldría inflada.
        for _ in range(n_slots):
            executor.submit(_tarea_cpu, 1).result()
    try:
        tiempos = []
        for i in range(1, BENCHMARK_RUNS + 1):
            t0 = time.perf_counter()
            if n_slots == 1:
                execute_cpu_work(BENCHMARK_UNITS)
            else:
                futs = [executor.submit(_tarea_cpu, BENCHMARK_UNITS)
                        for _ in range(n_slots)]
                for f in futs:
                    f.result()
            tiempos.append(time.perf_counter() - t0)
            print(f"  corrida {i:2d}/{BENCHMARK_RUNS}: {tiempos[-1]:.4f} s")
        media = statistics.fmean(tiempos)
        stdev = statistics.stdev(tiempos) if len(tiempos) > 1 else 0.0
        por_slot = BENCHMARK_UNITS / media              # por slot
        agregada = por_slot * n_slots                   # capacidad del nodo
        print(f"[BENCH] tiempo medio : {media:.4f} s")
        print(f"[BENCH] desv. estandar: {stdev:.4f} s")
        print(f"[BENCH] velocidad por slot: {por_slot:,.0f} unidades/s")
        print(f"[BENCH] velocidad AGREGADA del nodo ({n_slots} slot(s)): "
              f"{agregada:,.0f} unidades/s (usar como speed del host en SimGrid)")
    finally:
        if executor is not None:
            executor.shutdown()
    return media, stdev, agregada


# ----------------------------------------------------------------------
# Modo normal (conectado al coordinador)
# ----------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(description="Worker real ZeroMQ del laboratorio")
    p.add_argument("--coordinator-ip", default="192.168.56.10",
                   help="IP del nodo coordinador (default 192.168.56.10)")
    p.add_argument("--worker-id", default=socket.gethostname(),
                   help="identidad DEALER (default: hostname de la VM)")
    p.add_argument("--benchmark", action="store_true",
                   help="no conecta al coordinador; calibra la vCPU y sale")
    p.add_argument("--threads", type=int, default=1,
                   help="slots de ejecución en paralelo (procesos). El valor "
                        "debe coincidir entre el servicio y --benchmark; "
                        "worker-1 (2 vCPU) corre con 2")
    return p.parse_args()


def run_worker(args):
    ctx = zmq.Context.instance()

    # Canal de datos: DEALER con identidad = worker_id
    dealer = ctx.socket(zmq.DEALER)
    dealer.setsockopt(zmq.LINGER, 1000)
    dealer.setsockopt_string(zmq.IDENTITY, args.worker_id)
    dealer.connect(f"tcp://{args.coordinator_ip}:{PORT_TASKS}")

    # Canal de control: REP local; el coordinador hace REQ connect al
    # status_endpoint anunciado en el REGISTER.
    rep = ctx.socket(zmq.REP)
    rep.setsockopt(zmq.LINGER, 1000)
    rep.bind(f"tcp://*:{PORT_STATUS}")

    estado = WorkerState()

    try:
        # 1) REGISTER con REINTENTOS hasta ACK. El worker corre como servicio
        #    systemd y puede arrancar ANTES que exista cualquier coordinador:
        #    el REGISTER enviado una sola vez queda en cola de ZMQ y lo
        #    consume el primer ROUTER que aparezca (o nadie) — sin reintentos,
        #    TODA corrida posterior del coordinador vería 0 workers.
        h_receptor = threading.Thread(target=hilo_receptor,
                                      args=(dealer, estado, args.worker_id),
                                      daemon=True, name="receptor")
        h_receptor.start()

        mi_ip = get_local_ip(args.coordinator_ip)
        status_endpoint = f"tcp://{mi_ip}:{PORT_STATUS}"
        reg = {"type": "REGISTER",
               "worker_id": args.worker_id,
               "status_endpoint": status_endpoint}
        while not estado.registrado.is_set() and not estado.stop_event.is_set():
            dealer.send_json(reg)
            print(f"[REG] {args.worker_id} -> {args.coordinator_ip}:{PORT_TASKS} "
                  f"(control: {status_endpoint})")
            estado.registrado.wait(timeout=2.5)

        # 2) Slots de ejecución: pool de procesos si --threads > 1 (los
        #    threads de Python no paralelizan el hashing por el GIL).
        executor, send_lock = None, threading.Lock()
        estado.n_slots = max(1, args.threads)
        if estado.n_slots > 1:
            executor = ProcessPoolExecutor(max_workers=estado.n_slots)
            # pre-calentar: el forkserver tarda ~0.3 s por proceso la 1ª vez
            for _ in range(estado.n_slots):
                executor.submit(_tarea_cpu, 1).result()

        # 3) Lanzar los hilos restantes de servicio (1 hilo consumidor por slot)
        h_control = threading.Thread(target=hilo_control,
                                     args=(rep, estado, args.worker_id),
                                     daemon=True, name="control")
        hilos_trabajo = [
            threading.Thread(target=hilo_trabajador,
                             args=(dealer, estado, args.worker_id,
                                   executor, send_lock),
                             daemon=True, name=f"trabajador-{i}")
            for i in range(estado.n_slots)
        ]
        h_control.start()
        for h in hilos_trabajo:
            h.start()

        # 4) Esperar el cierre limpio (STOP o Ctrl+C)
        while not estado.stop_event.is_set():
            time.sleep(0.2)
        for h in hilos_trabajo:
            h.join(timeout=5)

        # 5) Resumen final: active_cpu_s para load_imbalance_cv
        dealer.send_json({"type": "FINAL_REPORT",
                          "worker": args.worker_id,
                          "tasks_completed": estado.tasks_completed,
                          "active_cpu_s": round(estado.active_cpu_s, 6)})
        print(f"[OK] {estado.tasks_completed} tareas | "
              f"active_cpu_s={estado.active_cpu_s:.3f} -> resumen enviado")
    finally:
        estado.stop_event.set()  # despierta a los hilos daemon
        if 'executor' in locals() and executor is not None:
            executor.shutdown(wait=False, cancel_futures=True)
        if 'hilos_trabajo' in locals():
            for h in hilos_trabajo:
                if h.is_alive():
                    h.join(timeout=3)  # deja terminar la tarea en curso
        dealer.close(1000)
        rep.close(1000)
        ctx.term()


def main():
    args = parse_args()
    try:
        if args.benchmark:
            run_benchmark(max(1, args.threads))
        else:
            run_worker(args)
    except KeyboardInterrupt:
        print("\n[EXIT] interrumpido por el usuario", file=sys.stderr)
        sys.exit(130)


if __name__ == "__main__":
    main()
