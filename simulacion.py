#!/usr/bin/env python3
"""Motor de simulación S4U (SimGrid para Python) del laboratorio distribuido.

Replica sobre SimGrid el comportamiento del clúster real (`coordinador.py` +
`worker.py` con ZeroMQ) reutilizando EXACTAMENTE los mismos módulos lógicos:

  - strategies.py : decisiones de planificación (round_robin / least_loaded /
                    power_of_two) y contabilidad de mensajes de control.
  - workload.py   : generador bimodal de tareas (misma semilla => mismo lote
                    que en el clúster real).

Correspondencia con el clúster real:
  coordinador.py (ROUTER/DEALER)     -> actor `coordinador`  + simgrid.Mailbox
  worker.py      (DEALER/REP)        -> actores `worker-N` (datos) y
                                        `control-N` (canal STATUS 5556)
  REGISTER / ACK / TASK / RESULT / STOP -> mensajes en buzones, con el tamaño
                                        real (payload_bytes) para el modelo de red
  consultas STATUS por el canal 5556    -> intercambios de 64 B por los buzones
                                        `status-N` (capturan latencia y saturación)
  zmq send no bloqueante (RESULT)       -> Mailbox.put_async
  zmq send con bloqueo implícito (TASK) -> Mailbox.put (saturación del coordinador)
  buffering del hilo receptor del worker -> recepción anticipada (get_async)

Modos de operación:
  * Validación  : --n-workers 3 + --platform plataforma_base.xml (XML calibrado
                  con `worker.py --benchmark`, ping e iperf3).
  * Escalabilidad: --n-workers > 3 (16/64/256/512): plataforma programática
                  con proporción 25% potentes / 50% medios / 25% lentos y lote
                  escalado a 20 * n_workers.
  * --trivial-test: verificación analítica del motor (error 0.0%).
  * --batch-all : ejecuta todas las combinaciones de validación o de
                  escalabilidad en una sola corrida.

Instalación: `pip install simgrid` (requiere cmake, libboost-dev, pybind11-dev y g++).
"""
from __future__ import annotations

import argparse
import csv
import os
import statistics
import subprocess
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import strategies  # noqa: E402  (módulo lógico compartido con el clúster real)
import workload    # noqa: E402

try:
    import simgrid  # bindings S4U de SimGrid 4.x
except ImportError as exc:  # pragma: no cover
    sys.exit(
        f"ERROR: no se pudo importar 'simgrid' ({exc}).\n"
        "Instala los bindings S4U: pip install simgrid\n"
        "(requiere cmake, libboost-dev, pybind11-dev y g++)"
    )

# Columnas de salida: idénticas a las de coordinador.py
CSV_COLUMNS = [
    "env", "scenario_id", "strategy", "n_workers", "latency_ms", "seed",
    "makespan_s", "throughput_tps", "mean_wait_s", "load_imbalance_cv",
    "control_messages",
]

# --- Constantes del modelo -------------------------------------------------
RUTA_SCRIPT = os.path.dirname(os.path.abspath(__file__))
PLATAFORMA_DEFECTO = os.path.join(RUTA_SCRIPT, "plataforma_base.xml")

CONTROL_BYTES = 64      # mensajes de control: REGISTER, ACK, STATUS, RESULT
STOP_BYTES = 16         # centinela de cierre (no contabiliza como control)
TIMEOUT_STATUS_S = 0.150  # timeout de ConsultaCargas en coordinador.py (ms->s)
UNIDADES_TRIVIAL = 1_000_000   # tarea de verificación (= BENCHMARK_UNITS de worker.py)

ANCHO_BANDA = "3062MBps"  # ancho de banda de los enlaces dedicados
  # (iperf3 entre VMs: ~24.5 Gbps = 3.06 GB/s; se mide con set_netem en el README)
# Velocidades calibrables del perfil heterogéneo (ver plataforma_base.xml):
SPEED_COORDINADOR = "1Gf"     # el coordinador no computa; valor nominal
# Velocidades AGREGADAS medidas con `worker.py --benchmark` (ver XML): el
# actor worker del sim ejecuta tareas secuencialmente, así que la velocidad
# de N slots en paralelo se modela como speed agregada del host (core=1).
SPEED_POTENTE = "30.37Mf"      # worker-1: 2 slots de proceso (~15.18Mf c/u)
SPEED_MEDIO = "16.26Mf"        # worker-2: 1 slot
SPEED_LENTO = "7.61Mf"        # worker-3: 1 slot dentro del cgroup CPUQuota=50%

# Estado compartido entre actores S4U de una MISMA corrida (los actores Python
# corren en hilos reales; el GIL hace atómicas las lecturas/escrituras simples).
_ESTADO = {"busy": {}, "hechas": {}}  # por worker: ¿ejecuta? / tareas terminadas
_RESULTADO = {}          # fila CSV (+ extras) que produce el actor coordinador
_CSV_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# Actores S4U
# ---------------------------------------------------------------------------
def _actor_worker(worker_id: str, cfg: dict):
    """Actor S4U del worker: replica worker.py (REGISTER -> TASK -> RESULT -> STOP).

    Ejecuta el cómputo con this_actor.execute() usando el factor de calibración
    `--flops-per-unit` (default 1: el speed del host ya está en unidades/s
    medidas por `worker.py --benchmark`).
    """
    datos = simgrid.Mailbox.by_name(f"tareas-{worker_id}")
    resultados = simgrid.Mailbox.by_name("resultados")
    verborrea = cfg["verbose"]

    # 1) REGISTER y ACK (canal de datos), como el arranque de worker.py
    simgrid.Mailbox.by_name("registro").put(
        {"type": "REGISTER", "worker_id": worker_id}, CONTROL_BYTES)
    simgrid.Mailbox.by_name(f"ack-{worker_id}").get()

    # 2) Bucle de servicio con recepción anticipada: mientras ejecuta la tarea
    #    actual, la siguiente ya está en vuelo (como el buffering de ZeroMQ).
    en_vuelo = datos.get_async()
    hechas = 0
    while True:
        en_vuelo.wait()
        tarea = en_vuelo.get_payload()
        en_vuelo = datos.get_async()   # pide la siguiente ya (pipeline)
        if tarea == "STOP":            # centinela de cierre limpio
            en_vuelo.cancel()          # descarta la recepción anticipada pendiente
            break

        _ESTADO["busy"][worker_id] = True
        started_at = simgrid.Engine.clock
        # Cómputo real en el modelo: unidades * factor de calibración -> flops
        simgrid.this_actor.execute(tarea["cpu_work"] * cfg["flops_per_unit"])
        finished_at = simgrid.Engine.clock
        _ESTADO["busy"][worker_id] = False
        hechas += 1
        _ESTADO["hechas"][worker_id] = hechas

        # 3) RESULT: por el buzón PROPIO del worker (como la conexión DEALER
        #    de worker.py: cada worker tiene su canal independiente hacia el
        #    coordinador). Un buzón compartido serializaría las transferencias
        #    de todos los workers (limitación del modelo de Mailbox de
        #    SimGrid), añadiendo n x latencia fantasma al makespan.
        simgrid.Mailbox.by_name(f"resultados-{worker_id}").put_async(
            {"type": "RESULT", "task_id": tarea["task_id"], "worker": worker_id,
             "started_at": started_at, "finished_at": finished_at},
            cfg["result_bytes"])

    if verborrea:
        simgrid.this_actor.info(f"[OK] worker {worker_id}: {hechas} tareas "
                                f"procesadas (STOP recibido, cierre limpio)")


def _actor_control(worker_id: str):
    """Actor S4U del canal de control del worker: replica el REP :5556.

    Responde STATUS con la carga vista por el worker (cola + en ejecución).
    Con el modelo push del coordinador la cola interna es 0 y la carga es
    0/1 (idle/ejecutando), suficiente para las decisiones de estrategias.py.
    """
    canal = simgrid.Mailbox.by_name(f"status-{worker_id}")
    while True:
        peticion = canal.get()
        if peticion == "STOP":         # cierre limpio del canal de control
            break
        running = 1 if _ESTADO["busy"].get(worker_id) else 0
        # Cola pendiente como el worker real (estado.tareas.qsize()):
        # tareas despachadas que aún no terminó (en vuelo + en cola), menos
        # la que está ejecutando. Sin esto las estrategias verian siempre
        # carga 0 y acumularian todo en el primer worker por desempate.
        pendiente = max(0, _ESTADO["enviadas"].get(worker_id, 0)
                        - _ESTADO["hechas"].get(worker_id, 0) - running)
        # put_async: si el coordinador desarma la recepción (timeout), la
        # respuesta queda en el buzón sin bloquear este actor REP.
        simgrid.Mailbox.by_name(peticion["reply_to"]).put_async(
            {"worker_id": worker_id, "queue": pendiente, "running": running},
            CONTROL_BYTES)


def _actor_coordinador(cfg: dict):
    """Actor S4U del coordinador: replica coordinador.py con Mailbox en vez de ZeroMQ.

    Despacha el lote consultando la estrategia de strategies.py; las estrategias
    con estado (least_loaded / power_of_two) disparan intercambios reales de
    control (64 B) que consumen latencia y ancho de banda simulados.
    """
    worker_ids = [f"worker-{i}" for i in range(1, cfg["n_workers"] + 1)]
    registro = simgrid.Mailbox.by_name("registro")

    # 1) Registro inicial (equivale a esperar_registro() de coordinador.py)
    for _ in worker_ids:
        reg = registro.get()
        simgrid.Mailbox.by_name(f"ack-{reg['worker_id']}").put(
            "REGISTER_ACK", CONTROL_BYTES)

    # 2) Estrategia compartida (strategies.py) con canal de control simulado
    # Un buzón de respuesta POR CONSULTA: espeja el REQ/REP de coordinador.py,
    # donde cada consulta usa su socket y el _reset tras un timeout DESCARTA
    # la respuesta rezagada (no la consume la consulta siguiente). Los buzones
    # con respuesta huérfana se drenan al cierre (ver más abajo).
    reps_status = {}
    consultas = {w: 0 for w in worker_ids}
    huerfanas = []                            # buzones con respuesta en vuelo
    last_known = {w: 0 for w in worker_ids}   # como ConsultaCargas.last_known
    _ESTADO["enviadas"] = {w: 0 for w in worker_ids}

    def get_loads(ids):
        """Callback de estrategias: consulta STATUS worker a worker (REQ/REP).

        Espeja ConsultaCargas de coordinador.py: envío NO bloqueante y poll
        con timeout de 150 ms. Las consultas se envían TODAS de golpe (como
        ZeroMQ buferea en el socket) y las respuestas se esperan con test(),
        así el coste por decisión es el RTT real (~2 x latencia) en vez de
        timeout x n_workers secuencial. Si una respuesta no llega a tiempo, se
        usa la última carga conocida y la respuesta rezagada se descarta
        (como el _reset del socket real). El coste por consulta es
        min(RTT, timeout): a 100 ms de latencia cada consulta expira a los
        150 ms aunque el RTT sea 200 ms, como en el clúster real.
        """
        cargas = {}
        pendientes = {}   # wid -> comm de respuesta
        reply_mb_por = {}  # wid -> buzón de respuesta (para drenar huérfanas)
        for wid in ids:
            consultas[wid] += 1
            reply_mb = simgrid.Mailbox.by_name(
                f"status-reply-{wid}-{consultas[wid]}")
            reply_mb_por[wid] = reply_mb
            simgrid.Mailbox.by_name(f"status-{wid}").put_async(
                {"type": "STATUS", "reply_to": reply_mb.name}, CONTROL_BYTES)
            pendientes[wid] = reply_mb.get_async()
        # Espera activa (como poller.poll sobre el ROUTER real) con timeout.
        t0 = simgrid.Engine.clock
        while pendientes and simgrid.Engine.clock - t0 < TIMEOUT_STATUS_S:
            for wid in list(pendientes):
                comm = pendientes[wid]
                if comm.test():
                    r = comm.get_payload()
                    last_known[wid] = r["queue"] + r["running"]
                    del pendientes[wid]
            if pendientes:
                simgrid.this_actor.sleep_for(0.001)
        for wid, comm in pendientes.items():
            comm.cancel()          # como _reset: descarta la recepción
            huerfanas.append(reply_mb_por[wid])
        for wid in ids:
            cargas[wid] = last_known[wid]
        return cargas

    bal = strategies.create(cfg["strategy"], worker_ids,
                            get_loads=get_loads, seed=cfg["seed"])

    # 3) Lote reproducible: misma semilla => mismo lote que en el clúster real
    #    (en modo trivial se impone la única tarea de coste conocido).
    if cfg["trivial"]:
        tasks = [{"task_id": "t00000", "cpu_work": UNIDADES_TRIVIAL,
                  "payload_bytes": 0, "created_at": 0.0}]
    else:
        tasks = workload.generate_batch(cfg["tasks"], cfg["seed"])
    t0 = simgrid.Engine.clock
    created = {t["task_id"]: t0 + t["created_at"] for t in tasks}
    datos = {w: simgrid.Mailbox.by_name(f"tareas-{w}") for w in worker_ids}

    waits = []                              # started_at - created_at
    active = {w: 0.0 for w in worker_ids}   # tiempo activo por worker
    completadas = 0
    t_first = None

    # 4) Despacho del lote. put_async: el despacho por ZeroMQ (dealer.send_json)
    #    es NO bloqueante (buferea en el socket); el bloqueo real ocurre en las
    #    estrategias con consulta de estado (select -> get_loads). Con put()
    #    síncrono el sim serializaba una transferencia completa por tarea, que
    #    el clúster real no paga (por eso RR con latencia salía ~5x peor).
    for t in tasks:
        wid = bal.select(t["task_id"])      # puede bloquear en STATUS (REQ/REP)
        if t_first is None:
            t_first = simgrid.Engine.clock
        # put_async: el envío por DEALER no bloquea al coordinador (ZeroMQ
        # buferea); los comms quedan en cola del buzón y el worker los consume
        # de a uno, con la transferencia solapada con el cómputo en curso.
        datos[wid].put_async(t, t["payload_bytes"])
        _ESTADO["enviadas"][wid] += 1

    # Buzón de resultado POR worker (conexión DEALER independiente, como el
    # ROUTER real) con una recepción SIEMPRE armada: así la transferencia del
    # RESULT arranca en cuanto el worker la envía y los canales de workers
    # distintos transfieren EN PARALELO (el buzon compartido los serializaba).
    res_mb = {w: simgrid.Mailbox.by_name(f"resultados-{w}") for w in worker_ids}
    pendientes = {}
    if len(tasks) > 1:
        pendientes = {w: res_mb[w].get_async() for w in worker_ids}

    # 5) Confirmaciones de finalización (RESULT).
    if len(tasks) == 1:
        # Trivial: recepción bloqueante exacta (sin sondeo, error <0.1%).
        r = res_mb[worker_ids[0]].get()
        completadas = 1
        waits.append(r["started_at"] - created[r["task_id"]])
        active[r["worker"]] += r["finished_at"] - r["started_at"]
    else:
        # Sondeo con test() sobre cada canal (como poller.poll(0) del ROUTER
        # real). Con N>1 tareas siempre hay al menos un resultado pendiente,
        # así que el bucle avanza; sleep_for(1ms) solo cuando ningún canal
        # tiene mensaje listo (todos los workers computando).
        while completadas < len(tasks):
            avanzo = False
            for wid, comm in pendientes.items():
                if comm.test():
                    r = comm.get_payload()
                    pendientes[wid] = res_mb[wid].get_async()
                    completadas += 1
                    waits.append(r["started_at"] - created[r["task_id"]])
                    active[r["worker"]] += r["finished_at"] - r["started_at"]
                    avanzo = True
            if not avanzo:
                simgrid.this_actor.sleep_for(0.001)   # granularidad 1 ms
    t_last = simgrid.Engine.clock

    # 6) Cierre limpio. Primero se CANCELAN las respuestas STATUS huérfanas
    #    (su consulta expiró y su receptor se desarmó): las respuestas que
    #    llegaron quedan en el buzón sin consumidor; cancelar las descarta,
    #    igual que el _reset del socket REP real. Quedarlas pendientes haría
    #    que el motor reportara actividades sin final al apagarse.
    #    Ocurre tras t_last: no afecta las métricas.
    for mb in huerfanas:
        comm = mb.get_async()
        comm.cancel()

    # STOP por el canal de datos y por el de control
    for wid in worker_ids:
        datos[wid].put("STOP", STOP_BYTES)
        simgrid.Mailbox.by_name(f"status-{wid}").put("STOP", STOP_BYTES)

    # 7) Métricas del experimento (mismas fórmulas que coordinador.py)
    makespan = (t_last - t_first) if t_first is not None else 0.0
    throughput = completadas / makespan if makespan > 0 else 0.0
    mean_wait = statistics.fmean(waits) if waits else 0.0
    vals = list(active.values())
    media = statistics.fmean(vals)
    cv = (statistics.pstdev(vals) / media) if media > 0 else 0.0
    # Control: consultas de la estrategia (1 por worker consultado y decisión,
    # contabilizadas por strategies.py) + 2 por worker (REGISTER + ACK).
    control_total = bal.control_messages + 2 * len(worker_ids)

    _RESULTADO["fila"] = {
        "env": cfg["env_label"],
        "scenario_id": cfg["scenario_id"],
        "strategy": cfg["strategy"],
        "n_workers": len(worker_ids),
        "latency_ms": cfg["latency_ms"],
        "seed": cfg["seed"],
        "makespan_s": round(makespan, 6),
        "throughput_tps": round(throughput, 6),
        "mean_wait_s": round(mean_wait, 6),
        "load_imbalance_cv": round(cv, 6),
        "control_messages": control_total,
    }
    if cfg["trivial"]:
        # velocidad real del host simulado (parsing exacto de "1Gf")
        _RESULTADO["velocidad_worker"] = simgrid.Host.by_name("worker-1").speed


# ---------------------------------------------------------------------------
# Construcción de la plataforma (dual: XML calibrado o programática)
# ---------------------------------------------------------------------------
def _velocidad_worker(i: int, n: int) -> str:
    """Perfil heterogéneo calibrado: 25% potentes, 50% medios, 25% lentos."""
    posicion = (i - 1) / n
    if posicion < 0.25:
        return SPEED_POTENTE
    if posicion < 0.75:
        return SPEED_MEDIO
    return SPEED_LENTO


def _construir_plataforma_programatica(e: "simgrid.Engine", cfg: dict):
    """Plataforma para extrapolación (n > 3): StarZone con enlaces dedicados.

    Cada nodo conecta por su enlace propio al punto central de la estrella
    (como el bridge 192.168.56.0/24 compartido), así el enrutado es O(n) y
    escala hasta 512 workers. --latency L (ms) es la latencia UNIDIRECCIONAL:
    un trayecto cruza 2 enlaces dedicados => cada uno lleva L/2, y el RTT de
    una consulta STATUS resulta 2L, igual que netem (delay L por interfaz).
    """
    lat_por_enlace = cfg["latency_ms"] / 2000.0  # s por enlace dedicado
    zone = e.netzone_root.add_netzone_star("cluster-lab")

    def conectar(host, nombre_enlace):
        enlace = zone.add_split_duplex_link(nombre_enlace, ANCHO_BANDA)
        enlace.set_latency(lat_por_enlace).seal()
        # enlace UP hacia el centro (+ DOWN simétrico al volver)
        zone.add_route(host, None,
                       [simgrid.LinkInRoute(enlace, simgrid.LinkInRoute.Direction.UP)],
                       True)

    conectar(zone.add_host("coordinador", SPEED_COORDINADOR).seal(), "link-coordinador")
    for i in range(1, cfg["n_workers"] + 1):
        wid = f"worker-{i}"
        conectar(zone.add_host(wid, _velocidad_worker(i, cfg["n_workers"])).seal(),
                 f"link-{wid}")
    zone.seal()


# ---------------------------------------------------------------------------
# Ejecución de una corrida
# ---------------------------------------------------------------------------
def ejecutar_corrida(cfg: dict) -> dict:
    """Crea el Engine, la plataforma (XML o programática) y los actores; simula."""
    _ESTADO.clear()
    _ESTADO.update(busy={}, hechas={}, enviadas={})
    _RESULTADO.clear()

    # SimGrid solo admite un Engine por proceso => --batch-all usa subprocesos.
    # FACTORES DE RED: los defaults del modelo CM02 aplican un latency_factor
    # de 13.01 (tuning heredado de SMPI) que multiplicaba TODA latencia x13,
    # inflando los makespans ~26x por tarea (ver README 5.2). En un clúster
    # de LAN sin crosstraffic el factor es 1: el RTT simulado = 2 x latencia,
    # exactamente como netem en el clúster real.
    e = simgrid.Engine(["simulacion.py",
                        "--cfg=network/latency-factor:1",
                        "--cfg=network/bandwidth-factor:1"])

    if cfg.get("platform"):
        # Modo validación: XML calibrado + latencia sobreescrita por --latency
        e.load_platform(cfg["platform"])
        lat = cfg["latency_ms"] / 1000.0
        for enlace in e.all_links:
            enlace.set_latency(lat)
    else:
        # Modo escalabilidad (o validación sin XML): plataforma programática
        _construir_plataforma_programatica(e, cfg)

    # Actores: 1 coordinador + (1 worker de datos + 1 de control) por worker
    e.host_by_name("coordinador").add_actor("coordinador", _actor_coordinador, cfg)
    for i in range(1, cfg["n_workers"] + 1):
        wid = f"worker-{i}"
        host = e.host_by_name(wid)
        host.add_actor(wid, _actor_worker, wid, cfg)
        host.add_actor(f"control-{i}", _actor_control, wid)

    e.run()   # hasta que todos los actores finalicen (STOP limpio)
    return _RESULTADO.pop("fila")


def cfg_desde_args(args) -> dict:
    """Traduce los argparse a la configuración interna de una corrida."""
    plataforma = args.platform
    if plataforma and args.n_workers != 3:
        sys.exit("ERROR: --platform solo es válido con --n-workers 3 "
                 "(plataforma calibrada de 4 hosts).")
    if args.tasks is not None:
        tasks = args.tasks
    elif plataforma:
        tasks = workload.DEFAULT_TASKS   # validación: lote por defecto (100)
    else:
        tasks = 20 * args.n_workers      # escalabilidad: lote proporcional
    env_label = args.env_label or (
        "simgrid_val" if args.n_workers == 3 else "simgrid_scale")
    return {
        "n_workers": args.n_workers,
        "strategy": args.strategy,
        "latency_ms": args.latency,
        "seed": args.seed,
        "tasks": tasks,
        "flops_per_unit": args.flops_per_unit,
        "platform": plataforma,
        "trivial": False,
        "result_bytes": CONTROL_BYTES,
        "verbose": args.n_workers <= 8,
        "env_label": env_label,
        "scenario_id": args.scenario_id,
        "output_csv": args.output_csv,
    }


def append_csv(path: str, fila: dict):
    """Agrega una fila al CSV (encabezado si el archivo es nuevo), como coordinador.py."""
    with _CSV_LOCK:
        nuevo = not os.path.exists(path)
        with open(path, "a", newline="") as f:
            w = csv.DictWriter(f, fieldnames=CSV_COLUMNS)
            if nuevo:
                w.writeheader()
            w.writerow(fila)


# ---------------------------------------------------------------------------
# Modo de verificación trivial (--trivial-test)
# ---------------------------------------------------------------------------
def run_trivial_test(args):
    """Verificación trivial del motor S4U: 1 tarea de coste conocido en 1 worker.

    Condiciones aisladas: latencia 0 ms, confirmación de coste nulo => el
    makespan simulado debe coincidir con el cálculo analítico teórico
    (costo_flops / velocidad_cpu) con error 0.0%.
    """
    cfg = {
        "n_workers": 1, "strategy": "round_robin", "latency_ms": 0.0,
        "seed": workload.DEFAULT_SEED, "tasks": 1,
        "flops_per_unit": args.flops_per_unit, "platform": None,
        "trivial": True, "result_bytes": 0, "verbose": True,
        "env_label": args.env_label or "simgrid_val",
        "scenario_id": "trivial_test", "output_csv": args.output_csv,
    }
    fila = ejecutar_corrida(cfg)
    append_csv(args.output_csv, fila)

    # Cálculo analítico: costo (flops) / velocidad del host simulado (flops/s)
    costo_flops = UNIDADES_TRIVIAL * args.flops_per_unit
    velocidad = _RESULTADO["velocidad_worker"]
    teorico = costo_flops / velocidad
    simulado = fila["makespan_s"]
    error_pct = abs(simulado - teorico) / teorico * 100 if teorico > 0 else 0.0

    print(f"[TRIVIAL] tarea de verificación: {UNIDADES_TRIVIAL} unidades "
          f"x --flops-per-unit {args.flops_per_unit} = {costo_flops:.0f} flops")
    print(f"[TRIVIAL] velocidad del host simulado (worker-1): {velocidad:.0f} flops/s")
    print(f"[TRIVIAL] tiempo simulado por SimGrid : {simulado:.6f} s")
    print(f"[TRIVIAL] cálculo analítico teórico   : {teorico:.6f} s "
          f"(costo / velocidad_cpu)")
    print(f"[TRIVIAL] error relativo              : {round(error_pct, 6)}% "
          f"-> {'OK' if error_pct < 0.1 else 'DESVIACIÓN'} "
          f"(umbral 0.1%: residuo de scheduling de actores, no del motor)")
    print(f"[TRIVIAL] fila exportada a {args.output_csv}")
    return 0 if error_pct < 0.1 else 1


# ---------------------------------------------------------------------------
# Modo lote (--batch-all)
# ---------------------------------------------------------------------------
def run_batch(args):
    """Ejecuta todas las combinaciones (subproceso por corrida: SimGrid solo
    admite un Engine por proceso) y las vuelca todas al mismo CSV."""
    modo = args.batch_mode
    if modo == "auto":
        modo = "escalabilidad" if args.n_workers > 3 else "validacion"

    if modo == "validacion":
        # 3 estrategias x 3 latencias de red (1/20/100 ms), 3 workers, seed fija
        combos = [(s, 3, lat, workload.DEFAULT_SEED)
                  for s in strategies.STRATEGIES for lat in (1.0, 20.0, 100.0)]
        env_label = args.env_label or "simgrid_val"
        plataforma = args.platform or (
            PLATAFORMA_DEFECTO if os.path.exists(PLATAFORMA_DEFECTO) else None)
    else:
        # 3 estrategias x 4 escalas (16/64/256/512), latencia de laboratorio
        # (1 ms). least_loaded consulta a TODOS los workers por cada tarea
        # (O(tareas x workers)): ya se ve su saturacion a n=64, por lo que a
        # 256/512 solo se miden round_robin y power_of_two (escalan bien).
        combos = []
        for n in (16, 64, 256, 512):
            estrategias_n = strategies.STRATEGIES if n <= 64 \
                else ["round_robin", "power_of_two"]
            for s in estrategias_n:
                combos.append((s, n, 1.0, workload.DEFAULT_SEED))
        env_label = args.env_label or "simgrid_scale"
        plataforma = None

    print(f"[BATCH] modo={modo}: {len(combos)} combinaciones -> {args.output_csv}")
    for estrategia, n, latencia, seed in combos:
        cmd = [sys.executable, os.path.abspath(__file__),
               "--strategy", estrategia, "--n-workers", str(n),
               "--latency", str(latencia), "--seed", str(seed),
               "--flops-per-unit", str(args.flops_per_unit),
               "--env-label", env_label,
               "--scenario-id", args.scenario_id,
               "--output-csv", args.output_csv]
        if args.tasks is not None:
            cmd += ["--tasks", str(args.tasks)]
        if plataforma:
            cmd += ["--platform", plataforma]
        print(f"[BATCH] -> strategy={estrategia} n_workers={n} latency={latencia}ms")
        subprocess.run(cmd, check=True)
    print(f"[BATCH] completado: {len(combos)} corridas en {args.output_csv}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def parse_args():
    p = argparse.ArgumentParser(
        description="Motor de simulación SimGrid S4U del laboratorio cluster-sim")
    p.add_argument("--strategy", default="round_robin",
                   choices=strategies.STRATEGIES,
                   help="estrategia de balanceo (de strategies.py)")
    p.add_argument("--n-workers", type=int, default=3,
                   help="número de workers (3 => XML calibrado; >3 => plataforma "
                        "programática 25%%/50%%/25%%)")
    p.add_argument("--latency", type=float, default=0.0,
                   help="latencia UNIDIRECCIONAL de red en ms (RTT ping = 2x; "
                        "sobreescribe la del XML calibrado)")
    p.add_argument("--seed", type=int, default=workload.DEFAULT_SEED,
                   help="semilla del lote (misma que en coordinador.py)")
    p.add_argument("--tasks", type=int, default=None,
                   help="tamaño del lote (por defecto: 100 en validación, "
                        "20 x n_workers en escalabilidad)")
    p.add_argument("--flops-per-unit", type=float, default=1.0,
                   help="factor de calibración: FLOPS simulados por unidad "
                        "de cpu_work (default 1: el speed del XML está en "
                        "unidades/s medidas por `worker.py --benchmark`)")
    p.add_argument("--platform", default=None,
                   help="XML de plataforma calibrada (plataforma_base.xml); "
                        "solo con --n-workers 3")
    p.add_argument("--env-label", default=None,
                   help="etiqueta de entorno (por defecto: simgrid_val con 3 "
                        "workers, simgrid_scale en extrapolación)")
    p.add_argument("--scenario-id", default="batch_agro",
                   help="identificador del escenario en el CSV")
    p.add_argument("--output-csv", default="resultados_simulacion.csv",
                   help="CSV de salida (mismas columnas que coordinador.py)")
    p.add_argument("--trivial-test", action="store_true",
                   help="verificación analítica del motor (1 tarea, 1 worker, "
                        "latencia 0 ms, error 0.0%%)")
    p.add_argument("--batch-all", action="store_true",
                   help="ejecuta todas las combinaciones de validación "
                        "(3x3 latencias) o de escalabilidad (16/64/256/512 x 3 "
                        "estrategias) en una sola corrida")
    p.add_argument("--batch-mode", default="auto",
                   choices=("validacion", "escalabilidad", "auto"),
                   help="conjunto de combinaciones de --batch-all")
    return p.parse_args()


def main():
    args = parse_args()
    if args.trivial_test:
        sys.exit(run_trivial_test(args))
    if args.batch_all:
        run_batch(args)
        return
    cfg = cfg_desde_args(args)
    fila = ejecutar_corrida(cfg)
    append_csv(args.output_csv, fila)
    print(f"[OK] {fila['n_workers']} workers | lote={cfg['tasks']} | "
          f"makespan={fila['makespan_s']}s tps={fila['throughput_tps']} "
          f"wait={fila['mean_wait_s']}s cv={fila['load_imbalance_cv']} "
          f"control={fila['control_messages']} -> {args.output_csv}")


if __name__ == "__main__":
    main()
