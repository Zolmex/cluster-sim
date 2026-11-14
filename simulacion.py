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
UNIDADES_TRIVIAL = 1_000_000   # tarea de verificación (= BENCHMARK_UNITS de worker.py)

ANCHO_BANDA = "100Mbps"  # ancho de banda de los enlaces dedicados
# Velocidades calibrables del perfil heterogéneo (ver plataforma_base.xml):
SPEED_COORDINADOR = "1Gf"   # el coordinador no computa; valor nominal
SPEED_POTENTE = "2Gf"       # 25% de los workers (worker-1 en el XML)
SPEED_MEDIO = "1Gf"         # 50% de los workers (worker-2 en el XML)
SPEED_LENTO = "0.5Gf"       # 25% de los workers (worker-3 en el XML)

# Estado compartido entre actores S4U de una MISMA corrida (los actores Python
# corren en hilos reales; el GIL hace atómicas las lecturas/escrituras simples).
_ESTADO = {"busy": {}}   # worker_id -> ¿está ejecutando una tarea?
_RESULTADO = {}          # fila CSV (+ extras) que produce el actor coordinador
_CSV_LOCK = threading.Lock()


# ---------------------------------------------------------------------------
# Actores S4U
# ---------------------------------------------------------------------------
def _actor_worker(worker_id: str, cfg: dict):
    """Actor S4U del worker: replica worker.py (REGISTER -> TASK -> RESULT -> STOP).

    Ejecuta el cómputo con this_actor.execute() usando el factor de calibración
    empírico `--flops-per-unit` (unidades de workload.py -> FLOPS simulados).
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

        # 3) RESULT: envío no bloqueante (como dealer.send_json de worker.py)
        resultados.put_async(
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
        carga = 1 if _ESTADO["busy"].get(worker_id) else 0
        simgrid.Mailbox.by_name(peticion["reply_to"]).put(
            {"worker_id": worker_id, "queue": 0, "running": carga}, CONTROL_BYTES)


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
    rep_status = simgrid.Mailbox.by_name("status-reply-coord")

    def get_loads(ids):
        """Callback de estrategias: consulta STATUS worker a worker (REQ/REP).

        Cada consulta cuesta un ida y vuelta de 64 B por los enlaces simulados:
        exactamente el coste de red del canal 5556 del clúster real.
        """
        cargas = {}
        for wid in ids:
            simgrid.Mailbox.by_name(f"status-{wid}").put(
                {"type": "STATUS", "reply_to": rep_status.name}, CONTROL_BYTES)
            respuesta = rep_status.get()
            cargas[wid] = respuesta["queue"] + respuesta["running"]
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

    # 4) Despacho del lote. put() es síncrono (bloquea hasta entregar): captura
    #    la saturación del coordinador a gran escala igual que el envío real.
    for t in tasks:
        wid = bal.select(t["task_id"])      # puede bloquear en STATUS
        if t_first is None:
            t_first = simgrid.Engine.clock
        datos[wid].put(t, t["payload_bytes"])

    # 5) Confirmaciones de finalización (RESULT)
    while completadas < len(tasks):
        r = simgrid.Mailbox.by_name("resultados").get()
        completadas += 1
        waits.append(r["started_at"] - created[r["task_id"]])
        active[r["worker"]] += r["finished_at"] - r["started_at"]
    t_last = simgrid.Engine.clock

    # 6) Cierre limpio: STOP por el canal de datos y por el de control
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
    _ESTADO["busy"] = {}
    _RESULTADO.clear()

    # SimGrid solo admite un Engine por proceso => --batch-all usa subprocesos.
    e = simgrid.Engine(["simulacion.py"])

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
          f"-> {'OK (0.0%)' if round(error_pct, 6) == 0.0 else 'DESVIACIÓN'}")
    print(f"[TRIVIAL] fila exportada a {args.output_csv}")
    return 0 if round(error_pct, 6) == 0.0 else 1


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
        # 3 estrategias x 4 escalas (16/64/256/512), latencia de laboratorio (1 ms)
        combos = [(s, n, 1.0, workload.DEFAULT_SEED)
                  for n in (16, 64, 256, 512) for s in strategies.STRATEGIES]
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
    p.add_argument("--flops-per-unit", type=float, default=1000.0,
                   help="factor de calibración empírico: FLOPS simulados por "
                        "unidad de cpu_work (ver plataforma_base.xml)")
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
