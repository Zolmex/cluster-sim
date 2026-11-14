#!/usr/bin/env python3
"""Módulo lógico común: estrategias de balanceo de carga.

Python 100% puro (sin dependencias de red ni de SimGrid) para poder
importarse idénticamente en el clúster real (ZeroMQ) y en la simulación
SimGrid S4U.

La consulta de carga se abstrae mediante un callback inyectado
(`get_loads`), que el entorno anfitrión implementa:
  - Clúster real: round-trip de mensajes de estado por ZeroMQ.
  - Simulación: lectura del estado interno de los procesos S4U.

Costo de mensajes de control por decisión:
  round_robin  -> 0  (estático, no consulta estado)
  least_loaded -> N  (consulta a los N workers)
  power_of_two -> 2  (consulta solo a los 2 workers muestreados)
"""
from __future__ import annotations

import random

# Estrategias disponibles (selección por nombre)
STRATEGIES = ("round_robin", "least_loaded", "power_of_two")


class LoadBalancer:
    """Clase base: decide a qué worker enviar la siguiente tarea.

    :param worker_ids: identificadores de los workers registrados.
    :param get_loads: callback opcional `get_loads(ids) -> {id: carga}`.
                      La carga se define como tareas en cola + en ejecución.
    :param seed: semilla para decisiones pseudoaleatorias reproducibles.
    """

    name = "base"

    def __init__(self, worker_ids, get_loads=None, seed=None):
        if not worker_ids:
            raise ValueError("Se requiere al menos un worker registrado")
        self.worker_ids = sorted(worker_ids)
        self.get_loads = get_loads
        self.rng = random.Random(seed)
        # Mensajes de control acumulados (consultas de estado por decisión)
        self.control_messages = 0

    def _query_loads(self, ids):
        """Consulta la carga de `ids`; contabiliza 1 mensaje de control
        por worker consultado (criterio compartido con la simulación)."""
        ids = list(ids)
        self.control_messages += len(ids)
        if self.get_loads is None:
            # Sin interfaz de estado (modo offline): se asume carga 0.
            return {w: 0 for w in ids}
        return self.get_loads(ids)

    def select(self, task_id):
        """Devuelve el worker_id asignado a la tarea `task_id`."""
        raise NotImplementedError

    def stats(self):
        """Resumen del costo de control acumulado por la estrategia."""
        return {"strategy": self.name, "control_messages": self.control_messages}


class RoundRobinBalancer(LoadBalancer):
    """round_robin: asignación cíclica estática, sin consultar estado."""

    name = "round_robin"

    def __init__(self, worker_ids, get_loads=None, seed=None):
        super().__init__(worker_ids, get_loads, seed)
        self._next = 0

    def select(self, task_id):
        wid = self.worker_ids[self._next % len(self.worker_ids)]
        self._next += 1
        return wid  # 0 mensajes de control


class LeastLoadedBalancer(LoadBalancer):
    """least_loaded: consulta la carga de TODOS los workers y elige el menor."""

    name = "least_loaded"

    def select(self, task_id):
        loads = self._query_loads(self.worker_ids)  # costo: N mensajes
        # Desempate determinista: menor identificador.
        return min(loads.items(), key=lambda kv: (kv[1], kv[0]))[0]


class PowerOfTwoBalancer(LoadBalancer):
    """power_of_two: elige 2 workers al azar y manda la tarea al menos cargado."""

    name = "power_of_two"

    def select(self, task_id):
        if len(self.worker_ids) < 2:
            # Con un único worker no hay muestreo posible.
            return self.worker_ids[0]
        candidatos = self.rng.sample(self.worker_ids, 2)  # muestra de 2
        loads = self._query_loads(candidatos)            # costo: 2 mensajes
        return min(loads.items(), key=lambda kv: (kv[1], kv[0]))[0]


_REGISTRY = {
    RoundRobinBalancer.name: RoundRobinBalancer,
    LeastLoadedBalancer.name: LeastLoadedBalancer,
    PowerOfTwoBalancer.name: PowerOfTwoBalancer,
}


def create(strategy, worker_ids, get_loads=None, seed=None):
    """Fábrica: crea el balanceador por nombre, validando la estrategia."""
    try:
        cls = _REGISTRY[strategy]
    except KeyError:
        raise ValueError(
            f"Estrategia desconocida: {strategy!r}. Válidas: {', '.join(STRATEGIES)}"
        ) from None
    return cls(worker_ids, get_loads=get_loads, seed=seed)


if __name__ == "__main__":
    # Prueba rápida sin red: carga simulada fija.
    cargas = {"w1": 3, "w2": 1, "w3": 2}
    for s in STRATEGIES:
        bal = create(s, cargas.keys(), get_loads=lambda ids: {i: cargas[i] for i in ids}, seed=7)
        asignaciones = [bal.select(f"t{k}") for k in range(12)]
        print(f"{s:13s} -> {asignaciones}  control={bal.control_messages}")
