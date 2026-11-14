#!/usr/bin/env python3
"""Generador de carga sintética: lotes de tareas agrometeorológicas.

Distribución bimodal (80/20):
  - Livianas (telemetría de estaciones): CPU 200_000 u, payload 10 KB.
  - Pesadas (imágenes de cultivo):       CPU 3_000_000 u, payload 2 MB.

La semilla fija garantiza lotes reproducibles entre el clúster real
y la simulación SimGrid S4U. Sin dependencias externas.
"""
from __future__ import annotations

import json
import random
from typing import List

# Parámetros de la distribución bimodal
HEAVY_FRACTION = 0.20        # proporción de tareas pesadas
LIGHT_CPU_WORK = 200_000     # costo CPU de telemetría (unidades)
LIGHT_PAYLOAD = 10 * 1024    # 10 KB
HEAVY_CPU_WORK = 3_000_000   # costo CPU de imágenes de cultivo
HEAVY_PAYLOAD = 2 * 1024 * 1024  # 2 MB

DEFAULT_TASKS = 100
DEFAULT_SEED = 42


def generate_batch(n: int = DEFAULT_TASKS, seed: int = DEFAULT_SEED) -> List[dict]:
    """Genera un lote de N tareas bimodales reproducibles.

    :param n: cantidad de tareas (por defecto 100).
    :param seed: semilla aleatoria fija.
    :return: lista de dicts con task_id, cpu_work, payload_bytes, created_at.

    `created_at` es un timestamp LÓGICO (segundos desde el origen del
    lote) para que el lote sea 100% determinista. El anfitrión real o
    simulado lo convierte a tiempo absoluto sumando su instante t0.
    """
    rng = random.Random(seed)
    tasks = []
    for i in range(n):
        pesada = rng.random() < HEAVY_FRACTION
        tasks.append(
            {
                "task_id": f"t{i:05d}",
                "cpu_work": HEAVY_CPU_WORK if pesada else LIGHT_CPU_WORK,
                "payload_bytes": HEAVY_PAYLOAD if pesada else LIGHT_PAYLOAD,
                # desplazamiento nominal de 1 µs: despreciable y determinista
                "created_at": i * 1e-6,
            }
        )
    return tasks


def summarize(tasks: List[dict]) -> dict:
    """Resumen del lote para verificación rápida."""
    pesadas = sum(1 for t in tasks if t["cpu_work"] == HEAVY_CPU_WORK)
    return {
        "n_tasks": len(tasks),
        "livianas": len(tasks) - pesadas,
        "pesadas": pesadas,
        "cpu_total": sum(t["cpu_work"] for t in tasks),
        "bytes_total": sum(t["payload_bytes"] for t in tasks),
    }


if __name__ == "__main__":
    import argparse

    p = argparse.ArgumentParser(description="Generador de carga sintética bimodal")
    p.add_argument("--tasks", type=int, default=DEFAULT_TASKS)
    p.add_argument("--seed", type=int, default=DEFAULT_SEED)
    args = p.parse_args()

    lote = generate_batch(args.tasks, args.seed)
    # JSON del lote a stdout (consumible por otros procesos); resumen a stderr.
    json.dump(lote, __import__("sys").stdout)
    print(f"resumen: {summarize(lote)}", file=__import__("sys").stderr)
