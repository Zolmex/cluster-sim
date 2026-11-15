#!/usr/bin/env bash
#
# correr_simulacion.sh — lanzador de simulacion.py con el entorno correcto.
#
# Los bindings S4U de SimGrid 4.1 (pip, sdist) no incorporan RUNPATH, por lo
# que hay que exponer libsimgrid.so.4.1 al enlazador dinámico. Este script
# usa el venv dedicado .venv-simgrid (simgrid + pybind11==2.13.6 anclado).
#
# Uso (los argumentos se pasan íntegros a simulacion.py):
#   ./scripts/correr_simulacion.sh --trivial-test
#   ./scripts/correr_simulacion.sh --strategy least_loaded --n-workers 3 \
#       --latency 1 --platform plataforma_base.xml
#   ./scripts/correr_simulacion.sh --batch-all --batch-mode escalabilidad
set -euo pipefail

RAIZ="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_PY="${RAIZ}/.venv-simgrid/bin/python"
# La versión dentro de site-packages depende del python que creó el venv.
SITE_PKGS="$(ls -d "${RAIZ}"/.venv-simgrid/lib/python3.*/site-packages 2>/dev/null | head -1)"

if [[ ! -x "${VENV_PY}" || -z "${SITE_PKGS}" ]]; then
    echo "ERROR: falta el venv ${RAIZ}/.venv-simgrid (o su site-packages)" >&2
    echo "Créalo con:" >&2
    echo "  python3 -m venv .venv-simgrid && .venv-simgrid/bin/pip install \\" >&2
    echo "      pybind11==2.13.6 cmake setuptools wheel && \\" >&2
    echo "      .venv-simgrid/bin/pip install --no-build-isolation simgrid" >&2
    exit 1
fi

export LD_LIBRARY_PATH="${SITE_PKGS}${LD_LIBRARY_PATH:+:${LD_LIBRARY_PATH}}"
exec "${VENV_PY}" "${RAIZ}/simulacion.py" "$@"
