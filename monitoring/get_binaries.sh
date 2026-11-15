#!/usr/bin/env bash
#
# get_binaries.sh — descarga los tarballs de Prometheus y Grafana en
# monitoring/bin/ (opción autocontenida, sin root ni docker).
#
# ¡Es OPCIONAL! Si ya tenés Prometheus/Grafana instalados (paquetes de tu
# distro, Docker, o binarios propios), no los necesitás: ver README.md §4.
#
set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$REPO/monitoring/bin"
mkdir -p "$BIN"

PROM_VER=3.16.0
GRAF_VER=13.3.1

if [ ! -f "$BIN/prometheus.tar.gz" ]; then
    echo "==> Prometheus $PROM_VER..."
    curl -fL -o "$BIN/prometheus.tar.gz" \
        "https://github.com/prometheus/prometheus/releases/download/v${PROM_VER}/prometheus-${PROM_VER}.linux-amd64.tar.gz"
else
    echo "==> Prometheus ya descargado."
fi

if [ ! -f "$BIN/grafana.tar.gz" ]; then
    echo "==> Grafana $GRAF_VER (~460 MB, tarda un rato)..."
    curl -fL -o "$BIN/grafana.tar.gz" \
        "https://dl.grafana.com/oss/release/grafana-${GRAF_VER}.linux-amd64.tar.gz"
else
    echo "==> Grafana ya descargado."
fi

echo "Listo. Ahora: ./monitoring/start_monitoring.sh"
