#!/usr/bin/env bash
#
# stop_monitoring.sh — detiene Prometheus y Grafana levantados por
# start_monitoring.sh (usa los PID guardados en monitoring/.pids).
#
set -u
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PIDS="$REPO/monitoring/.pids"

for nombre in grafana prometheus; do
    f="$PIDS/$nombre.pid"
    if [ -f "$f" ]; then
        pid=$(cat "$f")
        if kill -0 "$pid" 2>/dev/null; then
            kill "$pid" && echo "==> $nombre detenido (pid $pid)"
        else
            echo "==> $nombre ya no estaba corriendo (pid $pid)"
        fi
        rm -f "$f"
    else
        echo "==> $nombre: sin PID registrado."
    fi
done
