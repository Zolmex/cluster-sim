#!/usr/bin/env bash
#
# start_monitoring.sh — levanta Prometheus (:9090) y Grafana (:3000) en el
# HOST, sin root ni docker, con binarios pre-descargados en monitoring/bin/.
#
#   ./monitoring/start_monitoring.sh
#
# Grafana queda con login anónimo como Admin (laboratorio local): abrir
# http://localhost:3000 → carpeta "SOD" → dashboard "Clúster SOD".
# Prometheus: http://localhost:9090
#
# El scraping apunta a los node_exporter de las VMs (192.168.56.10-13:9100),
# instalados por ansible/playbook.yml — el clúster debe estar levantado
# (vagrant up) para que haya datos que monitorear.
#
# Parar todo con: ./monitoring/stop_monitoring.sh

set -euo pipefail
REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="$REPO/monitoring/bin"
LOGS="$REPO/monitoring/logs"
PIDS="$REPO/monitoring/.pids"
mkdir -p "$LOGS" "$PIDS" "$REPO/monitoring/data-prom"

# --- 1) Extraer los tarballs la primera vez ---------------------------------
if [ ! -x "$BIN"/prometheus-*/prometheus ] 2>/dev/null; then
    echo "==> Extrayendo Prometheus..."
    tar -xzf "$BIN/prometheus.tar.gz" -C "$BIN"
fi
if [ ! -d "$BIN"/grafana-* ] 2>/dev/null || ! ls -d "$BIN"/grafana-*/bin/grafana >/dev/null 2>&1; then
    echo "==> Extrayendo Grafana..."
    tar -xzf "$BIN/grafana.tar.gz" -C "$BIN"
fi
PROM_DIR="$(ls -d "$BIN"/prometheus-* | head -1)"
GRAF_DIR="$(ls -d "$BIN"/grafana-* | grep -v '\.tar' | head -1)"

# --- 2) Renderizar la ruta absoluta del provisioning de dashboards ----------
sed -i "s|^      path: .*|      path: $REPO/monitoring/grafana/dashboards|" \
    "$REPO/monitoring/grafana/provisioning/dashboards/dashboards.yml"

# --- 3) Prometheus -----------------------------------------------------------
if ! curl -s -m 2 http://localhost:9090/-/ready >/dev/null 2>&1; then
    echo "==> Prometheus en :9090 ..."
    nohup "$PROM_DIR/prometheus" \
        --config.file="$REPO/monitoring/prometheus.yml" \
        --storage.tsdb.path="$REPO/monitoring/data-prom" \
        --web.listen-address=:9090 \
        > "$LOGS/prometheus.log" 2>&1 &
    echo $! > "$PIDS/prometheus.pid"
else
    echo "==> Prometheus ya estaba corriendo."
fi

# --- 4) Grafana --------------------------------------------------------------
if ! curl -s -m 2 http://localhost:3000/api/health >/dev/null 2>&1; then
    echo "==> Grafana en :3000 ..."
    env \
        GF_PATHS_DATA="$REPO/monitoring/data-grafana" \
        GF_PATHS_PLUGINS="$REPO/monitoring/data-grafana/plugins" \
        GF_PATHS_PROVISIONING="$REPO/monitoring/grafana/provisioning" \
        GF_PATHS_LOGS="$LOGS" \
        GF_AUTH_ANONYMOUS_ENABLED=true \
        GF_AUTH_ANONYMOUS_ORG_ROLE=Admin \
        GF_USERS_ALLOW_SIGN_UP=false \
        GF_ANALYTICS_REPORTING_ENABLED=false \
        GF_ANALYTICS_CHECK_FOR_UPDATES=false \
        nohup "$GRAF_DIR/bin/grafana" server --homepath "$GRAF_DIR" \
        > "$LOGS/grafana.log" 2>&1 &
    echo $! > "$PIDS/grafana.pid"
else
    echo "==> Grafana ya estaba corriendo."
fi

# --- 5) Salud ---------------------------------------------------------------
echo "==> Esperando a que levanten..."
for _ in $(seq 1 30); do
    PROM_OK=$(curl -s -m 2 http://localhost:9090/-/ready >/dev/null 2>&1 && echo sí || echo no)
    GRAF_OK=$(curl -s -m 2 http://localhost:3000/api/health >/dev/null 2>&1 && echo sí || echo no)
    [ "$PROM_OK" = "sí" ] && [ "$GRAF_OK" = "sí" ] && break
    sleep 2
done
echo "    Prometheus ready: $PROM_OK   (http://localhost:9090)"
echo "    Grafana     ready: $GRAF_OK   (http://localhost:3000)"
if [ "$GRAF_OK" != "sí" ]; then
    echo "ERROR: revisar $LOGS/grafana.log" >&2
    exit 1
fi

# --- 6) Estado de los targets -------------------------------------------------
echo "==> Targets del clúster (requieren vagrant up):"
curl -s http://localhost:9090/api/v1/targets | python3 -c "
import json, sys
for t in json.load(sys.stdin)['data']['activeTargets']:
    print(f\"    {t['labels'].get('node','?'):12} {t['health']:6}  {t['scrapeUrl']}\")" 2>/dev/null || true
echo "Listo. Dashboard: http://localhost:3000 (login anónimo como Admin, carpeta SOD)."
