#!/usr/bin/env bash
#
# set_netem.sh — aplica (o limpia) una latencia tc/netem en la interfaz
# privada de TODAS las VMs del clúster.
#
# Uso:
#   ./scripts/set_netem.sh 20     # 20 ms en todas las VMs
#   ./scripts/set_netem.sh 1      # 1 ms
#   ./scripts/set_netem.sh 100    # 100 ms
#   ./scripts/set_netem.sh 0      # limpiar qdisc (sin latencia)
#
# Requisito: el clúster debe estar levantado (vagrant up) y ejecutarse
# desde la raíz del repo (donde está el Vagrantfile).

set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "Uso: $0 <latencia_ms|0>   (ej.: 1, 20, 100; 0 = limpiar)" >&2
    exit 1
fi

LATENCY="$1"
if [[ ! "$LATENCY" =~ ^[0-9]+$ ]]; then
    echo "Error: la latencia debe ser un número entero de ms (o 0)." >&2
    exit 1
fi

# Mismo esquema de nombres que el Vagrantfile (respeta N_WORKERS).
N_WORKERS="${N_WORKERS:-3}"
VMS=(coordinador)
for i in $(seq 1 "$N_WORKERS"); do
    VMS+=("worker-${i}")
done

apply_netem() {
    local vm="$1"
    printf '==> %-12s ' "$vm"
    # El bloque REMOTE va con delimitador entrecomillado: no se expande
    # NADA en el host; LATENCY se inyecta como variable de entorno remota.
    if ! vagrant ssh "$vm" -- sudo env LATENCY="$LATENCY" bash -s <<'REMOTE'
set -euo pipefail
# La interfaz privada NO se llama eth1 en KVM: suele ser ens5/ens6.
# Se detecta dinámicamente por la IP del laboratorio (192.168.56.0/24).
IFACE="$(ip -o -4 addr show | awk '/192\.168\.56\./ {print $2}')"
if [[ -z "$IFACE" ]]; then
    echo "ERROR: no hay interfaz en 192.168.56.0/24" >&2
    exit 1
fi
if [[ "$LATENCY" == "0" ]]; then
    tc qdisc del dev "$IFACE" root 2>/dev/null || true
    echo "$IFACE: netem eliminado (0 ms)"
else
    tc qdisc replace dev "$IFACE" root netem delay "${LATENCY}ms"
    echo "$IFACE: delay ${LATENCY}ms"
fi
REMOTE
    then
        echo "ERROR al aplicar netem en $vm" >&2
        exit 1
    fi
}

echo "Aplicando netem (latencia=${LATENCY}ms) en ${#VMS[@]} VMs..."
for vm in "${VMS[@]}"; do
    apply_netem "$vm"
done

echo "Hecho. Verifica con: vagrant ssh worker-1 -- 'tc qdisc show'"
