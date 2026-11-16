#!/usr/bin/env bash
# campana_real.sh :: Campaña completa del experimento REAL (emulación Vagrant).
#
# Ejecuta el diseño factorial del docs/diseno_experimento.md §5.1:
#   3 estrategias x 3 latencias (1/20/100 ms) x 5 semillas = 45 corridas
#   (o el subconjunto que se pase por argumento; ya-hechas se saltean).
#
# USO:
#   ./scripts/campana_real.sh                          # campaña completa
#   ./scripts/campana_real.sh round_robin              # una estrategia
#   ./scripts/campana_real.sh round_robin least_loaded 1 11 42   # lat 1, semillas 11 42
#   ./scripts/campana_real.sh --seed 11 42 round_robin # semillas antes de estrategias
#
# Robustez respecto al bucle manual del README:
#   1. Verifica que las 4 VMs estén UP y con los 3 workers registrados ANTES
#      de empezar (falla rápido, no acumula basura).
#   2. En vez de `sleep 45` a ciegas, SONDEA el log del coordinador hasta ver
#      [OK] o error (timeout 150 s por corrida: a 100 ms de latencia dura más).
#   3. Solo acumula la fila si el CSV de la corrida existe y empieza con
#      "real," — jamás redirige stderr de `vagrant ssh` al CSV de datos.
#   4. Reanudable: saltea combinaciones ya presentes en data/resultados_reales.csv.
#   5. trap: limpia el qdisc (set_netem 0) aunque se corte con Ctrl+C.

set -u
cd "$(dirname "$0")/.."

CSV_REAL="data/resultados_reales.csv"
TIMEOUT_CORRIDA=150     # s máx por corrida (100 ms de latencia tarda ~60 s)
SONDEO=5                # s entre sondeos del log

# ---------------------------------------------------------------- argumentos
estrategias=(round_robin least_loaded power_of_two)
latencias=(1 20 100)
semillas=(11 42 73 101 137)
while [[ $# -gt 0 ]]; do
    case "$1" in
        --seed) shift; semillas=()
                while [[ $# -gt 0 && "$1" != --* ]]; do semillas+=("$1"); shift; done ;;
        --lat)  shift; latencias=()
                while [[ $# -gt 0 && "$1" != --* ]]; do latencias+=("$1"); shift; done ;;
        *)      estrategias=()
                while [[ $# -gt 0 && "$1" != --* ]]; do estrategias+=("$1"); shift; done ;;
    esac
done

# ---------------------------------------------------------------- precondiciones
die() { echo "[CAMPANA] ERROR: $*" >&2; exit 1; }

# vagrant ssh con TIMEOUT duro: un cuelgue de vagrant/sshd no puede bloquear
# la campaña ni dejar locks eternos (timeout mata el proceso y libera).
# --foreground es CRÍTICO bajo pty (terminal interactiva): sin él, timeout
# pone a vagrant en otro grupo de procesos y ssh se congela al tocar la tty
# (SIGTTIN) -> salida vacía o cuelgue, aunque la VM esté perfecta.
vssh() { # vssh <timeout_s> <comando_remoto>  -> stdout+stderr, sin \r
    timeout --foreground -k 5 "$1" vagrant ssh coordinador --command "$2" 2>&1 | tr -d '\r\000'
}

echo "[CAMPANA] 0/4 Precondiciones..."
vssh 60 true | grep -q . || true
vagrant ssh coordinador --command true 2>/dev/null \
    || die "La VM coordinador no responde. Levantá el clúster: vagrant up"
for m in worker-1 worker-2 worker-3; do
    activo=$(timeout --foreground -k 5 60 vagrant ssh "$m" --command 'systemctl is-active sod-worker' 2>/dev/null \
             | tr -d '\r\000' | tail -1)
    [[ "$activo" == "active" ]] || die "sod-worker en $m no está active (estado: ${activo:-n/d})"
done
# Los workers deben estar en el bucle de registro o ya registrados (sin ACK
# hasta que exista coordinador: se validará con el REGISTER del 1er log).
echo "[CAMPANA]    VMs y servicios OK (3 workers active)."
mkdir -p data
touch "$CSV_REAL"
# Cabecera del CSV si el archivo está vacío (formato idéntico al del sim):
grep -q '^env,' "$CSV_REAL" || echo "env,scenario_id,strategy,n_workers,latency_ms,seed,makespan_s,throughput_tps,mean_wait_s,load_imbalance_cv,control_messages" > "$CSV_REAL"

hecha() { # true si la combinación (estrategia, latencia, semilla) ya está en el CSV
    awk -F, -v s="$1" -v l="$2" -v sd="$3" \
        '$1=="real" && $3==s && $4==3 && ($5==l || $5==l".0") && $6==sd {f=1} END{exit !f}' \
        "$CSV_REAL"
}

limpiar() { ./scripts/set_netem.sh 0 >/dev/null 2>&1; }
trap limpiar EXIT INT TERM

total=0; saltadas=0; fallidas=0
for lat in "${latencias[@]}"; do
    echo "[CAMPANA] 1/4 Aplicando netem: ${lat} ms"
    ./scripts/set_netem.sh "$lat" >/dev/null || die "set_netem $lat falló"

    for strat in "${estrategias[@]}"; do
        for seed in "${semillas[@]}"; do
            total=$((total + 1))
            if hecha "$strat" "$lat" "$seed"; then
                saltadas=$((saltadas + 1))
                echo "[CAMPANA] -- $strat lat=${lat} seed=$seed ya en $CSV_REAL (saltada)"
                continue
            fi

            out="/tmp/run_${strat}_${lat}_${seed}.csv"
            log="${out%.csv}.log"
            echo "[CAMPANA] ==> $strat lat=${lat}ms seed=$seed"

            # 2/4 Lanzar el coordinador (1 llamada ssh con timeout) y verificar
            # que arrancó de verdad: el log debe existir y tener contenido.
            lanzo=0
            for intento in 1 2 3; do
                # pkill va en llamada aparte: en la misma línea del nohup el
                # patrón matchearía el propio wrapper ssh (que contiene la
                # ruta literal) y se mataría antes de lanzar.
                vssh 30 "pkill -f 'coordinador.p[y]' 2>/dev/null; true" >/dev/null 2>&1
                sal=$(vssh 45 "rm -f $out $log; nohup python3 -u /opt/sod_cluster/coordinador.py \
                     --strategy $strat --workers 3 --latency $lat --seed $seed \
                     --output-csv $out > $log 2>&1 < /dev/null & echo LANZADO; sleep 2")
                if ! echo "$sal" | grep -q LANZADO; then
                    echo "[CAMPANA]    lanzamiento $intento: vagrant ssh no respondió:"
                    echo "$sal" | tail -3 | sed 's/^/       | /'
                    continue
                fi
                sleep 5
                # El coordinador puede terminar en <5 s (lote de 100 tareas a
                # 1 ms dura ~7 s): lo que verifica que arrancó es que el log
                # exista y tenga contenido ([REG]... o el traceback si falló).
                sal=$(vssh 30 "test -s $log && echo LOG_OK")
                if echo "$sal" | grep -q LOG_OK; then
                    lanzo=1; break
                fi
                echo "[CAMPANA]    lanzamiento $intento no arrancó el coordinador; reintentando..."
                echo "$sal" | grep -v 'fog\|LANZADO\|LOG_OK' | tail -3 | sed 's/^/       | /'
            done
            if (( ! lanzo )); then
                fallidas=$((fallidas + 1))
                echo "[CAMPANA]    !! el coordinador no arrancó tras 3 intentos (VM caída?)."
                continue
            fi

            # 3/4 Esperar el fin DENTRO de la VM (1 sola llamada ssh): un bucle
            # remoto sondea el log y sale con CAM_OK / CAM_ERR / nada=timeout.
            reps=$((TIMEOUT_CORRIDA / SONDEO))
            sal=$(vssh $((TIMEOUT_CORRIDA + 90)) "test -s $log || { echo CAM_NOLOG; exit 0; }
for i in \$(seq 1 $reps); do
    sleep $SONDEO
    if grep -q '\[OK\]' $log 2>/dev/null; then echo CAM_OK; break; fi
    if grep -qE 'Traceback|TimeoutError|error:|usage:' $log 2>/dev/null; then echo CAM_ERR; break; fi
done
tail -c 400 $log 2>/dev/null")

            if echo "$sal" | grep -q CAM_OK \
               && echo "$(vssh 30 "test -s $out && echo CSV_OK")" | grep -q CSV_OK; then
                estado=OK
            elif echo "$sal" | grep -q CAM_ERR; then
                estado=ERROR
            elif echo "$sal" | grep -q CAM_NOLOG; then
                estado=SIN_LOG
            else
                estado=TIMEOUT
            fi

            if [[ "$estado" == "OK" ]]; then
                # 4/4 Acumular SOLO la fila de datos: el filtro $1=="real"
                # descarta warnings de vagrant y cualquier texto ajeno.
                vagrant ssh coordinador --command "cat $out" 2>/dev/null \
                    | tr -d '\r' \
                    | awk -F, '$1=="real"' \
                    | while IFS= read -r fila; do
                        clave=$(echo "$fila" | cut -d, -f1-6)
                        grep -qF "$clave" "$CSV_REAL" || echo "$fila" >> "$CSV_REAL"
                      done
                makespan=$(awk -F, -v s="$strat" -v l="$lat" -v sd="$seed" \
                    '$3==s && $4==3 && ($5==l || $5==l".0") && $6==sd {m=$7} END{print m}' \
                    "$CSV_REAL")
                echo "[CAMPANA]    OK: makespan=${makespan:-?} s -> acumulada en $CSV_REAL"
            else
                fallidas=$((fallidas + 1))
                echo "[CAMPANA]    !! corrida $estado (log: $log). No se acumula nada."
                echo "$sal" | grep -v 'fog\|CAM_' | tail -4 | sed 's/^/       | /'
            fi
        done
    done
done

echo "[CAMPANA] Fin: $total combinaciones | $saltadas saltadas (ya hechas) | $fallidas fallidas"
echo "[CAMPANA] netem limpiado (trap). Filas en $CSV_REAL: $(grep -c '^real,' "$CSV_REAL" || true)"
