# Diseño experimental completo

**Proyecto:** cluster-sim — *Del modelo a la máquina: simulación con SimGrid y emulación reproducible con Vagrant de estrategias de planificación y balanceo de carga* (AE4, Propuesta B).
**Escenario:** red de estaciones agrometeorológicas del Chaco; lotes de datos (telemetría e imágenes de cultivo) procesados por un clúster heterogéneo.
**Pregunta guía:** ¿cuánto se puede confiar en una simulación para tomar decisiones de diseño de un sistema distribuido?
**Metodología de referencia:** Jain (1991) — diseño de experimentos con factores, niveles y repeticiones; medición; simulación; validación y extrapolación.

---

## 1. Objetivos específicos del experimento

1. **Comparar** tres estrategias de planificación/balanceo bajo un diseño factorial controlado, tanto en el clúster real (emulación con Vagrant) como en simulación (SimGrid S4U).
2. **Calibrar** el modelo de simulación con mediciones del clúster real (velocidad de CPU agregada, ancho de banda, latencia).
3. **Validar** el simulador cuantificando el error relativo por escenario comparable (misma estrategia, latencia, semilla y lote).
4. **Extrapolar** el comportamiento a escalas imposibles de montar físicamente (16, 64, 256 y 512 nodos) y discutir la validez de esas predicciones.

## 2. Plataforma bajo estudio

### 2.1 Clúster real (emulación)

4 VMs Debian 12 (box `debian/bookworm64`, proveedor libvirt) sobre una red privada `192.168.56.0/24` (bridge host-only). La heterogeneidad del hardware del anfitrión es inexistente (mismo CPU físico): la heterogeneidad es **emulada** con recursos diferenciados + cuotas cgroup + paralelismo real.

| Nodo | vCPU | RAM | Configuración de servicio | Rol |
|---|---|---|---|---|
| `coordinador` | 1 | 2048 MB | `coordinador.py` por corrida | ROUTER :5555 (datos), control |
| `worker-1` | 2 | 1536 MB | `worker.py --threads 2` (pool de **procesos**) | nodo potente |
| `worker-2` | 1 | 1024 MB | `worker.py --threads 1` | nodo medio |
| `worker-3` | 1 | 512 MB | `worker.py --threads 1` **dentro de cgroup `CPUQuota=50%`** (systemd) | nodo antiguo |

Detalles críticos de fidelidad del emulado:

- El GIL de Python no paraleliza el cómputo (hashing SHA-256 encadenado, verificado: speedup de threads 0.98×); por eso el paralelismo de worker-1 se logra con **procesos** (`ProcessPoolExecutor`, speedup medido ~2×). Cada slot ejecuta tareas en paralelo real.
- El servicio corre como unidad systemd `sod-worker.service` con `Restart=always`: tras cada corrida el coordinador envía STOP, el worker sale limpio y systemd lo relanza con re-registro automático (REGISTER con reintento hasta ACK). El ciclo de campaña de corridas no requiere intervención manual.
- El registro del worker ante el coordinador usa su IP de la red de laboratorio (`192.168.56.x`, no la de gestión `192.168.121.x`).

### 2.2 Modelo de simulación (SimGrid)

| Componente real | Modelo SimGrid |
|---|---|
| Velocidad efectiva de cada nodo | `speed` del host en unidades/s **agregadas** (ver §4.1) |
| Red privada compartida | Enlaces dedicados par a par con routing Full (validación) o StarZone con enlace por nodo (escalabilidad) |
| Ancho de banda | `bandwidth` en bytes/s medido con iperf3 |
| Latencia | `latency` **unidireccional** por enlace = delay netem / 2 (el RTT del real es 2×) |
| Coordinador + workers | Actores S4U que replican el protocolo REGISTER → TASK → RESULT → STOP |
| Estrategias | **El mismo módulo** `strategies.py` ejecuta en ambos mundos (lógica común): el sim consulta STATUS vía mailboxes con coste de mensajes contabilizado |

La plataforma de validación (3 workers) se describe en `plataforma_base.xml` calibrado; la de escalabilidad (n > 3) se construye programáticamente con proporción 25 % potentes / 50 % medios / 25 % lentos, análogo a la heterogeneidad del diseño.

## 3. Factores, niveles y métricas

### 3.1 Factores

| Factor | Niveles | Aplica a |
|---|---|---|
| **Estrategia** (factor principal) | `round_robin`, `least_loaded`, `power_of_two` | real + sim |
| **Latencia de red** (netem) | 1, 20, 100 ms | real + sim |
| **Semilla del lote** | 11, 42, 73, 101, 137 | real (5 réplicas); sim (42 fija, determinista) |
| **Escala** (nº de workers) | 3 (validación); 16, 64, 256, 512 (extrapolación) | sim |

Definición de las tres estrategias (implementadas en `strategies.py`, módulo común):

1. **`round_robin`** — estática: turnos secuenciales entre los N workers. Coste de control por decisión: **0 mensajes** (no consulta carga).
2. **`least_loaded`** — dinámica centralizada: consulta STATUS de **todos** los workers (coste O(N) mensajes por tarea) y elige el de menor carga (`queue + slots_busy`).
3. **`power_of_two`** — dinámica muestreada (Mitzenmacher, 2001): elige 2 workers al azar con la semilla de la corrida y consulta solo esos 2 (**2 mensajes** por decisión), enviando la tarea al menos cargado.

### 3.2 Carga de trabajo

Distribución **bimodal 80/20** (`workload.py`), determinista por semilla:

| Clase | Frecuencia | Costo CPU | Payload | Análogo |
|---|---|---|---|---|
| Liviana | 80 % | 200 000 u | 10 KB | telemetría de estaciones |
| Pesada | 20 % | 3 000 000 u | 2 MB | imágenes de cultivo |

- Lote estándar: **100 tareas** (`created_at` lógico con desplazamiento de 1 µs: lote 100 % reproducible).
- Lote de escalabilidad: **20 × n_workers** tareas (la carga crece con la plataforma).
- Unidad de trabajo (`cpu_work`): medida por el benchmark del worker (§4.1); en el sim 1 unidad = 1 flop simulado (`--flops-per-unit 1`).

### 3.3 Métricas (mismas fórmulas en real y sim)

| Métrica | Definición | Interés |
|---|---|---|
| `makespan_s` | t_último_resultado − t_primer_despacho | tiempo total de procesamiento |
| `throughput_tps` | tareas completadas / makespan | rendimiento |
| `mean_wait_s` | media de (started_at − created_at) | tiempo medio de espera en cola |
| `load_imbalance_cv` | desv. estándar / media del tiempo activo entre nodos | desbalance de carga |
| `control_messages` | consultas de la estrategia + REGISTER/ACK por worker | sobrecarga del protocolo |

Salida: CSV versionados en `data/` — `resultados_reales.csv` (45 filas), `resultados_simgrid_val.csv` (9), `resultados_simgrid_scale.csv` (12). Columnas comunes: `env, scenario_id, strategy, n_workers, latency_ms, seed, makespan_s, throughput_tps, mean_wait_s, load_imbalance_cv, control_messages`.

## 4. Protocolo de calibración (medir el real → parametrizar el sim)

### 4.1 Velocidad de CPU (agregada)

El benchmark mide **la misma configuración que ejecuta el servicio**:

```bash
vagrant ssh worker-1 --command 'python3 /opt/sod_cluster/worker.py --benchmark --threads 2'
vagrant ssh worker-2 --command 'python3 /opt/sod_cluster/worker.py --benchmark'
vagrant ssh worker-3 --command 'sudo systemd-run --scope -p CPUQuota=50% -p User=vagrant \
    python3 /opt/sod_cluster/worker.py --benchmark'
```

Protocolo: 10 corridas de 1 000 000 de unidades; se reporta media, desvío estándar, velocidad por slot y **velocidad agregada del nodo** (por slot × slots). Valores medidos (2026-11, CPU física común i3-10100F):

| Nodo | Slots | Agregada medida (u/s) | `speed` en `plataforma_base.xml` |
|---|---|---|---|
| worker-1 | 2 | 30 508 335 | `30.5Mf` |
| worker-2 | 1 | 15 905 023 | `15.9Mf` |
| worker-3 | 1 (in-cgroup 50 %) | 7 825 950 | `7.83Mf` |

Jerarquía efectiva ≈ **4 : 2 : 1**. Justificación del modelado: el actor worker del sim ejecuta tareas secuencialmente; la capacidad de N slots en paralelo se modela como `speed` agregada del host (núcleo único), que reproduce el throughput observado sin alterar el modelo de ejecución. La cuota de worker-3 se calibra corriendo el benchmark **dentro** del cgroup (fuera del servicio mediría ~15.9M y falsearía el modelo).

### 4.2 Ancho de banda

iperf3 worker-2 → worker-1 durante 5 s: **24.5 Gbps** mínimo medido (media 24.8). Conversión a bytes/s: `24.5 Gbps ÷ 8 × 1000 = 3062 MBps` → `bandwidth="3062MBps"` en los 6 enlaces del XML y en `ANCHO_BANDA` del sim. Implicancia declarada: con payloads de 10 KB / 2 MB la transferencia es despreciable (< 1 ms); el makespan está dominado por el CPU, y la red de laboratorio no es cuello de botella.

### 4.3 Latencia

Es factor experimental, no medible una sola vez: `./scripts/set_netem.sh L` aplica delay L por interfaz en las 4 VMs (RTT ≈ 2×L, verificado con `ping`). En el sim, `latency` unidireccional por enlace = L/2, equivalente. Verificación puntual antes de cada nivel de latencia.

### 4.4 Verificación del motor (sanity check, no calibración)

`--trivial-test`: 1 tarea de 1 000 000 u en 1 worker, latencia 0. El makespan simulado debe coincidir con el cálculo analítico (`costo / velocidad_cpu`): criterio de aceptación **error relativo < 0.1 %** (el residuo es overhead de scheduling de actores del sim, no del modelo). Mide el motor; la calidad del calibrado se mide en §5.

## 5. Diseño de corridas

### 5.1 Experimento real (emulación): 45 corridas

Diseño factorial 3 estrategias × 3 latencias × 5 semillas, lote de 100 tareas:

| Estrategia | Latencia (ms) | Semillas | Corridas |
|---|---|---|---|
| round_robin / least_loaded / power_of_two | 1, 20, 100 | 11, 42, 73, 101, 137 | 3×3×5 = **45** |

Protocolo por corrida (latencia global, bucle exterior por latencia):

```bash
./scripts/set_netem.sh "$lat"
vagrant ssh coordinador --command "nohup python3 /opt/sod_cluster/coordinador.py \
    --strategy $strat --workers 3 --latency $lat --seed $seed \
    --output-csv /tmp/run_${strat}_${lat}_${seed}.csv > ${out%.csv}.log 2>&1 &"
sleep 45   # margen; la corrida termina sola (~30 s): despacha, espera, STOP, escribe CSV
# copiar la fila y acumularla en data/resultados_reales.csv (cabecera solo la 1ª vez)
```

Al finalizar el nivel: `./scripts/set_netem.sh 0` (limpieza obligatoria del qdisc).

**Análisis:** por cada celda (estrategia × latencia) hay 5 réplicas independientes → media ± desvío estándar de cada métrica (Jain: repeticiones ≥ 5 para intervalos de confianza razonables). La variabilidad entre semillas captura el efecto de la composición aleatoria del lote bimodal.

### 5.2 Validación del simulador: 9 corridas

Mismas combinaciones que el real (3 × 3) con semilla fija 42 y plataforma XML calibrada:

```bash
./scripts/correr_simulacion.sh --batch-all --batch-mode validacion \
    --output-csv data/resultados_simgrid_val.csv
```

Comparación punto a punto (9 escenarios comparables):

```
error_relativo = |makespan_sim − makespan_real| / makespan_real   (× 100 %)
```

Se reporta por escenario y agregado; el notebook (§7) produce tabla + heatmap de error por estrategia × latencia para localizar **dónde** diverge el modelo (qué parámetro retocar en la siguiente iteración de calibración).

### 5.3 Extrapolación a gran escala: 12 corridas

3 estrategias × 4 escalas (16, 64, 256, 512 workers), latencia de laboratorio (1 ms), semilla 42, plataforma programática (StarZone, proporción 25/50/25, lote 20 × n):

```bash
./scripts/correr_simulacion.sh --batch-all --batch-mode escalabilidad \
    --output-csv data/resultados_simgrid_scale.csv
```

Preguntas a responder con estas corridas: ¿se mantiene el ranking de estrategias al escalar? ¿En qué n el coordinador centralizado (y el coste O(N) por decisión de `least_loaded`) se vuelve cuello de botella? ¿Qué fracción del makespan es despacho/sobrecarga de control?

## 6. Control de validez y amenazas

| Amenaza | Control |
|---|---|
| Mismo lote entre mundos | Semilla fija → lote 100 % determinista (`workload.generate_batch`) |
| Misma lógica de decisión | Módulo `strategies.py` compartido real/sim (un solo algoritmo, dos APIs de consulta de carga) |
| Contaminación por corridas previas | STOP + salida limpia; `Restart=always` re-registra; verificación de `[REG]` de los 3 workers en cada log |
| Benchmark no representativo | Benchmark con `--threads` del servicio; worker-3 medido in-cgroup |
| IPs de gestión en el registro | `get_local_ip` resuelve contra la IP del coordinador; verificado en logs |
| Sobrecarga del anfitrión | Monitorización Prometheus/Grafana durante corridas (CPU por nodo, RAM, tráfico rx/tx de eth1, load1); los paneles validan que el sistema real se comportó como el modelo asume (worker-3 techo a ~50 %, tráfico acorde a payloads) |
| Qdisc residual | `set_netem.sh 0` al final de cada nivel de latencia |
| Exactitud del motor sim | `--trivial-test` (< 0.1 %) antes de los lotes |
| Distinta escala de tiempo sim/real | `--flops-per-unit 1` y speed en unidades/s medidas: el sim repoduce los tiempos reales, no una escala arbitraria |

Fuentes de error esperadas a discutir en el análisis (§8 de la consigna): overhead de despacho del coordinador real (construcción de JSON, syscalls ZMQ) no modelado en el sim; interferencia del anfitrión entre VMs; simplificación del modelo de red (enlaces dedicados vs. bridge compartido); variance de la cuota cgroup de worker-3.

## 7. Análisis (cuaderno Jupyter)

`analisis_resultados.py` / notebook del §7 del README sobre los 3 CSV de `data/`:

1. **Desempeño por estrategia** (real y sim): media ± desv de cada métrica por celda; gráficos comparativos.
2. **Efecto de factores**: latencia y heterogeneidad sobre cada estrategia (p. ej. `least_loaded` gana a baja latencia por consultas frescas; a 100 ms las consultas rancias penalizan y `power_of_two` compite con 4× menos mensajes).
3. **Validación**: tabla + heatmap de error relativo (real vs. sim) por estrategia × latencia.
4. **Escalabilidad**: curvas de makespan, throughput, desbalance y `control_messages` vs. n (16→512); punto de saturación del coordinador centralizado.
5. **Recomendación final** fundamentada: qué estrategia conviene y hasta qué escala se confía en la extrapolación.

## 8. Reproducibilidad (prueba externa)

Un tercero debe poder replicar con solo `README.md` (ruta de trabajo 0→8): requisitos por SO → `vagrant up` → (opcional monitoreo) → calibración §5.1 → `--trivial-test` → campaña real → lotes sim → notebook → reset (`vagrant destroy -f`). Criterio: `vagrant up` sin pasos manuales fuera de la guía; los lotes sim corren incluso sin clúster (el notebook genera datos sintéticos si faltan CSV, para probar el pipeline de análisis de punta a punta).

## 9. Cronograma de ejecución (mapeado a las semanas de la cátedra)

| Semana | Actividad | Entregable | Estado en repo |
|---|---|---|---|
| 1 | Acta de roles + diseño | Este documento, diagrama | `docs/`, `cluster-sim.drawio` |
| 2 | Infraestructura como código | Vagrantfile + playbook idempotente | `Vagrantfile`, `ansible/` |
| 3 | Estrategias + calibración + corridas | CSVs crudos, logs, capturas | `strategies.py`, `data/`, `log.txt` |
| 4 | Análisis + informe + defensa | Informe APA, video, bitácoras | notebook, `figuras/`, `tablas/` |

## 10. Referencias del diseño

- Jain, R. (1991). *The art of computer systems performance analysis*. Wiley. (diseño factorial, réplicas, IC, errores comunes de medición).
- Casanova, H., Giersch, A., Legrand, A., Quinson, M., & Suter, F. (2014). Versatile, scalable, and accurate simulation of distributed applications and platforms. *JPDC, 74*(10). (SimGrid y sus estudios de validación).
- Mitzenmacher, M. (2001). The power of two choices in randomized load balancing. *IEEE TPDS, 12*(10). (fundamento de `power_of_two`).
- Sinha, P. K. (1997). *Distributed operating systems: Concepts and design*. IEEE Press. (políticas de transferencia, selección y ubicación).
- Singhal, M., & Shivaratri, N. G. (1994). *Advanced concepts in operating systems*. McGraw-Hill. (planificación estática vs. dinámica, centralizada vs. distribuida).
