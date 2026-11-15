# cluster-sim

Simulación y emulación de un clúster de nodos distribuidos mediante
Infraestructura como Código (Vagrant + Ansible para las VMs, SimGrid para la
simulación, ZeroMQ para el coordinador real) y evaluación de desempeño bajo
la metodología de Raj Jain (diseño factorial, IC 95%, validación de simulador
y extrapolación).

```
                    ┌──────────────────────────────┐
   netem (tc)       │        coordinador VM         │
   1/20/100 ms  ───►│  coordinador.py (ZMQ ROUTER)  │
                    │  SimGrid (S4U) + simulacion.py│
                    └───────┬──────────┬────────────┘
                            │ 5555     │ 5556 (control)
              ┌─────────────┼──────────┼─────────────┐
       worker-1 (potente) worker-2 (medio) worker-3 (antiguo, CPUQuota 50%)
```

Estructura del repo:

```
Vagrantfile              4 VMs KVM (libvirt): coordinador + 3 workers heterogéneos
ansible/playbook.yml     Provision: paquetes, sod-worker.service, node_exporter, SimGrid
worker.py / coordinador.py  Sistema real de tareas ZeroMQ (puertos 5555/5556)
simulacion.py            Simulación SimGrid S4U (misma lógica, modelo calibrado)
strategies.py            round_robin / least_loaded / power_of_two
scripts/                 set_netem.sh (latencia) y correr_simulacion.sh (launcher SimGrid)
monitoring/              Prometheus + Grafana autocontenidos (binarios en bin/, sin root)
analisis_resultados.py   Notebook de análisis (celdas # %%): descriptiva, validación, escalado
```

---

## 0. Requisitos previos (host Arch/Omarchy)

- Vagrant + plugin `vagrant-libvirt`, libvirt/KVM activos (`systemctl status libvirtd`).
- Box `debian/bookworm64` para libvirt: `vagrant box add --provider libvirt debian/bookworm64`.
- **Firewall**: el ufw del host bloquea por defecto el tráfico de los bridges de
  libvirt (sin esto las VMs bootean pero NUNCA obtienen IP). Aplicar una vez:

  ```bash
  sudo ufw allow in on virbr1     # red de gestión de vagrant-libvirt (192.168.121.0/24)
  sudo ufw allow in on virbr2     # red del laboratorio cluster-sim (192.168.56.0/24)
  sudo ufw route allow in on virbr1   # salida a internet de los guests
  sudo ufw route allow in on virbr2
  ```

- Dos venvs ya creados en el repo (si no, ver §3 y §5):
  `.venv-simgrid/` (SimGrid) y `.venv/` (análisis).

> Síntoma de firewall mal configurado: `vagrant up` queda en
> "Waiting for domain to get an IP address..." y no hay leases en
> `virsh net-dhcp-leases vagrant-libvirt`.

---

## 1. Levantar las VMs (Vagrant + Ansible)

```bash
vagrant up                 # 4 VMs + provision Ansible al final (limit=all, una vez)
vagrant status             # coordinador, worker-1/2/3 -> running (libvirt)
```

Topología (red privada del lab `192.168.56.0/24`, gestión `192.168.121.0/24`):

| VM | IP del lab | Recursos |
|---|---|---|
| `coordinador` | 192.168.56.10 | 1 vCPU / 2048 MB (corre además las simulaciones SimGrid) |
| `worker-1` | 192.168.56.11 | 2 vCPU / 1536 MB — perfil *potente* |
| `worker-2` | 192.168.56.12 | 1 vCPU / 1024 MB — perfil *medio* |
| `worker-3` | 192.168.56.13 | 1 vCPU / 512 MB + `netem` + **CPUQuota=50%** — perfil *antiguo* |

Ampliar el clúster: `N_WORKERS=5 vagrant up` (perfiles asignados cíclicamente).

**Configurar la red del lab (solo si las VMs se crearon de cero):**
vagrant-libvirt no configura eth1 en el guest; crearla a mano en cada VM
(las IPs son fijas por diseño):

```bash
vagrant ssh coordinador --command "sudo bash -c 'ip addr add 192.168.56.10/24 dev eth1 2>/dev/null; ip link set eth1 up; printf \"auto eth1\niface eth1 inet static\n    address 192.168.56.10\n    netmask 255.255.255.0\n\" > /etc/network/interfaces.d/eth1'"
# y el mismo patrón con .11/.12/.13 en worker-1/2/3 (verificar con: ip -br a show eth1)
```

**Provision:** se ejecuta automáticamente al final del primer `vagrant up`
(instala rsync, python3-pip, iperf3, prometheus-node-exporter, unit
`sod-worker.service` en los 3 workers, y SimGrid en el coordinador).
Re-corrida manual: `vagrant provision`.
Si falla por caché APT vieja: `vagrant ssh worker-N --command 'sudo apt-get update'`
y repetir `vagrant provision`.

Apagar / limpiar:

```bash
vagrant halt        # apaga (conserva provision e in-guest config) — usar para pausar
vagrant destroy -f  # BORRA todo: hay que re-configurar eth1 y provisionar de nuevo
```

---

## 2. Monitoreo con Prometheus + Grafana

Prometheus (puerto 9090) scrapea el `node_exporter` (:9100) de cada VM por la
red del lab; Grafana (puerto 3000) lo visualiza. Ambos corren en el **host**
con binarios autocontenidos en `monitoring/bin/` (sin root ni docker):

```bash
vagrant up                          # los targets están en las VMs: sin VMs no hay datos
./monitoring/start_monitoring.sh    # levanta ambos y espera health-check
./monitoring/stop_monitoring.sh     # los detiene
```

- **Grafana**: <http://localhost:3000> → carpeta **SOD** → dashboard
  *"Clúster SOD — monitoreo en vivo"* (login anónimo con rol Admin, ya
  provisionado). Paneles: CPU por nodo, RAM por nodo, tráfico en eth1
  (red del lab) y load1, con auto-refresh de 5 s.
- **Prometheus**: <http://localhost:9090> (consultas PromQL directas).

Provisioning automático (se aplica al arrancar Grafana):

```
monitoring/prometheus.yml                         targets del clúster (scrape 5 s)
monitoring/grafana/provisioning/datasources/      datasource "Prometheus SOD"
monitoring/grafana/provisioning/dashboards/       registra monitoring/grafana/dashboards/
monitoring/grafana/dashboards/sod-cluster.json    el dashboard
```

Verificar targets: en Prometheus → *Status → Targets*, o ver el resumen que
imprime `start_monitoring.sh` (los 4 deben estar `up`).

---

## 3. Ejecutar la simulación con SimGrid

El launcher `scripts/correr_simulacion.sh` usa el venv dedicado
`.venv-simgrid` (bindings S4U de SimGrid 4.1). Si no existe:

```bash
python -m venv .venv-simgrid
.venv-simgrid/bin/pip install pybind11==2.13.6 cmake setuptools wheel
.venv-simgrid/bin/pip install --no-build-isolation simgrid
```

**Test de calibración (empezar por acá):**

```bash
./scripts/correr_simulacion.sh --trivial-test
```

Simula 1 tarea de 1 GFLOP en un host de 2 GFLOPS: el tiempo simulado debe
coincidir con el analítico (`error relativo: 0.0% -> OK`) y la fila queda en
`resultados_simulacion.csv`.

**Corrida individual** (misma semilla => lote reproducible):

```bash
./scripts/correr_simulacion.sh --strategy least_loaded --n-workers 3 \
    --latency 20 --seed 42 --platform plataforma_base.xml
```

**Lotes automáticos** (SimGrid admite un Engine por proceso, así que cada
combinación va en un subproceso):

```bash
./scripts/correr_simulacion.sh --batch-all --batch-mode validacion      # n=3, 45 corridas
./scripts/correr_simulacion.sh --batch-all --batch-mode escalabilidad   # n=16..512
```

Salidas: filas con el esquema
`env,scenario_id,strategy,n_workers,latency_ms,seed,makespan_s,
throughput_tps,mean_wait_s,load_imbalance_cv,control_messages`.

> Nota: los backtraces de pybind11 al final del proceso son ruido de
> teardown, no errores; el criterio de éxito es `-> OK`.

---

## 4. Experimento REAL en el clúster (emulación)

El sistema real usa ZeroMQ: los workers corren como servicio systemd y el
coordinador se lanza por corrida.

```bash
# 1) Latencia de red en TODAS las VMs (global, una a la vez):
./scripts/set_netem.sh 1        # 1 ms (o 20, 100; 0 = limpiar)

# 2) Lanzar el coordinador DENTRO de su VM (los workers se registran solos):
vagrant ssh coordinador --command 'nohup python3 /opt/sod_cluster/coordinador.py \
  --strategy round_robin --workers 3 --latency 1 --seed 42 \
  --output-csv /tmp/run_rr_1_42.csv > /tmp/run_rr_1_42.log 2>&1 & echo lanzado'

# 3) Seguir la corrida y copiar el resultado al host:
vagrant ssh coordinador --command 'tail -5 /tmp/run_rr_1_42.log; cat /tmp/run_rr_1_42.csv'
```

La corrida termina sola (~30 s: despacha 100 tareas, espera resultados,
manda STOP y escribe el CSV). Conviene **acumular** las filas en
`data/resultados_reales.csv` del host (escribir el header una vez).

Parámetros del diseño: 3 estrategias × 3 latencias (1/20/100 ms) × 5 semillas
(11, 42, 73, 101, 137) = **45 corridas**.

**Gotchas:**

- En el log deben verse `[REG] worker-N (control: tcp://192.168.56.1x:5556)`.
  Si aparecen IPs `192.168.121.x` el worker cacheó la IP de gestión
  (`worker.py:get_local_ip` corre una vez al arrancar): fix con
  `vagrant ssh worker-N --command 'sudo systemctl restart sod-worker'`.
- El netem es global (afecta a las 4 VMs): el bucle de la campaña va por
  latencia. Terminar siempre con `./scripts/set_netem.sh 0`.
- worker-3 está limitado a 50% de CPU por cgroups (emula el nodo antiguo).

---

## 5. Ejecutar el notebook de análisis

`analisis_resultados.py` está escrito en formato "percent" (celdas `# %%`).
Se convierte a Jupyter y se ejecuta con el venv de análisis (el python del
sistema no permite pip, PEP 668):

```bash
# venv + kernel (una vez):
python -m venv .venv
.venv/bin/pip install pandas numpy matplotlib scipy jinja2 jupytext nbformat nbconvert ipykernel
.venv/bin/python -m ipykernel install --user --name cluster-sim --display-name "Python (cluster-sim)"

# regenerar el notebook desde el script:
.venv/bin/jupytext --to notebook analisis_resultados.py -o analisis_resultados.ipynb

# ejecutarlo de punta a punta (headless, exporta figuras/ y tablas/):
.venv/bin/jupyter nbconvert --to notebook --execute --inplace \
    --ExecutePreprocessor.kernel_name=cluster-sim analisis_resultados.ipynb

# o abrirlo interactivo:
.venv/bin/jupyter lab analisis_resultados.ipynb   # kernel "Python (cluster-sim)"
```

Qué produce (todo a 300 DPI, estilo APA, etiquetas en español):

1. Tabla descriptiva del clúster real (media, DE, IC 95% con t de Student).
2. Barras agrupadas con IC 95%: efecto de la latencia sobre cada estrategia.
3. Validación Real vs SimGrid: error relativo % (tabla, heatmap, barras).
4. Extrapolación 16→512 nodos (escala log₂) con anotación del punto de
   saturación del coordinador en `least_loaded`.
5. Export: `figuras/*.png` y `tablas/*.{csv,tex}`.

**Importante:** si los CSV de `data/` no existen, la primera celda genera
**datos sintéticos** para que el pipeline sea ejecutable de inmediato.
Cuando haya datos reales, ponerlos en `data/resultados_reales.csv`,
`data/resultados_simgrid_val.csv` y `data/resultados_simgrid_scale.csv`
(con el esquema del §3) y el análisis corre sobre ellos sin cambiar nada.

---

## 6. Reset del experimento

```bash
rm -f data/*.csv && rm -rf figuras tablas analisis_resultados.ipynb
# resultados_simulacion.csv (raíz) puede conservar filas de corridas:
# para dejarlo "en blanco" quedarse solo con la cabecera.
vagrant halt     # pausa;  vagrant destroy -f borra TODO (ver §1: re-configurar eth1)
```

---

## Licencia

MIT — ver `LICENSE`.
