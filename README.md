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
Vagrantfile              4 VMs: coordinador + 3 workers heterogéneos (libvirt o VirtualBox)
ansible/playbook.yml     Provision: paquetes, sod-worker.service, node_exporter, SimGrid
worker.py / coordinador.py  Sistema real de tareas ZeroMQ (puertos 5555/5556)
simulacion.py            Simulación SimGrid S4U (misma lógica, modelo calibrado)
strategies.py            round_robin / least_loaded / power_of_two
workload.py / monitoring/ scripts/   carga sintética / observabilidad / utilidades
analisis_resultados.py   Notebook de análisis (celdas # %%): descriptiva, validación, escalado
```

---

## 0. Requisitos de software

| Herramienta | Para qué | Dónde se instala |
|---|---|---|
| Vagrant ≥ 2.3 | levantar las VMs | host |
| Plugin `vagrant-libvirt` *(Linux)* o VirtualBox ≥ 7 *(Windows)* | provider de las VMs | host |
| libvirt + QEMU/KVM *(Linux)* | hipervisor | host |
| Ansible ≥ 2.12 | provision de las VMs | host (o dentro con `ansible_local`) |
| Python ≥ 3.10 + pip/venv | SimGrid, análisis | host y guests |
| SimGrid 4.1 (pip) | simulación S4U | `.venv-simgrid/` del repo |
| pandas/numpy/matplotlib/scipy/jinja2/jupytext | análisis | `.venv/` del repo |
| Prometheus + Grafana | monitoreo | host — **opciones en §4** (los binarios de `monitoring/bin/` son solo una de ellas) |
| rsync + git | synced folder y clonar el repo | host |

> Los binarios de `monitoring/bin/` **no son obligatorios**: son la opción
> cero-configuración para no depender de paquetes del sistema. Si ya tenés
> Prometheus/Grafana por otro lado, podés usarlos igual (§4, opción B).
> Esa carpeta está en `.gitignore`: en un clon fresco no existe.

---

## 1. Instalación por sistema operativo

### 1.1 Linux — Arch (incluye Omarchy)

```bash
sudo pacman -S --needed vagrant libvirt qemu-base virt-manager dnsmasq ansible rsync base-devel
sudo usermod -aG libvirt "$USER"   # re-login para que aplique
sudo systemctl enable --now libvirtd
vagrant plugin install vagrant-libvirt
```

- Python/pip: ya viene con el sistema; usar venvs (no instales nada con pip
  global: Arch es PEP 668 "externally-managed").
- Firewall ufw (si lo usás): ver §2.

### 1.2 Linux — Debian / Ubuntu

```bash
sudo apt update
sudo apt install -y vagrant libvirt-daemon-system libvirt-clients virtinst \
                    dnsmasq-base ansible rsync python3-venv python3-dev \
                    build-essential ruby-dev libvirt-dev zlib1g-dev pkg-config
sudo usermod -aG libvirt "$USER"   # re-login
sudo systemctl enable --now libvirtd
# Debian trae el plugin como paquete; si no existe, instalarlo con vagrant:
sudo apt install -y vagrant-libvirt || vagrant plugin install vagrant-libvirt
```

### 1.3 Linux — Fedora

```bash
sudo dnf install -y @virtualization vagrant ansible rsync \
                    gcc gcc-c++ python3-devel ruby-devel libvirt-devel zlib-devel
sudo usermod -aG libvirt "$USER"   # re-login
sudo systemctl enable --now libvirtd
vagrant plugin install vagrant-libvirt
```

### 1.4 Windows

**Recomendado: WSL2 (Ubuntu)** — así corren *todos* los componentes igual que
en Linux (libvirt funciona dentro de WSL2 con virtualización anidada):

1. En PowerShell (admin): `wsl --install -d Ubuntu-22.04` y reiniciar.
2. Habilitar virtualización anidada (una vez, PowerShell admin):
   `Set-VMProcessor -ExposeVirtualizationExtensions $true` no aplica en WSL2;
   en su lugar verificar: `wsl -e ls /dev/kvm` (debe existir; requiere
   Hyper-V/"Virtual Machine Platform" activo, que el `wsl --install` ya habilita).
3. Dentro de WSL2 seguir la sección **1.2 (Debian/Ubuntu)** completa.
4. `git clone` el repo **dentro del filesystem de WSL** (~/…), no en /mnt/c
   (el rsync synced folder es mucho más rápido ahí).

**Alternativa: VirtualBox nativo** (sin WSL):

1. Instalar [Vagrant](https://developer.hashicorp.com/vagrant/downloads) y
   [VirtualBox](https://www.virtualbox.org/) para Windows.
2. Descomentar el bloque `config.vm.provider :virtualbox` del final del
   `Vagrantfile` y usar el box con ese provider:
   `vagrant box add bento/debian-12` + `vagrant up --provider=virtualbox`.
3. Cambiar el provisioner a `ansible_local` (Ansible no corre nativo en
   Windows): en el `Vagrantfile`, `config.vm.provision 'ansible'` →
   `'ansible_local'`.
4. Prometheus/Grafana: Docker Desktop o binarios de Windows (§4).
5. SimGrid y el análisis: hacerlos dentro de WSL2 igualmente (compilar
   SimGrid nativamente en Windows es frágil).

**Diferencias Windows/Linux a saber:**
- `scripts/set_netem.sh` usa `tc/netem` **dentro de las VMs** (que son
  Debian): funciona igual en ambos casos — el host no interviene.
- El firewall del host (§2) no aplica en Windows: VirtualBox/WSL2 NAT no lo
  necesita.
- Si usás Docker Desktop en Windows, montá el repo en el contenedor o apuntá
  los configs por ruta absoluta (§4 opción B).

---

## 2. Firewall del host (solo Linux con libvirt)

El firewall (ufw en Arch/Omarchy, o el que uses en Debian/Fedora) bloquea por
defecto el tráfico de los bridges de libvirt: sin esto las VMs bootean pero
**NUNCA obtienen IP**. Aplicar una vez:

```bash
sudo ufw allow in on virbr1        # red de gestión de vagrant-libvirt (192.168.121.0/24)
sudo ufw allow in on virbr2        # red del laboratorio (192.168.56.0/24)
sudo ufw route allow in on virbr1  # salida a internet de los guests
sudo ufw route allow in on virbr2
# Debian/Fedora con firewalld:
sudo firewall-cmd --zone=libvirt --add-service=libvirt --permanent && sudo firewall-cmd --reload
```

> Síntoma de firewall mal configurado: `vagrant up` queda en
> "Waiting for domain to get an IP address..." y no hay leases en
> `virsh net-dhcp-leases vagrant-libvirt`.

---

## 3. Levantar las VMs (Vagrant + Ansible)

```bash
vagrant up                 # 4 VMs + provision Ansible al final (limit=all, una vez)
                           # la 1ª vez descarga el box debian/bookworm64 (~400 MB)
vagrant status             # coordinador, worker-1/2/3 -> running
```

Topología (red privada del lab `192.168.56.0/24`, gestión `192.168.121.0/24`
con libvirt; VirtualBox usa su host-only net):

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
# mismo patrón con .11/.12/.13 en worker-1/2/3; verificar: ip -br a show eth1
```

**Provision:** corre automático al final del primer `vagrant up` (instala
rsync, python3-pip, iperf3, prometheus-node-exporter, unit `sod-worker.service`
en los workers y SimGrid en el coordinador). Re-corrida manual:
`vagrant provision`. Si falla por caché APT vieja:
`vagrant ssh worker-N --command 'sudo apt-get update'` y repetir.

**Carpeta sincronizada:** el repo se sincroniza a `/vagrant` en cada VM por
rsync, pero **solo el código** (el `Vagrantfile` excluye `monitoring/`, los
venvs, `data/`, etc. — sin esos excludes cada `vagrant up` subiría ~2.6 GB a
CADA VM: red y CPU al 100% por varios minutos). Gotcha si cambiás los
excludes: Vagrant cachea esta config en
`.vagrant/machines/<vm>/libvirt/synced_folders` al momento del `vagrant up`;
un `vagrant reload` (o borrar esos archivos y volver a levantar) la regenera.

Apagar / limpiar:

```bash
vagrant halt        # pausa (conserva provision y config in-guest) — para pausar
vagrant destroy -f  # BORRA todo: hay que re-configurar eth1 y provisionar de nuevo
```

---

## 4. Monitoreo con Prometheus + Grafana

Prometheus (puerto 9090) scrapea el `node_exporter` (:9100) de cada VM;
Grafana (puerto 3000) lo visualiza. Todo corre en el **host**. Los binarios
de `monitoring/bin/` son solo **una** de las tres formas de proveerlos:

**Opción A — autocontenidos (recomendada, sin root ni docker):**

```bash
./monitoring/get_binaries.sh        # una vez: descarga Prometheus + Grafana (~570 MB)
vagrant up                          # los targets están en las VMs
./monitoring/start_monitoring.sh    # levanta ambos + health-check
./monitoring/stop_monitoring.sh     # los detiene
```

**Opción B — tus propias instalaciones** (paquetes de distro, Docker, binarios
propios): lo único obligatorio es que usen **los mismos configs** del repo:

```bash
# Prometheus (cualquier instalación):
prometheus --config.file=monitoring/prometheus.yml --storage.tsdb.path=/tmp/prom-data
# Grafana: apuntar GF_PATHS_PROVISIONING al repo para auto-provisionar datasource+dashboard:
GF_PATHS_DATA=~/.grafana-data GF_PATHS_PROVISIONING="$(pwd)/monitoring/grafana/provisioning" \
    grafana server --homepath /usr/share/grafana     # ruta según instalación
# Docker (en el repo):
docker run -d -p 9090:9090 -v "$PWD/monitoring/prometheus.yml:/etc/prometheus/prometheus.yml" prom/prometheus
docker run -d -p 3000:3000 -e GF_PATHS_PROVISIONING=/provisioning \
    -v "$PWD/monitoring/grafana/provisioning:/provisioning" \
    -v "$PWD/monitoring/grafana/dashboards:/dashboards" grafana/grafana
```

**Lo que vas a ver:**

- **Grafana** <http://localhost:3000> → carpeta **SOD** → dashboard
  *"Clúster SOD — monitoreo en vivo"* (login anónimo Admin, ya provisionado).
  Paneles: CPU por nodo, RAM, tráfico en eth1 (red del lab) y load1, con
  auto-refresh de 5 s.
- **Prometheus** <http://localhost:9090> (PromQL directo).

Provisioning automático (lo lee Grafana al arrancar):

```
monitoring/prometheus.yml                         targets del clúster (scrape 5 s)
monitoring/grafana/provisioning/datasources/      datasource "Prometheus SOD"
monitoring/grafana/provisioning/dashboards/       registra monitoring/grafana/dashboards/
monitoring/grafana/dashboards/sod-cluster.json    el dashboard
```

Verificar targets: Prometheus → *Status → Targets*, o el resumen que imprime
`start_monitoring.sh` (los 4 deben estar `up`; `down` = VMs apagadas o firewall).

---

## 5. Ejecutar la simulación con SimGrid

El launcher `scripts/correr_simulacion.sh` usa el venv dedicado
`.venv-simgrid` (bindings S4U de SimGrid 4.1). Si no existe:

```bash
python3 -m venv .venv-simgrid
.venv-simgrid/bin/pip install pybind11==2.13.6 cmake setuptools wheel
.venv-simgrid/bin/pip install --no-build-isolation simgrid
```

> La instalación de `simgrid` **compila código C++ (~5-10 min)**: necesita el
> compilador y los headers de Python de tu distro (§1 ya los incluye:
> `base-devel` en Arch, `python3-dev` en Debian, `python3-devel` en Fedora).

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

**Lotes automáticos** (SimGrid admite un Engine por proceso: cada combinación
va en un subproceso). Ambos lotes usan semilla fija (42), y escriben **directo
en `data/`** con los nombres que espera el análisis del §7:

```bash
mkdir -p data
./scripts/correr_simulacion.sh --batch-all --batch-mode validacion \
    --output-csv data/resultados_simgrid_val.csv      # 9 corridas: 3 estrategias × latencias 1/20/100 ms
./scripts/correr_simulacion.sh --batch-all --batch-mode escalabilidad \
    --output-csv data/resultados_simgrid_scale.csv    # 12 corridas: 3 estrategias × n = 16/64/256/512
```

- `validacion` replica el escenario del clúster real (3 workers, mismas
  latencias) → es lo que el §7 compara contra `data/resultados_reales.csv`.
- `escalabilidad` extrapola a 16/64/256/512 workers con latencia fija de 1 ms.
- Sin `--output-csv`, las filas caen en `resultados_simulacion.csv` de la
  raíz (ahí también va el `--trivial-test`).

Salidas: filas con el esquema
`env,scenario_id,strategy,n_workers,latency_ms,seed,makespan_s,
throughput_tps,mean_wait_s,load_imbalance_cv,control_messages`.

> Los backtraces de pybind11 al final del proceso son ruido de teardown, no
> errores; el criterio de éxito es `-> OK`.

---

## 6. Experimento REAL en el clúster (emulación)

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

La corrida termina sola (~30 s: despacha 100 tareas, espera resultados, manda
STOP y escribe el CSV). Copiar la fila al host y **acumularla** en
`data/resultados_reales.csv` (la primera vez incluye la cabecera; después se
agregan solo las filas):

```bash
mkdir -p data
vagrant ssh coordinador --command 'cat /tmp/run_rr_1_42.csv' \
  | (grep -q strategy data/resultados_reales.csv 2>/dev/null && tail -n +2 || cat) \
  >> data/resultados_reales.csv
```

**Campaña completa del diseño**: 3 estrategias × 3 latencias (1/20/100 ms) ×
5 semillas (11, 42, 73, 101, 137) = **45 corridas** (~30-40 min en total).
El netem es global, así que el bucle exterior va por latencia:

```bash
for lat in 1 20 100; do
  ./scripts/set_netem.sh "$lat"
  for strat in round_robin least_loaded power_of_two; do
    for seed in 11 42 73 101 137; do
      out="/tmp/run_${strat}_${lat}_${seed}.csv"
      vagrant ssh coordinador --command "nohup python3 /opt/sod_cluster/coordinador.py \
          --strategy $strat --workers 3 --latency $lat --seed $seed \
          --output-csv $out > ${out%.csv}.log 2>&1 & echo lanzado"
      sleep 45    # margen: la corrida dura ~30 s (más con latencia 100 ms)
      vagrant ssh coordinador --command "tail -1 ${out%.csv}.log"
      vagrant ssh coordinador --command "cat $out" \
        | (grep -q strategy data/resultados_reales.csv 2>/dev/null && tail -n +2 || cat) \
        >> data/resultados_reales.csv
      echo "==> done: $strat lat=${lat}ms seed=$seed"
    done
  done
done
./scripts/set_netem.sh 0    # NUNCA olvidar: limpia el qdisc
```

**Gotchas:**

- En el log deben verse `[REG] worker-N (control: tcp://192.168.56.1x:5556)`.
  Si aparecen IPs `192.168.121.x`, el worker cacheó la IP de gestión
  (`worker.py:get_local_ip` corre una vez al arrancar): fix con
  `vagrant ssh worker-N --command 'sudo systemctl restart sod-worker'`.
- El netem es global (afecta a las 4 VMs): el bucle de la campaña va por
  latencia. Terminar siempre con `./scripts/set_netem.sh 0`.
- worker-3 está limitado a 50% de CPU por cgroups (emula el nodo antiguo).

---

## 7. Ejecutar el notebook de análisis

`analisis_resultados.py` está en formato "percent" (celdas `# %%`). Se
convierte a Jupyter y corre con el venv de análisis:

```bash
# venv + kernel (una vez):
python3 -m venv .venv
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

Qué produce (300 DPI, estilo APA, etiquetas en español):

1. Tabla descriptiva del clúster real (media, DE, IC 95% con t de Student).
2. Barras agrupadas con IC 95%: efecto de la latencia sobre cada estrategia.
3. Validación Real vs SimGrid: error relativo % (tabla, heatmap, barras).
4. Extrapolación 16→512 nodos (escala log₂) con anotación del punto de
   saturación del coordinador en `least_loaded`.
5. Export: `figuras/*.png` y `tablas/*.{csv,tex}`.

**Importante:** la primera celda revisa `data/`. Si **ninguno** de los 3 CSV
existe, genera los tres con **datos sintéticos** para que el pipeline sea
ejecutable de inmediato. Si existe un subconjunto, genera sintéticos **solo
para los faltantes** (y avisa que el análisis mezclará datos reales y
sintéticos — llenar los tres con datos de verdad es lo ideal:
`data/resultados_reales.csv` del §6, `data/resultados_simgrid_val.csv` y
`data/resultados_simgrid_scale.csv` del §5, con el esquema indicado ahí).
El análisis corre sobre ellos sin cambiar nada.

---

## 8. Reset del experimento

```bash
rm -f data/*.csv && rm -rf figuras tablas analisis_resultados.ipynb
# resultados_simulacion.csv (raíz) acumula corridas: para dejarlo "en blanco"
# quedarse solo con la cabecera.
vagrant halt     # pausa;  vagrant destroy -f borra TODO (ver §3: re-configurar eth1)
```

---

## Licencia

MIT — ver `LICENSE`.
