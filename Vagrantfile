# -*- mode: ruby -*-
# vi: set ft=ruby :
#
# Clúster heterogéneo SOD (1 coordinador + N workers) sobre KVM/libvirt.
#
#   vagrant up                     # usa libvirt por defecto
#   N_WORKERS=5 vagrant up         # amplía el clúster a 5 workers
#   vagrant destroy -f             # destruye el clúster
#
# Perfil heterogéneo (se asigna cíclicamente si N_WORKERS > 3):
#   worker-1: nodo potente   -> 2 vCPU / 1536 MB
#   worker-2: nodo medio     -> 1 vCPU / 1024 MB
#   worker-3: nodo antiguo   -> 1 vCPU /  512 MB (además, tc/netem +
#                               CPUQuota=50% vía systemd, ver playbook)

# Proveedor por defecto: libvirt (no hace falta `--provider=libvirt`).
ENV['VAGRANT_DEFAULT_PROVIDER'] ||= 'libvirt'

N_WORKERS = (ENV['N_WORKERS'] || 3).to_i

# Red privada del laboratorio.
NET_PREFIX = '192.168.56'

# Perfiles de los tres primeros workers (heterogéneos a propósito).
WORKER_PROFILES = [
  { cpu: 2, memory: 1536 },   # worker-1: potente
  { cpu: 1, memory: 1024 },   # worker-2: medio
  { cpu: 1, memory: 512 },    # worker-3: antiguo/lento
].freeze

Vagrant.configure('2') do |config|
  config.vm.box = 'debian/bookworm64'

  # rsync en lugar de NFS/SSHFS: evita errores de NFS en hosts Arch Linux.
  #
  # IMPORTANTE: solo se sincroniza el código del lab. Lo demás se excluye
  # porque no lo usa ningún guest y puede pesar GIGAS (monitoring/bin/ tiene
  # los binarios de Prometheus/Grafana; los venvs, etc.). Sin estos excludes,
  # cada `vagrant up` sube ~2.6 GB a CADA VM por SSH (red + CPU al 100%).
  config.vm.synced_folder '.', '/vagrant', type: 'rsync',
    rsync__exclude: [
      '.git/', '.gitbutler/', '.vagrant/',
      '.venv/', '.venv-simgrid/',
      '__pycache__/',
      'monitoring/',      # corre en el host (ver README §4)
      'data/', 'figuras/', 'tablas/',
      'docs/', '*.ipynb'
    ]

  # Red privada común a todas las VMs (definida por VM con IP fija).
  # NOTA: vagrant-libvirt crea además su red de gestión (192.168.121.0/24);
  # las IPs de abajo son las del laboratorio, dentro de 192.168.56.0/24.

  # ------------------------------------------------------------------
  # Coordinador: también ejecuta las simulaciones SimGrid de hasta 512
  # nodos, por eso se le da 2048 MB de RAM.
  # ------------------------------------------------------------------
  config.vm.define 'coordinador' do |coord|
    coord.vm.hostname = 'coordinador'
    coord.vm.network 'private_network', ip: "#{NET_PREFIX}.10"

    coord.vm.provider :libvirt do |libvirt|
      libvirt.cpus   = 1
      libvirt.memory = 2048
    end
  end

  # ------------------------------------------------------------------
  # Workers (heterogéneos; perfiles cíclicos si N_WORKERS > 3)
  # ------------------------------------------------------------------
  (1..N_WORKERS).each do |i|
    profile = WORKER_PROFILES[(i - 1) % WORKER_PROFILES.length]

    config.vm.define "worker-#{i}" do |worker|
      worker.vm.hostname = "worker-#{i}"
      worker.vm.network 'private_network', ip: "#{NET_PREFIX}.#{10 + i}"

      worker.vm.provider :libvirt do |libvirt|
        libvirt.cpus   = profile[:cpu]
        libvirt.memory = profile[:memory]
      end
    end
  end

  # ------------------------------------------------------------------
  # Aprovisionamiento con Ansible, UNA SOLA VEZ y al final, cuando todas
  # las VMs están levantadas (limit = "all" evita correr el playbook por
  # cada máquina).
  # ------------------------------------------------------------------
  config.vm.provision 'ansible' do |ansible|
    ansible.playbook = 'ansible/playbook.yml'
    ansible.limit    = 'all'
    ansible.compatibility_mode = '2.0'
  end

  # ==================================================================
  # Alternativa para integrantes con Windows/macOS (sin KVM):
  # descomentar el bloque de abajo y ejecutar:
  #   vagrant up --provider=virtualbox
  # Requiere la versión virtualbox del box: vagrant box add \
  #   --provider virtualbox debian/bookworm64 ... (o bento/debian-12)
  # ==================================================================
  #
  # config.vm.provider :virtualbox do |vb|
  #   vb.linked_clone = true
  #   vb.memory = 1024
  #   vb.cpus = 1
  # end
  #
  # (dentro de cada define se puede sobreescribir, p. ej. para el
  # nodo antiguo):
  #
  # config.vm.define 'worker-3' do |worker3|
  #   worker3.vm.provider :virtualbox do |vb3|
  #     vb3.memory = 512
  #     vb3.cpus   = 1
  #     # Mitigación del nodo lento en VirtualBox: no hay cgroups v2
  #     # garantizados en el guest, así que se limita desde el hipervisor.
  #     vb3.customize ['modifyvm', :id, '--cpuexecutioncap', '50']
  #   end
  # end
end
