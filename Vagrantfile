Vagrant.configure(2) do |config|
  ## User configurable settings
  registry         = ENV.fetch("REGISTRY", "docker-registry.zende.sk")
  docker_host_ip   = ENV.fetch("DOCKER_HOST_IP", "192.168.42.100")
  docker_host_port = ENV.fetch("DOCKER_HOST_PORT", 2375)
  memory_size      = 4096
  cpus             = 2

  ## Machine configurable settings
  config.vm.box      = "phusion/ubuntu-14.04-amd64"
  config.vm.hostname = "maxscale"
  config.vm.network :private_network, ip: docker_host_ip
  config.vm.synced_folder "../", "/zendesk", type: "nfs", mount_options: ["nolock,vers=3,udp"]

  ## Provider specific configuration(s)
  # Virtualbox will be preferenced over VMware
  config.vm.provider :virtualbox do |vm|
    vm.name   = "maxscale-docker"
    vm.cpus   = cpus
    vm.memory = memory_size
  end

  config.vm.provider :vmware_fusion do |vm|
    vm.vmx["numvcpus"] = cpus
    vm.vmx["memsize"]  = memory_size
  end

  ## Provisioning
  # Make Docker listen on #{docker_host_port}
  config.vm.provision :shell do |s|
    s.inline = "echo $1 > /etc/default/docker"
    s.args   = "'DOCKER_OPTS=\"-H tcp://0.0.0.0:#{docker_host_port} -H unix:///var/run/docker.sock ${DOCKER_OPTS}\"'"
  end

  # Add docker, setup default images
  config.vm.provision :docker
end
