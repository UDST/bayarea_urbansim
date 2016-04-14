# -*- mode: ruby -*-
# vi: set ft=ruby :

# All Vagrant configuration is done below. The "2" in Vagrant.configure
# configures the configuration version (we support older styles for
# backwards compatibility). Please don't change it unless you know what
# you're doing.
Vagrant.configure(2) do |config|
  config.vm.box = "ubuntu/trusty64"

  config.vm.synced_folder ".", "/bayarea_urbansim"

  config.vm.provider "virtualbox" do |vb|
    vb.memory = "50000"
    vb.cpus = 8
  end

  config.vm.provision :shell, :inline => 'apt-get -y install dos2unix'

  config.vm.provision :shell, 
    :inline => 'dos2unix /bayarea_urbansim/scripts/vagrant/*.sh', privileged: false

  config.vm.provision :shell, :path => "scripts/vagrant/bootstrap.sh", privileged: false

end
