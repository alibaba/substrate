set -euo pipefail

cd /opt/beegfs-poc
stage="rpms.$(date +%s)"
mkdir -p "$stage"
tar -xzf beegfs-rpms.tar.gz -C "$stage"

yum -y install \
  "kernel-devel-$(uname -r)" \
  dkms \
  gcc \
  make \
  elfutils-libelf-devel \
  rdma-core \
  librdmacm \
  >/tmp/beegfs-poc-yum.log 2>&1

rpm -Uvh --replacepkgs "$stage"/*.rpm >/tmp/beegfs-poc-rpm.log 2>&1
dkms add -m beegfs -v 8.4.0 >/tmp/beegfs-poc-dkms-add.log 2>&1 || true
dkms build -m beegfs -v 8.4.0 -k "$(uname -r)" --force >/tmp/beegfs-poc-dkms-build.log 2>&1
dkms install -m beegfs -v 8.4.0 -k "$(uname -r)" --force >/tmp/beegfs-poc-dkms-install.log 2>&1
depmod
modprobe beegfs
lsmod | grep -q beegfs
dkms status | grep -q beegfs
