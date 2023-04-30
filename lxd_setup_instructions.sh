#!/bin/bash

# This script tries to build and setup LXD
# It should not be run in one go, it might return errors that are annoying to handle in bash.
# Instead, each of its commands should be run separaretly, so errors (like missing dependencies for some 'dependency of dependency' installs) 
# can be captured and handled manually by the one running them.

# Some commands assume a DEBIAN/UBUNTU system, adapt them accordingly.

# This script depends on a system that has golang version >= 1.14 already installed
# Check your version by executing `go version` on your terminal
# If it returns something before 1.14, on Ubuntu you may install and update it with:
# cd /tmp
# wget https://go.dev/dl/go1.20.2.linux-amd64.tar.gz
# sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go1.20.2.linux-amd64.tar.gz
# sudo update-alternatives --install /usr/bin/go go /usr/local/go/bin/go 10
# go version

# Besides golang, there are many dependencies not covered in here
# Check them as you execute the commands

# WARNING: BEFORE YOU BEGIN, PLEASE, IF APPLICABLE, UNINSTALL ANY PREVIOUS VERSION OF LXC/LXD/LXCFS CURRENTLY INSTALLED IN YOUR SYSTEM. I.E:
# sudo apt purge lxcfs liblxc-common lxc lxd-client

LXC_DEPS="python3-six libselinux1-dev cgroupfs-mount libsemanage1-dev libsepol1-dev golang-github-opencontainers-selinux-dev policycoreutils-dev selinux-policy-dev libpam-cgfs python3-pip attr libapparmor-dev libseccomp-dev libcap-dev acl autoconf dnsmasq-base git libacl1-dev libcap-dev libsqlite3-dev libtool libudev-dev liblz4-dev libuv1-dev make pkg-config rsync squashfs-tools tar tcl xz-utils graphviz libpam0g-dev libssl-dev doxygen docbook2x libtool m4 automake pkg-config autoconf gccgo-8"
LXD_APT_DEPS="attr lvm2 thin-provisioning-tools btrfs-progs zfsutils-linux libzfslinux-dev golang-go-zfs-dev libjansson4 libjansson-dev xtables-addons-dkms libxtables12 libxtables-dev libmnl-dev libmnl0 libnftnl-dev flex libfl-dev asciidoc libgmp-dev libedit-dev libxslt1-dev docbook-xsl xsltproc libxml2-utils ubuntu-fan"
sudo apt install $LXC_DEPS $LXD_APT_DEPS

# LXC SETUP
LATEST=4.0.11
LXC_GIT="https://github.com/lxc/lxc"
git clone $LXC_GIT
cd lxc 
git checkout lxc-$LATEST
./autogen.sh
./configure --prefix=/usr/ --enable-capabilities --enable-openssl --enable-pam --enable-doc --enable-api-docs --enable-apparmor
make
sudo make install
#export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/usr/local/lib
cd ..

# LXCFS SETUP
sudo -H pip3 install --upgrade pip
sudo -H pip3 install meson ninja jinja2
# If previous cmd doesn't work, try: pip3 install --user meson
git clone https://github.com/libfuse/libfuse
cd libfuse
mkdir build && cd build
meson ..
ninja
sudo ninja install
cd ../..
LATEST=4.0.11
LXCFS="https://github.com/lxc/lxcfs"
git clone $LXCFS
cd lxcfs
git checkout $LATEST
./bootstrap.sh
./configure
make -j
sudo make install
#meson setup -Dinit-script=systemd --prefix=/usr build/
#meson compile -C build/
#sudo meson install -C build/
sudo mkdir -p /usr/local/var/lib/lxcfs
cd ..

# LXD SETUP
LXD_URL="github.com/lxc/lxd"
export GOPATH=/mnt/nfs/go/ # Change to whatever you need.
mkdir -p $GOPATH/src/$LXD_URL
export GO111MODULE=auto
cd $GOPATH/src/$LXD_URL/../
git clone https://$LXD_URL
cd $GOPATH/src/$LXD_URL/
mkdir _dist
export GOPATH=`pwd`/_dist # This is the ultimate GOPATH we set. It is a trick to avoid go weird recursions and to contain LXD binaries within one directory only.
export GOBIN=$GOPATH/bin
make deps

# AFTER RUNNING THE ABOVE COMMAND (make deps), IT WILL TELL YOU TO RUN SOME ADDITIONAL COMMANDS. HOWEVER, THERE IS NO NEED TO RUN THEM, BECAUSE WE WILL HANDLE THESE BELOW
# READ THE INITIAL NOTES IF SOMETHING UP TO THIS POINT DOES NOT WORK!

LXD_PROFILE="/etc/profile.d/lxd.sh"
sudo echo '#!/bin/bash' >> $LXD_PROFILE
sudo echo "export CGO_CFLAGS=-I$GOPATH/deps/raft/include/ -I$GOPATH/deps/dqlite/include/" >> $LXD_PROFILE
sudo echo "export CGO_LDFLAGS=-L$GOPATH/deps/raft/.libs -L$GOPATH/deps/dqlite/.libs/" >> $LXD_PROFILE
sudo echo "export LD_LIBRARY_PATH="$LD_LIBRARY_PATH:$GOPATH/deps/raft/.libs/:$GOPATH/deps/dqlite/.libs/ >> $LXD_PROFILE
sudo echo "export CGO_LDFLAGS_ALLOW=(-Wl,-wrap,pthread_create)|(-Wl,-z,now)" >> $LXD_PROFILE
echo "source $LXD_PROFILE" >> /etc/bash.bashrc
source $LXD_PROFILE

sudo cp -a $GOPATH/deps/raft/.libs/libraft.so.0.0.7 /usr/lib
sudo ln -s /usr/lib/libraft.so.0.0.7 /usr/lib/libraft.so.0
sudo cp -a $GOPATH/deps/dqlite/.libs/libdqlite.so.0.0.1 /usr/lib
sudo ln -s /usr/lib/libdqlite.so.0.0.1 /usr/lib/libdqlite.so.0

sudo echo "$GOPATH/deps/raft/.libs/" > /etc/ld.so.conf.d/raft.conf
sudo echo $GOPATH/deps/dqlite/.libs/ > /etc/ld.so.conf.d/dqlite.conf
sudo ldconfig
#sudo ln -s /usr/local/lib/liblxc.so /usr/lib/liblxc.so
sudo echo "PATH=`cat /etc/environment | awk -F = '{print $2}'`:$GOBIN" > /etc/environment

go get ./...
# IF YOU GET ERRORS AFTER RUNNING go get ..., TRY THIS:
# export GO111MODULE=off
# make deps
# go get ./...

make

# AT THIS POINT, THE ABOVE MAKE COMMAND SHOULD RETURN: 
# 'LXD built successfully"

# UNPRIVILEGED CONTAINERS NEED TO HAVE THEIR PIDS REMAPPED:
echo "root:1000000:65536" | sudo tee -a /etc/subuid /etc/subgid
sudo adduser --system lxd --home /var/lib/lxd/ --shell /bin/false
sudo addgroup --system lxd
# CHANGE $USER FOR YOUR OWN, AND OTHER USERS THAT WILL BE OPERATORS IN THE CLUSTER
sudo adduser $USER lxd 


# AND FINALLY,
# CMDS TO RUN LXCFS and then LXD:

sudo lxcfs -d --enable-loadavg -u --enable-cfs -o default_permissions,allow_other --enable-pidf /usr/local/var/lib/lxcfs/
sudo -E PATH=${PATH} LD_LIBRARY_PATH=${LD_LIBRARY_PATH} $(go env GOPATH)/bin/lxd --group lxd
lxd init

# CONGRATS!
