# energy-hypervisor

Repo for energy hypervisor

## Functionality to run energy-hypervisor locally as well


Energy hypervisor can be run locally.

### Setup

To run this, you should have lxd installed.
If not, follow the instructions below:
```
Snap install lxd
lxd init
```

You can choose the default values or custom options as per your system. For storage backend, I recommend using brtfs. 

Then try creating a container using ubuntu image:
```
lxc launch ubuntu:22.04 <name of container> 
```

If this is successful, you can see the list of running containers using:
```
lxc list
```

Other commands to explore:
```
lxc exec <name of container> – /bin/bash
Lxc <start/stop/restart/pause/delete> <container name> (you can use –force flag to force it)
```

Once you have lxd installed and running, install the following packages to setup energy hypervisor locally:

```
Install pip
pip install rpyc
pip install pylxd
```

Now you would need to fetch the lxd certificate and key to run lxd via energy-hypervisor.
These are usually two ways to get certificates to access LXD’s API. The easiest, is the following:
1. Become admin, i.e., sudo su
2. Copy the two certificate and key files into your directory:
```
cp /var/snap/lxd/{server.crt,server.key} .
```

If this returns an error, try one of the following:
```
cp /var/lib/lxd/{server.crt,server.key} .
cp /var/snap/lxd/common/lxd/{server.crt,server.key} .
```

Once you have the path to certificate and key, you are ready to run energy-hypervisor.

### Run

1. First run Carbon Manager using:
```
python3 CarbonManager.py --lxd-cert <path to lxd certificate in single quotes> --lxd-key <path to lxd key in single quotes>
```

2. Now run ApplicationController as following:
```
python3 ApplicationController.py
```

### Customizing Application json

If you are running energy-hypervisor in a container, then it can use HPC_APPLICATION image available on the container. This is mentioned in application.json under "image". If you are running locally, we are providing a default ubuntu image in application json (as "server") and protocol (as "protocol") to initiate a container. If you want to replace any of this with your own image, you can do so by using any of the following keys in application.json:
1. Custom image can be provided under "image" keys.
2. A different image from server can be pulled using "server" and "protocol" keys.


