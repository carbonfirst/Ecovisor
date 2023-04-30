# Ecovisor

## Functionality to run Ecovisor in local or cluster modes

In local mode, applications are managed as containers within your computer or server.
In the cluster mode, the Ecovisor can manage a cluster of LXD servers.

### Setup

To run this, you should have LXD installed.
If not, follow the instructions below:
```
snap install lxd
lxd init
```

For more information about setting up LXD, please visit https://linuxcontainers.org/lxd/docs/latest/installing/

Once you have LXD set, install the following dependencies with pip:

```
pip install rpyc pylxd
```

Now you would need to create and add a certificate to properly authenticate the Ecovisor with LXD.
These are usually two ways to get certificates to access LXD’s API. The easiest is to use the one already created with LXD:

1. Become admin (e.g., sudo su, or su)
2. Copy the two certificate and key files into your directory:
```
cp /var/snap/lxd/{server.crt,server.key} .
```

If this returns an error, try one of the following:
```
cp /var/lib/lxd/{server.crt,server.key} .
cp /var/snap/lxd/common/lxd/{server.crt,server.key} .
```

A second way, is to generate and sign the certificates yourself, and then add them into LXD to grant users owning such certificate with access. To do so, follow these commands:

1. ```lxc config set core.https_address [::]:8443```
2. ```lxc config set core.https_allowed_origin "*"```
3. ```lxc config set core.https_allowed_methods "GET, POST, PUT, DELETE, OPTIONS"```
4. ```lxc config set core.https_allowed_headers "Content-Type"```
5. This is just to keep the files related to authentication in a separate directory:
```mkdir lxd-api-access-cert-key-files; cd lxd-api-access-cert-key-files```
6. This generates a private key for you:
```openssl genrsa -out lxd.key 4096```
7. This creates a certificate signing request, to be signed with the key you created in Step 6:
```openssl req -new -key lxd.key -out lxd.csr```
8. This finally self-signs and create an expiration date for the certificate:
```openssl x509 -req -days 3650 -in lxd.csr -signkey lxd.key -out lxd.crt```
9. This adds the certificate you just created into the LXD cluster, granting access to the cluster to whoever uses it (i.e., the Ecovisor):
```lxc config trust add lxd.crt```

Once you have the path to certificate and key, you are ready to run Ecovisor.

### Run

1. First run Carbon Manager using:
```
python3 CarbonManager.py --rm-endpoint `localhost` --lxd-cert <path to lxd certificate in single quotes> --lxd-key <path to lxd key in single quotes>
```

2. Now run ApplicationController as following:
```
python3 ApplicationController.py
```

### Customizing Application json

If you are running energy-hypervisor in a container, then it can use HPC_APPLICATION image available on the container. This is mentioned in application.json under "image". If you are running locally, we are providing a default ubuntu image in application json (as "server") and protocol (as "protocol") to initiate a container. If you want to replace any of this with your own image, you can do so by using any of the following keys in application.json:
1. Custom image can be provided under "image" keys.
2. A different image from server can be pulled using "server" and "protocol" keys.


