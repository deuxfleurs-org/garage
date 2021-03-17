# Configure the daemon

Garage is a software that can be run only in a cluster and requires at least 3 instances.
In our getting started guide, we document two deployment types:
  - [Single machine deployment](#single-machine-deployment) though `docker-compose`
  - [Multiple machine deployment](#multiple-machine-deployment) through `docker` or `systemd`

In any case, you first need to generate TLS certificates, as traffic is encrypted between Garage's nodes.

## Generating a TLS Certificate

Next, to generate your TLS certificates, run on your machine:

```
wget https://git.deuxfleurs.fr/Deuxfleurs/garage/raw/branch/master/genkeys.sh
chmod +x genkeys.sh
./genkeys.sh
```

It will creates a folder named `pki` containing the keys that you will used for the cluster.

### Single machine deployment

Single machine deployment is only described through docker compose.

```yml
version: '3.4'

networks: { virtnet: { ipam: { config: [ subnet: 172.20.0.0/24 ]}}}

services:
  g1:
    image: lxpz/garage_amd64:v0.1.1d
    networks: { virtnet: { ipv4_address: 172.20.0.101 }}
    volumes:
      - "./pki:/pki"
      - "./config.toml:/garage/config.toml"

  g2:
    image: lxpz/garage_amd64:v0.1.1d
    networks: { virtnet: { ipv4_address: 172.20.0.102 }}
    volumes:
      - "./pki:/pki"
      - "./config.toml:/garage/config.toml"

  g3:
    image: lxpz/garage_amd64:v0.1.1d
    networks: { virtnet: { ipv4_address: 172.20.0.103 }}
    volumes:
      - "./pki:/pki"
      - "./config.toml:/garage/config.toml"
```

*We define a static network here which is not considered as a best practise on Docker.
The rational is that Garage only supports IP address and not domain names in its configuration, so we need to know the IP address in advance.*

and then create the `config.toml` file as follow:

```toml
metadata_dir = "/garage/meta"
data_dir = "/garage/data"
rpc_bind_addr = "[::]:3901"
bootstrap_peers = [
  "172.20.0.101:3901",
  "172.20.0.102:3901",
  "172.20.0.103:3901",
]

[rpc_tls]
ca_cert = "/pki/garage-ca.crt"
node_cert = "/pki/garage.crt"
node_key = "/pki/garage.key"

[s3_api]
s3_region = "garage"
api_bind_addr = "[::]:3900"

[s3_web]
bind_addr = "[::]:3902"
root_domain = ".web.garage"
index = "index.html"
```

*Please note that we have not mounted `/garage/meta` or `/garage/data` on the host: data will be lost when the container will be destroyed.*

And that's all, you are ready to launch your cluster!

```
sudo docker-compose up
```

While your daemons are up, your cluster is still not configured yet.
However, you can check that your services are still listening as expected by querying them from your host:

```bash
curl http://172.20.0.{101,102,103}:3902
```

which should give you:

```
Not found
Not found
Not found
```

### Multiple machine deployment

Before deploying garage on your infrastructure, you must inventory your machines.
For our example, we will suppose the following infrastructure:

| Location | Name    | IP Address | Disk Space |
|----------|---------|------------|------------|
| Paris    | Mercury | fc00:1::1  | 1 To       |
| Paris    | Venus   | fc00:1::2  | 2 To       |
| London   | Earth   | fc00:1::2  | 2 To       |
| Brussels | Mars    | fc00:B::1  | 1.5 To     |

First, you need to setup your machines/VMs by copying on them the `pki` folder in `/etc/garage/pki`.
All your machines will also share the same configuration file, stored in `/etc/garage/config.toml`:

```toml
metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"
rpc_bind_addr = "[::]:3901"
bootstrap_peers = [
  "[fc00:1::1]:3901",
  "[fc00:1::2]:3901",
  "[fc00:B::1]:3901",
  "[fc00:F::1]:3901",
]

[rpc_tls]
ca_cert = "/pki/garage-ca.crt"
node_cert = "/pki/garage.crt"
node_key = "/pki/garage.key"

[s3_api]
s3_region = "garage"
api_bind_addr = "[::]:3900"

[s3_web]
bind_addr = "[::]:3902"
root_domain = ".web.garage"
index = "index.html"
```


