# Deploying Garage on a real-world cluster

To run Garage in cluster mode, we recommend having at least 3 nodes.
This will allow you to setup Garage for three-way replication of your data,
the safest and most available mode proposed by Garage.

We recommend first following the [quick start guide](../quick_start/index.md) in order
to get familiar with Garage's command line and usage patterns.



## Prerequisites

To run a real-world deployment, make sure you the following conditions are met:

- You have at least three machines with sufficient storage space available.

- Each machine has a public IP address which is reachable by other machines.
  Running behind a NAT is possible, but having several Garage nodes behind a single NAT
  is slightly more involved as each will have to have a different RPC port number
  (the local port number of a node must be the same as the port number exposed publicly
  by the NAT).

- Ideally, each machine should have a SSD available in addition to the HDD you are dedicating
  to Garage. This will allow for faster access to metadata and has the potential
  to drastically reduce Garage's response times.

- This guide will assume you are using Docker containers to deploy Garage on each node. 
  Garage can also be run independently, for instance as a [Systemd service](systemd.md).
  You can also use an orchestrator such as Nomad or Kubernetes to automatically manage
  Docker containers on a fleet of nodes.

Before deploying Garage on your infrastructure, you must inventory your machines.
For our example, we will suppose the following infrastructure with IPv6 connectivity:

| Location | Name    | IP Address | Disk Space |
|----------|---------|------------|------------|
| Paris    | Mercury | fc00:1::1  | 1 To       |
| Paris    | Venus   | fc00:1::2  | 2 To       |
| London   | Earth   | fc00:B::1  | 2 To       |
| Brussels | Mars    | fc00:F::1  | 1.5 To     |



## Get a Docker image

Our docker image is currently named `lxpz/garage_amd64` and is stored on the [Docker Hub](https://hub.docker.com/r/lxpz/garage_amd64/tags?page=1&ordering=last_updated).
We encourage you to use a fixed tag (eg. `v0.3.0`) and not the `latest` tag.
For this example, we will use the latest published version at the time of the writing which is `v0.3.0` but it's up to you
to check [the most recent versions on the Docker Hub](https://hub.docker.com/r/lxpz/garage_amd64/tags?page=1&ordering=last_updated).

For example:

```
sudo docker pull lxpz/garage_amd64:v0.3.0
```


## Generating TLS certificates

You first need to generate TLS certificates to encrypt traffic between Garage nodes
(reffered to as RPC traffic).

To generate your TLS certificates, run on your machine:

```
wget https://git.deuxfleurs.fr/Deuxfleurs/garage/raw/branch/main/genkeys.sh
chmod +x genkeys.sh
./genkeys.sh
```

It will creates a folder named `pki/` containing the keys that you will used for the cluster.
These files will have to be copied to all of your cluster nodes, as explained below.


## Deploying and configuring Garage

On each machine, we will have a similar setup,
especially you must consider the following folders/files:

- `/etc/garage/garage.toml`: Garage daemon's configuration (see below)

- `/etc/garage/pki/`: Folder containing Garage certificates,
  must be generated on your computer and copied on the servers.
  Only the files `garage-ca.crt`, `garage.crt` and `garage.key` are necessary.

- `/var/lib/garage/meta/`: Folder containing Garage's metadata,
  put this folder on a SSD if possible

- `/var/lib/garage/data/`: Folder containing Garage's data,
  this folder will be your main data storage and must be on a large storage (e.g. large HDD)


A valid `/etc/garage/garage.toml` for our cluster would be:

```toml
metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"

replication_mode = "3"

rpc_bind_addr = "[::]:3901"

bootstrap_peers = [
  "[fc00:1::1]:3901",
  "[fc00:1::2]:3901",
  "[fc00:B::1]:3901",
  "[fc00:F::1]:3901",
]

[rpc_tls]
ca_cert = "/etc/garage/pki/garage-ca.crt"
node_cert = "/etc/garage/pki/garage.crt"
node_key = "/etc/garage/pki/garage.key"

[s3_api]
s3_region = "garage"
api_bind_addr = "[::]:3900"

[s3_web]
bind_addr = "[::]:3902"
root_domain = ".web.garage"
index = "index.html"
```

Please make sure to change `bootstrap_peers` to **your** IP addresses!

Check the [configuration file reference documentation](../reference_manual/configuration.md)
to learn more about all available configuration options.


## Starting Garage using Docker

On each machine, you can run the daemon with:

```bash
docker run \
  -d \
  --name garaged \
  --restart always \
  --network host \
  -v /etc/garage/pki:/etc/garage/pki \
  -v /etc/garage/garage.toml:/garage/garage.toml \
  -v /var/lib/garage/meta:/var/lib/garage/meta \
  -v /var/lib/garage/data:/var/lib/garage/data \
  lxpz/garage_amd64:v0.3.0
```

It should be restarted automatically at each reboot.
Please note that we use host networking as otherwise Docker containers
can not communicate with IPv6.

Upgrading between Garage versions should be supported transparently,
but please check the relase notes before doing so!
To upgrade, simply stop and remove this container and
start again the command with a new version of Garage.


## Controling the daemon

The `garage` binary has two purposes:
  - it acts as a daemon when launched with `garage server ...`
  - it acts as a control tool for the daemon when launched with any other command

In this section, we will see how to use the `garage` binary as a control tool for the daemon we just started.
You first need to get a shell having access to this binary. For instance, enter the Docker container with:

```bash
sudo docker exec -ti garaged bash
```

You will now have a shell where the Garage binary is available as `/garage/garage`

*You can also install the binary on your machine to remotely control the cluster.*

## Talk to the daemon and create an alias

`garage` requires 4 options to talk with the daemon:

```
--ca-cert <ca-cert>            
--client-cert <client-cert>    
--client-key <client-key>      
-h, --rpc-host <rpc-host>
```

The 3 first ones are certificates and keys needed by TLS, the last one is simply the address of Garage's RPC endpoint.

If you are invoking `garage` from a server node directly, you do not need to set `--rpc-host`
as the default value `127.0.0.1:3901` will allow it to contact Garage correctly.

To avoid typing the 3 first options each time we want to run a command,
you can use the following alias:

```bash
alias garagectl='/garage/garage \
  --ca-cert /etc/garage/pki/garage-ca.crt \
  --client-cert /etc/garage/pki/garage.crt \
  --client-key /etc/garage/pki/garage.key'
```

You can now use all of the commands presented in the [quick start guide](../quick_start/index.md),
simply replace occurences of `garage` by `garagectl`.

#### Test the alias

You can test your alias by running a simple command such as:

```
garagectl status
```

You should get something like that as result:

```
Healthy nodes:
8781c50c410a41b3…	Mercury	[fc00:1::1]:3901	UNCONFIGURED/REMOVED
2a638ed6c775b69a…	Venus	[fc00:1::2]:3901	UNCONFIGURED/REMOVED
68143d720f20c89d…	Earth	[fc00:B::1]:3901	UNCONFIGURED/REMOVED
212f7572f0c89da9…	Mars	[fc00:F::1]:3901	UNCONFIGURED/REMOVED
```


## Configuring a cluster

We will now inform Garage of the disk space available on each node of the cluster
as well as the zone (e.g. datacenter) in which each machine is located.

For our example, we will suppose we have the following infrastructure (Capacity, Identifier and Datacenter are specific values to Garage described in the following):

| Location | Name    | Disk Space | `Capacity` | `Identifier` | `Zone` |
|----------|---------|------------|------------|--------------|--------------|
| Paris    | Mercury | 1 To       | `2`        | `8781c5`     | `par1`       |
| Paris    | Venus   | 2 To       | `4`        | `2a638e`     | `par1`       |
| London   | Earth   | 2 To       | `4`        | `68143d`     | `lon1`       |
| Brussels | Mars    | 1.5 To     | `3`        | `212f75`     | `bru1`       |

#### Node identifiers

After its first launch, Garage generates a random and unique identifier for each nodes, such as:

```
8781c50c410a41b363167e9d49cc468b6b9e4449b6577b64f15a249a149bdcbc
```

Often a shorter form can be used, containing only the beginning of the identifier, like `8781c5`,
which identifies the server "Mercury" located in "Paris" according to our previous table.

The most simple way to match an identifier to a node is to run:

```
garagectl status
```

It will display the IP address associated with each node;
from the IP address you will be able to recognize the node.

#### Zones

Zones are simply a user-chosen identifier that identify a group of server that are grouped together logically.
It is up to the system administrator deploying Garage to identify what does "grouped together" means.

In most cases, a zone will correspond to a geographical location (i.e. a datacenter).
Behind the scene, Garage will use zone definition to try to store the same data on different zones,
in order to provide high availability despite failure of a zone.

#### Capacity

Garage reasons on an abstract metric about disk storage that is named the *capacity* of a node.
The capacity configured in Garage must be proportional to the disk space dedicated to the node.
Due to the way the Garage allocation algorithm works, capacity values must
be **integers**, and must be **as small as possible**, for instance with
1 representing the size of your smallest server.

Here we chose that 1 unit of capacity = 0.5 To, so that we can express servers of size
1 To and 2 To, as wel as the intermediate size 1.5 To, with the integer values 2, 4 and
3 respectively (see table above).

Note that the amount of data stored by Garage on each server may not be strictly proportional to
its capacity value, as Garage will priorize having 3 copies of data in different zones,
even if this means that capacities will not be strictly respected. For example in our above examples,
nodes Earth and Mars will always store a copy of everything each, and the third copy will
have 66% chance of being stored by Venus and 33% chance of being stored by Mercury.

#### Injecting the topology

Given the information above, we will configure our cluster as follow:

```
garagectl node configure -z par1 -c 2 -t mercury 8781c5
garagectl node configure -z par1 -c 4 -t venus 2a638e
garagectl node configure -z lon1 -c 4 -t earth 68143d
garagectl node configure -z bru1 -c 3 -t mars 212f75
```


## Using your Garage cluster

Creating buckets and managing keys is done using the `garagectl` CLI,
and is covered in the [quick start guide](../quick_start/index.md).
Remember also that the CLI is self-documented thanks to the `--help` flag and
the `help` subcommand (e.g. `garage help`, `garage key --help`).

Configuring an S3 client to interact with Garage is covered
[in the next section](clients.md).
