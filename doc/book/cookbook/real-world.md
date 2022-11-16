+++
title = "Deployment on a cluster"
weight = 5
+++

To run Garage in cluster mode, we recommend having at least 3 nodes.
This will allow you to setup Garage for three-way replication of your data,
the safest and most available mode proposed by Garage.

We recommend first following the [quick start guide](@/documentation/quick-start/_index.md) in order
to get familiar with Garage's command line and usage patterns.


## Preparing your environment

### Prerequisites

To run a real-world deployment, make sure the following conditions are met:

- You have at least three machines with sufficient storage space available.

- Each machine has a public IP address which is reachable by other machines.
  Running behind a NAT is likely to be possible but hasn't been tested for the latest version (TODO).

- This guide will assume you are using Docker containers to deploy Garage on each node. 
  Garage can also be run independently, for instance as a [Systemd service](@/documentation/cookbook/systemd.md).
  You can also use an orchestrator such as Nomad or Kubernetes to automatically manage
  Docker containers on a fleet of nodes.

Before deploying Garage on your infrastructure, you must inventory your machines.
For our example, we will suppose the following infrastructure with IPv6 connectivity:

| Location | Name    | IP Address | Disk Space |
|----------|---------|------------|------------|
| Paris    | Mercury | fc00:1::1  | 1 TB       |
| Paris    | Venus   | fc00:1::2  | 2 TB       |
| London   | Earth   | fc00:B::1  | 2 TB       |
| Brussels | Mars    | fc00:F::1  | 1.5 TB     |

Note that Garage will **always** store the three copies of your data on nodes at different
locations. This means that in the case of this small example, the available capacity
of the cluster is in fact only 1.5 TB, because nodes in Brussels can't store more than that.
This also means that nodes in Paris and London will be under-utilized.
To make better use of the available hardware, you should ensure that the capacity
available in the different locations of your cluster is roughly the same.
For instance, here, the Mercury node could be moved to Brussels; this would allow the cluster
to store 2 TB of data in total.

### Best practices

- If you have fast dedicated networking between all your nodes, and are planing to store
  very large files, bump the `block_size` configuration parameter to 10 MB
  (`block_size = 10485760`).

- Garage stores its files in two locations: it uses a metadata directory to store frequently-accessed
  small metadata items, and a data directory to store data blocks of uploaded objects.
  Ideally, the metadata directory would be stored on an SSD (smaller but faster),
  and the data directory would be stored on an HDD (larger but slower).

- For the data directory, Garage already does checksumming and integrity verification,
  so there is no need to use a filesystem such as BTRFS or ZFS that does it.
  We recommend using XFS for the data partition, as it has the best performance.
  Ext4 is not recommended as it has more strict limitations on the number of inodes,
  which might cause issues with Garage when large numbers of objects are stored.

- If you only have an HDD and no SSD, it's fine to put your metadata alongside the data
  on the same drive. Having lots of RAM for your kernel to cache the metadata will
  help a lot with performance.  Make sure to use the LMDB database engine,
  instead of Sled, which suffers from quite bad performance degradation on HDDs.
  Sled is still the default for legacy reasons, but is not recommended anymore.

- For the metadata storage, Garage does not do checksumming and integrity
  verification on its own. If you are afraid of bitrot/data corruption,
  put your metadata directory on a BTRFS partition. Otherwise, just use regular
  EXT4 or XFS.

- Having a single server with several storage drives is currently not very well
  supported in Garage ([#218](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/218)).
  For an easy setup, just put all your drives in a RAID0 or a ZFS RAIDZ array.
  If you're adventurous, you can try to format each of your disk as
  a separate XFS partition, and then run one `garage` daemon per disk drive,
  or use something like [`mergerfs`](https://github.com/trapexit/mergerfs) to merge
  all your disks in a single union filesystem that spreads load over them.

## Get a Docker image

Our docker image is currently named `dxflrs/garage` and is stored on the [Docker Hub](https://hub.docker.com/r/dxflrs/garage/tags?page=1&ordering=last_updated).
We encourage you to use a fixed tag (eg. `v0.8.0`) and not the `latest` tag.
For this example, we will use the latest published version at the time of the writing which is `v0.8.0` but it's up to you
to check [the most recent versions on the Docker Hub](https://hub.docker.com/r/dxflrs/garage/tags?page=1&ordering=last_updated).

For example:

```
sudo docker pull dxflrs/garage:v0.8.0
```

## Deploying and configuring Garage

On each machine, we will have a similar setup,
especially you must consider the following folders/files:

- `/etc/garage.toml`: Garage daemon's configuration (see below)

- `/var/lib/garage/meta/`: Folder containing Garage's metadata,
  put this folder on a SSD if possible

- `/var/lib/garage/data/`: Folder containing Garage's data,
  this folder will be your main data storage and must be on a large storage (e.g. large HDD)


A valid `/etc/garage/garage.toml` for our cluster would look as follows:

```toml
metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"
db_engine = "lmdb"

replication_mode = "3"

compression_level = 2

rpc_bind_addr = "[::]:3901"
rpc_public_addr = "<this node's public IP>:3901"
rpc_secret = "<RPC secret>"

[s3_api]
s3_region = "garage"
api_bind_addr = "[::]:3900"
root_domain = ".s3.garage"

[s3_web]
bind_addr = "[::]:3902"
root_domain = ".web.garage"
index = "index.html"
```

Check the following for your configuration files:

- Make sure `rpc_public_addr` contains the public IP address of the node you are configuring.
  This parameter is optional but recommended: if your nodes have trouble communicating with
  one another, consider adding it.

- Make sure `rpc_secret` is the same value on all nodes. It should be a 32-bytes hex-encoded secret key.
  You can generate such a key with `openssl rand -hex 32`.

## Starting Garage using Docker

On each machine, you can run the daemon with:

```bash
docker run \
  -d \
  --name garaged \
  --restart always \
  --network host \
  -v /etc/garage.toml:/etc/garage.toml \
  -v /var/lib/garage/meta:/var/lib/garage/meta \
  -v /var/lib/garage/data:/var/lib/garage/data \
  dxflrs/garage:v0.8.0
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
  - it acts as a daemon when launched with `garage server`
  - it acts as a control tool for the daemon when launched with any other command

Ensure an appropriate `garage` binary (the same version as your Docker image) is available in your path.
If your configuration file is at `/etc/garage.toml`, the `garage` binary should work with no further change.

You can test your `garage` CLI utility by running a simple command such as:

```bash
garage status
```

At this point, nodes are not yet talking to one another.
Your output should therefore look like follows:

```
Mercury$ garage status
==== HEALTHY NODES ====
ID                  Hostname  Address           Tag                   Zone  Capacity
563e1ac825ee3323…   Mercury   [fc00:1::1]:3901  NO ROLE ASSIGNED
```


## Connecting nodes together

When your Garage nodes first start, they will generate a local node identifier
(based on a public/private key pair).

To obtain the node identifier of a node, once it is generated,
run `garage node id`.
This will print keys as follows:

```bash
Mercury$ garage node id
563e1ac825ee3323aa441e72c26d1030d6d4414aeb3dd25287c531e7fc2bc95d@[fc00:1::1]:3901

Venus$ garage node id
86f0f26ae4afbd59aaf9cfb059eefac844951efd5b8caeec0d53f4ed6c85f332@[fc00:1::2]:3901

etc.
```

You can then instruct nodes to connect to one another as follows:

```bash
# Instruct Venus to connect to Mercury (this will establish communication both ways)
Venus$ garage node connect 563e1ac825ee3323aa441e72c26d1030d6d4414aeb3dd25287c531e7fc2bc95d@[fc00:1::1]:3901
```

You don't nead to instruct all node to connect to all other nodes:
nodes will discover one another transitively.

Now if your run `garage status` on any node, you should have an output that looks as follows:

```
==== HEALTHY NODES ====
ID                  Hostname  Address           Tag                   Zone  Capacity
563e1ac825ee3323…   Mercury   [fc00:1::1]:3901  NO ROLE ASSIGNED
86f0f26ae4afbd59…   Venus     [fc00:1::2]:3901  NO ROLE ASSIGNED
68143d720f20c89d…   Earth     [fc00:B::1]:3901  NO ROLE ASSIGNED
212f7572f0c89da9…   Mars      [fc00:F::1]:3901  NO ROLE ASSIGNED
```

## Creating a cluster layout

We will now inform Garage of the disk space available on each node of the cluster
as well as the zone (e.g. datacenter) in which each machine is located.
This information is called the **cluster layout** and consists
of a role that is assigned to each active cluster node.

For our example, we will suppose we have the following infrastructure
(Capacity, Identifier and Zone are specific values to Garage described in the following):

| Location | Name    | Disk Space | `Capacity` | `Identifier` | `Zone` |
|----------|---------|------------|------------|--------------|--------------|
| Paris    | Mercury | 1 TB       | `10`       | `563e`     | `par1`       |
| Paris    | Venus   | 2 TB       | `20`       | `86f0`     | `par1`       |
| London   | Earth   | 2 TB       | `20`       | `6814`     | `lon1`       |
| Brussels | Mars    | 1.5 TB     | `15`       | `212f`     | `bru1`       |

#### Node identifiers

After its first launch, Garage generates a random and unique identifier for each nodes, such as:

```
563e1ac825ee3323aa441e72c26d1030d6d4414aeb3dd25287c531e7fc2bc95d
```

Often a shorter form can be used, containing only the beginning of the identifier, like `563e`,
which identifies the server "Mercury" located in "Paris" according to our previous table.

The most simple way to match an identifier to a node is to run:

```
garage status
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

Capacity values must be **integers** but can be given any signification.
Here we chose that 1 unit of capacity = 100 GB.

Note that the amount of data stored by Garage on each server may not be strictly proportional to
its capacity value, as Garage will priorize having 3 copies of data in different zones,
even if this means that capacities will not be strictly respected. For example in our above examples,
nodes Earth and Mars will always store a copy of everything each, and the third copy will
have 66% chance of being stored by Venus and 33% chance of being stored by Mercury.

#### Injecting the topology

Given the information above, we will configure our cluster as follow:

```bash
garage layout assign 563e -z par1 -c 10 -t mercury
garage layout assign 86f0 -z par1 -c 20 -t venus
garage layout assign 6814 -z lon1 -c 20 -t earth 
garage layout assign 212f -z bru1 -c 15 -t mars 
```

At this point, the changes in the cluster layout have not yet been applied.
To show the new layout that will be applied, call:

```bash
garage layout show
```

Once you are satisfied with your new layout, apply it with:

```bash
garage layout apply
```

**WARNING:** if you want to use the layout modification commands in a script,
make sure to read [this page](@/documentation/reference-manual/layout.md) first.


## Using your Garage cluster

Creating buckets and managing keys is done using the `garage` CLI,
and is covered in the [quick start guide](@/documentation/quick-start/_index.md).
Remember also that the CLI is self-documented thanks to the `--help` flag and
the `help` subcommand (e.g. `garage help`, `garage key --help`).

Configuring S3-compatible applicatiosn to interact with Garage
is covered in the [Integrations](@/documentation/connect/_index.md) section.
