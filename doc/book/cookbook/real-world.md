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

- Each machine has an IP address which makes it directly reachable by all other machines.
  In many cases, nodes will be behind a NAT and will not each have a public
  IPv4 addresses. In this case, is recommended that you use IPv6 for this
  end-to-end connectivity if it is available. Otherwise, using a mesh VPN such as
  [Nebula](https://github.com/slackhq/nebula) or
  [Yggdrasil](https://yggdrasil-network.github.io/) are approaches to consider
  in addition to building out your own VPN tunneling.

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
locations. This means that in the case of this small example, the usable capacity
of the cluster is in fact only 1.5 TB, because nodes in Brussels can't store more than that.
This also means that nodes in Paris and London will be under-utilized.
To make better use of the available hardware, you should ensure that the capacity
available in the different locations of your cluster is roughly the same.
For instance, here, the Mercury node could be moved to Brussels; this would allow the cluster
to store 2 TB of data in total.

### Best practices

- If you have reasonably fast networking between all your nodes, and are planing to store
  mostly large files, bump the `block_size` configuration parameter to 10 MB
  (`block_size = "10M"`).

- Garage stores its files in two locations: it uses a metadata directory to store frequently-accessed
  small metadata items, and a data directory to store data blocks of uploaded objects.
  Ideally, the metadata directory would be stored on an SSD (smaller but faster),
  and the data directory would be stored on an HDD (larger but slower).

- For the data directory, Garage already does checksumming and integrity verification,
  so there is no need to use a filesystem such as BTRFS or ZFS that does it.
  We recommend using XFS for the data partition, as it has the best performance.
  EXT4 is not recommended as it has more strict limitations on the number of inodes,
  which might cause issues with Garage when large numbers of objects are stored.

- Servers with multiple HDDs are supported natively by Garage without resorting
  to RAID, see [our dedicated documentation page](@/documentation/operations/multi-hdd.md).

- For the metadata storage, Garage does not do checksumming and integrity
  verification on its own, so it is better to use a robust filesystem such as
  BTRFS or ZFS. Users have reported that when using the LMDB database engine
  (the default), database files have a tendency of becoming corrupted after an
  unclean shutdown (e.g. a power outage), so you should take regular snapshots
  to be able to recover from such a situation.  This can be done using Garage's
  built-in automatic snapshotting (since v0.9.4), or by using filesystem level
  snapshots. If you cannot do so, you might want to switch to Sqlite which is
  more robust.

- LMDB is the fastest and most tested database engine, but it has the following
  weaknesses: 1/ data files are not architecture-independent, you cannot simply
  move a Garage metadata directory between nodes running different architectures,
  and 2/ LMDB is not suited for 32-bit platforms. Sqlite is a viable alternative
  if any of these are of concern.

- If you only have an HDD and no SSD, it's fine to put your metadata alongside
  the data on the same drive, but then consider your filesystem choice wisely
  (see above). Having lots of RAM for your kernel to cache the metadata will
  help a lot with performance.  The default LMDB database engine is the most
  tested and has good performance.

## Get a Docker image

Our docker image is currently named `dxflrs/garage` and is stored on the [Docker Hub](https://hub.docker.com/r/dxflrs/garage/tags?page=1&ordering=last_updated).
We encourage you to use a fixed tag (eg. `v1.2.0`) and not the `latest` tag.
For this example, we will use the latest published version at the time of the writing which is `v1.2.0` but it's up to you
to check [the most recent versions on the Docker Hub](https://hub.docker.com/r/dxflrs/garage/tags?page=1&ordering=last_updated).

For example:

```
sudo docker pull dxflrs/garage:v1.2.0
```

## Deploying and configuring Garage

On each machine, we will have a similar setup,
especially you must consider the following folders/files:

- `/etc/garage.toml`: Garage daemon's configuration (see below)

- `/var/lib/garage/meta/`: Folder containing Garage's metadata,
  put this folder on a SSD if possible

- `/var/lib/garage/data/`: Folder containing Garage's data,
  this folder will be your main data storage and must be on a large storage (e.g. large HDD)


A valid `/etc/garage.toml` for our cluster would look as follows:

```toml
metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"
db_engine = "lmdb"
metadata_auto_snapshot_interval = "6h"

replication_factor = 3

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
  Alternatively, you can also set `rpc_public_addr_subnet`, which can filter
  the addresses announced to other peers to a specific subnet.

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
  dxflrs/garage:v1.2.0
```

With this command line, Garage should be started automatically at each boot.
Please note that we use host networking as otherwise the network indirection
added by Docker would prevent Garage nodes from communicating with one another
(especially if using IPv6).

If you want to use `docker-compose`, you may use the following `docker-compose.yml` file as a reference:

```yaml
version: "3"
services:
  garage:
    image: dxflrs/garage:v1.2.0
    network_mode: "host"
    restart: unless-stopped
    volumes:
      - /etc/garage.toml:/etc/garage.toml
      - /var/lib/garage/meta:/var/lib/garage/meta
      - /var/lib/garage/data:/var/lib/garage/data
```

If you wish to upgrade your cluster, make sure to read the corresponding
[documentation page](@/documentation/operations/upgrading.md) first, as well as
the documentation relevant to your version of Garage in the case of major
upgrades.  With the containerized setup proposed here, the upgrade process
will require stopping and removing the existing container, and re-creating it
with the upgraded version.

## Controlling the daemon

The `garage` binary has two purposes:
  - it acts as a daemon when launched with `garage server`
  - it acts as a control tool for the daemon when launched with any other command

Ensure an appropriate `garage` binary (the same version as your Docker image) is available in your path.
If your configuration file is at `/etc/garage.toml`, the `garage` binary should work with no further change.

You can also use an alias as follows to use the Garage binary inside your docker container:

```bash
alias garage="docker exec -ti <container name> /garage"
```

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

You don't need to instruct all node to connect to all other nodes:
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

| Location | Name    | Disk Space | Identifier | Zone (`-z`) | Capacity (`-c`) |
|----------|---------|------------|------------|-------------|-----------------|
| Paris    | Mercury | 1 TB       | `563e`     | `par1`      | `1T`            |
| Paris    | Venus   | 2 TB       | `86f0`     | `par1`      | `2T`            |
| London   | Earth   | 2 TB       | `6814`     | `lon1`      | `2T`            |
| Brussels | Mars    | 1.5 TB     | `212f`     | `bru1`      | `1.5T`          |

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

We will now use the `garage layout assign` command to configure the correct parameters for each node.

#### Zones

Zones are simply a user-chosen identifier that identify a group of server that are grouped together logically.
It is up to the system administrator deploying Garage to identify what does "grouped together" means.

In most cases, a zone will correspond to a geographical location (i.e. a datacenter).
Behind the scene, Garage will use zone definition to try to store the same data on different zones,
in order to provide high availability despite failure of a zone.

Zones are passed to Garage using the `-z` flag of `garage layout assign` (see below).

#### Capacity

Garage needs to know the storage capacity (disk space) it can/should use on
each node, to be able to correctly balance data.

Capacity values are expressed in bytes and are passed to Garage using the `-c` flag of `garage layout assign` (see below).

#### Tags

You can add additional tags to nodes using the `-t` flag of `garage layout assign` (see below).
Tags have no specific meaning for Garage and can be used at your convenience.

#### Injecting the topology

Given the information above, we will configure our cluster as follow:

```bash
garage layout assign 563e -z par1 -c 1T -t mercury
garage layout assign 86f0 -z par1 -c 2T -t venus
garage layout assign 6814 -z lon1 -c 2T -t earth
garage layout assign 212f -z bru1 -c 1.5T -t mars
```

At this point, the changes in the cluster layout have not yet been applied.
To show the new layout that will be applied, call:

```bash
garage layout show
```

Make sure to read carefully the output of `garage layout show`.
Once you are satisfied with your new layout, apply it with:

```bash
garage layout apply
```

**WARNING:** if you want to use the layout modification commands in a script,
make sure to read [this page](@/documentation/operations/layout.md) first.


## Using your Garage cluster

Creating buckets and managing keys is done using the `garage` CLI,
and is covered in the [quick start guide](@/documentation/quick-start/_index.md).
Remember also that the CLI is self-documented thanks to the `--help` flag and
the `help` subcommand (e.g. `garage help`, `garage key --help`).

Configuring S3-compatible applications to interact with Garage
is covered in the [Integrations](@/documentation/connect/_index.md) section.
