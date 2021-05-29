# Configure a cluster

*We use a command named `garagectl` which is in fact an alias you must define as explained in the [Control the daemon](./daemon.md) section.*

In this section, we will inform garage of the disk space available on each node of the cluster
as well as the site (think datacenter) of each machine.

## Test cluster

As this part is not relevant for a test cluster, you can use this three-liner to create a basic topology:

```bash
garagectl status | grep UNCONFIGURED | grep -Po '^[0-9a-f]+' | while read id; do 
    garagectl node configure -z dc1 -c 1 $id
done
```

## Real-world cluster

For our example, we will suppose we have the following infrastructure (Capacity, Identifier and Datacenter are specific values to garage described in the following):

| Location | Name    | Disk Space | `Capacity` | `Identifier` | `Zone` |
|----------|---------|------------|------------|--------------|--------------|
| Paris    | Mercury | 1 To       | `2`        | `8781c5`     | `par1`       |
| Paris    | Venus   | 2 To       | `4`        | `2a638e`     | `par1`       |
| London   | Earth   | 2 To       | `4`        | `68143d`     | `lon1`       |
| Brussels | Mars    | 1.5 To     | `3`        | `212f75`     | `bru1`       |

### Identifier

After its first launch, garage generates a random and unique identifier for each nodes, such as:

```
8781c50c410a41b363167e9d49cc468b6b9e4449b6577b64f15a249a149bdcbc
```

Often a shorter form can be used, containing only the beginning of the identifier, like `8781c5`,
which identifies the server "Mercury" located in "Paris" according to our previous table.

The most simple way to match an identifier to a node is to run:

```
garagectl status
```

It will display the IP address associated with each node; from the IP address you will be able to recognize the node.

### Zones

Zones are simply a user-chosen identifier that identify a group of server that are grouped together logically.
It is up to the system administrator deploying garage to identify what does "grouped together" means.

In most cases, a zone will correspond to a geographical location (i.e. a datacenter).
Behind the scene, Garage will use zone definition to try to store the same data on different zones,
in order to provide high availability despite failure of a zone.

### Capacity

Garage reasons on an arbitrary metric about disk storage that is named the *capacity* of a node.
The capacity configured in Garage must be proportional to the disk space dedicated to the node.
Additionaly, the capacity values used in Garage should be as small as possible, with
1 ideally representing the size of your smallest server.

Here we chose that 1 unit of capacity = 0.5 To, so that we can express servers of size
1 To and 2 To, as wel as the intermediate size 1.5 To.

Note that the amount of data stored by Garage on each server may not be strictly proportional to
its capacity value, as Garage will priorize having 3 copies of data in different zones,
even if this means that capacities will not be strictly respected. For example in our above examples,
nodes Earth and Mars will always store a copy of everything each, and the third copy will
have 66% chance of being stored by Venus and 33% chance of being stored by Mercury.

### Inject the topology

Given the information above, we will configure our cluster as follow:

```
garagectl node configure -z par1 -c 2 -t mercury 8781c5
garagectl node configure -z par1 -c 4 -t venus 2a638e
garagectl node configure -z lon1 -c 4 -t earth 68143d
garagectl node configure -z bru1 -c 3 -t mars 212f75
```
