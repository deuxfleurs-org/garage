# Configure a cluster

*We use a command named `garagectl` which is in fact an alias you must define as explained in the [Control the daemon](./daemon.md) section.*

In this section, we will inform garage of the disk space available on each node of the cluster
as well as the site (think datacenter) of each machine.

## Test cluster

As this part is not relevant for a test cluster, you can use this one-liner to create a basic topology:

```bash
garagectl status | grep UNCONFIGURED | grep -Po '^[0-9a-f]+' | while read id; do 
    garagectl node configure -d dc1 -n 10 $id
done
```

## Real-world cluster

For our example, we will suppose we have the following infrastructure (Tokens, Identifier and Datacenter are specific values to garage described in the following):

| Location | Name    | Disk Space | `Tokens` | `Identifier` | `Datacenter` |
|----------|---------|------------|----------|--------------|--------------|
| Paris    | Mercury | 1 To       | `100`    | `8781c5`     | `par1`       |
| Paris    | Venus   | 2 To       | `200`    | `2a638e`     | `par1`       |
| London   | Earth   | 2 To       | `200`    | `68143d`     | `lon1`       |
| Brussels | Mars    | 1.5 To     | `150`    | `212f75`     | `bru1`       |

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

### Tokens

Garage reasons on an arbitrary metric about disk storage that is named "tokens".
The number of tokens must be proportional to the disk space dedicated to the node.
Additionaly, ideally the number of tokens must be in the order of magnitude of 100
to provide a good trade-off between data load balancing and performances (*this sentence must be verified, it may be wrong*).

Here we chose 1 token = 10 Go but you are free to select the value that best fit your needs.

### Datacenter

Datacenter are simply a user-chosen identifier that identify a group of server that are located in the same place.
It is up to the system administrator deploying garage to identify what does "the same place" means.
Behind the scene, garage will try to store the same data on different sites to provide high availability despite a data center failure.

### Inject the topology

Given the information above, we will configure our cluster as follow:

```
garagectl node configure --datacenter par1 -n 100 -t mercury 8781c5
garagectl node configure --datacenter par1 -n 200 -t venus 2a638e
garagectl node configure --datacenter lon1 -n 200 -t earth 68143d
garagectl node configure --datacenter bru1 -n 150 -t mars 212f75
```
