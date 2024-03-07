+++
title = "Cluster layout management"
weight = 20
+++

The cluster layout in Garage is a table that assigns to each node a role in
the cluster. The role of a node in Garage can either be a storage node with
a certain capacity, or a gateway node that does not store data and is only
used as an API entry point for faster cluster access.
An introduction to building cluster layouts can be found in the [production deployment](@/documentation/cookbook/real-world.md) page.

In Garage, all of the data that can be stored in a given cluster is divided
into slices which we call *partitions*. Each partition is stored by
one or several nodes in the cluster
(see [`replication_factor`](@/documentation/reference-manual/configuration.md#replication_factor)).
The layout determines the correspondence between these partitions,
which exist on a logical level, and actual storage nodes.

## How cluster layouts work in Garage

A cluster layout is composed of the following components:

- a table of roles assigned to nodes, defined by the user
- an optimal assignation of partitions to nodes, computed by an algorithm that is ran once when calling `garage layout apply` or the ApplyClusterLayout API endpoint
- a version number

Garage nodes will always use the cluster layout with the highest version number.

Garage nodes also maintain and synchronize between them a set of proposed role
changes that haven't yet been applied. These changes will be applied (or
canceled) in the next version of the layout.

All operations on the layout can be realized using the `garage` CLI or using the
[administration API endpoint](@/documentation/reference-manual/admin-api.md).
We give here a description of CLI commands, the admin API semantics are very similar.

The following commands insert modifications to the set of proposed role changes
for the next layout version (but they do not create the new layout immediately):

```bash
garage layout assign [...]
garage layout remove [...]
```

The following command can be used to inspect the layout that is currently set in the cluster
and the changes proposed for the next layout version, if any:

```bash
garage layout show
```

The following commands create a new layout with the specified version number,
that either takes into account the proposed changes or cancels them:

```bash
garage layout apply --version <new_version_number>
garage layout revert --version <new_version_number>
```

The version number of the new layout to create must be 1 + the version number
of the previous layout that existed in the cluster.  The `apply` and `revert`
commands will fail otherwise.

## Warnings about Garage cluster layout management

**⚠️ Never make several calls to `garage layout apply` or `garage layout
revert` with the same value of the `--version` flag. Doing so can lead to the
creation of several different layouts with the same version number, in which
case your Garage cluster will become inconsistent until fixed.** If a call to
`garage layout apply` or `garage layout revert` has failed and `garage layout
show` indicates that a new layout with the given version number has not been
set in the cluster, then it is fine to call the command again with the same
version number.

If you are using the `garage` CLI by typing individual commands in your
shell, you shouldn't have much issues as long as you run commands one after
the other and take care of checking the output of `garage layout show`
before applying any changes.

If you are using the `garage` CLI or the admin API to script layout changes,
follow the following recommendations:

- If using the CLI, make all of your `garage` CLI calls to the same RPC host.
  If using the admin API, make all of your API calls to the same Garage node. Do
  not connect to individual nodes to send them each a piece of the layout changes
  you are making, as the changes propagate asynchronously between nodes and might
  not all be taken into account at the time when the new layout is applied.

- **Only call `garage layout apply`/ApplyClusterLayout once**, and call it
  **strictly after** all of the `layout assign` and `layout remove`
  commands/UpdateClusterLayout API calls have returned.


## Understanding unexpected layout calculations

When adding, removing or modifying nodes in a cluster layout, sometimes
unexpected assignations of partitions to node can occur. These assignations
are in fact normal and logical, given the objectives of the algorithm.  Indeed,
**the layout algorithm prioritizes moving less data between nodes over
achieving equal distribution of load. It also tries to use all links between
pairs of nodes in equal proportions when moving data.**  This section presents
two examples and illustrates how one can control Garage's behavior to obtain
the desired results.

### Example 1

In this example, a cluster is originally composed of 3 nodes in 3 different
zones (data centers).  The three nodes are of equal capacity, therefore they
are all fully exploited and all store a copy of all of the data in the cluster.

Then, a fourth node of the same size is added in the datacenter `dc1`.
As illustrated by the following, **Garage will by default not store any data on the new node**:

```
$ garage layout show
==== CURRENT CLUSTER LAYOUT ====
ID                Tags   Zone  Capacity   Usable capacity
b10c110e4e854e5a  node1  dc1   1000.0 MB  1000.0 MB (100.0%)
a235ac7695e0c54d  node2  dc2   1000.0 MB  1000.0 MB (100.0%)
62b218d848e86a64  node3  dc3   1000.0 MB  1000.0 MB (100.0%)

Zone redundancy: maximum

Current cluster layout version: 6

==== STAGED ROLE CHANGES ====
ID                Tags   Zone  Capacity
a11c7cf18af29737  node4  dc1   1000.0 MB


==== NEW CLUSTER LAYOUT AFTER APPLYING CHANGES ====
ID                Tags   Zone  Capacity   Usable capacity
b10c110e4e854e5a  node1  dc1   1000.0 MB  1000.0 MB (100.0%)
a11c7cf18af29737  node4  dc1   1000.0 MB  0 B (0.0%)
a235ac7695e0c54d  node2  dc2   1000.0 MB  1000.0 MB (100.0%)
62b218d848e86a64  node3  dc3   1000.0 MB  1000.0 MB (100.0%)

Zone redundancy: maximum

==== COMPUTATION OF A NEW PARTITION ASSIGNATION ====

Partitions are replicated 3 times on at least 3 distinct zones.

Optimal partition size:                     3.9 MB (3.9 MB in previous layout)
Usable capacity / total cluster capacity:   3.0 GB / 4.0 GB (75.0 %)
Effective capacity (replication factor 3):  1000.0 MB

A total of 0 new copies of partitions need to be transferred.

dc1                 Tags   Partitions        Capacity   Usable capacity
  b10c110e4e854e5a  node1  256 (0 new)       1000.0 MB  1000.0 MB (100.0%)
  a11c7cf18af29737  node4  0 (0 new)         1000.0 MB  0 B (0.0%)
  TOTAL                    256 (256 unique)  2.0 GB     1000.0 MB (50.0%)

dc2                 Tags   Partitions        Capacity   Usable capacity
  a235ac7695e0c54d  node2  256 (0 new)       1000.0 MB  1000.0 MB (100.0%)
  TOTAL                    256 (256 unique)  1000.0 MB  1000.0 MB (100.0%)

dc3                 Tags   Partitions        Capacity   Usable capacity
  62b218d848e86a64  node3  256 (0 new)       1000.0 MB  1000.0 MB (100.0%)
  TOTAL                    256 (256 unique)  1000.0 MB  1000.0 MB (100.0%)
```

While unexpected, this is logical because of the following facts:

- storing some data on the new node does not help increase the total quantity
  of data that can be stored on the cluster, as the two other zones (`dc2` and
  `dc3`) still need to store a full copy of everything, and their capacity is
  still the same;

- there is therefore no need to move any data on the new node as this would be pointless;

- moving data to the new node has a cost which the algorithm decides to not pay if not necessary.

This distribution of data can however not be what the administrator wanted: if
they added a new node to `dc1`, it might be because the existing node is too
slow, and they wish to divide its load by half. In that case, what they need to
do to force Garage to distribute the data between the two nodes is to attribute
only half of the capacity to each node in `dc1` (in our example, 500M instead of 1G).
In that case, Garage would determine that to be able to store 1G in total, it
would need to store 500M on the old node and 500M on the added one.


### Example 2

The following example is a slightly different scenario, where `dc1` had two
nodes that were used at 50%, and `dc2` and `dc3` each have one node that is
100% used. All node capacities are the same.

Then, a node from `dc1` is moved into `dc3`. One could expect that the roles of
`dc1` and `dc3` would simply be swapped: the remaining node in `dc1` would be
used at 100%, and the two nodes now in `dc3` would be used at 50%. Instead,
this happens:

```
==== CURRENT CLUSTER LAYOUT ====
ID                Tags   Zone  Capacity   Usable capacity
b10c110e4e854e5a  node1  dc1   1000.0 MB  500.0 MB (50.0%)
a11c7cf18af29737  node4  dc1   1000.0 MB  500.0 MB (50.0%)
a235ac7695e0c54d  node2  dc2   1000.0 MB  1000.0 MB (100.0%)
62b218d848e86a64  node3  dc3   1000.0 MB  1000.0 MB (100.0%)

Zone redundancy: maximum

Current cluster layout version: 8

==== STAGED ROLE CHANGES ====
ID                Tags   Zone  Capacity
a11c7cf18af29737  node4  dc3   1000.0 MB


==== NEW CLUSTER LAYOUT AFTER APPLYING CHANGES ====
ID                Tags   Zone  Capacity   Usable capacity
b10c110e4e854e5a  node1  dc1   1000.0 MB  1000.0 MB (100.0%)
a235ac7695e0c54d  node2  dc2   1000.0 MB  1000.0 MB (100.0%)
62b218d848e86a64  node3  dc3   1000.0 MB  753.9 MB (75.4%)
a11c7cf18af29737  node4  dc3   1000.0 MB  246.1 MB (24.6%)

Zone redundancy: maximum

==== COMPUTATION OF A NEW PARTITION ASSIGNATION ====

Partitions are replicated 3 times on at least 3 distinct zones.

Optimal partition size:                     3.9 MB (3.9 MB in previous layout)
Usable capacity / total cluster capacity:   3.0 GB / 4.0 GB (75.0 %)
Effective capacity (replication factor 3):  1000.0 MB

A total of 128 new copies of partitions need to be transferred.

dc1                 Tags   Partitions        Capacity   Usable capacity
  b10c110e4e854e5a  node1  256 (128 new)     1000.0 MB  1000.0 MB (100.0%)
  TOTAL                    256 (256 unique)  1000.0 MB  1000.0 MB (100.0%)

dc2                 Tags   Partitions        Capacity   Usable capacity
  a235ac7695e0c54d  node2  256 (0 new)       1000.0 MB  1000.0 MB (100.0%)
  TOTAL                    256 (256 unique)  1000.0 MB  1000.0 MB (100.0%)

dc3                 Tags   Partitions        Capacity   Usable capacity
  62b218d848e86a64  node3  193 (0 new)       1000.0 MB  753.9 MB (75.4%)
  a11c7cf18af29737  node4  63 (0 new)        1000.0 MB  246.1 MB (24.6%)
  TOTAL                    256 (256 unique)  2.0 GB     1000.0 MB (50.0%)
```

As we can see, the node that was moved to `dc3` (node4) is only used at 25% (approximatively),
whereas the node that was already in `dc3` (node3) is used at 75%.

This can be explained by the following:

- node1 will now be the only node remaining in `dc1`, thus it has to store all
  of the data in the cluster. Since it was storing only half of it before, it has
  to retrieve the other half from other nodes in the cluster.

- The data which it does not have is entirely stored by the other node that was
  in `dc1` and that is now in `dc3` (node4). There is also a copy of it on node2
  and node3 since both these nodes have a copy of everything.

- node3 and node4 are the two nodes that will now be in a datacenter that is
 under-utilized (`dc3`), this means that those are the two candidates from which
 data can be removed to be moved to node1.

- Garage will move data in equal proportions from all possible sources, in this
  case it means that it will tranfer 25% of the entire data set from node3 to
  node1 and another 25% from node4 to node1.

This explains why node3 ends with 75% utilization (100% from before minus 25%
that is moved to node1), and node4 ends with 25% (50% from before minus 25%
that is moved to node1).

This illustrates the second principle of the layout computation: **if there is
a choice in moving data out of some nodes, then all links between pairs of
nodes are used in equal proportions** (this is approximately true, there is
randomness in the algorithm to achieve this so there might be some small
fluctuations, as we see above).
