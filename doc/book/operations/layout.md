+++
title = "Cluster layout management"
weight = 20
+++

The cluster layout in Garage is a table that assigns to each node a role in
the cluster. The role of a node in Garage can either be a storage node with
a certain capacity, or a gateway node that does not store data and is only
used as an API entry point for faster cluster access.
An introduction to building cluster layouts can be found in the [production deployment](@/documentation/cookbook/real-world.md) page.

## How cluster layouts work in Garage

In Garage, a cluster layout is composed of the following components:

- a table of roles assigned to nodes
- a version number

Garage nodes will always use the cluster layout with the highest version number.

Garage nodes also maintain and synchronize between them a set of proposed role
changes that haven't yet been applied. These changes will be applied (or
canceled) in the next version of the layout

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

**Warning: never make several calls to `garage layout apply` or `garage layout
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

If you are using the `garage` CLI to script layout changes, follow the following recommendations:

- Make all of your `garage` CLI calls to the same RPC host. Do not use the
  `garage` CLI to connect to individual nodes to send them each a piece of the
  layout changes you are making, as the changes propagate asynchronously
  between nodes and might not all be taken into account at the time when the
  new layout is applied.

- **Only call `garage layout apply` once**, and call it **strictly after** all
  of the `layout assign` and `layout remove` commands have returned.


## Understanding unexpected layout calculations


### Example 1

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

### Example 2

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
