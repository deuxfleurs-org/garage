+++
title = "Cluster layout management"
weight = 10
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
