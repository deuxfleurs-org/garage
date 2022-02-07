+++
title = "Internals"
weight = 20
+++

## Overview

TODO: write this section

- The Dynamo ring (see [this paper](https://dl.acm.org/doi/abs/10.1145/1323293.1294281) and [that paper](https://www.usenix.org/conference/nsdi16/technical-sessions/presentation/eisenbud))

- CRDTs (see [this paper](https://link.springer.com/chapter/10.1007/978-3-642-24550-3_29))

- Consistency model of Garage tables

In the meantime, you can find some information at the following links:

- [this presentation (in French)](https://git.deuxfleurs.fr/Deuxfleurs/garage/src/branch/main/doc/talks/2020-12-02_wide-team/talk.pdf)

- [an old design draft](@/documentation/working-documents/design-draft.md)


## Garbage collection

A faulty garbage collection procedure has been the cause of
[critical bug #39](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/39).
This precise bug was fixed in the code, however there are potentially more
general issues with the garbage collector being too eager and deleting things
too early. This has been the subject of
[PR #135](https://git.deuxfleurs.fr/Deuxfleurs/garage/pulls/135).
This section summarizes the discussions on this topic.

Rationale: we want to ensure Garage's safety by making sure things don't get
deleted from disk if they are still needed. Two aspects are involved in this.

### 1. Garbage collection of table entries (in `meta/` directory)

The `Entry` trait used for table entries (defined in `tables/schema.rs`)
defines a function `is_tombstone()` that returns `true` if that entry
represents an entry that is deleted in the table. CRDT semantics by default
keep all tombstones, because they are necessary for reconciliation: if node A
has a tombstone that supersedes a value `x`, and node B has value `x`, A has to
keep the tombstone in memory so that the value `x` can be properly deleted at
node `B`. Otherwise, due to the CRDT reconciliation rule, the value `x` from B
would flow back to A and a deleted item would reappear in the system.

Here, we have some control on the nodes involved in storing Garage data.
Therefore we have a garbage collector that is able to delete tombstones UNDER
CERTAIN CONDITIONS. This garbage collector is implemented in `table/gc.rs`. To
delete a tombstone, the following condition has to be met:

- All nodes responsible for storing this entry are aware of the existence of
  the tombstone, i.e. they cannot hold another version of the entry that is
  superseeded by the tombstone. This ensures that deleting the tombstone is
  safe and that no deleted value will come back in the system.

Garage makes use of Sled's atomic operations (such as compare-and-swap and
transactions) to ensure that only tombstones that have been correctly
propagated to other nodes are ever deleted from the local entry tree.

This GC is safe in the following sense: no non-tombstone data is ever deleted
from Garage tables.

**However**, there is an issue with the way this interacts with data
rebalancing in the case when a partition is moving between nodes. If a node has
some data of a partition for which it is not responsible, it has to offload it.
However that offload process takes some time. In that interval, the GC does not
check with that node if it has the tombstone before deleting the tombstone, so
perhaps it doesn't have it and when the offload finally happens, old data comes
back in the system.

**PR 135 mostly fixes this** by implementing a 24-hour delay before anything is
garbage collected in a table. This works under the assumption that rebalances
that follow data shuffling terminate in less than 24 hours.

**However**, in distributed systems, it is generally considered a bad practice
to make assumptions that information propagates in a certain time interval:
this consists in making a synchrony assumption, meaning that we are basically
assuming a computing model that has much stronger properties than otherwise. To
maximize the applicability of Garage, we would like to remove this assumption,
and implement a system where time does not play a role. To do this, we would
need to find a way to safely disable the GC when data is being shuffled around,
and safely detect that the shuffling has terminated and thus the GC can be
resumed. This introduces some complexity to the protocol and hasn't been
tackled yet.

### 2. Garbage collection of data blocks (in `data/` directory)

Blocks in the data directory are reference-counted. In Garage versions before
PR #135, blocks could get deleted from local disk as soon as their reference
counter reached zero. We had a mechanism to not trigger this immediately at the
rc-reaches-zero event, but the cleanup could be triggered by other means (for
example by a block repair operation...). PR #135 added a safety measure so that
blocks never get deleted in a 10 minute interval following the time when the RC
reaches zero. This is a measure to make impossible race conditions such as #39.
We would have liked to use a larger delay (e.g. 24 hours), but in the case of a
rebalance of data, this would have led to the disk utilization to explode
during the rebalancing, only to shrink again after 24 hours. The 10-minute
delay is a compromise that gives good security while not having this problem of
disk space explosion on rebalance.

