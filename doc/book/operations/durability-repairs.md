+++
title = "Durability & Repairs"
weight = 30
+++

To ensure the best durability of your data and to fix any inconsistencies that may
pop up in a distributed system, Garage provides a series of repair operations.
This guide will explain the meaning of each of them and when they should be applied.


# General syntax of repair operations

Repair operations described below are of the form `garage repair <repair_name>`.
These repairs will not launch without the `--yes` flag, which should
be added as follows: `garage repair --yes <repair_name>`.
By default these repair procedures will only run on the Garage node your CLI is
connecting to. To run on all nodes, add the `-a` flag as follows:
`garage repair -a --yes <repair_name>`.

# Data block operations

## Data store scrub {#scrub}

Scrubbing the data store means examining each individual data block to check that
their content is correct, by verifying their hash. Any block found to be corrupted
(e.g. by bitrot or by an accidental manipulation of the datastore) will be
restored from another node that holds a valid copy.

Scrubs are automatically scheduled by Garage to run every 25-35 days (the
actual time is randomized to spread load across nodes). The next scheduled run
can be viewed with `garage worker get`.

A scrub can also be launched manually using `garage repair scrub start`.

To view the status of an ongoing scrub, first find the task ID of the scrub worker
using `garage worker list`. Then, run `garage worker info <scrub_task_id>` to
view detailed runtime statistics of the scrub. To gather cluster-wide information,
this command has to be run on each individual node.

A scrub is a very disk-intensive operation that might slow down your cluster.
You may pause an ongoing scrub using `garage repair scrub pause`, but note that
the scrub will resume automatically 24 hours later as Garage will not let your
cluster run without a regular scrub. If the scrub procedure is too intensive
for your servers and is slowing down your workload, the recommended solution
is to increase the "scrub tranquility" using `garage repair scrub set-tranquility`.
A higher tranquility value will make Garage take longer pauses between two block
verifications. Of course, scrubbing the entire data store will also take longer.

## Block check and resync

In some cases, nodes hold a reference to a block but do not actually have the block
stored on disk. Conversely, they may also have on-disk blocks that are not referenced
any more. To fix both cases, a block repair may be run with `garage repair blocks`.
This will scan the entire block reference counter table to check that the blocks
exist on disk, and will scan the entire disk store to check that stored blocks
are referenced.

It is recommended to run this procedure when changing your cluster layout,
after the metadata tables have finished synchronizing between nodes
(usually a few hours after `garage layout apply`).

## Inspecting lost blocks

In extremely rare situations, data blocks may be unavailable from the entire cluster.
This means that even using `garage repair blocks`, some nodes may be unable
to fetch data blocks for which they hold a reference.

These errors are stored on each node in a list of "block resync errors", i.e.
blocks for which the last resync operation failed.
This list can be inspected using `garage block list-errors`.
These errors usually fall into one of the following categories:

1. a block is still referenced but the object was deleted, this is a case
   of metadata reference inconsistency (see below for the fix)
2. a block is referenced by a non-deleted object, but could not be fetched due
   to a transient error such as a network failure
3. a block is referenced by a non-deleted object, but could not be fetched due
   to a permanent error such as there not being any valid copy of the block on the
   entire cluster

To help make the difference between cases 1 and cases 2 and 3, you may use the
`garage block info` command to see which objects hold a reference to each block.

In the second case (transient errors), Garage will try to fetch the block again
after a certain time, so the error should disappear naturally. You can also
request Garage to try to fetch the block immediately using `garage block retry-now`
if you have fixed the transient issue.

If you are confident that you are in the third scenario and that your data block
is definitely lost, then there is no other choice than to declare your S3 objects
as unrecoverable, and to delete them properly from the data store. This can be done
using the `garage block purge` command.

## Rebalancing data directories

In [multi-HDD setups](@/documentation/operations/multi-hdd.md), to ensure that
data blocks are well balanced between storage locations, you may run a
rebalance operation using `garage repair rebalance`. This is useful when
adding storage locations or when capacities of the storage locations have been
changed.  Once this is finished, Garage will know for each block of a single
possible location where it can be, which can increase access speed.  This
operation will also move out all data from locations marked as read-only.


# Metadata operations

## Metadata snapshotting

It is good practice to setup automatic snapshotting of your metadata database
file, to recover from situations where it becomes corrupted on disk. This can
be done at the filesystem level if you are using ZFS or BTRFS.

Since Garage v0.9.4, Garage is able to take snapshots of the metadata database
itself. This basically amounts to copying the database file, except that it can
be run live while Garage is running without the risk of corruption or
inconsistencies.  This can be setup to run automatically on a schedule using
[`metadata_auto_snapshot_interval`](@/documentation/reference-manual/configuration.md#metadata_auto_snapshot_interval).
A snapshot can also be triggered manually using the `garage meta snapshot`
command. Note that taking a snapshot using this method is very intensive as it
requires making a full copy of the database file, so you might prefer using
filesystem-level snapshots if possible. To recover a corrupted node from such a
snapshot, read the instructions
[here](@/documentation/operations/recovering.md#corrupted_meta).

## Metadata table resync

Garage automatically resyncs all entries stored in the metadata tables every hour,
to ensure that all nodes have the most up-to-date version of all the information
they should be holding.
The resync procedure is based on a Merkle tree that allows to efficiently find
differences between nodes.

In some special cases, e.g. before an upgrade, you might want to run a table
resync manually. This can be done using `garage repair tables`.

## Metadata table reference fixes

In some very rare cases where nodes are unavailable, some references between objects
are broken. For instance, if an object is deleted, the underlying versions or data
blocks may still be held by Garage. If you suspect that such corruption has occurred
in your cluster, you can run one of the following repair procedures:

- `garage repair versions`: checks that all versions belong to a non-deleted object, and purges any orphan version

- `garage repair block-refs`: checks that all block references belong to a non-deleted object version, and purges any orphan block reference (this will then allow the blocks to be garbage-collected)

- `garage repair block-rc`: checks that the reference counters for blocks are in sync with the actual number of non-deleted entries in the block reference table
