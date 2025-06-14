+++
title = "Migrating from 0.9 to 1.0"
weight = 71
+++

**This guide explains how to migrate to 1.0 if you have an existing 0.9 cluster.
We don't recommend trying to migrate to 1.0 directly from 0.8 or older.**

This migration procedure has been tested on several clusters without issues.
However, it is still a *critical procedure* that might cause issues.
**Make sure to back up all your data before attempting it!**

You might also want to read our [general documentation on upgrading Garage](@/documentation/operations/upgrading.md).

## Changes introduced in v1.0

The following are **breaking changes** in Garage v1.0 that require your attention when migrating:

- The Sled metadata db engine has been **removed**. If your cluster was still
  using Sled, you will need to **use a Garage v0.9.x binary** to convert the
  database using the `garage convert-db` subcommand. See
  [here](@/documentation/reference-manual/configuration.md#db_engine) for the
  details of the procedure.

The following syntax changes have been made to the configuration file:

- The `replication_mode` parameter has been split into two parameters:
  [`replication_factor`](@/documentation/reference-manual/configuration.md#replication_factor)
  and
  [`consistency_mode`](@/documentation/reference-manual/configuration.md#consistency_mode).
  The old syntax using `replication_mode` is still supported for legacy
  reasons and can still be used.

- The parameters `sled_cache_capacity` and `sled_flush_every_ms` have been removed.

## Migration procedure

The migration to Garage v1.0 can be done with almost no downtime,
by restarting all nodes at once in the new version.

The migration steps are as follows:

1. Do a `garage repair --all-nodes --yes tables`, check the logs and check that
   all data seems to be synced correctly between nodes. If you have time, do
   additional `garage repair` procedures (`blocks`, `versions`, `block_refs`,
   etc.)

2. Ensure you have a snapshot of your Garage installation that you can restore
   to in case the upgrade goes wrong:

   - If you are running Garage v0.9.4 or later, use the `garage meta snapshot
     --all` to make a backup snapshot of the metadata directories of your nodes
     for backup purposes, and save a copy of the following files in the
     metadata directories of your nodes: `cluster_layout`, `data_layout`,
     `node_key`, `node_key.pub`.

   - If you are running a filesystem such as ZFS or BTRFS that support
     snapshotting, you can create a filesystem-level snapshot to be used as a
     restoration point if needed.

   - In other cases, make a backup using the old procedure: turn off each node
     individually; back up its metadata folder (for instance, use the following
     command if your metadata directory is `/var/lib/garage/meta`: `cd
     /var/lib/garage ; tar -acf meta-v0.9.tar.zst meta/`); turn it back on
     again.  This will allow you to take a backup of all nodes without
     impacting global cluster availability.  You can do all nodes of a single
     zone at once as this does not impact the availability of Garage.

3. Prepare your updated binaries and configuration files for Garage v1.0

4. Shut down all v0.9 nodes simultaneously, and restart them all simultaneously
   in v1.0.  Use your favorite deployment tool (Ansible, Kubernetes, Nomad) to
   achieve this as fast as possible.  Garage v1.0 should be in a working state
   as soon as enough nodes have started.

5. Monitor your cluster in the following hours to see if it works well under
   your production load.
