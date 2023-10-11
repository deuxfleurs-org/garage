+++
title = "Migrating from 0.8 to 0.9"
weight = 12
+++

**This guide explains how to migrate to 0.9 if you have an existing 0.8 cluster.
We don't recommend trying to migrate to 0.9 directly from 0.7 or older.**

This migration procedure has been tested on several clusters without issues.
However, it is still a *critical procedure* that might cause issues.
**Make sure to back up all your data before attempting it!**

You might also want to read our [general documentation on upgrading Garage](@/documentation/operations/upgrading.md).

The following are **breaking changes** in Garage v0.9 that require your attention when migrating:

- LMDB is now the default metadata db engine and Sled is deprecated. If you were using Sled, make sure to specify `db_engine = "sled"` in your configuration file, or take the time to [convert your database](https://garagehq.deuxfleurs.fr/documentation/reference-manual/configuration/#db-engine-since-v0-8-0).

- Capacity values are now in actual byte units. The translation from the old layout will assign 1 capacity = 1Gb by default, which might be wrong for your cluster. This does not cause any data to be moved around, but you might want to re-assign correct capacity values post-migration.

- Multipart uploads that were started in Garage v0.8 will not be visible in Garage v0.9 and will have to be restarted from scratch.

- Changes to the admin API: some `v0/` endpoints have been replaced by `v1/` counterparts with updated/uniformized syntax. All other endpoints have also moved to `v1/` by default, without syntax changes, but are still available under `v0/` for compatibility.


## Simple migration procedure (takes cluster offline for a while)

The migration steps are as follows:

1. Disable API and web access. You may do this by stopping your reverse proxy or by commenting out
   the `api_bind_addr` values in your `config.toml` file and restarting Garage.
2. Do `garage repair --all-nodes --yes tables` and `garage repair --all-nodes --yes blocks`,
   check the logs and check that all data seems to be synced correctly between
   nodes. If you have time, do additional checks (`versions`, `block_refs`, etc.)
3. Check that the block resync queue and Merkle queue are empty:
   run `garage stats -a` to query them or inspect metrics in the Grafana dashboard.
4. Turn off Garage v0.8
5. **Backup the metadata folder of all your nodes!** For instance, use the following command
	if your metadata directory is `/var/lib/garage/meta`: `cd /var/lib/garage ; tar -acf meta-v0.8.tar.zst meta/`
6. Install Garage v0.9
7. Update your configuration file if necessary.
8. Turn on Garage v0.9
9. Do `garage repair --all-nodes --yes tables` and `garage repair --all-nodes --yes blocks`.
   Wait for a full table sync to run.
10. Your upgraded cluster should be in a working state. Re-enable API and Web
    access and check that everything went well.
11. Monitor your cluster in the next hours to see if it works well under your production load, report any issue.
12. You might want to assign correct capacity values to all your nodes. Doing so might cause data to be moved
    in your cluster, which should also be monitored carefully.

## Minimal downtime migration procedure

The migration to Garage v0.9 can be done with almost no downtime,
by restarting all nodes at once in the new version.

The migration steps are as follows:

1. Do `garage repair --all-nodes --yes tables` and `garage repair --all-nodes --yes blocks`,
   check the logs and check that all data seems to be synced correctly between
   nodes. If you have time, do additional checks (`versions`, `block_refs`, etc.)

2. Turn off each node individually; back up its metadata folder (see above); turn it back on again.
   This will allow you to take a backup of all nodes without impacting global cluster availability.
   You can do all nodes of a single zone at once as this does not impact the availability of Garage.

3. Prepare your binaries and configuration files for Garage v0.9

4. Shut down all v0.8 nodes simultaneously, and restart them all simultaneously in v0.9.
   Use your favorite deployment tool (Ansible, Kubernetes, Nomad) to achieve this as fast as possible.
   Garage v0.9 should be in a working state as soon as it starts.

5. Proceed with repair and monitoring as described in steps 9-12 above.
