+++
title = "Migrating from 0.7 to 0.8"
weight = 13
+++

**This guide explains how to migrate to 0.8 if you have an existing 0.7 cluster.
We don't recommend trying to migrate to 0.8 directly from 0.6 or older.**

**We make no guarantee that this migration will work perfectly:
back up all your data before attempting it!**

Garage v0.8 introduces new data tables that allow the counting of objects in buckets in order to implement bucket quotas.
A manual migration step is required to first count objects in Garage buckets and populate these tables with accurate data.

The migration steps are as follows:

1. Disable API and web access. Garage v0.7 does not support disabling
   these endpoints but you can change the port number or stop your reverse proxy for instance.
2. Do `garage repair --all-nodes --yes tables` and `garage repair --all-nodes --yes blocks`,
   check the logs and check that all data seems to be synced correctly between
   nodes. If you have time, do additional checks (`scrub`, `block_refs`, etc.)
3. Check that queues are empty: run `garage stats` to query them or inspect metrics in the Grafana dashboard.
4. Turn off Garage v0.7
5. **Backup the metadata folder of all your nodes!** For instance, use the following command
	if your metadata directory is `/var/lib/garage/meta`: `cd /var/lib/garage ; tar -acf meta-v0.7.tar.zst meta/`
6. Install Garage v0.8
7. **Before starting Garage v0.8**, run the offline migration step: `garage offline-repair --yes object_counters`.
   This can take a while to run, depending on the number of objects stored in your cluster.
8. Turn on Garage v0.8
9. Do `garage repair --all-nodes --yes tables` and `garage repair --all-nodes --yes blocks`.
   Wait for a full table sync to run.
10. Your upgraded cluster should be in a working state. Re-enable API and Web
    access and check that everything went well.
11. Monitor your cluster in the next hours to see if it works well under your production load, report any issue.
