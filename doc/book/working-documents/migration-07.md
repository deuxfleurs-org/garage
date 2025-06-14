+++
title = "Migrating from 0.6 to 0.7"
weight = 74
+++
**This guide explains how to migrate to 0.7 if you have an existing 0.6 cluster.
We don't recommend trying to migrate to 0.7 directly from 0.5 or older.**

**We make no guarantee that this migration will work perfectly:
back up all your data before attempting it!**

Garage v0.7 introduces a cluster protocol change to support request tracing through OpenTelemetry.
No data structure is changed, so no data migration is required.

The migration steps are as follows:

1. Do `garage repair --all-nodes --yes tables` and `garage repair --all-nodes --yes blocks`,
   check the logs and check that all data seems to be synced correctly between
   nodes. If you have time, do additional checks (`scrub`, `block_refs`, etc.)
2. Disable API and web access. Garage does not support disabling
   these endpoints but you can change the port number or stop your reverse
   proxy for instance.
3. Check once again that your cluster is healty. Run again `garage repair --all-nodes --yes tables` which is quick.
   Also check your queues are empty, run `garage stats` to query them.
4. Turn off Garage v0.6
5. Backup the metadata folder of all your nodes: `cd /var/lib/garage ; tar -acf meta-v0.6.tar.zst meta/`
6. Install Garage v0.7, edit the configuration if you plan to use OpenTelemetry or the Kubernetes integration
7. Turn on Garage v0.7
8. Do `garage repair --all-nodes --yes tables` and `garage repair --all-nodes --yes blocks`
9. Your upgraded cluster should be in a working state. Re-enable API and Web
    access and check that everything went well.
10. Monitor your cluster in the next hours to see if it works well under your production load, report any issue.
