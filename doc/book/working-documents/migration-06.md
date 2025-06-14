+++
title = "Migrating from 0.5 to 0.6"
weight = 75
+++

**This guide explains how to migrate to 0.6 if you have an existing 0.5 cluster.
We don't recommend trying to migrate to 0.6 directly from 0.4 or older.**

**We make no guarantee that this migration will work perfectly:
back up all your data before attempting it!**

Garage v0.6 introduces a new data model for buckets,
that allows buckets to have many names (aliases).
Buckets can also have "private" aliases (called local aliases),
which are only visible when using a certain access key.

This new data model means that the metadata tables have changed quite a bit in structure,
and a manual migration step is required.

The migration steps are as follows:

1. Disable api and web access for some time (Garage does not support disabling
   these endpoints but you can change the port number or stop your reverse
   proxy for instance).

2. Do `garage repair -a --yes tables` and `garage repair -a --yes blocks`,
   check the logs and check that all data seems to be synced correctly between
   nodes.

4. Turn off Garage 0.5

5. **Backup your metadata folders!!**

6. Turn on Garage 0.6

7. At this point, `garage bucket list` should indicate that no buckets are present
   in the cluster. `garage key list` should show all of the previously existing
   access key, however these keys should not have any permissions to access buckets.

8. Run `garage migrate buckets050`: this will populate the new bucket table with
   the buckets that existed previously. This will also give access to API keys
   as it was before.

9. Do `garage repair -a --yes tables` and `garage repair -a --yes blocks`,
   check the logs and check that all data seems to be synced correctly between
   nodes.

10. Check that all your buckets indeed appear in `garage bucket list`, and that
    keys have the proper access flags set. If that is not the case, revert
    everything and file a bug!

11. Your upgraded cluster should be in a working state. Re-enable API and Web
    access and check that everything went well.
