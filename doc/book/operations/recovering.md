+++
title = "Recovering from failures"
weight = 40
+++

Garage is meant to work on old, second-hand hardware.
In particular, this makes it likely that some of your drives will fail, and some manual intervention will be needed.
Fear not! Garage is fully equipped to handle drive failures, in most common cases.

## A note on availability of Garage

With nodes dispersed in 3 zones or more, here are the guarantees Garage provides with the 3-way replication strategy (3 copies of all data, which is the recommended replication mode):

- The cluster remains fully functional as long as the machines that fail are in only one zone. This includes a whole zone going down due to power/Internet outage.
- No data is lost as long as the machines that fail are in at most two zones.

Of course this only works if your Garage nodes are correctly configured to be aware of the zone in which they are located.
Make sure this is the case using `garage status` to check on the state of your cluster's configuration.

In case of temporarily disconnected nodes, Garage should automatically re-synchronize
when the nodes come back up. This guide will deal with recovering from disk failures
that caused the loss of the data of a node.


## First option: removing a node

If you don't have spare parts (HDD, SDD) to replace the failed component, and if there are enough remaining nodes in your cluster
(at least 3), you can simply remove the failed node from Garage's configuration.
Note that if you **do** intend to replace the failed parts by new ones, using this method followed by adding back the node is **not recommended** (although it should work),
and you should instead use one of the methods detailed in the next sections.

Removing a node is done with the following command:

```bash
garage layout remove <node_id>
garage layout show    # review the changes you are making
garage layout apply   # once satisfied, apply the changes
```

(you can get the `node_id` of the failed node by running `garage status`)

This will repartition the data and ensure that 3 copies of everything are present on the nodes that remain available.



## Replacement scenario 1: only data is lost, metadata is fine

The recommended deployment for Garage uses an SSD to store metadata, and an HDD to store blocks of data.
In the case where only a single HDD crashes, the blocks of data are lost but the metadata is still fine.

This is very easy to recover by setting up a new HDD to replace the failed one.
The node does not need to be fully replaced and the configuration doesn't need to change.
We just need to tell Garage to get back all the data blocks and store them on the new HDD.

First, set up a new HDD to store Garage's data directory on the failed node, and restart Garage using
the existing configuration.  Then, run:

```bash
garage repair -a --yes blocks
```

This will re-synchronize blocks of data that are missing to the new HDD, reading them from copies located on other nodes.

You can check on the advancement of this process by doing the following command:

```bash
garage stats -a
```

Look out for the following output:

```
Block manager stats:
  resync queue length: 26541
```

This indicates that one of the Garage node is in the process of retrieving missing data from other nodes.
This number decreases to zero when the node is fully synchronized.


## Replacement scenario 2: metadata (and possibly data) is lost

This scenario covers the case where a full node fails, i.e. both the metadata directory and
the data directory are lost, as well as the case where only the metadata directory is lost.

To replace the lost node, we will start from an empty metadata directory, which means
Garage will generate a new node ID for the replacement node.
We will thus need to remove the previous node ID from Garage's configuration and replace it by the ID of the new node.

If your data directory is stored on a separate drive and is still fine, you can keep it, but it is not necessary to do so.
In all cases, the data will be rebalanced and the replacement node will not store the same pieces of data
as were originally stored on the one that failed. So if you keep the data files, the rebalancing
might be faster but most of the pieces will be deleted anyway from the disk and replaced by other ones.

First, set up a new drive to store the metadata directory for the replacement node (a SSD is recommended),
and for the data directory if necessary. You can then start Garage on the new node.
The restarted node should generate a new node ID, and it should be shown with `NO ROLE ASSIGNED` in `garage status`.
The ID of the lost node should be shown in `garage status` in the section for disconnected/unavailable nodes.

Then, replace the broken node by the new one, using:

```bash
garage layout assign <new_node_id> --replace <old_node_id> \
		-c <capacity> -z <zone> -t <node_tag>
garage layout show    # review the changes you are making
garage layout apply   # once satisfied, apply the changes
```

Garage will then start synchronizing all required data on the new node.
This process can be monitored using the `garage stats -a` command.

## Replacement scenario 3: corrupted metadata {#corrupted_meta}

In some cases, your metadata DB file might become corrupted, for instance if
your node suffered a power outage and did not shut down properly. In this case,
you can recover without having to change the node ID and rebuilding a cluster
layout. This means that data blocks will not need to be shuffled around, you
must simply find a way to repair the metadata file. The best way is generally
to discard the corrupted file and recover it from another source.

First of all, start by locating the database file in your metadata directory,
which [depends on your `db_engine`
choice](@/documentation/reference-manual/configuration.md#db_engine).  Then,
your recovery options are as follows:

- **Option 1: resyncing from other nodes.** In case your cluster is replicated
  with two or three copies, you can simply delete the database file, and Garage
  will resync from other nodes. To do so, stop Garage, delete the database file
  or directory, and restart Garage. Then, do a full table repair by calling
  `garage repair -a --yes tables`.  This will take a bit of time to complete as
  the new node will need to receive copies of the metadata tables from the
  network.

- **Option 2: restoring a snapshot taken by Garage.** Since v0.9.4, Garage can
  [automatically take regular
  snapshots](@/documentation/reference-manual/configuration.md#metadata_auto_snapshot_interval)
  of your metadata DB file. This file or directory should be located under
  `<metadata_dir>/snapshots`, and is named according to the UTC time at which it
  was taken. Stop Garage, discard the database file/directory and replace it by the
  snapshot you want to use. For instance, in the case of LMDB:

  ```bash
  cd $METADATA_DIR
  mv db.lmdb db.lmdb.bak
  cp -r snapshots/2024-03-15T12:13:52Z db.lmdb
  ```

  And for Sqlite:

  ```bash
  cd $METADATA_DIR
  mv db.sqlite db.sqlite.bak
  cp snapshots/2024-03-15T12:13:52Z db.sqlite
  ```

  Then, restart Garage and run a full table repair by calling `garage repair -a
  --yes tables`.  This should run relatively fast as only the changes that
  occurred since the snapshot was taken will need to be resynchronized. Of
  course, if your cluster is not replicated, you will lose all changes that
  occurred since the snapshot was taken.

- **Option 3: restoring a filesystem-level snapshot.** If you are using ZFS or
  BTRFS to snapshot your metadata partition, refer to their specific
  documentation on rolling back or copying files from an old snapshot.
