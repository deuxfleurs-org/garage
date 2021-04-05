# Recovering from failures

Garage is meant to work on old, second-hand hardware.
In particular, this makes it likely that some of your drives will fail, and some manual intervention will be needed.
Fear not! For Garage is fully equipped to handle drive failures, in most common cases.

## A note on availability of Garage

With nodes dispersed in 3 datacenters or more, here are the guarantees Garage provides with the default replication strategy (3 copies of all data, which is the recommended value):

- The cluster remains fully functional as long as the machines that fail are in only one datacenter. This includes a whole datacenter going down due to power/Internet outage.
- No data is lost as long as the machines that fail are in at most two datacenters.

Of course this only works if your Garage nodes are correctly configured to be aware of the datacenter in which they are located.
Make sure this is the case using `garage status` to check on the state of your cluster's configuration.


## First option: removing a node

If you don't have spare parts (HDD, SDD) to replace the failed component, and if there are enough remaining nodes in your cluster
(at least 3), you can simply remove the failed node from Garage's configuration.
Note that if you **do** intend to replace the failed parts by new ones, using this method followed by adding back the node is **not recommended** (although it should work),
and you should instead use one of the methods detailed in the next sections.

Removing a node is done with the following command:

```
garage node remove --yes <node_id>
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

```
garage repair -a --yes blocks
```

This will re-synchronize blocks of data that are missing to the new HDD, reading them from copies located on other nodes.

You can check on the advancement of this process by doing the following command: 

```
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
The restarted node should generate a new node ID, and it should be shown as `NOT CONFIGURED` in `garage status`.
The ID of the lost node should be shown in `garage status` in the section for disconnected/unavailable nodes.

Then, replace the broken node by the new one, using:

```
garage node configure --replace <old_node_id> \
		-c <capacity> -d <datacenter> -t <node_tag> <new_node_id>
```

Garage will then start synchronizing all required data on the new node.
This process can be monitored using the `garage stats -a` command.
