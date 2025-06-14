+++
title = "Migrating from 0.3 to 0.4"
weight = 80
+++

**Migrating from 0.3 to 0.4 is unsupported. This document is only intended to
document the process internally for the Deuxfleurs cluster where we have to do
it. Do not try it yourself, you will lose your data and we will not help you.**

**Migrating from 0.2 to 0.4 will break everything for sure. Never try it.**

The internal data format of Garage hasn't changed much between 0.3 and 0.4.
The Sled database is still the same, and the data directory as well.

The following has changed, all in the meta directory:

- `node_id` in 0.3 contains the identifier of the current node. In 0.4, this
  file does nothing and should be deleted. It is replaced by `node_key` (the
  secret key) and `node_key.pub` (the associated public key). A node's
  identifier on the ring is its public key.

- `peer_info` in 0.3 contains the list of peers saved automatically by Garage.
  The format has changed and it is now stored in `peer_list` (`peer_info`
  should be deleted).

When migrating, all node identifiers will change. This also means that the
affectation of data partitions on the ring will change, and lots of data will
have to be rebalanced.

- If your cluster has only 3 nodes, all nodes store everything, therefore nothing has to be rebalanced.

- If your cluster has only 4 nodes, for any partition there will always be at
  least 2 nodes that stored data before that still store it after. Therefore
  the migration should in theory be transparent and Garage should continue to
  work during the rebalance.

- If your cluster has 5 or more nodes, data will disappear during the
  migration. Do not migrate (fortunately we don't have this scenario at
  Deuxfleurs), or if you do, make Garage unavailable until things stabilize
  (disable web and api access).


The migration steps are as follows:

1. Prepare a new configuration file for 0.4. For each node, point to the same
   meta and data directories as Garage 0.3. Basically, the things that change
   are the following:

  - No more `rpc_tls` section
  - You have to generate a shared `rpc_secret` and put it in all config files
  - `bootstrap_peers` has a different syntax as it has to contain node keys.
    Leave it empty and use `garage node-id` and `garage node connect` instead (new features of 0.4)
  - put the publicly accessible RPC address of your node in `rpc_public_addr` if possible (its optional but recommended)
  - If you are using Consul, change the `consul_service_name` to NOT be the name advertised by Nomad.
    Now Garage is responsible for advertising its own service itself.

2. Disable api and web access for some time (Garage does not support disabling
   these endpoints but you can change the port number or stop your reverse
   proxy for instance).

3. Do `garage repair -a --yes tables` and `garage repair -a --yes blocks`,
   check the logs and check that all data seems to be synced correctly between
   nodes.

4. Save somewhere the output of `garage status`. We will need this to remember
   how to reconfigure nodes in 0.4.

5. Turn off Garage 0.3

6. Backup metadata folders if you can (i.e. if you have space to do it
   somewhere). Backuping data folders could also be useful but that's much
   harder to do. If your filesystem supports snapshots, this could be a good
   time to use them.

7. Turn on Garage 0.4

8. At this point, running `garage status` should indicate that all nodes of the
   previous cluster are "unavailable". The nodes have new identifiers that
   should appear in healthy nodes once they can talk to one another (use
   `garage node connect` if necessary`). They should have NO ROLE ASSIGNED at
   the moment.

9. Prepare a script with several `garage node configure` commands that replace
   each of the v0.3 node ID with the corresponding v0.4 node ID, with the same
   zone/tag/capacity. For example if your node `drosera` had identifier `c24e`
   before and now has identifier `789a`, and it was configured with capacity
   `2` in zone `dc1`, put the following command in your script:

```bash
garage node configure 789a -z dc1 -c 2 -t drosera --replace c24e
```

10. Run your reconfiguration script. Check that the new output of `garage
	status` contains the correct node IDs with the correct values for capacity
	and zone. Old nodes should no longer be mentioned.

11. If your cluster has 4 nodes or less, and you are feeling adventurous, you
	can reenable Web and API access now. Things will probably work.

12. Garage might already be resyncing stuff. Issue a `garage repair -a --yes
	tables` and `garage repair -a --yes blocks` to force it to do so.

13. Wait for resyncing activity to stop in the logs. Do steps 12 and 13 two or
	three times, until you see that when you issue the repair commands, nothing
	gets resynced any longer.

14. Your upgraded cluster should be in a working state. Re-enable API and Web
	access and check that everything went well.
