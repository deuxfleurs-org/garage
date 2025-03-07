+++
title = "Upgrading Garage"
weight = 10
+++

Garage is a stateful clustered application, where all nodes are communicating together and share data structures.
It makes upgrade more difficult than stateless applications so you must be more careful when upgrading.
On a new version release, there is 2 possibilities:
  - protocols and data structures remained the same ➡️ this is a **minor upgrade**
  - protocols or data structures changed  ➡️  this is a **major upgrade**

You can quickly know what type of update you will have to operate by looking at the version identifier:
when we require our users to do a major upgrade, we will always bump the first nonzero component of the version identifier
(e.g. from v0.7.2 to v0.8.0).
Conversely, for versions that only require a minor upgrade, the first nonzero component will always stay the same (e.g. from v0.8.0 to v0.8.1).

Major upgrades are designed to be run only between contiguous versions.
Example: migrations from v0.7.1 to v0.8.0 and from v0.7.0 to v0.8.2 are supported but migrations from v0.6.0 to v0.8.0 are not supported.

The `garage_build_info`
[Prometheus metric](@/documentation/reference-manual/monitoring.md) provides
an overview for which Garage versions are currently in use within a cluster.

## Minor upgrades

Minor upgrades do not imply cluster downtime.
Before upgrading, you should still read [the changelog](https://git.deuxfleurs.fr/Deuxfleurs/garage/releases) and ideally test your deployment on a staging cluster before.

When you are ready, start by checking the health of your cluster.
You can force some checks with `garage repair`, we recommend at least running `garage repair --all-nodes --yes tables` which is very quick to run (less than a minute).
You will see that the command correctly terminated in the logs of your daemon, or using `garage worker list` (the repair workers should be in the `Done` state).

Finally, you can simply upgrade nodes one by one.
For each node: stop it, install the new binary, edit the configuration if needed, restart it.

## Major upgrades

Major upgrades can be done with minimal downtime with a bit of preparation, but the simplest way is usually to put the cluster offline for the duration of the migration.
Before upgrading, you must read [the changelog](https://git.deuxfleurs.fr/Deuxfleurs/garage/releases) and you must test your deployment on a staging cluster before.

We write guides for each major upgrade, they are stored under the "Working Documents" section of this documentation.

### Major upgrades with full downtime

From a high level perspective, a major upgrade looks like this:

  1. Disable API access (for instance in your reverse proxy, or by commenting the corresponding section in your Garage configuration file and restarting Garage)
  2. Check that your cluster is idle
  3. Make sure the health of your cluster is good (see `garage repair`)
  4. Stop the whole cluster
  5. Back up the metadata folder of all your nodes, so that you will be able to restore it if the upgrade fails (data blocks being immutable, they should not be impacted)
  6. Install the new binary, update the configuration
  7. Start the whole cluster
  8. If needed, run the corresponding migration from `garage migrate`
  9. Make sure the health of your cluster is good
  10. Enable API access (reverse step 1)
  11. Monitor your cluster while load comes back, check that all your applications are happy with this new version

### Major upgarades with minimal downtime

There is only one operation that has to be coordinated cluster-wide: the switch of one version of the internal RPC protocol to the next.
This means that an upgrade with very limited downtime can simply be performed from one major version to the next by restarting all nodes
simultaneously in the new version.
The downtime will simply be the time required for all nodes to stop and start again, which should be less than a minute.
If all nodes fail to stop and restart simultaneously, some nodes might be temporarily shut out from the cluster as nodes using different RPC protocol
versions are prevented to talk to one another.

The entire procedure would look something like this:

1. Make sure the health of your cluster is good (see `garage repair`)

2. Take each node offline individually to back up its metadata folder, bring them back online once the backup is done.
  You can do all of the nodes in a single zone at once as that won't impact global cluster availability.
  Do not try to manually copy the metadata folder of a running node.

  **Since Garage v0.9.4,** you can use the `garage meta snapshot --all` command
  to take a simultaneous snapshot of the metadata database files of all your
  nodes.  This avoids the tedious process of having to take them down one by
  one before upgrading. Be careful that if automatic snapshotting is enabled,
  Garage only keeps the last two snapshots and deletes older ones, so you might
  want to disable automatic snapshotting in your upgraded configuration file
  until you have confirmed that the upgrade ran successfully.  In addition to
  snapshotting the metadata databases of your nodes, you should back-up at
  least the `cluster_layout` file of one of your Garage instances (this file
  should be the same on all nodes and you can copy it safely while Garage is
  running).

3. Prepare your binaries and configuration files for the new Garage version

4. Restart all nodes simultaneously in the new version

5. If any specific migration procedure is required, it is usually in one of the two cases:

  - It can be run on online nodes after the new version has started, during regular cluster operation.
  - it has to be run offline, in which case you will have to again take all nodes offline one after the other to run the repair

   For this last step, please refer to the specific documentation pertaining to the version upgrade you are doing.
