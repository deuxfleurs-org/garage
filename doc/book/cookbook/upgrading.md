+++
title = "Upgrading Garage"
weight = 40
+++

Garage is a stateful clustered application, where all nodes are communicating together and share data structures.
It makes upgrade more difficult than stateless applications so you must be more careful when upgrading.
On a new version release, there is 2 possibilities:
  - protocols and data structures remained the same ➡️ this is a **straightforward upgrade**
  - protocols or data structures changed  ➡️  this is an **advanced upgrade**

You can quickly now what type of update you will have to operate by looking at the version identifier.
Following the [SemVer ](https://semver.org/) terminology, if only the *patch* number changed, it will only need a straightforward upgrade.
Example: an upgrade from v0.6.0 from v0.6.1 is a straightforward upgrade.
If the *minor* or *major* number changed however, you will have to do an advanced upgrade. Example: from v0.6.1 to v0.7.0.

Migrations are designed to be run only between contiguous versions (from a *major*.*minor* perspective, *patches* can be skipped).
Example: migrations from v0.6.1 to v0.7.0 and from v0.6.0 to v0.7.0 are supported but migrations from v0.5.0 to v0.7.0 are not supported.

## Straightforward upgrades

Straightforward upgrades do not imply cluster downtime.
Before upgrading, you should still read [the changelog](https://git.deuxfleurs.fr/Deuxfleurs/garage/releases) and ideally test your deployment on a staging cluster before.

When you are ready, start by checking the health of your cluster.
You can force some checks with `garage repair`, we recommend at least running `garage repair --all-nodes --yes` that is very quick to run (less than a minute).
You will see that the command correctly terminated in the logs of your daemon.

Finally, you can simply upgrades nodes one by one. 
For each node: stop it, install the new binary, edit the configuration if needed, restart it. 

## Advanced upgrades

Advanced upgrades will imply cluster downtime.
Before upgrading, you must read [the changelog](https://git.deuxfleurs.fr/Deuxfleurs/garage/releases) and you must test your deployment on a staging cluster before.

From a high level perspective, an advanced upgrade looks like this:
  1. Make sure the health of your cluster is good (see `garage repair`)
  2. Disable API access (comment the configuration in your reverse proxy)
  3. Check that your cluster is idle
  4. Stop the whole cluster
  5. Backup the metadata folder of all your nodes, so that you will be able to restore it quickly if the upgrade fails (blocks being immutable, they should not be impacted)
  6. Install the new binary, update the configuration
  7. Start the whole cluster
  8. If needed, run the corresponding migration from `garage migrate`
  9. Make sure the health of your cluster is good
  10. Enable API access (uncomment the configuration in your reverse proxy)
  11. Monitor your cluster while load comes back, check that all your applications are happy with this new version

We write guides for each advanced upgrade, they are stored under the "Working Documents" section of this documentation.
