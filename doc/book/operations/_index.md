+++
title = "Operations & Maintenance"
weight = 50
sort_by = "weight"
template = "documentation.html"
+++

This section contains a number of important information on how to best operate a Garage cluster,
to ensure integrity and availability of your data:

- **[Upgrading Garage](@/documentation/operations/upgrading.md):** General instructions on how to
  upgrade your cluster from one version to the next. Instructions specific for each version upgrade
  can bef ound in the [working documents](@/documentation/working-documents/_index.md) section.

- **[Layout management](@/documentation/operations/layout.md):** Best practices for using the `garage layout`
  commands when adding or removing nodes from your cluster.

- **[Durability and repairs](@/documentation/operations/durability-repairs.md):** How to check for small things
  that might be going wrong, and how to recover from such failures.

- **[Recovering from failures](@/documentation/operations/recovering.md):** Garage's first selling point is resilience
  to hardware failures. This section explains how to recover from such a failure in the
  best possible way.
