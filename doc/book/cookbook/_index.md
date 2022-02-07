+++
title="Cookbook"
template = "documentation.html"
weight = 2
sort_by = "weight"
+++

A cookbook, when you cook, is a collection of recipes.
Similarly, Garage's cookbook contains a collection of recipes that are known to works well!
This chapter could also be referred as "Tutorials" or "Best practices".

- **[Multi-node deployment](@/documentation/cookbook/real-world.md):** This page will walk you through all of the necessary
  steps to deploy Garage in a real-world setting.

- **[Building from source](@/documentation/cookbook/from-source.md):** This page explains how to build Garage from
  source in case a binary is not provided for your architecture, or if you want to
  hack with us!

- **[Integration with Systemd](@/documentation/cookbook/systemd.md):** This page explains how to run Garage
  as a Systemd service (instead of as a Docker container).

- **[Configuring a gateway node](@/documentation/cookbook/gateways.md):** This page explains how to run a gateway node in a Garage cluster, i.e. a Garage node that doesn't store data but accelerates access to data present on the other nodes.

- **[Hosting a website](@/documentation/cookbook/exposing-websites.md):** This page explains how to use Garage
  to host a static website.

- **[Configuring a reverse-proxy](@/documentation/cookbook/reverse-proxy.md):** This page explains how to configure a reverse-proxy to add TLS support to your S3 api endpoint.

- **[Recovering from failures](@/documentation/cookbook/recovering.md):** Garage's first selling point is resilience
  to hardware failures. This section explains how to recover from such a failure in the
  best possible way.
