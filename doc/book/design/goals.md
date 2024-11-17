+++
title = "Goals and use cases"
weight = 10
+++

## Goals and non-goals

Garage is a lightweight geo-distributed data store that implements the
[Amazon S3](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html)
object storage protocol. It enables applications to store large blobs such
as pictures, video, images, documents, etc., in a redundant multi-node
setting. S3 is versatile enough to also be used to publish a static
website.

Garage is an opinionated object storage solution, we focus on the following **desirable properties**:

  - **Internet enabled**: made for multi-sites (eg. datacenters, offices, households, etc.) interconnected through regular Internet connections.
  - **Self-contained & lightweight**: works everywhere and integrates well in existing environments to target [hyperconverged infrastructures](https://en.wikipedia.org/wiki/Hyper-converged_infrastructure).
  - **Highly resilient**: highly resilient to network failures, network latency, disk failures, sysadmin failures.
  - **Simple**: simple to understand, simple to operate, simple to debug.

We also noted that the pursuit of some other goals are detrimental to our initial goals.
The following has been identified as **non-goals** (if these points matter to you, you should not use Garage):

  - **Extreme performances**: high performances constrain a lot the design and the infrastructure; we seek performances through minimalism only.
  - **Feature extensiveness**: we do not plan to add additional features compared to the ones provided by the S3 API.
  - **Storage optimizations**: erasure coding or any other coding technique both increase the difficulty of placing data and synchronizing; we limit ourselves to duplication.
  - **POSIX/Filesystem compatibility**: we do not aim at being POSIX compatible or to emulate any kind of filesystem. Indeed, in a distributed environment, such synchronizations are translated in network messages that impose severe constraints on the deployment.

## Use-cases

*Are you also using Garage in your organization? [Open a PR](https://git.deuxfleurs.fr/Deuxfleurs/garage) to add your use case here!*

### Deuxfleurs

[Deuxfleurs](https://deuxfleurs.fr) is an experimental non-profit hosting
organization that develops Garage. Deuxfleurs is focused on building highly
available infrastructure through redundancy in multiple geographical
locations. They use Garage themselves for the following tasks:

- Hosting of [main website](https://deuxfleurs.fr), [this website](https://garagehq.deuxfleurs.fr), as well as the personal website of many of the members of the organization

- As a [Matrix media backend](https://github.com/matrix-org/synapse-s3-storage-provider)

- As a Nix binary cache

- To store personal data and shared documents through [Bagage](https://git.deuxfleurs.fr/Deuxfleurs/bagage), a homegrown WebDav-to-S3 and SFTP-to-S3 proxy

- As a backup target using `rclone` and `restic`

The Deuxfleurs Garage cluster is a multi-site cluster currently composed of
9 nodes in 3 physical locations.

### Triplebit

[Triplebit](https://www.triplebit.org) is a non-profit hosting provider and
ISP focused on improving access to privacy-related services. They use
Garage themselves for the following tasks:

- Hosting of their homepage, [privacyguides.org](https://www.privacyguides.org/), and various other static sites

- As a Mastodon object storage backend for [mstdn.party](https://mstdn.party/) and [mstdn.plus](https://mstdn.plus/)

- As a PeerTube storage backend for [neat.tube](https://neat.tube/)

- As a [Matrix media backend](https://github.com/matrix-org/synapse-s3-storage-provider)

Triplebit's Garage cluster is a multi-site cluster currently composed of
10 nodes in 3 physical locations.
