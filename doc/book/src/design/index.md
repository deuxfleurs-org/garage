# Design

The design section helps you to see Garage from a "big picture" perspective.
It will allow you to understand if Garage is a good fit for you,
how to better use it, how to contribute to it, what can Garage could and could not do, etc.

## Goals and non-goals

Garage is an opinionated object storage solutoin, we focus on the following **desirable properties**:

  - **Self-contained & lightweight**: works everywhere and integrates well in existing environments to target [hyperconverged infrastructures](https://en.wikipedia.org/wiki/Hyper-converged_infrastructure).
  - **Highly resilient**: highly resilient to network failures, network latency, disk failures, sysadmin failures.
  - **Simple**: simple to understand, simple to operate, simple to debug.
  - **Internet enabled**: made for multi-sites (eg. datacenters, offices, households, etc.) interconnected through regular Internet connections.

We also noted that the pursuit of some other goals are detrimental to our initial goals.
The following has been identified as **non-goals** (if these points matter to you, you should not use Garage):

  - **Extreme performances**: high performances constrain a lot the design and the infrastructure; we seek performances through minimalism only.
  - **Feature extensiveness**: we do not plan to add additional features compared to the ones provided by the S3 API.
  - **Storage optimizations**: erasure coding or any other coding technique both increase the difficulty of placing data and synchronizing; we limit ourselves to duplication.
  - **POSIX/Filesystem compatibility**: we do not aim at being POSIX compatible or to emulate any kind of filesystem. Indeed, in a distributed environment, such synchronizations are translated in network messages that impose severe constraints on the deployment.

## Talks

We love to talk and hear about Garage, that's why we keep a log here:

  - [(en, 2021-04-28) Distributed object storage is centralised](https://git.deuxfleurs.fr/Deuxfleurs/garage/raw/commit/b1f60579a13d3c5eba7f74b1775c84639ea9b51a/doc/talks/2021-04-28_spirals-team/talk.pdf)

  - [(fr, 2020-12-02) Garage : jouer dans la cour des grands quand on est un hébergeur associatif](https://git.deuxfleurs.fr/Deuxfleurs/garage/raw/commit/b1f60579a13d3c5eba7f74b1775c84639ea9b51a/doc/talks/2020-12-02_wide-team/talk.pdf)

*Did you write or talk about Garage? [Open a pull request](https://git.deuxfleurs.fr/Deuxfleurs/garage/) to add a link here!*


