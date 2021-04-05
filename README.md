Garage [![Build Status](https://drone.deuxfleurs.fr/api/badges/Deuxfleurs/garage/status.svg?ref=refs/heads/main)](https://drone.deuxfleurs.fr/Deuxfleurs/garage)
===

<p align="center" style="text-align:center;">
	<a href="https://garagehq.deuxfleurs.fr">
	<img alt="Garage logo" src="doc/logo/garage.png" height="200" />
	</a>
</p>

Garage is a lightweight S3-compatible distributed object store, with the following goals:

- As self-contained as possible
- Easy to set up
- Highly resilient to network failures, network latency, disk failures, sysadmin failures
- Relatively simple
- Made for multi-datacenter deployments

Non-goals include:

- Extremely high performance
- Complete implementation of the S3 API
- Erasure coding (our replication model is simply to copy the data as is on several nodes, in different datacenters if possible)

Our main use case is to provide a distributed storage layer for small-scale self hosted services such as [Deuxfleurs](https://deuxfleurs.fr).

**[Go to the documentation](https://garagehq.deuxfleurs.fr)**
