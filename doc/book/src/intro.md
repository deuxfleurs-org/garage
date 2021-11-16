<p align="center" style="text-align:center;">
	<a href="https://garagehq.deuxfleurs.fr">
	<img alt="Garage's Logo" src="img/logo.svg" height="200" />
	</a>
</p>

<p align="center" style="text-align:center;">
	[ <a href="https://garagehq.deuxfleurs.fr/_releases.html">Download</a>
	| <a href="https://git.deuxfleurs.fr/Deuxfleurs/garage">Git repository</a>
	| <a href="https://matrix.to/#/%23garage:deuxfleurs.fr">Matrix channel</a>
	| <a href="https://drone.deuxfleurs.fr/Deuxfleurs/garage">Drone CI</a>
	]
</p>

```
This very website is hosted using Garage. In other words: the doc is the PoC!
```

# The Garage Geo-Distributed Data Store

Garage is a lightweight geo-distributed data store.
It comes from the observation that despite numerous object stores
many people have broken data management policies (backup/replication on a single site or none at all).
To promote better data management policies, we focused on the following **desirable properties**:

  - **Self-contained & lightweight**: works everywhere and integrates well in existing environments to target [hyperconverged infrastructures](https://en.wikipedia.org/wiki/Hyper-converged_infrastructure).
  - **Highly resilient**: highly resilient to network failures, network latency, disk failures, sysadmin failures.
  - **Simple**: simple to understand, simple to operate, simple to debug.
  - **Internet enabled**: made for multi-sites (eg. datacenters, offices, households, etc.) interconnected through regular Internet connections.

We also noted that the pursuit of some other goals are detrimental to our initial goals.
The following has been identified as **non-goals** (if these points matter to you, you should not use Garage):

  - **Extreme performances**: high performances constrain a lot the design and the infrastructure; we seek performances through minimalism only.
  - **Feature extensiveness**: complete implementation of the S3 API or any other API to make garage a drop-in replacement is not targeted as it could lead to decisions impacting our desirable properties.
  - **Storage optimizations**: erasure coding or any other coding technique both increase the difficulty of placing data and synchronizing; we limit ourselves to duplication.
  - **POSIX/Filesystem compatibility**: we do not aim at being POSIX compatible or to emulate any kind of filesystem. Indeed, in a distributed environment, such synchronizations are translated in network messages that impose severe constraints on the deployment.

## Supported and planned protocols

Garage speaks (or will speak) the following protocols:

  - [S3](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) - *SUPPORTED* - Enable applications to store large blobs such as pictures, video, images, documents, etc. S3 is versatile enough to also be used to publish a static website.
  - [IMAP](https://github.com/go-pluto/pluto) - *PLANNED* - email storage is quite complex to get good performances.
To keep performances optimal, most IMAP servers only support on-disk storage.
We plan to add logic to Garage to make it a viable solution for email storage.
  - *More to come*

## Use Cases

**[Deuxfleurs](https://deuxfleurs.fr):** Garage is used by Deuxfleurs which is a non-profit hosting organization.
Especially, it is used to host their main website, this documentation and some of its members' blogs. 
Additionally, Garage is used as a [backend for Nextcloud](https://docs.nextcloud.com/server/20/admin_manual/configuration_files/primary_storage.html). 
Deuxfleurs also plans to use Garage as their [Matrix's media backend](https://github.com/matrix-org/synapse-s3-storage-provider) and as the backend of [OCIS](https://github.com/owncloud/ocis).

*Are you using Garage? [Open a pull request](https://git.deuxfleurs.fr/Deuxfleurs/garage/) to add your organization here!*

## Comparison to existing software

**[MinIO](https://min.io/):** MinIO shares our *Self-contained & lightweight* goal but selected two of our non-goals: *Storage optimizations* through erasure coding and *POSIX/Filesystem compatibility* through strong consistency.
However, by pursuing these two non-goals, MinIO do not reach our desirable properties.
Firstly, it fails on the *Simple* property: due to the erasure coding, MinIO has severe limitations on how drives can be added or deleted from a cluster.
Secondly, it fails on the *Internet enabled* property: due to its strong consistency, MinIO is latency sensitive.
Furthermore, MinIO has no knowledge of "sites" and thus can not distribute data to minimize the failure of a given site.

**[Openstack Swift](https://docs.openstack.org/swift/latest/):**
OpenStack Swift at least fails on the *Self-contained & lightweight* goal.
Starting it requires around 8GB of RAM, which is too much especially in an hyperconverged infrastructure.
We also do not classify Swift as *Simple*.

**[Ceph](https://ceph.io/ceph-storage/object-storage/):**
This review holds for the whole Ceph stack, including the RADOS paper, Ceph Object Storage module, the RADOS Gateway, etc.
At its core, Ceph has been designed to provide *POSIX/Filesystem compatibility* which requires strong consistency, which in turn
makes Ceph latency-sensitive and fails our *Internet enabled* goal.
Due to its industry oriented design, Ceph is also far from being *Simple* to operate and from being *Self-contained & lightweight* which makes it hard to integrate it in an hyperconverged infrastructure.
In a certain way, Ceph and MinIO are closer together than they are from Garage or OpenStack Swift.

*More comparisons are available in our [Related Work](design/related_work.md) chapter.*

## Other Resources

This website is not the only source of information about Garage!
We reference here other places on the Internet where you can learn more about Garage.

### Rust API (docs.rs)

If you encounter a specific bug in Garage or plan to patch it, you may jump directly to the source code's documentation!

  - [garage\_api](https://docs.rs/garage_api/latest/garage_api/) - contains the S3 standard API endpoint
  - [garage\_model](https://docs.rs/garage_model/latest/garage_model/) - contains Garage's model built on the table abstraction
  - [garage\_rpc](https://docs.rs/garage_rpc/latest/garage_rpc/) - contains Garage's federation protocol
  - [garage\_table](https://docs.rs/garage_table/latest/garage_table/) - contains core Garage's CRDT datatypes
  - [garage\_util](https://docs.rs/garage_util/latest/garage_util/) - contains garage helpers
  - [garage\_web](https://docs.rs/garage_web/latest/garage_web/) - contains the S3 website endpoint

### Talks

We love to talk and hear about Garage, that's why we keep a log here:

  - [(fr, 2021-11-13, video) Garage : Mille et une façons de stocker vos données](https://video.tedomum.net/w/moYKcv198dyMrT8hCS5jz9) and [slides (html)](https://rfid.deuxfleurs.fr/presentations/2021-11-13/garage/) - during [RFID#1](https://rfid.deuxfleurs.fr/programme/2021-11-13/) event

  - [(en, 2021-04-28, pdf) Distributed object storage is centralised](https://git.deuxfleurs.fr/Deuxfleurs/garage/raw/commit/b1f60579a13d3c5eba7f74b1775c84639ea9b51a/doc/talks/2021-04-28_spirals-team/talk.pdf)

  - [(fr, 2020-12-02, pdf) Garage : jouer dans la cour des grands quand on est un hébergeur associatif](https://git.deuxfleurs.fr/Deuxfleurs/garage/raw/commit/b1f60579a13d3c5eba7f74b1775c84639ea9b51a/doc/talks/2020-12-02_wide-team/talk.pdf)

*Did you write or talk about Garage? [Open a pull request](https://git.deuxfleurs.fr/Deuxfleurs/garage/) to add a link here!*

## Community

If you want to discuss with us, you can join our Matrix channel at [#garage:deuxfleurs.fr](https://matrix.to/#/#garage:deuxfleurs.fr).
Our code repository and issue tracker, which is the place where you should report bugs, is managed on [Deuxfleurs' Gitea](https://git.deuxfleurs.fr/Deuxfleurs/garage).

## License

Garage's source code, is released under the [AGPL v3 License](https://www.gnu.org/licenses/agpl-3.0.en.html).
Please note that if you patch Garage and then use it to provide any service over a network, you must share your code!

# Funding

The Deuxfleurs association has received a grant from [NGI POINTER](https://pointer.ngi.eu/), to fund 3 people working on Garage full-time for a year: from October 2021 to September 2022.

<div style="display: flex; justify-content: space-around">
  <a href="https://pointer.ngi.eu/">
    <img style="height:100px" src="img/ngi-logo.png" alt="NGI Pointer logo">
  </a>
  <a href="https://ec.europa.eu/programmes/horizon2020/what-horizon-2020">
    <img style="height:100px" src="img/eu-flag-logo.png" alt="EU flag logo">
  </a>
</div>

_This project has received funding from the European Union’s Horizon 2020 research and innovation programme within the framework of the NGI-POINTER Project funded under grant agreement N° 871528._
