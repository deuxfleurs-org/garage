![Garage's Logo](img/logo.svg)

# The Garage Geo-Distributed Data Store

Garage is a lightweight geo-distributed data store.
It comes from the observation that despite numerous object stores
many people have broken data management policies (backup/replication on a single site or none at all).
To promote better data management policies, with focused on the following desirable properties:

  - **Self-contained & lightweight**: works everywhere and integrates well in existing environments to target hyperconverged infrastructures
  - **Highly resilient**: highly resilient to network failures, network latency, disk failures, sysadmin failures
  - **Simple**: simple to understand, simple to operate, simple to debug
  - **Internet enabled**: Made for multi-sites (eg. datacenter, offices, etc.) interconnected through a regular internet connection.

We also noted that the pursuit of some other goals are detrimental to our initial goals.
The following have been identified has non-goals, if it matters to you, you should not use Garage:

  - **Extreme performances**: high performances constrain a lot the design and the deployment. We always prioritize 
  - **Feature extensiveness**: Complete implementation of the S3 API
  - **Storage optimizations**: Erasure coding (our replication model is simply to copy the data as is on several nodes, in different datacenters if possible)
  - **POSIX/Filesystem compatibility**: We do not aim at being POSIX compatible or to emulate any kind of filesystem. Indeed, in a distributed environment, such syncronizations are translated in network messages that impose severe constraints on the deployment.

## Integration in environments

Garage speaks (or will speak) the following protocols:

  - [S3](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) - *SUPPORTED* - Enable applications to store large blobs such as pictures, video, images, documents, etc. S3 is versatile enough to also be used to publish a static website.
  - [IMAP](https://github.com/go-pluto/pluto) - *PLANNED* - email storage is quite complex to get good oerformances.
To keep performances optimals, most imap servers only support on-disk storage.
We plan to add logic to Garage to make it a viable solution for email storage.
  - *More to come*

## Use Cases

**[Deuxfleurs](https://deuxfleurs.fr) :** Garage is used by Deuxfleurs which is a non-profit hosting organization.
Especially, it is used to host their main website, this documentation and some of its members's blogs. Additionally,
Garage is used as a [backend for Nextcloud](https://docs.nextcloud.com/server/20/admin_manual/configuration_files/primary_storage.html). Deuxfleurs also plans to use Garage as their [Matrix's media backend](https://github.com/matrix-org/synapse-s3-storage-provider) and has the backend of [OCIS](https://github.com/owncloud/ocis).

*Are you using Garage? Open a pull request to add your organization here!*

## Comparisons to existing software

**[Minio](https://min.io/) :** Minio shares our *self-contained & lightweight* goal but selected two of our non-goals: *storage optimizations* through erasure coding and *POSIX/Filesystem compatibility* through strong consistency.
However, by pursuing these two non-goals, minio do not reach our desirable properties.
First, it fails on the *simple* property: due to the erasure coding, minio has severe limitations on how drives can be added or deleted from a cluster.
Second, it fails on the *interned enabled* property: due to its strong consistency, minio is latency sensitive.
Furthermore, minio has no knowledge of "sites" and thus can not distribute data to minimize the failure of a given site.

**[Openstack Swift](https://docs.openstack.org/swift/latest/)**
OpenStack Swift at least fails on the *self-contained & lightweight* goal.
Starting it requires around 8Gb of RAM, which is too much especially in an hyperconverged infrastructure.
It seems also to be far from *Simple*.

**[Pithos](https://github.com/exoscale/pithos)** 
Pithos has been abandonned and should probably not used yet, in the following we explain why we did not pick their design.
Pithos was relying as a S3 proxy in front of Cassandra (and was working with Scylla DB too).
From its designers' mouth, storing data in Cassandra has shown its limitations justifying the project abandonment.
They built a closed-source version 2 that does not store blobs in the database (only metadata) but did not communicate further on it.
We considered there v2's design but concluded that it does not fit both our *Self-contained & lightweight* and *Simple* properties. It makes the development, the deployment and the operations more complicated while reducing the flexibility.

**[IPFS](https://ipfs.io/)**
*Not written yet*
