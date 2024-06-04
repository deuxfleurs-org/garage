+++
title = "Related work"
weight = 50
+++

## Context

Data storage is critical: it can lead to data loss if done badly and/or on hardware failure.
Filesystems + RAID can help on a single machine but a machine failure can put the whole storage offline.
Moreover, it put a hard limit on scalability. Often this limit can be pushed back far away by buying expensive machines.
But here we consider non specialized off the shelf machines that can be as low powered and subject to failures as a raspberry pi.

Distributed storage may help to solve both availability and scalability problems on these machines.
Many solutions were proposed, they can be categorized as block storage, file storage and object storage depending on the abstraction they provide.

## Overview

Block storage is the most low level one, it's like exposing your raw hard drive over the network.
It requires very low latencies and stable network, that are often dedicated.
However it provides disk devices that can be manipulated by the operating system with the less constraints: it can be partitioned with any filesystem, meaning that it supports even the most exotic features.
We can cite [iSCSI](https://en.wikipedia.org/wiki/ISCSI) or [Fibre Channel](https://en.wikipedia.org/wiki/Fibre_Channel).
Openstack Cinder proxy previous solution to provide an uniform API.

File storage provides a higher abstraction, they are one filesystem among others, which means they don't necessarily have all the exotic features of every filesystem.
Often, they relax some POSIX constraints while many applications will still be compatible without any modification.
As an example, we are able to run MariaDB (very slowly) over GlusterFS...
We can also mention CephFS (read [RADOS](https://doi.org/10.1145/1374596.1374606) whitepaper [[pdf](https://ceph.com/assets/pdfs/weil-rados-pdsw07.pdf)]), Lustre, LizardFS, MooseFS, etc.
OpenStack Manila proxy previous solutions to provide an uniform API.

Finally object storages provide the highest level abstraction.
They are the testimony that the POSIX filesystem API is not adapted to distributed filesystems.
Especially, the strong concistency has been dropped in favor of eventual consistency which is way more convenient and powerful in presence of high latencies and unreliability.
We often read about S3 that pioneered the concept that it's a filesystem for the WAN.
Applications must be adapted to work for the desired object storage service.
Today, the S3 HTTP REST API acts as a standard in the industry.
However, Amazon S3 source code is not open but alternatives were proposed.
We identified Minio, Pithos, Swift and Ceph.
Minio/Ceph enforces a total order, so properties similar to a (relaxed) filesystem.
Swift and Pithos are probably the most similar to AWS S3 with their consistent hashing ring.
However Pithos is not maintained anymore. More precisely the company that published Pithos version 1 has developped a second version 2 but has not open sourced it.
Some tests conducted by the [ACIDES project](https://acides.org/) have shown that Openstack Swift consumes way more resources (CPU+RAM) that we can afford. Furthermore, people developing Swift have not designed their software for geo-distribution.

There were many attempts in research too. I am only thinking to [LBFS](https://pdos.csail.mit.edu/papers/lbfs:sosp01/lbfs.pdf) that was used as a basis for Seafile. But none of them have been effectively implemented yet.

## Existing software

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

**[Pithos](https://github.com/exoscale/pithos):**
Pithos has been abandonned and should probably not used yet, in the following we explain why we did not pick their design.
Pithos was relying as a S3 proxy in front of Cassandra (and was working with Scylla DB too).
From its designers' mouth, storing data in Cassandra has shown its limitations justifying the project abandonment.
They built a closed-source version 2 that does not store blobs in the database (only metadata) but did not communicate further on it.
We considered their v2's design but concluded that it does not fit both our *Self-contained & lightweight* and *Simple* properties. It makes the development, the deployment and the operations more complicated while reducing the flexibility.

**[Riak CS](https://docs.riak.com/riak/cs/2.1.1/index.html):**
*Not written yet*

**[IPFS](https://ipfs.io/):** IPFS has design goals radically different from Garage, we have [a blog post](@/blog/2022-ipfs/index.md) talking about it.

## Specific research papers

*Not yet written*
