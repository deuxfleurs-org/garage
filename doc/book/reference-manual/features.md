+++
title = "List of Garage features"
weight = 10
+++


### S3 API

The main goal of Garage is to provide an object storage service that is compatible with the
[S3 API](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) from Amazon Web Services.
We try to adhere as strictly as possible to the semantics of the API as implemented by Amazon
and other vendors such as Minio or CEPH.

Of course Garage does not implement the full span of API endpoints that AWS S3 does;
the exact list of S3 features implemented by Garage can be found [on our S3 compatibility page](@/documentation/reference-manual/s3-compatibility.md).

### Geo-distribution

Garage allows you to store copies of your data in multiple geographical locations in order to maximize resilience
to adverse events, such as network/power outages or hardware failures.
This allows Garage to run very well even at home, using consumer-grade Internet connectivity
(such as FTTH) and power, as long as cluster nodes can be spawned at several physical locations.
Garage exploits knowledge of the capacity and physical location of each storage node to design
a storage plan that best exploits the available storage capacity while satisfying the geo-distributed replication constraint.

To learn more about geo-distributed Garage clusters,
read our documentation on [setting up a real-world deployment](@/documentation/cookbook/real-world.md).

### Standalone/self-contained

Garage is extremely simple to deploy, and does not depend on any external service to run.
This makes setting up and administering storage clusters, we hope, as easy as it could be.

### Flexible topology

A Garage cluster can very easily evolve over time, as storage nodes are added or removed.
Garage will automatically rebalance data between nodes as needed to ensure the desired number of copies.
Read about cluster layout management [here](@/documentation/operations/layout.md).

### Several replication modes

Garage supports a variety of replication modes, with configurable replica count,
and with various levels of consistency, in order to adapt to a variety of usage scenarios.
Read our reference page on [supported replication modes](@/documentation/reference-manual/configuration.md#replication_factor)
to select the replication mode best suited to your use case (hint: in most cases, `replication_factor = 3` is what you want).

### Compression and deduplication

All data stored in Garage is deduplicated, and optionnally compressed using
Zstd.  Objects uploaded to Garage are chunked in blocks of constant sizes (see
[`block_size`](@/documentation/reference-manual/configuration.md#block_size)),
and the hashes of individual blocks are used to dispatch them to storage nodes
and to deduplicate them.

### No RAFT slowing you down

It might seem strange to tout the absence of something as a desirable feature,
but this is in fact a very important point! Garage does not use RAFT or another
consensus algorithm internally to order incoming requests: this means that all requests
directed to a Garage cluster can be handled independently of one another instead
of going through a central bottleneck (the leader node).
As a consequence, requests can be handled much faster, even in cases where latency
between cluster nodes is important (see our [benchmarks](@/documentation/design/benchmarks/index.md) for data on this).
This is particularly usefull when nodes are far from one another and talk to one other through standard Internet connections.

### Web server for static websites

A storage bucket can easily be configured to be served directly by Garage as a static web site.
Domain names for multiple websites directly map to bucket names, making it easy to build
a platform for your users to autonomously build and host their websites over Garage.
Surprisingly, none of the other alternative S3 implementations we surveyed (such as Minio
or CEPH) support publishing static websites from S3 buckets, a feature that is however
directly inherited from S3 on AWS.
Read more on our [dedicated documentation page](@/documentation/cookbook/exposing-websites.md).

### Bucket names as aliases

In Garage, a bucket may have several names, known as aliases.
Aliases can easily be added and removed on demand:
this allows to easily rename buckets if needed
without having to copy all of their content, something that cannot be done on AWS.
For buckets served as static websites, having multiple aliases for a bucket can allow
exposing the same content under different domain names.

Garage also supports bucket aliases which are local to a single user:
this allows different users to have different buckets with the same name, thus avoiding naming collisions.
This can be helpfull for instance if you want to write an application that creates per-user buckets with always the same name.

This feature is totally invisible to S3 clients and does not break compatibility with AWS.

### Cluster administration API

Garage provides a fully-fledged REST API to administer your cluster programatically.
Functionality included in the admin API include: setting up and monitoring
cluster nodes, managing access credentials, and managing storage buckets and bucket aliases.
A full reference of the administration API is available [here](@/documentation/reference-manual/admin-api.md).

### Metrics and traces

Garage makes some internal metrics available in the Prometheus data format,
which allows you to build interactive dashboards to visualize the load and internal state of your storage cluster.

For developpers and performance-savvy administrators,
Garage also supports exporting traces of what it does internally in OpenTelemetry format.
This allows to monitor the time spent at various steps of the processing of requests,
in order to detect potential performance bottlenecks.

### Kubernetes and Nomad integrations

Garage can automatically discover other nodes in the cluster thanks to integration
with orchestrators such as Kubernetes and Nomad (when used with Consul).
This eases the configuration of your cluster as it removes one step where nodes need
to be manually connected to one another.

### Support for changing IP addresses

As long as all of your nodes don't change their IP address at the same time,
Garage should be able to tolerate nodes with changing/dynamic IP addresses,
as nodes will regularly exchange the IP addresses of their peers and try to
reconnect using newer addresses when existing connections are broken.

### K2V API (experimental)

As part of an ongoing research project, Garage can expose an experimental key/value storage API called K2V.
K2V is made for the storage and retrieval of many small key/value pairs that need to be processed in bulk.
This completes the S3 API with an alternative that can be used to easily store and access metadata
related to objects stored in an S3 bucket.

In the context of our research project, [Aérogramme](https://aerogramme.deuxfleurs.fr),
K2V is used to provide metadata and log storage for operations on encrypted e-mail storage.

Learn more on the specification of K2V [here](https://git.deuxfleurs.fr/Deuxfleurs/garage/src/branch/k2v/doc/drafts/k2v-spec.md)
and on how to enable it in Garage [here](@/documentation/reference-manual/k2v.md).
