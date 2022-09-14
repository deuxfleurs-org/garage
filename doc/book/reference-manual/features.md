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

### Flexible topology

A Garage cluster can very easily evolve over time, as storage nodes are added or removed.
Garage will automatically rebalance data between nodes as needed to ensure the desired number of copies.
Read about cluster layout management [here](@/documentation/reference-manual/layout.md).

### No RAFT slowing you down

It might seem strange to tout the absence of something as a desirable feature,
but this is in fact a very important point! Garage does not use RAFT or another
consensus algorithm internally to order incoming requests: this means that all requests
directed to a Garage cluster can be handled independently of one another instead
of going through a central bottleneck (the leader node).
As a consequence, requests can be handled much faster, even in cases where latency
between cluster nodes is important (see our [benchmarks](@/documentation/design/benchmarks/index.md) for data on this).
This is particularly usefull when nodes are far from one another and talk to one other through standard Internet connections.
 
### Several replication modes

Garage supports a variety of replication modes, with 1 copy, 2 copies or 3 copies of your data,
and with various levels of consistency. 
Read our reference page on [supported replication modes](@/documentation/reference-manual/configuration.md#replication-mode)
to select the replication mode best suited to your use case (hint: in most cases, `replication_mode = "3"` is what you want).

### Web server for static websites

A storage bucket can easily be configured to be served directly by Garage as a static web site.
Domain names for multiple websites directly map to bucket names, making it easy to build
a platform for your user's to autonomously build and host their websites over Garage.
Surprisingly, none of the other alternative S3 implementations we surveyed (such as Minio
or CEPH) support publishing static websites from S3 buckets, a feature that is however
directly inherited from S3 on AWS.

### Bucket names as aliases

 - the same bucket may have multiple names (useful when exposing websites for example)

 - bucket renaming is possible

 - Scoped buckets: 2 users can have a different bucket with the same name -> avoid collision. Helpful if you want to write an application that creates per-user bucket always with the same name.

### Standalone/self contained

 
### Integration with Kubernetes and Nomad

Many node discovery methods: Kubernetes integration, Nomad integration through Consul
 
### Support for changing IP addresses

(as long as all nodes don't change their IP at the same time)

### Cluster administration API

### Metrics and traces
 
### (experimental) K2V API
