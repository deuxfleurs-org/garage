# Garage configuration file format reference

Here is an example `garage.toml` configuration file that illustrates all of the possible options:

```toml
metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"

block_size = 1048576

replication_mode = "3"

rpc_bind_addr = "[::]:3901"

bootstrap_peers = [
  "[fc00:1::1]:3901",
  "[fc00:1::2]:3901",
  "[fc00:B::1]:3901",
  "[fc00:F::1]:3901",
]

consul_host = "consul.service"
consul_service_name = "garage-daemon"

max_concurrent_rpc_requests = 12

sled_cache_capacity = 134217728
sled_flush_every_ms = 2000

[rpc_tls]
ca_cert = "/etc/garage/pki/garage-ca.crt"
node_cert = "/etc/garage/pki/garage.crt"
node_key = "/etc/garage/pki/garage.key"

[s3_api]
s3_region = "garage"
api_bind_addr = "[::]:3900"

[s3_web]
bind_addr = "[::]:3902"
root_domain = ".web.garage"
index = "index.html"
```

The following gives details about each available configuration option.

## Available configuration options

#### `metadata_dir`

The directory in which Garage will store its metadata. This contains the node identifier,
the network configuration and the peer list, the list of buckets and keys as well
as the index of all objects, object version and object blocks.

Store this folder on a fast SSD drive if possible to maximize Garage's performance.

#### `data_dir`

The directory in which Garage will store the data blocks of objects.
This folder can be placed on an HDD. The space available for `data_dir`
should be counted to determine a node's capacity
when [configuring it](../getting_started/05_cluster.md).

#### `block_size`

Garage splits stored objects in consecutive chunks of size `block_size` (except the last
one which might be standard). The default size is 1MB and should work in most cases.
If you are interested in tuning this, feel free to do so (and remember to report your
findings to us!)

#### `replication_mode`

Garage supports the following replication modes:

- `none` or `1`: data stored on Garage is stored on a single node. There is no redundancy,
  and data will be unavailable as soon as one node fails or its network is disconnected.
  Do not use this for anything else than test deployments.

- `2`: data stored on Garage will be stored on two different nodes, if possible in different
  zones. Garage tolerates one node failure before losing data. Data should be available
  read-only when one node is down, but write operations will fail.
  Use this only if you really have to.

- `3`: data stored on Garage will be stored on three different nodes, if possible each in
  a different zones.
  Garage tolerates two node failure before losing data. Data should be available
  read-only when two nodes are down, and writes should be possible if only a single node
  is down.

Note that in modes `2` and `3`,
if at least the same number of zones are available, an arbitrary number of failures in 
any given zone is tolerated as copies of data will be spread over several zones.

**Make sure `replication_mode` is the same in the configuration files of all nodes.
Never run a Garage cluster where that is not the case.**

Changing the `replication_mode` of a cluster might work (make sure to shut down all nodes
and changing it everywhere at the time), but is not officially supported.

#### `rpc_bind_addr`

The address and port on which to bind for inter-cluster communcations
(reffered to as RPC for remote procedure calls).
The port specified here should be the same one that other nodes will used to contact
the node, even in the case of a NAT: the NAT should be configured to forward the external
port number to the same internal port nubmer. This means that if you have several nodes running
behind a NAT, they should each use a different RPC port number.

#### `bootstrap_peers`

A list of IPs and ports on which to contact other Garage peers of this cluster.
This should correspond to the RPC ports set up with `rpc_bind_addr`.

#### `consul_host` and `consul_service_name`

Garage supports discovering other nodes of the cluster using Consul.
This works only when nodes are announced in Consul by an orchestrator such as Nomad,
as Garage is not able to announce itself.

The `consul_host` parameter should be set to the hostname of the Consul server,
and `consul_service_name` should be set to the service name under which Garage's
RPC ports are announced.

#### `max_concurrent_rpc_requests`

Garage implements rate limiting for RPC requests: no more than
`max_concurrent_rpc_requests` concurrent outbound RPC requests will be made
by a Garage node (additionnal requests will be put in a waiting queue).

#### `sled_cache_capacity`

This parameter can be used to tune the capacity of the cache used by
[sled](https://sled.rs), the database Garage uses internally to store metadata.
Tune this to fit the RAM you wish to make available to your Garage instance.
More cache means faster Garage, but the default value (128MB) should be plenty
for most use cases.

#### `sled_flush_every_ms`

This parameters can be used to tune the flushing interval of sled.
Increase this if sled is thrashing your SSD, at the risk of losing more data in case
of a power outage (though this should not matter much as data is replicated on other
nodes). The default value, 2000ms, should be appropriate for most use cases.


## The `[rpc_tls]` section

This section should be used to configure the TLS certificates used to encrypt
intra-cluster traffic (RPC traffic). The following parameters should be set:

- `ca_cert`: the certificate of the CA that is allowed to sign individual node certificates
- `node_cert`: the node certificate for the current node
- `node_key`: the key associated with the node certificate

Note tha several nodes may use the same node certificate, as long as it is signed
by the CA.

If this section is absent, TLS is not used to encrypt intra-cluster traffic.


## The `[s3_api]` section

#### `api_bind_addr`

The IP and port on which to bind for accepting S3 API calls.
This endpoint does not suport TLS: a reverse proxy should be used to provide it.

#### `s3_region`

Garage will accept S3 API calls that are targetted to the S3 region defined here.
API calls targetted to other regions will fail with a AuthorizationHeaderMalformed error
message that redirects the client to the correct region.


## The `[s3_web]` section

Garage allows to publish content of buckets as websites. This section configures the
behaviour of this module.

#### `bind_addr`

The IP and port on which to bind for accepting HTTP requests to buckets configured
for website access.
This endpoint does not suport TLS: a reverse proxy should be used to provide it.

#### `root_domain`

The optionnal suffix appended to bucket names for the corresponding HTTP Host.

For instance, if `root_domain` is `web.garage.eu`, a bucket called `deuxfleurs.fr`
will be accessible either with hostname `deuxfleurs.fr.web.garage.eu`
or with hostname `deuxfleurs.fr`.

#### `index`

The name of the index file to return for requests ending with `/` (usually `index.html`).
