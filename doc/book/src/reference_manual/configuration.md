# Garage configuration file format reference

Here is an example `garage.toml` configuration file that illustrates all of the possible options:

```toml
metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"

block_size = 1048576

replication_mode = "3"

compression_level = 1

rpc_secret = "4425f5c26c5e11581d3223904324dcb5b5d5dfb14e5e7f35e38c595424f5f1e6"
rpc_bind_addr = "[::]:3901"
rpc_public_addr = "[fc00:1::1]:3901"

bootstrap_peers = [
    "563e1ac825ee3323aa441e72c26d1030d6d4414aeb3dd25287c531e7fc2bc95d@[fc00:1::1]:3901",
    "86f0f26ae4afbd59aaf9cfb059eefac844951efd5b8caeec0d53f4ed6c85f332[fc00:1::2]:3901",
    "681456ab91350f92242e80a531a3ec9392cb7c974f72640112f90a600d7921a4@[fc00:B::1]:3901",
    "212fd62eeaca72c122b45a7f4fa0f55e012aa5e24ac384a72a3016413fa724ff@[fc00:F::1]:3901",
]

consul_host = "consul.service"
consul_service_name = "garage-daemon"

sled_cache_capacity = 134217728
sled_flush_every_ms = 2000

[s3_api]
api_bind_addr = "[::]:3900"
s3_region = "garage"
root_domain = ".s3.garage"

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

Garage splits stored objects in consecutive chunks of size `block_size`
(except the last one which might be smaller). The default size is 1MB and
should work in most cases.  If you are interested in tuning this, feel free
to do so (and remember to report your findings to us!). If this value is
changed for a running Garage installation, only files newly uploaded will be
affected. Previously uploaded files will remain available. This however
means that chunks from existing files will not be deduplicated with chunks
from newly uploaded files, meaning you might use more storage space that is
optimally possible.

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

### `compression_level`

Zstd compression level to use for storing blocks.

Values between `1` (faster compression) and `19` (smaller file) are standard compression
levels for zstd. From `20` to `22`, compression levels are referred as "ultra" and must be
used with extra care as it will use lot of memory. A value of `0` will let zstd choose a
default value (currently `3`). Finally, zstd has also compression designed to be faster
than default compression levels, they range from `-1` (smaller file) to `-99` (faster 
compression).

If you do not specify a `compression_level` entry, garage will set it to `1` for you. With
this parameters, zstd consumes low amount of cpu and should work faster than line speed in
most situations, while saving some space and intra-cluster
bandwidth.

If you want to totally deactivate zstd in garage, you can pass the special value `'none'`. No
zstd related code will be called, your chunks will be stored on disk without any processing.

Compression is done synchronously, setting a value too high will add latency to write queries.

This value can be different between nodes, compression is done by the node which receive the
API call.

#### `rpc_secret`

Garage uses a secret key that is shared between all nodes of the cluster
in order to identify these nodes and allow them to communicate together.
This key should be specified here in the form of a 32-byte hex-encoded
random string. Such a string can be generated with a command
such as `openssl rand -hex 32`.

#### `rpc_bind_addr`

The address and port on which to bind for inter-cluster communcations
(reffered to as RPC for remote procedure calls).
The port specified here should be the same one that other nodes will used to contact
the node, even in the case of a NAT: the NAT should be configured to forward the external
port number to the same internal port nubmer. This means that if you have several nodes running
behind a NAT, they should each use a different RPC port number.

#### `rpc_public_addr`

The address and port that other nodes need to use to contact this node for
RPC calls.  **This parameter is optional but recommended.** In case you have
a NAT that binds the RPC port to a port that is different on your public IP,
this field might help making it work.

#### `bootstrap_peers`

A list of peer identifiers on which to contact other Garage peers of this cluster.
These peer identifiers have the following syntax:

```
<node public key>@<node public IP or hostname>:<port>
```

In the case where `rpc_public_addr` is correctly specified in the
configuration file, the full identifier of a node including IP and port can
be obtained by running `garage node id` and then included directly in the
`bootstrap_peers` list of other nodes.  Otherwise, only the node's public
key will be returned by `garage node id` and you will have to add the IP
yourself.

#### `consul_host` and `consul_service_name`

Garage supports discovering other nodes of the cluster using Consul.
This works only when nodes are announced in Consul by an orchestrator such as Nomad,
as Garage is not able to announce itself.

The `consul_host` parameter should be set to the hostname of the Consul server,
and `consul_service_name` should be set to the service name under which Garage's
RPC ports are announced.

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


## The `[s3_api]` section

#### `api_bind_addr`

The IP and port on which to bind for accepting S3 API calls.
This endpoint does not suport TLS: a reverse proxy should be used to provide it.

#### `s3_region`

Garage will accept S3 API calls that are targetted to the S3 region defined here.
API calls targetted to other regions will fail with a AuthorizationHeaderMalformed error
message that redirects the client to the correct region.

#### `root_domain`

The optionnal suffix to access bucket using vhost-style in addition to path-style request.
Note path-style requests are always enabled, whether or not vhost-style is configured.
Configuring vhost-style S3 required a wildcard DNS entry, and possibly a wildcard TLS certificate,
but might be required by softwares not supporting path-style requests.

If `root_domain` is `s3.garage.eu`, a bucket called `my-bucket` can be interacted with
using the hostname `my-bucket.s3.garage.eu`.

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
