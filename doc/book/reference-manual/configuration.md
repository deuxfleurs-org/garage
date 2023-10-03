+++
title = "Configuration file format"
weight = 20
+++

## Full example

Here is an example `garage.toml` configuration file that illustrates all of the possible options:

```toml
metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"

db_engine = "lmdb"

block_size = 1048576

sled_cache_capacity = "128MiB"
sled_flush_every_ms = 2000
lmdb_map_size = "1T"

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


[consul_discovery]
api = "catalog"
consul_http_addr = "http://127.0.0.1:8500"
service_name = "garage-daemon"
ca_cert = "/etc/consul/consul-ca.crt"
client_cert = "/etc/consul/consul-client.crt"
client_key = "/etc/consul/consul-key.crt"
# for `agent` API mode, unset client_cert and client_key, and optionally enable `token`
# token = "abcdef-01234-56789"
tls_skip_verify = false
tags = [ "dns-enabled" ]
meta = { dns-acl = "allow trusted" }


[kubernetes_discovery]
namespace = "garage"
service_name = "garage-daemon"
skip_crd = false


[s3_api]
api_bind_addr = "[::]:3900"
s3_region = "garage"
root_domain = ".s3.garage"

[s3_web]
bind_addr = "[::]:3902"
root_domain = ".web.garage"

[admin]
api_bind_addr = "0.0.0.0:3903"
metrics_token = "cacce0b2de4bc2d9f5b5fdff551e01ac1496055aed248202d415398987e35f81"
admin_token = "ae8cb40ea7368bbdbb6430af11cca7da833d3458a5f52086f4e805a570fb5c2a"
trace_sink = "http://localhost:4317"
```

The following gives details about each available configuration option.

## Available configuration options

### `metadata_dir`

The directory in which Garage will store its metadata. This contains the node identifier,
the network configuration and the peer list, the list of buckets and keys as well
as the index of all objects, object version and object blocks.

Store this folder on a fast SSD drive if possible to maximize Garage's performance.

### `data_dir`

The directory in which Garage will store the data blocks of objects.
This folder can be placed on an HDD. The space available for `data_dir`
should be counted to determine a node's capacity
when [adding it to the cluster layout](@/documentation/cookbook/real-world.md).

### `db_engine` (since `v0.8.0`)

By default, Garage uses the Sled embedded database library
to store its metadata on-disk. Since `v0.8.0`, Garage can use alternative storage backends as follows:

| DB engine | `db_engine` value | Database path |
| --------- | ----------------- | ------------- |
| [Sled](https://sled.rs) | `"sled"` | `<metadata_dir>/db/` |
| [LMDB](https://www.lmdb.tech) | `"lmdb"` | `<metadata_dir>/db.lmdb/` |
| [Sqlite](https://sqlite.org) | `"sqlite"` | `<metadata_dir>/db.sqlite` |

Performance characteristics of the different DB engines are as follows:

- Sled: the default database engine, which tends to produce
  large data files and also has performance issues, especially when the metadata folder
  is on a traditional HDD and not on SSD.
- LMDB: the recommended alternative on 64-bit systems,
  much more space-efficiant and slightly faster. Note that the data format of LMDB is not portable
  between architectures, so for instance the Garage database of an x86-64
  node cannot be moved to an ARM64 node. Also note that, while LMDB can technically be used on 32-bit systems,
  this will limit your node to very small database sizes due to how LMDB works; it is therefore not recommended.
- Sqlite: Garage supports Sqlite as a storage backend for metadata,
  however it may have issues and is also very slow in its current implementation,
  so it is not recommended to be used for now.

It is possible to convert Garage's metadata directory from one format to another with a small utility named `convert_db`,
which can be downloaded at the following locations:
[for amd64](https://garagehq.deuxfleurs.fr/_releases/convert_db/amd64/convert_db),
[for i386](https://garagehq.deuxfleurs.fr/_releases/convert_db/i386/convert_db),
[for arm64](https://garagehq.deuxfleurs.fr/_releases/convert_db/arm64/convert_db),
[for arm](https://garagehq.deuxfleurs.fr/_releases/convert_db/arm/convert_db).
The `convert_db` utility is used as folows:

```
convert-db -a <input db engine> -i <input db path> \
  		   -b <output db engine> -o <output db path>
```

Make sure to specify the full database path as presented in the table above,
and not just the path to the metadata directory.

### `block_size`

Garage splits stored objects in consecutive chunks of size `block_size`
(except the last one which might be smaller). The default size is 1MiB and
should work in most cases. We recommend increasing it to e.g. 10MiB if
you are using Garage to store large files and have fast network connections
between all nodes (e.g. 1gbps).

If you are interested in tuning this, feel free to do so (and remember to
report your findings to us!). When this value is changed for a running Garage
installation, only files newly uploaded will be affected. Previously uploaded
files will remain available. This however means that chunks from existing files
will not be deduplicated with chunks from newly uploaded files, meaning you
might use more storage space that is optimally possible.

### `sled_cache_capacity`

This parameter can be used to tune the capacity of the cache used by
[sled](https://sled.rs), the database Garage uses internally to store metadata.
Tune this to fit the RAM you wish to make available to your Garage instance.
This value has a conservative default (128MB) so that Garage doesn't use too much
RAM by default, but feel free to increase this for higher performance.

### `sled_flush_every_ms`

This parameters can be used to tune the flushing interval of sled.
Increase this if sled is thrashing your SSD, at the risk of losing more data in case
of a power outage (though this should not matter much as data is replicated on other
nodes). The default value, 2000ms, should be appropriate for most use cases.

### `lmdb_map_size`

This parameters can be used to set the map size used by LMDB,
which is the size of the virtual memory region used for mapping the database file.
The value of this parameter is the maximum size the metadata database can take.
This value is not bound by the physical RAM size of the machine running Garage.
If not specified, it defaults to 1GiB on 32-bit machines and 1TiB on 64-bit machines.

### `replication_mode`

Garage supports the following replication modes:

- `none` or `1`: data stored on Garage is stored on a single node. There is no
  redundancy, and data will be unavailable as soon as one node fails or its
  network is disconnected.  Do not use this for anything else than test
  deployments.

- `2`: data stored on Garage will be stored on two different nodes, if possible
  in different zones. Garage tolerates one node failure, or several nodes
  failing but all in a single zone (in a deployment with at least two zones),
  before losing data. Data remains available in read-only mode when one node is
  down, but write operations will fail.

  - `2-dangerous`: a variant of mode `2`, where written objects are written to
    the second replica asynchronously. This means that Garage will return `200
    OK` to a PutObject request before the second copy is fully written (or even
    before it even starts being written).  This means that data can more easily
    be lost if the node crashes before a second copy can be completed.  This
    also means that written objects might not be visible immediately in read
    operations.  In other words, this mode severely breaks the consistency and
    durability guarantees of standard Garage cluster operation.  Benefits of
    this mode: you can still write to your cluster when one node is
    unavailable.

- `3`: data stored on Garage will be stored on three different nodes, if
  possible each in a different zones.  Garage tolerates two node failure, or
  several node failures but in no more than two zones (in a deployment with at
  least three zones), before losing data. As long as only a single node fails,
  or node failures are only in a single zone, reading and writing data to
  Garage can continue normally.

  - `3-degraded`: a variant of replication mode `3`, that lowers the read
    quorum to `1`, to allow you to read data from your cluster when several
    nodes (or nodes in several zones) are unavailable.  In this mode, Garage
    does not provide read-after-write consistency anymore.  The write quorum is
    still 2, ensuring that data successfully written to Garage is stored on at
    least two nodes.

  - `3-dangerous`: a variant of replication mode `3` that lowers both the read
    and write quorums to `1`, to allow you to both read and write to your
    cluster when several nodes (or nodes in several zones) are unavailable.  It
    is the least consistent mode of operation proposed by Garage, and also one
    that should probably never be used.

Note that in modes `2` and `3`,
if at least the same number of zones are available, an arbitrary number of failures in
any given zone is tolerated as copies of data will be spread over several zones.

**Make sure `replication_mode` is the same in the configuration files of all nodes.
Never run a Garage cluster where that is not the case.**

The quorums associated with each replication mode are described below:

| `replication_mode` | Number of replicas | Write quorum | Read quorum | Read-after-write consistency? |
| ------------------ | ------------------ | ------------ | ----------- | ----------------------------- |
| `none` or `1`      | 1                  | 1            | 1           | yes                           |
| `2`                | 2                  | 2            | 1           | yes                           |
| `2-dangerous`      | 2                  | 1            | 1           | NO                            |
| `3`                | 3                  | 2            | 2           | yes                           |
| `3-degraded`       | 3                  | 2            | 1           | NO                            |
| `3-dangerous`      | 3                  | 1            | 1           | NO                            |

Changing the `replication_mode` between modes with the same number of replicas
(e.g. from `3` to `3-degraded`, or from `2-dangerous` to `2`), can be done easily by
just changing the `replication_mode` parameter in your config files and restarting all your
Garage nodes.

It is also technically possible to change the replication mode to a mode with a
different numbers of replicas, although it's a dangerous operation that is not
officially supported.  This requires you to delete the existing cluster layout
and create a new layout from scratch, meaning that a full rebalancing of your
cluster's data will be needed.  To do it, shut down your cluster entirely,
delete the `custer_layout` files in the meta directories of all your nodes,
update all your configuration files with the new `replication_mode` parameter,
restart your cluster, and then create a new layout with all the nodes you want
to keep.  Rebalancing data will take some time, and data might temporarily
appear unavailable to your users.  It is recommended to shut down public access
to the cluster while rebalancing is in progress.  In theory, no data should be
lost as rebalancing is a routine operation for Garage, although we cannot
guarantee you that everything will go right in such an extreme scenario.

### `compression_level`

Zstd compression level to use for storing blocks.

Values between `1` (faster compression) and `19` (smaller file) are standard compression
levels for zstd. From `20` to `22`, compression levels are referred as "ultra" and must be
used with extra care as it will use lot of memory. A value of `0` will let zstd choose a
default value (currently `3`). Finally, zstd has also compression designed to be faster
than default compression levels, they range from `-1` (smaller file) to `-99` (faster
compression).

If you do not specify a `compression_level` entry, Garage will set it to `1` for you. With
this parameters, zstd consumes low amount of cpu and should work faster than line speed in
most situations, while saving some space and intra-cluster
bandwidth.

If you want to totally deactivate zstd in Garage, you can pass the special value `'none'`. No
zstd related code will be called, your chunks will be stored on disk without any processing.

Compression is done synchronously, setting a value too high will add latency to write queries.

This value can be different between nodes, compression is done by the node which receive the
API call.

### `rpc_secret`, `rpc_secret_file` or `GARAGE_RPC_SECRET` (env)

Garage uses a secret key, called an RPC secret, that is shared between all
nodes of the cluster in order to identify these nodes and allow them to
communicate together. The RPC secret is a 32-byte hex-encoded random string,
which can be generated with a command such as `openssl rand -hex 32`.

The RPC secret should be specified in the `rpc_secret` configuration variable.
Since Garage `v0.8.2`, the RPC secret can also be stored in a file whose path is
given in the configuration variable `rpc_secret_file`, or specified as an
environment variable `GARAGE_RPC_SECRET`.

### `rpc_bind_addr`

The address and port on which to bind for inter-cluster communcations
(reffered to as RPC for remote procedure calls).
The port specified here should be the same one that other nodes will used to contact
the node, even in the case of a NAT: the NAT should be configured to forward the external
port number to the same internal port nubmer. This means that if you have several nodes running
behind a NAT, they should each use a different RPC port number.

### `rpc_public_addr`

The address and port that other nodes need to use to contact this node for
RPC calls.  **This parameter is optional but recommended.** In case you have
a NAT that binds the RPC port to a port that is different on your public IP,
this field might help making it work.

### `bootstrap_peers`

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


## The `[consul_discovery]` section

Garage supports discovering other nodes of the cluster using Consul.  For this
to work correctly, nodes need to know their IP address by which they can be
reached by other nodes of the cluster, which should be set in `rpc_public_addr`.

### `consul_http_addr` and `service_name`

The `consul_http_addr` parameter should be set to the full HTTP(S) address of the Consul server.

### `api`

Two APIs for service registration are supported: `catalog`  and `agent`. `catalog`, the default, will register a service using
the `/v1/catalog` endpoints, enabling mTLS if `client_cert` and `client_key` are provided. The `agent` API uses the
`v1/agent` endpoints instead, where an optional `token` may be provided.

### `service_name`

`service_name` should be set to the service name under which Garage's
RPC ports are announced.

### `client_cert`, `client_key`

TLS client certificate and client key to use when communicating with Consul over TLS. Both are mandatory when doing so.
Only available when `api = "catalog"`.

### `ca_cert`

TLS CA certificate to use when communicating with Consul over TLS.

### `tls_skip_verify`

Skip server hostname verification in TLS handshake.
`ca_cert` is ignored when this is set.

### `token`

Uses the provided token for communication with Consul. Only available when `api = "agent"`.
The policy assigned to this token should at least have these rules:

```hcl
// the `service_name` specified above
service "garage" {
  policy = "write"
}

service_prefix "" {
  policy = "read"
}

node_prefix "" {
  policy = "read"
}
```

### `tags` and `meta`

Additional list of tags and map of service meta to add during service registration.

## The `[kubernetes_discovery]` section

Garage supports discovering other nodes of the cluster using kubernetes custom
resources. For this to work, a `[kubernetes_discovery]` section must be present
with at least the `namespace` and `service_name` parameters.

### `namespace`

`namespace` sets the namespace in which the custom resources are
configured.

### `service_name`

`service_name` is added as a label to the advertised resources to
filter them, to allow for multiple deployments in a single namespace.

### `skip_crd`

`skip_crd` can be set to true to disable the automatic creation and
patching of the `garagenodes.deuxfleurs.fr` CRD. You will need to create the CRD
manually.


## The `[s3_api]` section

### `api_bind_addr`

The IP and port on which to bind for accepting S3 API calls.
This endpoint does not suport TLS: a reverse proxy should be used to provide it.

Alternatively, since `v0.8.5`, a path can be used to create a unix socket with 0222 mode.

### `s3_region`

Garage will accept S3 API calls that are targetted to the S3 region defined here.
API calls targetted to other regions will fail with a AuthorizationHeaderMalformed error
message that redirects the client to the correct region.

### `root_domain` {#root_domain}

The optional suffix to access bucket using vhost-style in addition to path-style request.
Note path-style requests are always enabled, whether or not vhost-style is configured.
Configuring vhost-style S3 required a wildcard DNS entry, and possibly a wildcard TLS certificate,
but might be required by softwares not supporting path-style requests.

If `root_domain` is `s3.garage.eu`, a bucket called `my-bucket` can be interacted with
using the hostname `my-bucket.s3.garage.eu`.



## The `[s3_web]` section

Garage allows to publish content of buckets as websites. This section configures the
behaviour of this module.

### `bind_addr`

The IP and port on which to bind for accepting HTTP requests to buckets configured
for website access.
This endpoint does not suport TLS: a reverse proxy should be used to provide it.

Alternatively, since `v0.8.5`, a path can be used to create a unix socket with 0222 mode.

### `root_domain`

The optional suffix appended to bucket names for the corresponding HTTP Host.

For instance, if `root_domain` is `web.garage.eu`, a bucket called `deuxfleurs.fr`
will be accessible either with hostname `deuxfleurs.fr.web.garage.eu`
or with hostname `deuxfleurs.fr`.


## The `[admin]` section

Garage has a few administration capabilities, in particular to allow remote monitoring. These features are detailed below.

### `api_bind_addr`

If specified, Garage will bind an HTTP server to this port and address, on
which it will listen to requests for administration features.
See [administration API reference](@/documentation/reference-manual/admin-api.md) to learn more about these features.

Alternatively, since `v0.8.5`, a path can be used to create a unix socket. Note that for security reasons,
the socket will have 0220 mode. Make sure to set user and group permissions accordingly.

### `metrics_token`, `metrics_token_file` or `GARAGE_METRICS_TOKEN` (env)

The token for accessing the Metrics endpoint. If this token is not set, the
Metrics endpoint can be accessed without access control.

You can use any random string for this value. We recommend generating a random token with `openssl rand -hex 32`.

`metrics_token` was introduced in Garage `v0.7.2`.
`metrics_token_file` and the `GARAGE_METRICS_TOKEN` environment variable are supported since Garage `v0.8.2`.


### `admin_token`, `admin_token_file` or `GARAGE_ADMIN_TOKEN` (env)

The token for accessing all of the other administration endpoints.  If this
token is not set, access to these endpoints is disabled entirely.

You can use any random string for this value. We recommend generating a random token with `openssl rand -hex 32`.

`admin_token` was introduced in Garage `v0.7.2`.
`admin_token_file` and the `GARAGE_ADMIN_TOKEN` environment variable are supported since Garage `v0.8.2`.


### `trace_sink`

Optionally, the address of an OpenTelemetry collector.  If specified,
Garage will send traces in the OpenTelemetry format to this endpoint. These
trace allow to inspect Garage's operation when it handles S3 API requests.
