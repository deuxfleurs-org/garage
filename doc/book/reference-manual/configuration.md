+++
title = "Configuration file format"
weight = 20
+++

## Full example

Here is an example `garage.toml` configuration file that illustrates all of the possible options:

```toml
replication_factor = 3
consistency_mode = "consistent"

metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"
metadata_fsync = true
data_fsync = false
disable_scrub = false
metadata_auto_snapshot_interval = "6h"

db_engine = "lmdb"

block_size = "1M"
block_ram_buffer_max = "256MiB"

lmdb_map_size = "1T"

compression_level = 1

rpc_secret = "4425f5c26c5e11581d3223904324dcb5b5d5dfb14e5e7f35e38c595424f5f1e6"
rpc_bind_addr = "[::]:3901"
rpc_bind_outgoing = false
rpc_public_addr = "[fc00:1::1]:3901"
# or set rpc_public_adr_subnet to filter down autodiscovery to a subnet:
# rpc_public_addr_subnet = "2001:0db8:f00:b00:/64"


allow_world_readable_secrets = false

bootstrap_peers = [
    "563e1ac825ee3323aa441e72c26d1030d6d4414aeb3dd25287c531e7fc2bc95d@[fc00:1::1]:3901",
    "86f0f26ae4afbd59aaf9cfb059eefac844951efd5b8caeec0d53f4ed6c85f332@[fc00:1::2]:3901",
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
metrics_token = "BCAdFjoa9G0KJR0WXnHHm7fs1ZAbfpI8iIZ+Z/a2NgI="
admin_token = "UkLeGWEvHnXBqnueR3ISEMWpOnm40jH2tM2HnnL/0F4="
trace_sink = "http://localhost:4317"
```

The following gives details about each available configuration option.

## Available configuration options

### Index

[Environment variables](#env_variables).

Top-level configuration options:
[`allow_world_readable_secrets`](#allow_world_readable_secrets),
[`block_ram_buffer_max`](#block_ram_buffer_max),
[`block_size`](#block_size),
[`bootstrap_peers`](#bootstrap_peers),
[`compression_level`](#compression_level),
[`data_dir`](#data_dir),
[`data_fsync`](#data_fsync),
[`db_engine`](#db_engine),
[`disable_scrub`](#disable_scrub),
[`lmdb_map_size`](#lmdb_map_size),
[`metadata_auto_snapshot_interval`](#metadata_auto_snapshot_interval),
[`metadata_dir`](#metadata_dir),
[`metadata_fsync`](#metadata_fsync),
[`replication_factor`](#replication_factor),
[`consistency_mode`](#consistency_mode),
[`rpc_bind_addr`](#rpc_bind_addr),
[`rpc_bind_outgoing`](#rpc_bind_outgoing),
[`rpc_public_addr`](#rpc_public_addr),
[`rpc_public_addr_subnet`](#rpc_public_addr_subnet)
[`rpc_secret`/`rpc_secret_file`](#rpc_secret).

The `[consul_discovery]` section:
[`api`](#consul_api),
[`ca_cert`](#consul_ca_cert),
[`client_cert`](#consul_client_cert_and_key),
[`client_key`](#consul_client_cert_and_key),
[`consul_http_addr`](#consul_http_addr),
[`meta`](#consul_tags_and_meta),
[`service_name`](#consul_service_name),
[`tags`](#consul_tags_and_meta),
[`tls_skip_verify`](#consul_tls_skip_verify),
[`token`](#consul_token).

The `[kubernetes_discovery]` section:
[`namespace`](#kube_namespace),
[`service_name`](#kube_service_name),
[`skip_crd`](#kube_skip_crd).

The `[s3_api]` section:
[`api_bind_addr`](#s3_api_bind_addr),
[`root_domain`](#s3_root_domain),
[`s3_region`](#s3_region).

The `[s3_web]` section:
[`bind_addr`](#web_bind_addr),
[`root_domain`](#web_root_domain).

The `[admin]` section:
[`api_bind_addr`](#admin_api_bind_addr),
[`metrics_token`/`metrics_token_file`](#admin_metrics_token),
[`admin_token`/`admin_token_file`](#admin_token),
[`trace_sink`](#admin_trace_sink),

### Environment variables {#env_variables}

The following configuration parameter must be specified as an environment
variable, it does not exist in the configuration file:

- `GARAGE_LOG_TO_SYSLOG` (since v0.9.4): set this to `1` or `true` to make the
  Garage daemon send its logs to `syslog` (using the libc `syslog` function)
  instead of printing to stderr.

The following environment variables can be used to override the corresponding
values in the configuration file:

- [`GARAGE_ALLOW_WORLD_READABLE_SECRETS`](#allow_world_readable_secrets)
- [`GARAGE_RPC_SECRET` and `GARAGE_RPC_SECRET_FILE`](#rpc_secret)
- [`GARAGE_ADMIN_TOKEN` and `GARAGE_ADMIN_TOKEN_FILE`](#admin_token)
- [`GARAGE_METRICS_TOKEN` and `GARAGE_METRICS_TOKEN`](#admin_metrics_token)


### Top-level configuration options

#### `replication_factor` {#replication_factor}

The replication factor can be any positive integer smaller or equal the node count in your cluster.
The chosen replication factor has a big impact on the cluster's failure tolerancy and performance characteristics.

- `1`: data stored on Garage is stored on a single node. There is no
  redundancy, and data will be unavailable as soon as one node fails or its
  network is disconnected.  Do not use this for anything else than test
  deployments.

- `2`: data stored on Garage will be stored on two different nodes, if possible
  in different zones. Garage tolerates one node failure, or several nodes
  failing but all in a single zone (in a deployment with at least two zones),
  before losing data. Data remains available in read-only mode when one node is
  down, but write operations will fail.

- `3`: data stored on Garage will be stored on three different nodes, if
  possible each in a different zones.  Garage tolerates two node failure, or
  several node failures but in no more than two zones (in a deployment with at
  least three zones), before losing data. As long as only a single node fails,
  or node failures are only in a single zone, reading and writing data to
  Garage can continue normally.

- `5`, `7`, ...: When setting the replication factor above 3, it is most useful to
  choose an uneven value, since for every two copies added, one more node can fail
  before losing the ability to write and read to the cluster.

Note that in modes `2` and `3`,
if at least the same number of zones are available, an arbitrary number of failures in
any given zone is tolerated as copies of data will be spread over several zones.

**Make sure `replication_factor` is the same in the configuration files of all nodes.
Never run a Garage cluster where that is not the case.**

It is technically possible to change the replication factor although it's a
dangerous operation that is not officially supported.  This requires you to
delete the existing cluster layout and create a new layout from scratch,
meaning that a full rebalancing of your cluster's data will be needed.  To do
it, shut down your cluster entirely, delete the `custer_layout` files in the
meta directories of all your nodes, update all your configuration files with
the new `replication_factor` parameter, restart your cluster, and then create a
new layout with all the nodes you want to keep.  Rebalancing data will take
some time, and data might temporarily appear unavailable to your users.
It is recommended to shut down public access to the cluster while rebalancing
is in progress.  In theory, no data should be lost as rebalancing is a
routine operation for Garage, although we cannot guarantee you that everything
 will go right in such an extreme scenario.

#### `consistency_mode` {#consistency_mode}

The consistency mode setting determines the read and write behaviour of your cluster.

  - `consistent`: The default setting. This is what the paragraph above describes.
    The read and write quorum will be determined so that read-after-write consistency
    is guaranteed.
  - `degraded`: Lowers the read
    quorum to `1`, to allow you to read data from your cluster when several
    nodes (or nodes in several zones) are unavailable.  In this mode, Garage
    does not provide read-after-write consistency anymore.
    The write quorum stays the same as in the `consistent` mode, ensuring that
    data successfully written to Garage is stored on multiple nodes (depending
    the replication factor).
  - `dangerous`: This mode lowers both the read
    and write quorums to `1`, to allow you to both read and write to your
    cluster when several nodes (or nodes in several zones) are unavailable.  It
    is the least consistent mode of operation proposed by Garage, and also one
    that should probably never be used.

Changing the `consistency_mode` between modes while leaving the `replication_factor` untouched
(e.g. setting your node's `consistency_mode` to `degraded` when it was previously unset, or from
`dangerous` to `consistent`), can be done easily by just changing the `consistency_mode`
parameter in your config files and restarting all your Garage nodes.

The consistency mode can be used together with various replication factors, to achieve
a wide range of read and write characteristics. Some examples:

  - Replication factor `2`, consistency mode `degraded`: While this mode
    technically exists, its properties are the same as with consistency mode `consistent`,
    since the read quorum with replication factor `2`, consistency mode `consistent` is already 1.

  - Replication factor `2`, consistency mode `dangerous`: written objects are written to
    the second replica asynchronously. This means that Garage will return `200
    OK` to a PutObject request before the second copy is fully written (or even
    before it even starts being written).  This means that data can more easily
    be lost if the node crashes before a second copy can be completed.  This
    also means that written objects might not be visible immediately in read
    operations.  In other words, this configuration severely breaks the consistency and
    durability guarantees of standard Garage cluster operation.  Benefits of
    this configuration: you can still write to your cluster when one node is
    unavailable.

The quorums associated with each replication mode are described below:

| `consistency_mode` | `replication_factor` | Write quorum | Read quorum | Read-after-write consistency? |
| ------------------ | -------------------- | ------------ | ----------- | ----------------------------- |
| `consistent`       | 1                    | 1            | 1           | yes                           |
| `consistent`       | 2                    | 2            | 1           | yes                           |
| `dangerous`        | 2                    | 1            | 1           | NO                            |
| `consistent`       | 3                    | 2            | 2           | yes                           |
| `degraded`         | 3                    | 2            | 1           | NO                            |
| `dangerous`        | 3                    | 1            | 1           | NO                            |

#### `metadata_dir` {#metadata_dir}

The directory in which Garage will store its metadata. This contains the node identifier,
the network configuration and the peer list, the list of buckets and keys as well
as the index of all objects, object version and object blocks.

Store this folder on a fast SSD drive if possible to maximize Garage's performance.

#### `data_dir` {#data_dir}

The directory in which Garage will store the data blocks of objects.
This folder can be placed on an HDD. The space available for `data_dir`
should be counted to determine a node's capacity
when [adding it to the cluster layout](@/documentation/cookbook/real-world.md).

Since `v0.9.0`, Garage supports multiple data directories with the following syntax:

```toml
data_dir = [
    { path = "/path/to/old_data", read_only = true },
    { path = "/path/to/new_hdd1", capacity = "2T" },
    { path = "/path/to/new_hdd2", capacity = "4T" },
]
```

See [the dedicated documentation page](@/documentation/operations/multi-hdd.md)
on how to operate Garage in such a setup.

#### `db_engine` (since `v0.8.0`) {#db_engine}

Since `v0.8.0`, Garage can use alternative storage backends as follows:

| DB engine | `db_engine` value | Database path |
| --------- | ----------------- | ------------- |
| [LMDB](https://www.symas.com/lmdb) (since `v0.8.0`, default since `v0.9.0`) | `"lmdb"` | `<metadata_dir>/db.lmdb/` |
| [Sqlite](https://sqlite.org) (since `v0.8.0`) | `"sqlite"` | `<metadata_dir>/db.sqlite` |
| [Sled](https://sled.rs) (old default, removed since `v1.0`) | `"sled"` | `<metadata_dir>/db/` |

Sled was supported until Garage v0.9.x, and was removed in Garage v1.0.
You can still use an older binary of Garage (e.g. v0.9.4) to migrate
old Sled metadata databases to another engine.

Performance characteristics of the different DB engines are as follows:

- LMDB: the recommended database engine for high-performance distributed clusters.
LMDB works very well, but is known to have the following limitations:

  - The data format of LMDB is not portable between architectures, so for
    instance the Garage database of an x86-64 node cannot be moved to an ARM64
    node.

  - While LMDB can technically be used on 32-bit systems, this will limit your
    node to very small database sizes due to how LMDB works; it is therefore
    not recommended.

  - Several users have reported corrupted LMDB database files after an unclean
    shutdown (e.g. a power outage). This situation can generally be recovered
    from if your cluster is geo-replicated (by rebuilding your metadata db from
    other nodes), or if you have saved regular snapshots at the filesystem
    level.

  - Keys in LMDB are limited to 511 bytes. This limit translates to limits on
    object keys in S3 and sort keys in K2V that are limted to 479 bytes.

- Sqlite: Garage supports Sqlite as an alternative storage backend for
  metadata, which does not have the issues listed above for LMDB.
  On versions 0.8.x and earlier, Sqlite should be avoided due to abysmal
  performance, which was fixed with the addition of `metadata_fsync`.
  Sqlite is still probably slower than LMDB due to the way we use it,
  so it is not the best choice for high-performance storage clusters,
  but it should work fine in many cases.

It is possible to convert Garage's metadata directory from one format to another
using the `garage convert-db` command, which should be used as follows:

```
garage convert-db -a <input db engine> -i <input db path> \
                  -b <output db engine> -o <output db path>
```

Make sure to specify the full database path as presented in the table above
(third colummn), and not just the path to the metadata directory.

#### `metadata_fsync` {#metadata_fsync}

Whether to enable synchronous mode for the database engine or not.
This is disabled (`false`) by default.

This reduces the risk of metadata corruption in case of power failures,
at the cost of a significant drop in write performance,
as Garage will have to pause to sync data to disk much more often
(several times for API calls such as PutObject).

Using this option reduces the risk of simultaneous metadata corruption on several
cluster nodes, which could lead to data loss.

If multi-site replication is used, this option is most likely not necessary, as
it is extremely unlikely that two nodes in different locations will have a
power failure at the exact same time.

(Metadata corruption on a single node is not an issue, the corrupted data file
can always be deleted and reconstructed from the other nodes in the cluster.)

Here is how this option impacts the different database engines:

| Database | `metadata_fsync = false` (default) | `metadata_fsync = true`       |
|----------|------------------------------------|-------------------------------|
| Sqlite   | `PRAGMA synchronous = OFF`         | `PRAGMA synchronous = NORMAL` |
| LMDB     | `MDB_NOMETASYNC` + `MDB_NOSYNC`    | `MDB_NOMETASYNC`              |

Note that the Sqlite database is always ran in `WAL` mode (`PRAGMA journal_mode = WAL`).

#### `data_fsync` {#data_fsync}

Whether to `fsync` data blocks and their containing directory after they are
saved to disk.
This is disabled (`false`) by default.

This might reduce the risk that a data block is lost in rare
situations such as simultaneous node losing power,
at the cost of a moderate drop in write performance.

Similarly to `metatada_fsync`, this is likely not necessary
if geographical replication is used.

#### `metadata_auto_snapshot_interval` (since Garage v0.9.4) {#metadata_auto_snapshot_interval}

If this value is set, Garage will automatically take a snapshot of the metadata
DB file at a regular interval and save it in the metadata directory.
This parameter can take any duration string that can be parsed by
the [`parse_duration`](https://docs.rs/parse_duration/latest/parse_duration/#syntax) crate.

Snapshots can allow to recover from situations where the metadata DB file is
corrupted, for instance after an unclean shutdown.  See [this
page](@/documentation/operations/recovering.md#corrupted_meta) for details.
Garage keeps only the two most recent snapshots of the metadata DB and deletes
older ones automatically.

Note that taking a metadata snapshot is a relatively intensive operation as the
entire data file is copied. A snapshot being taken might have performance
impacts on the Garage node while it is running. If the cluster is under heavy
write load when a snapshot operation is running, this might also cause the
database file to grow in size significantly as pages cannot be recycled easily.
For this reason, it might be better to use filesystem-level snapshots instead
if possible.

#### `disable_scrub` {#disable_scrub}

By default, Garage runs a scrub of the data directory approximately once per
month, with a random delay to avoid all nodes running at the same time.  When
it scrubs the data directory, Garage will read all of the data files stored on
disk to check their integrity, and will rebuild any data files that it finds
corrupted, using the remaining valid copies stored on other nodes.
See [this page](@/documentation/operations/durability-repairs.md#scrub) for details.

Set the `disable_scrub` configuration value to `true` if you don't need Garage
to scrub the data directory, for instance if you are already scrubbing at the
filesystem level. Note that in this case, if you find a corrupted data file,
you should delete it from the data directory and then call `garage repair
blocks` on the node to ensure that it re-obtains a copy from another node on
the network.

#### `block_size` {#block_size}

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

#### `block_ram_buffer_max` (since v0.9.4) {#block_ram_buffer_max}

A limit on the total size of data blocks kept in RAM by S3 API nodes awaiting
to be sent to storage nodes asynchronously.

Explanation: since Garage wants to tolerate node failures, it uses quorum
writes to send data blocks to storage nodes: try to write the block to three
nodes, and return ok as soon as two writes complete. So even if all three nodes
are online, the third write always completes asynchronously.  In general, there
are not many writes to a cluster, and the third asynchronous write can
terminate early enough so as to not cause unbounded RAM growth.  However, if
the S3 API node is continuously receiving large quantities of data and the
third node is never able to catch up, many data blocks will be kept buffered in
RAM as they are awaiting transfer to the third node.

The `block_ram_buffer_max` sets a limit to the size of buffers that can be kept
in RAM in this process.  When the limit is reached, backpressure is applied
back to the S3 client.

Note that this only counts buffers that have arrived to a certain stage of
processing (received from the client + encrypted and/or compressed as
necessary) and are ready to send to the storage nodes. Many other buffers will
not be counted and this is not a hard limit on RAM consumption.  In particular,
if many clients send requests simultaneously with large objects, the RAM
consumption will always grow linearly with the number of concurrent requests,
as each request will use a few buffers of size `block_size` for receiving and
intermediate processing before even trying to send the data to the storage
node.

The default value is 256MiB.

#### `lmdb_map_size` {#lmdb_map_size}

This parameters can be used to set the map size used by LMDB,
which is the size of the virtual memory region used for mapping the database file.
The value of this parameter is the maximum size the metadata database can take.
This value is not bound by the physical RAM size of the machine running Garage.
If not specified, it defaults to 1GiB on 32-bit machines and 1TiB on 64-bit machines.

#### `compression_level` {#compression_level}

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

#### `rpc_secret`, `rpc_secret_file` or `GARAGE_RPC_SECRET`, `GARAGE_RPC_SECRET_FILE` (env) {#rpc_secret}

Garage uses a secret key, called an RPC secret, that is shared between all
nodes of the cluster in order to identify these nodes and allow them to
communicate together. The RPC secret is a 32-byte hex-encoded random string,
which can be generated with a command such as `openssl rand -hex 32`.

The RPC secret should be specified in the `rpc_secret` configuration variable.
Since Garage `v0.8.2`, the RPC secret can also be stored in a file whose path is
given in the configuration variable `rpc_secret_file`, or specified as an
environment variable `GARAGE_RPC_SECRET`.

Since Garage `v0.8.5` and `v0.9.1`, you can also specify the path of a file
storing the secret as the `GARAGE_RPC_SECRET_FILE` environment variable.

#### `rpc_bind_addr` {#rpc_bind_addr}

The address and port on which to bind for inter-cluster communcations
(reffered to as RPC for remote procedure calls).
The port specified here should be the same one that other nodes will used to contact
the node, even in the case of a NAT: the NAT should be configured to forward the external
port number to the same internal port nubmer. This means that if you have several nodes running
behind a NAT, they should each use a different RPC port number.

#### `rpc_bind_outgoing`(since v0.9.2) {#rpc_bind_outgoing}

If enabled, pre-bind all sockets for outgoing connections to the same IP address
used for listening (the IP address specified in `rpc_bind_addr`) before
trying to connect to remote nodes.
This can be necessary if a node has multiple IP addresses,
but only one is allowed or able to reach the other nodes,
for instance due to firewall rules or specific routing configuration.

Disabled by default.

#### `rpc_public_addr` {#rpc_public_addr}

The address and port that other nodes need to use to contact this node for
RPC calls.  **This parameter is optional but recommended.** In case you have
a NAT that binds the RPC port to a port that is different on your public IP,
this field might help making it work.

#### `rpc_public_addr_subnet` {#rpc_public_addr_subnet}
In case `rpc_public_addr` is not set, but autodiscovery is used, this allows
filtering the list of automatically discovered IPs to a specific subnet.

For example, if nodes should pick *their* IP inside a specific subnet, but you
don't want to explicitly write the IP down (as it's dynamic, or you want to
share configs across nodes), you can use this option.

#### `bootstrap_peers` {#bootstrap_peers}

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

### `allow_world_readable_secrets` or `GARAGE_ALLOW_WORLD_READABLE_SECRETS` (env) {#allow_world_readable_secrets}

Garage checks the permissions of your secret files to make sure they're not
world-readable. In some cases, the check might fail and consider your files as
world-readable even if they're not, for instance when using Posix ACLs.

Setting `allow_world_readable_secrets` to `true` bypass this
permission verification.

Alternatively, you can set the `GARAGE_ALLOW_WORLD_READABLE_SECRETS`
environment variable to `true` to bypass the permissions check.

### The `[consul_discovery]` section

Garage supports discovering other nodes of the cluster using Consul.  For this
to work correctly, nodes need to know their IP address by which they can be
reached by other nodes of the cluster, which should be set in `rpc_public_addr`.

#### `consul_http_addr` {#consul_http_addr}

The `consul_http_addr` parameter should be set to the full HTTP(S) address of the Consul server.

#### `api` {#consul_api}

Two APIs for service registration are supported: `catalog`  and `agent`. `catalog`, the default, will register a service using
the `/v1/catalog` endpoints, enabling mTLS if `client_cert` and `client_key` are provided. The `agent` API uses the
`v1/agent` endpoints instead, where an optional `token` may be provided.

#### `service_name` {#consul_service_name}

`service_name` should be set to the service name under which Garage's
RPC ports are announced.

#### `client_cert`, `client_key` {#consul_client_cert_and_key}

TLS client certificate and client key to use when communicating with Consul over TLS. Both are mandatory when doing so.
Only available when `api = "catalog"`.

#### `ca_cert` {#consul_ca_cert}

TLS CA certificate to use when communicating with Consul over TLS.

#### `tls_skip_verify` {#consul_tls_skip_verify}

Skip server hostname verification in TLS handshake.
`ca_cert` is ignored when this is set.

#### `token` {#consul_token}

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

#### `tags` and `meta` {#consul_tags_and_meta}

Additional list of tags and map of service meta to add during service registration.

### The `[kubernetes_discovery]` section

Garage supports discovering other nodes of the cluster using kubernetes custom
resources. For this to work, a `[kubernetes_discovery]` section must be present
with at least the `namespace` and `service_name` parameters.

#### `namespace` {#kube_namespace}

`namespace` sets the namespace in which the custom resources are
configured.

#### `service_name` {#kube_service_name}

`service_name` is added as a label to the advertised resources to
filter them, to allow for multiple deployments in a single namespace.

#### `skip_crd` {#kube_skip_crd}

`skip_crd` can be set to true to disable the automatic creation and
patching of the `garagenodes.deuxfleurs.fr` CRD. You will need to create the CRD
manually.


### The `[s3_api]` section

#### `api_bind_addr` {#s3_api_bind_addr}

The IP and port on which to bind for accepting S3 API calls.
This endpoint does not suport TLS: a reverse proxy should be used to provide it.

Alternatively, since `v0.8.5`, a path can be used to create a unix socket with 0222 mode.

#### `s3_region` {#s3_region}

Garage will accept S3 API calls that are targetted to the S3 region defined here.
API calls targetted to other regions will fail with a AuthorizationHeaderMalformed error
message that redirects the client to the correct region.

#### `root_domain` {#s3_root_domain}

The optional suffix to access bucket using vhost-style in addition to path-style request.
Note path-style requests are always enabled, whether or not vhost-style is configured.
Configuring vhost-style S3 required a wildcard DNS entry, and possibly a wildcard TLS certificate,
but might be required by softwares not supporting path-style requests.

If `root_domain` is `s3.garage.eu`, a bucket called `my-bucket` can be interacted with
using the hostname `my-bucket.s3.garage.eu`.



### The `[s3_web]` section

Garage allows to publish content of buckets as websites. This section configures the
behaviour of this module.

#### `bind_addr` {#web_bind_addr}

The IP and port on which to bind for accepting HTTP requests to buckets configured
for website access.
This endpoint does not suport TLS: a reverse proxy should be used to provide it.

Alternatively, since `v0.8.5`, a path can be used to create a unix socket with 0222 mode.

#### `root_domain` {#web_root_domain}

The optional suffix appended to bucket names for the corresponding HTTP Host.

For instance, if `root_domain` is `web.garage.eu`, a bucket called `deuxfleurs.fr`
will be accessible either with hostname `deuxfleurs.fr.web.garage.eu`
or with hostname `deuxfleurs.fr`.


### The `[admin]` section

Garage has a few administration capabilities, in particular to allow remote monitoring. These features are detailed below.

#### `api_bind_addr` {#admin_api_bind_addr}

If specified, Garage will bind an HTTP server to this port and address, on
which it will listen to requests for administration features.
See [administration API reference](@/documentation/reference-manual/admin-api.md) to learn more about these features.

Alternatively, since `v0.8.5`, a path can be used to create a unix socket. Note that for security reasons,
the socket will have 0220 mode. Make sure to set user and group permissions accordingly.

#### `metrics_token`, `metrics_token_file` or `GARAGE_METRICS_TOKEN`, `GARAGE_METRICS_TOKEN_FILE` (env) {#admin_metrics_token}

The token for accessing the Metrics endpoint. If this token is not set, the
Metrics endpoint can be accessed without access control.

You can use any random string for this value. We recommend generating a random token with `openssl rand -base64 32`.

`metrics_token` was introduced in Garage `v0.7.2`.
`metrics_token_file` and the `GARAGE_METRICS_TOKEN` environment variable are supported since Garage `v0.8.2`.

`GARAGE_METRICS_TOKEN_FILE` is supported since `v0.8.5` / `v0.9.1`.

#### `admin_token`, `admin_token_file` or `GARAGE_ADMIN_TOKEN`, `GARAGE_ADMIN_TOKEN_FILE` (env) {#admin_token}

The token for accessing all of the other administration endpoints.  If this
token is not set, access to these endpoints is disabled entirely.

You can use any random string for this value. We recommend generating a random token with `openssl rand -base64 32`.

`admin_token` was introduced in Garage `v0.7.2`.
`admin_token_file` and the `GARAGE_ADMIN_TOKEN` environment variable are supported since Garage `v0.8.2`.

`GARAGE_ADMIN_TOKEN_FILE` is supported since `v0.8.5` / `v0.9.1`.

#### `trace_sink` {#admin_trace_sink}

Optionally, the address of an OpenTelemetry collector.  If specified,
Garage will send traces in the OpenTelemetry format to this endpoint. These
trace allow to inspect Garage's operation when it handles S3 API requests.
