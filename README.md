# Garage

Garage is a lightweight S3-compatible distributed object store, with the following goals:

- As self-contained as possible
- Easy to set up
- Highly resilient to network failures, network latency, disk failures, sysadmin failures
- Relatively simple
- Made for multi-datacenter deployments

Non-goals include:

- Extremely high performance
- Complete implementation of the S3 API
- Erasure coding (our replication model is simply to copy the data as is on several nodes)

Our main use case is to provide a distributed storage layer for small-scale self hosted services such as [Deuxfleurs](https://deuxfleurs.fr).

## Development

We propose the following quickstart to setup a full dev. environment as quickly as possible:

  1. Setup a rust/cargo environment and install s3cmd. eg. `dnf install rust cargo s3cmd`
  2. Run `cargo build` to build the project
  3. Run `./script/dev-cluster.sh` to launch a test cluster (feel free to read the script)
	4. Run `./script/dev-configure.sh` to configure your test cluster with default values (same datacenter, 100 tokens)
	5. Run `./script/dev-bucket.sh` to create a bucket named `éprouvette` and API key stored in `/tmp/garage.s3`
	6. Run `source ./script/dev-env.sh` to configure your environment:
	  - `garage` to manage the cluster. Try `garage --help`.
		- `s3grg` to add, remove, and delete files. Try `s3grg --help`, `s3grg put /proc/cpuinfo s3://éprouvette/cpuinfo.txt`, `s3grg ls s3://éprouvette`. `s3grg` is a wrapper on `s3cmd` configured with previous API key (the one in `/tmp/garage.s3`).

Now you should be ready to start hacking on garage!

## Setting up Garage

Use the `genkeys.sh` script to generate TLS keys for encrypting communications between Garage nodes.
The script takes no arguments and will generate keys in `pki/`.
This script creates a certificate authority `garage-ca` which signs certificates for individual Garage nodes.
Garage nodes from a same cluster authenticate themselves by verifying that they have certificates signed by the same certificate authority.

Garage requires two locations to store its data: a metadata directory, and a data directory.
The metadata directory is used to store metadata such as object lists, and should ideally be located on an SSD drive.
The data directory is used to store the chunks of data of the objects stored in Garage.
In a typical deployment the data directory is stored on a standard HDD.

Garage does not handle TLS for its S3 API endpoint. This should be handled by adding a reverse proxy.

Create a configuration file with the following structure:

```
block_size = 1048576			# objects are split in blocks of maximum this number of bytes

metadata_dir = "/path/to/ssd/metadata/directory"
data_dir = "/path/to/hdd/data/directory"

rpc_bind_addr = "[::]:3901"		# the port other Garage nodes will use to talk to this node

bootstrap_peers = [
	# Ideally this list should contain the IP addresses of all other Garage nodes of the cluster.
	# Use Ansible or any kind of configuration templating to generate this automatically.
	"10.0.0.1:3901",
	"10.0.0.2:3901",
	"10.0.0.3:3901",
]

# optionnal: garage can find cluster nodes automatically using a Consul server
# garage only does lookup but does not register itself, registration should be handled externally by e.g. Nomad
consul_host = "localhost:8500"	# optionnal: host name of a Consul server for automatic peer discovery
consul_service_name = "garage"  # optionnal: service name to look up on Consul

max_concurrent_rpc_requests = 12
data_replication_factor = 3
meta_replication_factor = 3
meta_epidemic_fanout = 3

[rpc_tls]
# NOT RECOMMENDED: you can skip this section if you don't want to encrypt intra-cluster traffic
# Thanks to genkeys.sh, generating the keys and certificates is easy, so there is NO REASON NOT TO DO IT.
ca_cert = "/path/to/garage/pki/garage-ca.crt"
node_cert = "/path/to/garage/pki/garage.crt"
node_key = "/path/to/garage/pki/garage.key"

[s3_api]
api_bind_addr = "[::1]:3900"	# the S3 API port, HTTP without TLS. Add a reverse proxy for the TLS part.
s3_region = "garage"				# set this to anything. S3 API calls will fail if they are not made against the region set here.

[s3_web]
web_bind_addr = "[::1]:3902"
```

Build Garage using `cargo build --release`.
Then, run it using either `./target/release/garage server -c path/to/config_file.toml` or `cargo run --release -- server -c path/to/config_file.toml`.

Set the `RUST_LOG` environment to `garage=debug` to dump some debug information.
Set it to `garage=trace` to dump even more debug information.
Set it to `garage=warn` to show nothing except warnings and errors.

## Setting up cluster nodes

Once all your `garage` nodes are running, you will need to:

1. check that they are correctly talking to one another;
2. configure them with their physical location (in the case of a multi-dc deployment) and a number of "ring tokens" proportionnal to the storage space available on each node;
3. create some S3 API keys and buckets;
4. ???;
5. profit!

To run these administrative tasks, you will need to use the `garage` command line tool and it to connect to any of the cluster's nodes on the RPC port.
The `garage` CLI also needs TLS keys and certificates of its own to authenticate and be authenticated in the cluster.
A typicall invocation will be as follows:

```
./target/release/garage --ca-cert=pki/garage-ca.crt --client-cert=pki/garage-client.crt --client-key=pki/garage-client.key <...>
```


## Notes to self

### What to repair

- `tables`: to do a full sync of metadata, should not be necessary because it is done every hour by the system
- `versions` and `block_refs`: very time consuming, usefull if deletions have not been propagated, improves garbage collection
- `blocks`: very usefull to resync/rebalance blocks betweeen nodes
