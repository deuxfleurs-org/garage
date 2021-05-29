# Configuring a test deployment

This section describes how to run a simple test Garage deployment with a single node.
Note that this kind of deployment should not be used in production, as it provides
no redundancy for your data!
We will also skip intra-cluster TLS configuration, meaning that if you add nodes
to your cluster, communication between them will not be secure.

First, make sure that you have Garage installed in your command line environment.
We will explain how to launch Garage in a Docker container, however we still
recommend that you install the `garage` CLI on your host system in order to control
the daemon.

## Writing a first configuration file

This first configuration file should allow you to get started easily with the simplest
possible Garage deployment:

```toml
metadata_dir = "/tmp/meta"
data_dir = "/tmp/data"

replication_mode = "none"

rpc_bind_addr = "[::]:3901"

bootstrap_peers = []

[s3_api]
s3_region = "garage"
api_bind_addr = "[::]:3900"

[s3_web]
bind_addr = "[::]:3902"
root_domain = ".web.garage"
index = "index.html"
```

Save your configuration file as `garage.toml`.

As you can see in the `metadata_dir` and `data_dir` parameters, we are saving Garage's data
in `/tmp` which gets erased when your system reboots. This means that data stored on this
Garage server will not be persistent. Change these to locations on your HDD if you want
your data to be persisted properly.

## Launching the Garage server

#### Option 1: directly (without Docker)

Use the following command to launch the Garage server with our configuration file:

```
garage server -c garage.toml
```

By default, Garage displays almost no output. You can tune Garage's verbosity as follows
(from less verbose to more verbose):

```
RUST_LOG=garage=info garage server -c garage.toml
RUST_LOG=garage=debug garage server -c garage.toml
RUST_LOG=garage=trace garage server -c garage.toml
```

Log level `info` is recommended for most use cases.
Log level `debug` can help you check why your S3 API calls are not working.

#### Option 2: in a Docker container

Use the following command to start Garage in a docker container:

```
docker run -d \
		-p 3901:3901 -p 3902:3902 -p 3900:3900 \
		-v $PWD/garage.toml:/garage/garage.toml \
		lxpz/garage_amd64:v0.3.0
```

To tune Garage's verbosity level, set the `RUST_LOG` environment variable in the configuration
at launch time. For instance:

```
docker run -d \
		-p 3901:3901 -p 3902:3902 -p 3900:3900 \
		-v $PWD/garage.toml:/garage/garage.toml \
		-e RUST_LOG=garage=info \
		lxpz/garage_amd64:v0.3.0
```

## Checking that Garage runs correctly

The `garage` utility is also used as a CLI tool to configure your Garage deployment.
It tries to connect to a Garage server through the RPC protocol, by default looking
for a Garage server at `localhost:3901`.

Since our deployment already binds to port 3901, the following command should be sufficient
to show Garage's status, provided that you installed the `garage` binary on your host system:

```
garage status
```

Move on to [controlling the Garage daemon](04_control.md) to learn more about how to
use the Garage CLI to control your cluster.

Move on to [configuring your cluster](05_cluster.md) in order to configure
your single-node deployment for actual use!
