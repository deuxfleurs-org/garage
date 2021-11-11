# Quick Start

Let's start your Garage journey!
In this chapter, we explain how to deploy Garage as a single-node server
and how to interact with it.

Our goal is to introduce you to Garage's workflows.
Following this guide is recommended before moving on to
[configuring a real-world deployment](../cookbook/real_world.md).

Note that this kind of deployment should not be used in production, as it provides
no redundancy for your data!

## Get a binary

Download the latest Garage binary from the release pages on our repository:

<https://git.deuxfleurs.fr/Deuxfleurs/garage/releases>

Place this binary somewhere in your `$PATH` so that you can invoke the `garage`
command directly (for instance you can copy the binary in `/usr/local/bin`
or in `~/.local/bin`).

If a binary of the last version is not available for your architecture,
you can [build Garage from source](../cookbook/from_source.md).


## Writing a first configuration file

This first configuration file should allow you to get started easily with the simplest
possible Garage deployment.
**Save it as `/etc/garage.toml`.**
You can also store it somewhere else, but you will have to specify `-c path/to/garage.toml`
at each invocation of the `garage` binary (for example: `garage -c ./garage.toml server`, `garage -c ./garage.toml status`).

```toml
metadata_dir = "/tmp/meta"
data_dir = "/tmp/data"

replication_mode = "none"

rpc_bind_addr = "[::]:3901"
rpc_public_addr = "127.0.0.1:3901"
rpc_secret = "1799bccfd7411eddcf9ebd316bc1f5287ad12a68094e1c6ac6abde7e6feae1ec"

bootstrap_peers = []

[s3_api]
s3_region = "garage"
api_bind_addr = "[::]:3900"
root_domain = ".s3.garage"

[s3_web]
bind_addr = "[::]:3902"
root_domain = ".web.garage"
index = "index.html"
```

The `rpc_secret` value provided above is just an example. It will work, but in
order to secure your cluster you will need to use another one. You can generate
such a value with `openssl rand -hex 32`.


As you can see in the `metadata_dir` and `data_dir` parameters, we are saving Garage's data
in `/tmp` which gets erased when your system reboots. This means that data stored on this
Garage server will not be persistent. Change these to locations on your local disk if you want
your data to be persisted properly.


## Launching the Garage server

Use the following command to launch the Garage server with our configuration file:

```
RUST_LOG=garage=info garage server
```

You can tune Garage's verbosity as follows (from less verbose to more verbose):

```
RUST_LOG=garage=info garage server
RUST_LOG=garage=debug garage server
RUST_LOG=garage=trace garage server
```

Log level `info` is recommended for most use cases.
Log level `debug` can help you check why your S3 API calls are not working.


## Checking that Garage runs correctly

The `garage` utility is also used as a CLI tool to configure your Garage deployment.
It uses values from the TOML configuration file to find the Garage daemon running on the
local node, therefore if your configuration file is not at `/etc/garage.toml` you will
again have to specify `-c path/to/garage.toml`.

If the `garage` CLI is able to correctly detect the parameters of your local Garage node,
the following command should be enough to show the status of your cluster:

```
garage status
```

This should show something like this:

```
==== HEALTHY NODES ====
ID                 Hostname  Address         Tag                   Zone  Capacity
563e1ac825ee3323…  linuxbox  127.0.0.1:3901  NO ROLE ASSIGNED
```

## Configuring your Garage node

Configuring the nodes in a Garage deployment means informing Garage
of the disk space available on each node of the cluster
as well as the zone (e.g. datacenter) each machine is located in.

For our test deployment, we are using only one node. The way in which we configure
it does not matter, you can simply write:

```bash
garage node configure -z dc1 -c 1 <node_id>
```

where `<node_id>` corresponds to the identifier of the node shown by `garage status` (first column).
You can enter simply a prefix of that identifier.
For instance here you could write just `garage node configure -z dc1 -c 1 563e`.




## Creating buckets and keys

In this section, we will suppose that we want to create a bucket named `nextcloud-bucket`
that will be accessed through a key named `nextcloud-app-key`.

Don't forget that `help` command and `--help` subcommands can help you anywhere,
the CLI tool is self-documented! Two examples:

```
garage help
garage bucket allow --help
```

#### Create a bucket

Let's take an example where we want to deploy NextCloud using Garage as the
main data storage.

First, create a bucket with the following command:

```
garage bucket create nextcloud-bucket
```

Check that everything went well:

```
garage bucket list
garage bucket info nextcloud-bucket
```

#### Create an API key

The `nextcloud-bucket` bucket now exists on the Garage server,
however it cannot be accessed until we add an API key with the proper access rights.

Note that API keys are independent of buckets:
one key can access multiple buckets, multiple keys can access one bucket.

Create an API key using the following command:

```
garage key new --name nextcloud-app-key
```

The output should look as follows:

```
Key name: nextcloud-app-key
Key ID: GK3515373e4c851ebaad366558
Secret key: 7d37d093435a41f2aab8f13c19ba067d9776c90215f56614adad6ece597dbb34
Authorized buckets:
```

Check that everything works as intended:

```
garage key list
garage key info nextcloud-app-key
```

#### Allow a key to access a bucket

Now that we have a bucket and a key, we need to give permissions to the key on the bucket:

```
garage bucket allow \
  --read \
  --write 
  nextcloud-bucket \
  --key nextcloud-app-key
```

You can check at any time the allowed keys on your bucket with:

```
garage bucket info nextcloud-bucket
```


## Uploading and downlading from Garage

We recommend the use of MinIO Client to interact with Garage files (`mc`).
Instructions to install it and use it are provided on the
[MinIO website](https://docs.min.io/docs/minio-client-quickstart-guide.html).
Before reading the following, you need a working `mc` command on your path.

Note that on certain Linux distributions such as Arch Linux, the Minio client binary
is called `mcli` instead of `mc` (to avoid name clashes with the Midnight Commander).

#### Configure `mc`

You need your access key and secret key created above.
We will assume you are invoking `mc` on the same machine as the Garage server,
your S3 API endpoint is therefore `http://127.0.0.1:3900`.
For this whole configuration, you must set an alias name: we chose `my-garage`, that you will used for all commands.

Adapt the following command accordingly and run it:

```bash
mc alias set \
  my-garage \
  http://127.0.0.1:3900 \
  <access key> \
  <secret key> \
  --api S3v4
```

You must also add an environment variable to your configuration to
inform MinIO of our region (`garage` by default, corresponding to the `s3_region` parameter
in the configuration file).
The best way is to add the following snippet to your `$HOME/.bash_profile`
or `$HOME/.bashrc` file:

```bash
export MC_REGION=garage
```

#### Use `mc`

You can not list buckets from `mc` currently.

But the following commands and many more should work:

```bash
mc cp image.png my-garage/nextcloud-bucket
mc cp my-garage/nextcloud-bucket/image.png .
mc ls my-garage/nextcloud-bucket
mc mirror localdir/ my-garage/another-bucket
```


#### Other tools for interacting with Garage

The following tools can also be used to send and recieve files from/to Garage:

- the [AWS CLI](https://aws.amazon.com/cli/)
- [`rclone`](https://rclone.org/)
- [Cyberduck](https://cyberduck.io/)
- [`s3cmd`](https://s3tools.org/s3cmd)

Refer to the ["configuring clients"](../cookbook/clients.md) page to learn how to configure
these clients to interact with a Garage server.
