+++
title = "Quick Start"
weight = 10
sort_by = "weight"
template = "documentation.html"
+++

Let's start your Garage journey!
In this chapter, we explain how to deploy Garage as a single-node server
and how to interact with it.

## What is Garage?

Before jumping in, you might be interested in reading the following pages:

- [Goals and use cases](@/documentation/design/goals.md)
- [List of features](@/documentation/reference-manual/features.md)

## Scope of this tutorial

Our goal is to introduce you to Garage's workflows.
Following this guide is recommended before moving on to
[configuring a multi-node cluster](@/documentation/cookbook/real-world.md).

Note that this kind of deployment should not be used in production,
as it provides no redundancy for your data!

## Get a binary

Download the latest Garage binary from the release pages on our repository:

<https://garagehq.deuxfleurs.fr/download/>

Place this binary somewhere in your `$PATH` so that you can invoke the `garage`
command directly (for instance you can copy the binary in `/usr/local/bin`
or in `~/.local/bin`).

You may also check whether your distribution already includes a
[binary package for Garage](@/documentation/cookbook/binary-packages.md).

If a binary of the last version is not available for your architecture,
or if you want a build customized for your system,
you can [build Garage from source](@/documentation/cookbook/from-source.md).


## Configuring and starting Garage

### Generating a first configuration file

This first configuration file should allow you to get started easily with the simplest
possible Garage deployment.

We will create it with the following command line
to generate unique and private secrets for security reasons:

```bash
cat > garage.toml <<EOF
metadata_dir = "/tmp/meta"
data_dir = "/tmp/data"
db_engine = "lmdb"

replication_factor = 1

rpc_bind_addr = "[::]:3901"
rpc_public_addr = "127.0.0.1:3901"
rpc_secret = "$(openssl rand -hex 32)"

[s3_api]
s3_region = "garage"
api_bind_addr = "[::]:3900"
root_domain = ".s3.garage.localhost"

[s3_web]
bind_addr = "[::]:3902"
root_domain = ".web.garage.localhost"
index = "index.html"

[k2v_api]
api_bind_addr = "[::]:3904"

[admin]
api_bind_addr = "[::]:3903"
admin_token = "$(openssl rand -base64 32)"
metrics_token = "$(openssl rand -base64 32)"
EOF
```

Now that your configuration file has been created, you may save it to the directory of your choice.
By default, Garage looks for **`/etc/garage.toml`.**
You can also store it somewhere else, but you will have to specify `-c path/to/garage.toml`
at each invocation of the `garage` binary (for example: `garage -c ./garage.toml server`, `garage -c ./garage.toml status`).

As you can see, the `rpc_secret` is a 32 bytes hexadecimal string.
You can regenerate it with `openssl rand -hex 32`.
If you target a cluster deployment with multiple nodes, make sure that
you use the same value for all nodes.

As you can see in the `metadata_dir` and `data_dir` parameters, we are saving Garage's data
in `/tmp` which gets erased when your system reboots. This means that data stored on this
Garage server will not be persistent. Change these to locations on your local disk if you want
your data to be persisted properly.


### Launching the Garage server

Use the following command to launch the Garage server:

```
garage -c path/to/garage.toml server
```

If you have placed the `garage.toml` file in `/etc` (its default location), you can simply run `garage server`.

You can tune Garage's verbosity by setting the `RUST_LOG=` environment variable. \
Available log levels are (from less verbose to more verbose): `error`, `warn`, `info` *(default)*, `debug` and `trace`.

```bash
RUST_LOG=garage=info garage server # default
RUST_LOG=garage=debug garage server
RUST_LOG=garage=trace garage server
```

Log level `info` is the default value and is recommended for most use cases.
Log level `debug` can help you check why your S3 API calls are not working.


### Checking that Garage runs correctly

The `garage` utility is also used as a CLI tool to configure your Garage deployment.
It uses values from the TOML configuration file to find the Garage daemon running on the
local node, therefore if your configuration file is not at `/etc/garage.toml` you will
again have to specify `-c path/to/garage.toml` at each invocation.

If the `garage` CLI is able to correctly detect the parameters of your local Garage node,
the following command should be enough to show the status of your cluster:

```
garage status
```

This should show something like this:

```
==== HEALTHY NODES ====
ID                 Hostname  Address         Tag                   Zone  Capacity
563e1ac825ee3323   linuxbox  127.0.0.1:3901  NO ROLE ASSIGNED
```

## Creating a cluster layout

Creating a cluster layout for a Garage deployment means informing Garage
of the disk space available on each node of the cluster
as well as the zone (e.g. datacenter) each machine is located in.

For our test deployment, we are using only one node. The way in which we configure
it does not matter, you can simply write:

```bash
garage layout assign -z dc1 -c 1G <node_id>
```

where `<node_id>` corresponds to the identifier of the node shown by `garage status` (first column).
You can enter simply a prefix of that identifier.
For instance here you could write just `garage layout assign -z dc1 -c 1G 563e`.

The layout then has to be applied to the cluster, using:

```bash
garage layout apply
```


## Creating buckets and keys

In this section, we will suppose that we want to create a bucket named `nextcloud-bucket`
that will be accessed through a key named `nextcloud-app-key`.

Don't forget that `help` command and `--help` subcommands can help you anywhere,
the CLI tool is self-documented! Two examples:

```
garage help
garage bucket allow --help
```

### Create a bucket

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

### Create an API key

The `nextcloud-bucket` bucket now exists on the Garage server,
however it cannot be accessed until we add an API key with the proper access rights.

Note that API keys are independent of buckets:
one key can access multiple buckets, multiple keys can access one bucket.

Create an API key using the following command:

```
garage key create nextcloud-app-key
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

### Allow a key to access a bucket

Now that we have a bucket and a key, we need to give permissions to the key on the bucket:

```
garage bucket allow \
  --read \
  --write \
  --owner \
  nextcloud-bucket \
  --key nextcloud-app-key
```

You can check at any time the allowed keys on your bucket with:

```
garage bucket info nextcloud-bucket
```


## Uploading and downloading from Garage

To download and upload files on garage, we can use a third-party tool named `awscli`.


### Install and configure `awscli`

If you have python on your system, you can install it with:

```bash
python -m pip install --user awscli
```

Now that `awscli` is installed, you must configure it to talk to your Garage instance,
with your key. There are multiple ways to do that, the simplest one is to create a file
named `~/.awsrc` with this content:

```bash
export AWS_ACCESS_KEY_ID=xxxx      # put your Key ID here
export AWS_SECRET_ACCESS_KEY=xxxx  # put your Secret key here
export AWS_DEFAULT_REGION='garage'
export AWS_ENDPOINT_URL='http://localhost:3900'

aws --version
```

Note you need to have at least `awscli` `>=1.29.0` or `>=2.13.0`, otherwise you
need to specify `--endpoint-url` explicitly on each `awscli` invocation.

Now, each time you want to use `awscli` on this target, run:

```bash
source ~/.awsrc
```

*You can create multiple files with different names if you 
have multiple Garage clusters or different keys.
Switching from one cluster to another is as simple as
sourcing the right file.*

### Example usage of `awscli`

```bash
# list buckets
aws s3 ls

# list objects of a bucket
aws s3 ls s3://nextcloud-bucket

# copy from your filesystem to garage
aws s3 cp /proc/cpuinfo s3://nextcloud-bucket/cpuinfo.txt

# copy from garage to your filesystem
aws s3 cp s3://nextcloud-bucket/cpuinfo.txt /tmp/cpuinfo.txt
```

Note that you can use `awscli` for more advanced operations like
creating a bucket, pre-signing a request or managing your website.
[Read the full documentation to know more](https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3/index.html).

Some features are however not implemented like ACL or policy.
Check [our s3 compatibility list](@/documentation/reference-manual/s3-compatibility.md).

### Other tools for interacting with Garage

The following tools can also be used to send and recieve files from/to Garage:

- [minio-client](@/documentation/connect/cli.md#minio-client) 
- [s3cmd](@/documentation/connect/cli.md#s3cmd) 
- [rclone](@/documentation/connect/cli.md#rclone)
- [Cyberduck](@/documentation/connect/cli.md#cyberduck)
- [WinSCP](@/documentation/connect/cli.md#winscp) 

An exhaustive list is maintained in the ["Integrations" > "Browsing tools" section](@/documentation/connect/_index.md).
