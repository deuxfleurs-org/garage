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

If none of these option work for you, you can also run Garage in a Docker
container.  For simplicity, a minimal command to launch Garage using Docker is
provided in this quick start guide.  We recommend reading the tutorial on
[configuring a multi-node cluster](@/documentation/cookbook/real-world.md) to
learn about the full Docker workflow for Garage.

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
db_engine = "sqlite"

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

[admin]
api_bind_addr = "[::]:3903"
admin_token = "$(openssl rand -base64 32)"
metrics_token = "$(openssl rand -base64 32)"
EOF
```

See the [Configuration file format](https://garagehq.deuxfleurs.fr/documentation/reference-manual/configuration/)
for complete options and values.

By default, Garage looks for its configuration file in **`/etc/garage.toml`.**
Since we have written our configuration file in the working directory, we will have to set
the following environment variable:

```bash
export GARAGE_CONFIG_FILE=$(pwd)/garage.toml
```

As you can see, the `rpc_secret` is a 32 bytes hexadecimal string.
You can regenerate it with `openssl rand -hex 32`.
If you target a cluster deployment with multiple nodes, make sure that
you use the same value for all nodes.

As you can see in the `metadata_dir` and `data_dir` parameters, we are saving Garage's data
in `/tmp` which gets erased when your system reboots. This means that data stored on this
Garage server will not be persistent. Change these to locations on your local disk if you want
your data to be persisted properly.


### Configuring initial access credentials

Since `v2.3.0`, Garage can automatically create a default access key and a default storage bucket,
based on values provided in environment variables.

To use this feature, export the following environment variables:

```bash
export GARAGE_DEFAULT_ACCESS_KEY="GK$(openssl rand -hex 16)"
export GARAGE_DEFAULT_SECRET_KEY="$(openssl rand -hex 32)"
export GARAGE_DEFAULT_BUCKET="default-bucket"
```

The example above creates a random access key ID and associated secret key.
You can also provide an access key ID and secret key of your own.

### Launching the Garage server

Use the following command to launch the Garage server:

```bash
garage server --single-node --default-bucket
```

The `--single-node` flag instructs Garage to automatically configure a single-node cluster without data replication.
The `--default-bucket` flag instructs Garage to create a default access key and a default bucket using the environment variables we defined above.
Both flags are optional and can be omitted, in which case you will have to follow manual configuration steps described below.

**For older versions of Garage (before v2.3.0):** automatic configuration using `--single-node` and `--default-bucket` is not available,
you must follow the manual configuration steps.

Alternatively, if you cannot or do not wish to run the Garage binary directly,
you may use Docker to run Garage in a container using the following command:

```bash
docker run \
  -d \
  --name garage-container \
  -p 3900:3900 -p 3901:3901 -p 3902:3902 -p 3903:3903 \
  -v $(pwd)/garage.toml:/etc/garage.toml \
  -e GARAGE_DEFAULT_ACCESS_KEY \
  -e GARAGE_DEFAULT_SECRET_KEY \
  -e GARAGE_DEFAULT_BUCKET \
  dxflrs/garage:v2.3.0
  /garage server --single-node --default-bucket
```

Note that this command will NOT create persistent volumes for Garage's data, so
your cluster will be wiped if the container terminates.  To persist Garage's
data, you must manually add volumes for the `data` and `metadata` directories
and configure their correct paths in your `garage.toml` files (see [configuring
a multi-node cluster](@/documentation/cookbook/real-world.md)).

Under Linux, you can substitute `--network host` for `-p 3900:3900 -p 3901:3901 -p 3902:3902 -p 3903:3903`.

### Checking that Garage runs correctly

The `garage` utility is also used as a CLI tool to administrate your Garage
deployment.  It needs read access to your configuration file and to the metadata directory
to obtain connection parameters to contact the local Garage node.

Use the following command to show the status of your cluster:

```
garage status
```

If you are running Garage in a Docker container, you can use the following command instead:

NOTE: Garage 3.x uses docker `ENTRYPOINT` and it's easier to use,
while garage 2.x does not and you need to specify path `/garage`

```bash
docker exec garage-container status
```

This should show something like this:

```
==== HEALTHY NODES ====
ID                Hostname  Address         Tags       Zone  Capacity  DataAvail         Version
563e1ac825ee3323  linuxbox  127.0.0.1:3901  [default]  dc1   19.9 GiB  19.5 GiB (97.6%)  v2.3.0
```

### Troubleshooting

Ensure your configuration file, `metadata_dir` and `data_dir`  are readable by the user running the `garage` server or Docker.

When running the `garage` CLI, ensure that the path to your configuration file is correctly specified (see below),
and that it can read it and read from your metadata directory.

You can tune Garage's verbosity by setting the `RUST_LOG=` environment variable.
Available log levels are (from less verbose to more verbose): `error`, `warn`, `info` *(default)*, `debug` and `trace`.

```bash
RUST_LOG=garage=info garage server # default
RUST_LOG=garage=debug garage server
RUST_LOG=garage=trace garage server
```

Log level `info` is the default value and is recommended for most use cases.
Log level `debug` can help you check why your S3 API calls are not working.



## Uploading and downloading from Garage

This section will show how to download and upload files on Garage using a third-party tool named `awscli`.


### Install and configure `awscli`

If you have python on your system, you can install it with:

```bash
python -m pip install --user awscli
```

Now that `awscli` is installed, you must configure it to talk to your Garage
instance using the credentials defined above. Here is a simple way to create
a configuration file in `~/.awsrc` using a single command that will save the
secrets from your environment:

```bash
cat > ~/.awsrc <<EOF
export AWS_ENDPOINT_URL='http://localhost:3900'
export AWS_DEFAULT_REGION='garage'
export AWS_ACCESS_KEY_ID='$GARAGE_DEFAULT_ACCESS_KEY'
export AWS_SECRET_ACCESS_KEY='$GARAGE_DEFAULT_SECRET_KEY'

aws --version
EOF

```

Note that you need to have at least `awscli` `>=1.29.0` or `>=2.13.0`, otherwise you
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
aws s3 ls s3://default-bucket

# copy from your filesystem to garage
aws s3 cp /proc/cpuinfo s3://default-bucket/cpuinfo.txt

# copy from garage to your filesystem
aws s3 cp s3://default-bucket/cpuinfo.txt /tmp/cpuinfo.txt
```

Note that you can use `awscli` for more advanced operations like
creating a bucket, pre-signing a request or managing your website.
[Read the full documentation to know more](https://awscli.amazonaws.com/v2/documentation/api/latest/reference/s3/index.html).

Some features are however not implemented like ACL or policy.
Check [our S3 compatibility list](@/documentation/reference-manual/s3-compatibility.md).

### Other tools for interacting with Garage

The following tools can also be used to send and receive files from/to Garage:

- [minio-client](@/documentation/connect/cli.md#minio-client)
- [s3cmd](@/documentation/connect/cli.md#s3cmd)
- [rclone](@/documentation/connect/cli.md#rclone)
- [Cyberduck](@/documentation/connect/cli.md#cyberduck)
- [WinSCP](@/documentation/connect/cli.md#winscp)

An exhaustive list is maintained in the ["Integrations" > "Browsing tools" section](@/documentation/connect/_index.md).



## Manual configuration

This section provides instructions that are equivalent to using the
`--single-node` and `--default-bucket` flags for automatic configuration.  If
you are using an older version of Garage (before v2.3.0), you must follow
these instructions as automatic configuration is not available.

We will have to run quite a few `garage` administration commands to get started.
If you ever get lost, don't forget that the `help` command and the `--help` flags can help you anywhere,
the CLI tool is self-documented! Two examples:

```
garage help
garage bucket allow --help
```

### Configuring the `garage` CLI

Remember that the `garage` CLI needs to know the path of your `garage.toml` configuration file.
If it is not in the default location of `/etc/garage.toml`, you can specify it either:

- by setting the `GARAGE_CONFIG_FILE` environment variable;
- by adding the `-c` flag to each `garage` command, for example: `garage -c ./garage.toml status`.

If you are running Garage in a Docker container, you can set the following alias
to provide a fake `garage`command that uses the Garage binary inside your container:

```bash
alias garage="docker exec -ti <container name>"
```

You can test that your `garage` CLI is configured correctly by running a basic command such as `garage status`.

### Creating a cluster layout

When you first start a cluster without automatic configuration, the output of `garage status` will look as follows:

```
==== HEALTHY NODES ====
ID                Hostname  Address         Tags  Zone  Capacity          DataAvail  Version
563e1ac825ee3323  linuxbox  127.0.0.1:3901              NO ROLE ASSIGNED             v2.3.0
```

Creating a cluster layout for a Garage deployment means informing Garage of the
disk space available on each node of the cluster using the `-c` flag, as well
as the name of the zone (e.g. datacenter) each machine is located in using the
`-z` flag.

For our test deployment, we are have only one node with zone named `dc1` and a
capacity of `1G`, though the capacity is ignored for a single node deployment
and can be changed later when adding new nodes.

```bash
garage layout assign -z dc1 -c 1G <node_id>
```

where `<node_id>` corresponds to the identifier of the node shown by `garage status` (first column).
You can enter simply a prefix of that identifier.
For instance here you could write just `garage layout assign -z dc1 -c 1G 563e`.

The layout then has to be applied to the cluster, using:

```bash
garage layout apply --version 1
```


### Creating buckets and keys

Let's take an example where we want to deploy NextCloud using Garage as the
main data storage.  We will suppose that we want to create a bucket named
`nextcloud-bucket` that will be accessed through a key named
`nextcloud-app-key`.

#### Create a bucket

First, create the bucket with the following command:

```
garage bucket create nextcloud-bucket
```

Check that the bucket was created properly:

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
garage key create nextcloud-app-key
```

The output should look as follows:

```
Key name: nextcloud-app-key
Key ID: GK3515373e4c851ebaad366558
Secret key: 7d37d093435a41f2aab8f13c19ba067d9776c90215f56614adad6ece597dbb34
Authorized buckets:
```

Check that the key was created properly:

```
garage key list
garage key info nextcloud-app-key
```

#### Allow a key to access a bucket

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

You should now be able to read and write objects to the bucket using the
credentials created above.
