+++
title = "Development scripts"
weight = 10
+++

We maintain a `script/` folder that contains some useful script to ease testing on Garage.

A fully integrated script, `test-smoke.sh`, runs some basic tests on various tools such as minio client, awscli and rclone.
To run it, enter a `nix-shell` (or install all required tools) and simply run:

```bash
nix-build # or cargo build
./script/test-smoke.sh
```

If something fails, you can find useful logs in `/tmp/garage.log`.
You can inspect the generated configuration and local data created by inspecting your `/tmp` directory:
the script creates files and folder prefixed with the name "garage".

## Bootstrapping a test cluster

Under the hood `test-smoke.sh` uses multiple helpers scripts you can also run in case you want to manually test Garage.
In this section, we introduce 3 scripts to quickly bootstrap a full test cluster with 3 instances.

### 1. Start each daemon

```bash
./script/dev-cluster.sh
```

This script spawns 3 Garage instances with 3 configuration files.
You can inspect the detailed configuration, including ports, by inspecting `/tmp/config.1` (change 1 by the instance number you want).

This script also spawns a simple HTTPS reverse proxy through `socat` for the S3 endpoint that listens on port `4443`.
Some libraries might require a TLS endpoint to work, refer to our issue [#64](https://git.deuxfleurs.fr/Deuxfleurs/garage/issues/64) for more detailed information on this subject.

This script covers the [Launching the garage server](@/documentation/quick-start/_index.md#launching-the-garage-server) section of our Quick start page.

### 2. Make them join the cluster

```bash
./script/dev-configure.sh
```

This script will configure each instance by assigning them a zone (`dc1`) and a weight (`1`).

This script covers the [Creating a cluster layout](@/documentation/quick-start/_index.md#creating-a-cluster-layout) section of our Quick start page.

### 3. Create a key and a bucket

```bash
./script/dev-bucket.sh
```

This script will create a bucket named `eprouvette` with a key having read and write rights on this bucket.
The key is stored in a filed named `/tmp/garage.s3` and can be used by the following tools to pre-configure them.

This script covers the [Creating buckets and keys](@/documentation/quick-start/_index.md#creating-buckets-and-keys) section of our Quick start page.

## Handlers for generic tools

We provide wrappers for some CLI tools that configure themselves for your development cluster.
They are meant to save you some configuration time as to use them, you are only required to source the right file.

### awscli

```bash
source ./script/dev-env-aws.sh

# some examples
aws s3 ls s3://eprouvette
aws s3 cp /proc/cpuinfo s3://eprouvette/cpuinfo.txt
```

### minio-client


```bash
source ./script/dev-env-mc.sh

# some examples
mc ls garage/
mc cp /proc/cpuinfo garage/eprouvette/cpuinfo.txt
```

### rclone

```bash
source ./script/dev-env-rclone.sh

# some examples
rclone lsd garage:
rclone copy /proc/cpuinfo garage:eprouvette/cpuinfo.txt
```

### s3cmd

```bash
source ./script/dev-env-s3cmd.sh

# some examples
s3cmd ls
s3cmd put /proc/cpuinfo s3://eprouvette/cpuinfo.txt
```

### duck

*Warning! Duck is not yet provided by nix-shell.*

```bash
source ./script/dev-env-duck.sh

# some examples
duck --list garage:/
duck --upload garage:/eprouvette/ /proc/cpuinfo
```
