+++
title = "Compiling Garage from source"
weight = 10
+++


Garage is a standard Rust project.  First, you need `rust` and `cargo`.  For instance on Debian:

```bash
sudo apt-get update
sudo apt-get install -y rustc cargo
```

You can also use [Rustup](https://rustup.rs/) to setup a Rust toolchain easily.

In addition, you will need a full C toolchain. On Debian-based distributions, it can be installed as follows:

```bash
sudo apt-get update
sudo apt-get install build-essential
```

## Building from source from the Gitea repository

The primary location for Garage's source code is the
[Gitea repository](https://git.deuxfleurs.fr/Deuxfleurs/garage),
which contains all of the released versions as well as the code
for the developpement of the next version.

Clone the repository and enter it as follows:

```bash
git clone https://git.deuxfleurs.fr/Deuxfleurs/garage.git
cd garage
```

If you wish to build a specific version of Garage, check out the corresponding tag. For instance:

```bash
git tag  				# List available tags
git checkout v0.8.0		# Change v0.8.0 with the version you wish to build
```

Otherwise you will be building a developpement build from the `main` branch
that includes all of the changes to be released in the next version.
Be careful that such a build might be unstable or contain bugs,
and could be incompatible with nodes that run stable versions of Garage.

Finally, build Garage with the following command:

```bash
cargo build --release
```

The binary built this way can now be found in `target/release/garage`.
You may simply copy this binary to somewhere in your `$PATH` in order to
have the `garage` command available in your shell, for instance:

```bash
sudo cp target/release/garage /usr/local/bin/garage
```

If you are planning to develop Garage,
you might be interested in producing debug builds, which compile faster but run slower:
this can be done by removing the `--release` flag, and the resulting build can then
be found in `target/debug/garage`.

## List of available Cargo feature flags

Garage supports a number of compilation options in the form of Cargo feature flags,
which can be used to provide builds adapted to your system and your use case.
To produce a build with a given set of features, invoke the `cargo build` command
as follows:

```bash
# This will build the default feature set plus feature1, feature2 and feature3
cargo build --release --features feature1,feature2,feature3
# This will build ONLY feature1, feature2 and feature3
cargo build --release --no-default-features \
            --features feature1,feature2,feature3
```

The following feature flags are available in v0.8.0:

| Feature flag | Enabled | Description |
| ------------ | ------- | ----------- |
| `bundled-libs` | *by default* | Use bundled version of sqlite3, zstd, lmdb and libsodium |
| `system-libs` | optional | Use system version of sqlite3, zstd, lmdb and libsodium<br>if available (exclusive with `bundled-libs`, build using<br>`cargo build --no-default-features --features system-libs`) |
| `k2v` | optional | Enable the experimental K2V API (if used, all nodes on your<br>Garage cluster must have it enabled as well) |
| `kubernetes-discovery` | optional | Enable automatic registration and discovery<br>of cluster nodes through the Kubernetes API |
| `metrics` | *by default* | Enable collection of metrics in Prometheus format on the admin API |
| `telemetry-otlp` | optional | Enable collection of execution traces using OpenTelemetry |
| `syslog` | optional | Enable logging to Syslog |
| `lmdb` | *by default* | Enable using LMDB to store Garage's metadata |
| `sqlite` | *by default* | Enable using Sqlite3 to store Garage's metadata |
