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

## Using source from the Gitea repository (recommended)

The primary location for Garage's source code is the
[Gitea repository](https://git.deuxfleurs.fr/Deuxfleurs/garage).

Clone the repository and build Garage with the following commands:

```bash
git clone https://git.deuxfleurs.fr/Deuxfleurs/garage.git
cd garage
cargo build
```

Be careful, as this will make a debug build of Garage, which will be extremely slow!
To make a release build, invoke `cargo build --release` (this takes much longer).

The binaries built this way are found in `target/{debug,release}/garage`.

## Using source from `crates.io`

Garage's source code is published on `crates.io`, Rust's official package repository.
This means you can simply ask `cargo` to download and build this source code for you:

```bash
cargo install garage
```

That's all, `garage` should be in `$HOME/.cargo/bin`.

You can add this folder to your `$PATH` or copy the binary somewhere else on your system.
For instance:

```bash
sudo cp $HOME/.cargo/bin/garage /usr/local/bin/garage
```


## Selecting features to activate in your build

Garage supports a number of compilation options in the form of Cargo features,
which can be used to provide builds adapted to your system and your use case.
The following features are available:

| Feature | Enabled | Description |
| ------- | ------- | ----------- |
| `bundled-libs` | BY DEFAULT | Use bundled version of sqlite3, zstd, lmdb and libsodium |
| `system-libs` | optional | Use system version of sqlite3, zstd, lmdb and libsodium if available (exclusive with `bundled-libs`, build using `cargo build --no-default-features --features system-libs`) |
| `k2v` | optional | Enable the experimental K2V API (if used, all nodes on your Garage cluster must have it enabled as well) |
| `kubernetes-discovery` | optional | Enable automatic registration and discovery of cluster nodes through the Kubernetes API |
| `metrics` | BY DEFAULT | Enable collection of metrics in Prometheus format on the admin API |
| `telemetry-otlp` | optional | Enable collection of execution traces using OpenTelemetry |
| `sled` | BY DEFAULT | Enable using Sled to store Garage's metadata |
| `lmdb` | optional | Enable using LMDB to store Garage's metadata |
| `sqlite` | optional | Enable using Sqlite3 to store Garage's metadata |
