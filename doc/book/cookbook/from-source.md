+++
title = "Compiling Garage from source"
weight = 10
+++


Garage is a standard Rust project.
First, you need `rust` and `cargo`.
For instance on Debian:

```bash
sudo apt-get update
sudo apt-get install -y rustc cargo
```

You can also use [Rustup](https://rustup.rs/) to setup a Rust toolchain easily.

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


## Using source from the Gitea repository

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

