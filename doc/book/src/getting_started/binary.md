# Get a binary

Currently, only two installations procedures are supported for Garage: from Docker (x86\_64 for Linux) and from source.
In the future, we plan to add a third one, by publishing a compiled binary (x86\_64 for Linux).
We did not test other architecture/operating system but, as long as your architecture/operating system is supported by Rust, you should be able to run Garage (feel free to report your tests!).

## From Docker

Our docker image is currently named `lxpz/garage_amd64` and is stored on the [Docker Hub](https://hub.docker.com/r/lxpz/garage_amd64/tags?page=1&ordering=last_updated).
We encourage you to use a fixed tag (eg. `v0.2.1`) and not the `latest` tag.
For this example, we will use the latest published version at the time of the writing which is `v0.2.1` but it's up to you
to check [the most recent versions on the Docker Hub](https://hub.docker.com/r/lxpz/garage_amd64/tags?page=1&ordering=last_updated).

For example:

```
sudo docker pull lxpz/garage_amd64:v0.2.1
```

## From source

Garage is a standard Rust project.
First, you need `rust` and `cargo`.
On Debian:

```bash
sudo apt-get update
sudo apt-get install -y rustc cargo
```

Then, you can ask cargo to install the binary for you:

```bash
cargo install garage
```

That's all, `garage` should be in `$HOME/.cargo/bin`.
You can add this folder to your `$PATH` or copy the binary somewhere else on your system.
For the following, we will assume you copied it in `/usr/local/bin/garage`:

```bash
sudo cp $HOME/.cargo/bin/garage /usr/local/bin/garage
```

