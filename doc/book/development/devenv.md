+++
title = "Setup your environment"
weight = 5
+++

Depending on your tastes, you can bootstrap your development environment in a traditional Rust way or through Nix.

## The Nix way

Nix is a generic package manager we use to precisely define our development environment.
Instructions on how to install it are given on their [Download page](https://nixos.org/download.html).

Check that your installation is working by running the following commands:

```
nix-shell --version
nix-build --version
nix-env   --version
```

Now, you can clone our git repository (run `nix-env -iA git` if you do not have git yet):

```bash
git clone https://git.deuxfleurs.fr/Deuxfleurs/garage
cd garage
```

*Optionally, you can use our nix.conf file to speed up compilations:*

```bash
sudo mkdir -p /etc/nix
sudo cp nix/nix.conf /etc/nix/nix.conf
sudo killall nix-daemon
```

Now you can enter our nix-shell, all the required packages will be downloaded but they will not pollute your environment outside of the shell:

```bash
nix-shell -A devShell
```

You can use the traditional Rust development workflow:

```bash
cargo build  # compile the project
cargo run    # execute the project
cargo test   # run the tests
cargo fmt    # format the project, run it before any commit!
cargo clippy # run the linter, run it before any commit!
```

You can build the project with Nix by running:

```bash
nix-build
```

You can parallelize the build (if you use our nix.conf file, it is already automatically done).
To use all your cores when building a derivation use `-j`, and to build multiple derivations at once use `--max-jobs`.
The special value `auto` will be replaced by the number of cores of your computer.
An example:

```bash
nix-build -j $(nproc) --max-jobs auto
```

Our build has multiple parameters you might want to set:
  - `release` to build with release optimisations instead of debug
  - `target` allows for cross compilation
  - `compileMode` can be set to test or bench to build a unit test runner
  - `git_version` to inject the hash to display when running `garage stats`

An example:

```bash
nix-build \
  --arg release true \
  --argstr target x86_64-unknown-linux-musl \
  --argstr compileMode build \
  --git_version $(git rev-parse HEAD)
```

*The result is located in `result/bin`. You can pass arguments to cross compile: check `.woodpecker/release.yml` for examples.*

If you modify a `Cargo.toml` or regenerate any `Cargo.lock`, you must run `cargo2nix`:

```
cargo2nix -f
```

Many tools like rclone, `mc` (minio-client), or `aws` (awscliv2) will be available in your environment and will be useful to test Garage.

**This is the recommended method.**

## The Rust way

You need a Rust distribution installed on your computer.
The most simple way is to install it from [rustup](https://rustup.rs).
Please avoid using your package manager to install Rust as some tools might be outdated or missing.

Now, check your Rust distribution works by running the following commands:

```bash
rustc --version
cargo --version
rustfmt --version
clippy-driver --version
```

Now, you need to clone our git repository ([how to install git](https://git-scm.com/downloads)):

```bash
git clone https://git.deuxfleurs.fr/Deuxfleurs/garage
cd garage
```

You can now use the following commands:

```bash
cargo build  # compile the project
cargo run    # execute the project
cargo test   # run the tests
cargo fmt    # format the project, run it before any commit!
cargo clippy # run the linter, run it before any commit!
```

This is specific to our project, but you will need one last tool, `cargo2nix`.
To install it, run:

```bash
cargo install --git https://github.com/superboum/cargo2nix --branch main cargo2nix
```

You must use it every time you modify a `Cargo.toml` or regenerate a `Cargo.lock` file as follow:

```bash
cargo build  # Rebuild Cargo.lock if needed
cargo2nix -f
```

It will output a `Cargo.nix` file which is a specific `Cargo.lock` file dedicated to Nix that is required by our CI
which means you must include it in your commits.

Later, to use our scripts and integration tests, you might need additional tools.
These tools are listed at the end of the `shell.nix` package in the `nativeBuildInputs` part.
It is up to you to find a way to install the ones you need on your computer.

**A global drawback of this method is that it is up to you to adapt your environment to the one defined in the Nix files.**
