+++
title = "K2V"
weight = 70
+++

Starting with version 0.7.2, Garage introduces an optionnal feature, K2V,
which is an alternative storage API designed to help efficiently store
many small values in buckets (in opposition to S3 which is more designed
to store large blobs).

K2V is currently disabled at compile time in all builds, as the
specification is still subject to changes. To build a Garage version with
K2V, the Cargo feature flag `k2v` must be activated.  Special builds with
the `k2v` feature flag enabled can be obtained from our download page under
"Extra builds": such builds can be identified easily as their tag name ends
with `-k2v` (example: `v0.7.2-k2v`).

The specification of the K2V API can be found
[here](https://git.deuxfleurs.fr/Deuxfleurs/garage/src/branch/k2v/doc/drafts/k2v-spec.md).
This document also includes a high-level overview of K2V's design.

The K2V API uses AWSv4 signatures for authentification, same as the S3 API.
The AWS region used for signature calculation is always the same as the one
defined for the S3 API in the config file.

## Enabling and using K2V

To enable K2V, download and run a build that has the `k2v` feature flag
enabled, or produce one yourself. Then, add the following section to your
configuration file:

```toml
[k2v_api]
api_bind_addr = "<ip>:<port>"
```

Please select a port number that is not already in use by another API
endpoint (S3 api, admin API) or by the RPC server.

We provide an early-stage K2V client library for Rust which can be imported by adding the following to your `Cargo.toml` file:

```toml
k2v-client = { git = "https://git.deuxfleurs.fr/Deuxfleurs/garage.git" }
```

There is also a simple CLI utility which can be built from source in the
following way:

```sh
git clone https://git.deuxfleurs.fr/Deuxfleurs/garage.git
cd garage/src/k2v-client
cargo build --features cli --bin k2v-cli
```

The CLI utility is self-documented, run `k2v-cli --help` to learn how to use
it. There is also a short README.md in the `src/k2v-client` folder with some
instructions.

