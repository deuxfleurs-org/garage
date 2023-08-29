+++
title = "Binary packages"
weight = 11
+++

Garage is also available in binary packages on:

## Alpine Linux

If you use Alpine Linux, you can simply install the
[garage](https://pkgs.alpinelinux.org/packages?name=garage) package from the
Alpine Linux repositories (available since v3.17):

```bash
apk add garage
```

The default configuration file is installed to `/etc/garage.toml`. You can run
Garage using: `rc-service garage start`. If you don't specify `rpc_secret`, it
will be automatically replaced with a random string on the first start.

Please note that this package is built without Consul discovery, Kubernetes
discovery, OpenTelemetry exporter, and K2V features (K2V will be enabled once
it's stable).


## Arch Linux

Garage is available in the [AUR](https://aur.archlinux.org/packages/garage).

## FreeBSD

```bash
pkg install garage
```

## NixOS

```bash
nix-shell -p garage
```
