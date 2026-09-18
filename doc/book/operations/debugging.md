+++
title = "Debugging the Docker container"
weight = 45
+++

The official `dxflrs/garage` Docker image is built `FROM scratch`: it contains
nothing but the statically-linked `garage` binary itself. This is a deliberate choice
that keeps the image small (a few MB) and reduces its attack surface, but it
also means that the usual `docker exec -it garage /bin/sh` trick for poking
around inside the container **will not work**:

Here are a few ways to debug a Garage container without a shell.

## Check the logs

Most of the information you need to diagnose a problem is available in the
daemon's logs:

```shell
docker logs garage
```

You can increase verbosity by setting the `RUST_LOG` environment variable
(e.g. `RUST_LOG=garage=debug`) when starting the container. See the
[Reference Manual](@/documentation/reference-manual/configuration.md) for more
about logging configuration.

## Run `garage` commands directly

You don't need a shell to run the `garage` CLI: the container's entrypoint
*is* the `garage` binary, located at `/garage`. You can invoke any subcommand
directly with `docker exec`, using its absolute path:

```shell
docker exec garage /garage status
docker exec garage /garage --help
```

This covers the vast majority of maintenance tasks (checking cluster status,
managing the layout, listing buckets and keys, launching repairs, etc.), as
described throughout the [Operations & Maintenance](@/documentation/operations/_index.md)
and [Cookbook](@/documentation/cookbook/_index.md) sections.

## Inspect the filesystem or processes from another container

If you need to look at files inside the container (e.g. the configuration
file or the contents of the metadata/data directories) or otherwise poke at
the running process, you can attach a throwaway container that shares the
Garage container's namespaces, and use a full-featured image such as `alpine`
or `busybox` to do the inspection:

```bash
docker run --rm -it \
  --pid=container:garage \
  --net=container:garage \
  alpine sh
```

From inside this debug container, the target container's filesystem is
available under `/proc/1/root/`, since PID 1 in the shared
PID namespace is the Garage process:

```bash
ls /proc/1/root/etc/garage.toml
cat /proc/1/root/etc/garage.toml
```

> Note: The filesystem is read only.  

This also lets you use networking tools (`ping`, `curl`, `netstat`, etc.)
as if you were running them from inside the Garage container itself, since
the network namespace is shared as well.

## Build your own image

If none of the above is sufficient and you really need a shell or other
tools available directly inside your Garage container, you can build your
own image on top of a regular distribution, copying the `garage` binary from
the official image or from a [release](@/documentation/cookbook/from-source.md):

```dockerfile
FROM debian:bookworm-slim
COPY --from=dxflrs/garage:v2.4.1 /garage /garage
ENTRYPOINT ["/garage"]
```

Keep in mind that this reintroduces the extra size and attack surface that
the distroless image was designed to avoid, so it is only recommended for
temporary debugging purposes.
