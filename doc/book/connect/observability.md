+++
title = "Observability"
weight = 25
+++

An object store can be used as data storage location for metrics, and logs which
can then be leveraged for systems observability.

## Metrics

### Prometheus

Prometheus itself has no object store capabilities, however two projects exist
which support storing metrics in an object store:

 - [Cortex](https://cortexmetrics.io/)
 - [Thanos](https://thanos.io/)

## System logs

### Vector

[Vector](https://vector.dev/) natively supports S3 as a
[data sink](https://vector.dev/docs/reference/configuration/sinks/aws_s3/)
(and [source](https://vector.dev/docs/reference/configuration/sources/aws_s3/)).

This can be configured with Garage with the following:

```bash
garage key new --name vector-system-logs
garage bucket create system-logs
garage bucket allow system-logs --read --write --key vector-system-logs
```

The `vector.toml` can then be configured as follows:

```toml
[sources.journald]
type = "journald"
current_boot_only = true

[sinks.out]
encoding.codec = "json"
type = "aws_s3"
inputs = [ "journald" ]
bucket = "system-logs"
key_prefix = "%F/"
compression = "none"
region = "garage"
endpoint = "https://my-garage-instance.mydomain.tld"
auth.access_key_id = ""
auth.secret_access_key = ""
```

This is an example configuration - please refer to the Vector documentation for
all configuration and transformation possibilities. Also note that Garage
performs its own compression, so this should be disabled in Vector.
