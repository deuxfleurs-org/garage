# Garage telemetry test stack

A small local stack to look at Garage metrics and traces:

- [Prometheus](https://prometheus.io) scrapes Garage metrics from the admin API
- [Jaeger](https://www.jaegertracing.io) receives traces over OTLP
- [Grafana](https://grafana.com) with the Garage dashboard preloaded

## Configure Garage

Build Garage with the `telemetry-otlp` feature to export traces, then set in your config:

```toml
[admin]
api_bind_addr = "0.0.0.0:3903"
trace_sink = "http://localhost:4317"
```

The admin API must listen on an address reachable from containers, not the local loopback.
If you set `metrics_token`, uncomment the `authorization` section in `prometheus.yml`.

## Start the stack

```
cd script/telemetry
docker compose up
```

## Web interfaces

- [Grafana](http://localhost:3000): The *Garage* dashboard is preloaded, and traces can be browsed in Explore with the *Jaeger* data source.
- [Jaeger](http://localhost:16686): trace search UI
- [Prometheus](http://localhost:9090): check that the `garage` target is up under Status → Targets

Traces are kept in memory and are lost when Jaeger restarts.
Metrics are kept for 7 days.
Run `docker compose down -v` to remove all data.

## Upgrades

The versions are managed in the `.env` file.
