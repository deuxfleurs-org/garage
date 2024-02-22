+++
title = "Monitoring Garage"
weight = 40
+++

Garage exposes some internal metrics in the Prometheus data format.
This page explains how to exploit these metrics.

## Setting up monitoring

### Enabling the Admin API endpoint

If you have not already enabled the [administration API endpoint](@/documentation/reference-manual/admin-api.md), do so by adding the following lines to your configuration file:

```toml
[admin]
api_bind_addr = "0.0.0.0:3903"
```

This will allow anyone to scrape Prometheus metrics by fetching
`http://localhost:3903/metrics`. If you want to restrict access
to the exported metrics, set the `metrics_token` configuration value
to a bearer token to be used when fetching the metrics endpoint.

### Setting up Prometheus and Grafana

Add a scrape config to your Prometheus daemon to scrape metrics from
all of your nodes:

```yaml
scrape_configs:
  - job_name: 'garage'
    static_configs:
      - targets:
        - 'node1.mycluster:3903'
        - 'node2.mycluster:3903'
        - 'node3.mycluster:3903'
```

If you have set a metrics token in your Garage configuration file,
add the following lines in your Prometheus scrape config:

```yaml
    authorization:
      type: Bearer
      credentials: 'your metrics token'
```

To visualize the scraped data in Grafana,
you can either import our [Grafana dashboard for Garage](https://git.deuxfleurs.fr/Deuxfleurs/garage/raw/branch/main/script/telemetry/grafana-garage-dashboard-prometheus.json)
or make your own.

The list of exported metrics is available on our [dedicated page](@/documentation/reference-manual/monitoring.md) in the Reference manual section.
