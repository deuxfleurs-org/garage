Configure your `[admin-api]` endpoint:

```
[admin]
api_bind_addr = "0.0.0.0:3903"
trace_sink = "http://localhost:4317"
```

Start the test stack:

```
cd telemetry
docker-compose up
```

Access the web interfaces:
  - [Kibana](http://localhost:5601) - Click on the hamburger menu, in the Observability section, click APM
  - [Grafana](http://localhost:3000) - Set a password, then on the left menu, click Dashboard -> Browse. On the new page click Import -> Choose the test dashboard we ship `grafana-garage-dashboard-elasticsearch.json` 



