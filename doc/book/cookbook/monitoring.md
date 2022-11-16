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
`http://localhost:3093/metrics`. If you want to restrict access
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
We detail below the list of exposed metrics and their meaning.



## List of exported metrics


### Metrics of the API endpoints

#### `api_admin_request_counter` (counter)

Counts the number of requests to a given endpoint of the administration API. Example:

```
api_admin_request_counter{api_endpoint="Metrics"} 127041
```

#### `api_admin_request_duration` (histogram)

Evaluates the duration of API calls to the various administration API endpoint. Example:

```
api_admin_request_duration_bucket{api_endpoint="Metrics",le="0.5"} 127041
api_admin_request_duration_sum{api_endpoint="Metrics"} 605.250344830999
api_admin_request_duration_count{api_endpoint="Metrics"} 127041
```

#### `api_s3_request_counter` (counter)

Counts the number of requests to a given endpoint of the S3 API. Example:

```
api_s3_request_counter{api_endpoint="CreateMultipartUpload"} 1
```

#### `api_s3_error_counter` (counter)

Counts the number of requests to a given endpoint of the S3 API that returned an error. Example:

```
api_s3_error_counter{api_endpoint="GetObject",status_code="404"} 39
```

#### `api_s3_request_duration` (histogram)

Evaluates the duration of API calls to the various S3 API endpoints. Example:

```
api_s3_request_duration_bucket{api_endpoint="CreateMultipartUpload",le="0.5"} 1
api_s3_request_duration_sum{api_endpoint="CreateMultipartUpload"} 0.046340762
api_s3_request_duration_count{api_endpoint="CreateMultipartUpload"} 1
```

#### `api_k2v_request_counter` (counter), `api_k2v_error_counter` (counter), `api_k2v_error_duration` (histogram)

Same as for S3, for the K2V API.


### Metrics of the Web endpoint


#### `web_request_counter` (counter)

Number of requests to the web endpoint

```
web_request_counter{method="GET"} 80
```

#### `web_request_duration` (histogram)

Duration of requests to the web endpoint

```
web_request_duration_bucket{method="GET",le="0.5"} 80
web_request_duration_sum{method="GET"} 1.0528433229999998
web_request_duration_count{method="GET"} 80
```

#### `web_error_counter` (counter)

Number of requests to the web endpoint resulting in errors

```
web_error_counter{method="GET",status_code="404 Not Found"} 64
```


### Metrics of the data block manager

#### `block_bytes_read`, `block_bytes_written` (counter)

Number of bytes read/written to/from disk in the data storage directory.

```
block_bytes_read 120586322022
block_bytes_written 3386618077
```

#### `block_read_duration`, `block_write_duration` (histograms)

Evaluates the duration of the reading/writing of individual data blocks in the data storage directory.

```
block_read_duration_bucket{le="0.5"} 169229
block_read_duration_sum 2761.6902550310056
block_read_duration_count 169240
block_write_duration_bucket{le="0.5"} 3559
block_write_duration_sum 195.59170078500006
block_write_duration_count 3571
```

#### `block_delete_counter` (counter)

Counts the number of data blocks that have been deleted from storage.

```
block_delete_counter 122
```

#### `block_resync_counter` (counter), `block_resync_duration` (histogram)

Counts the number of resync operations the node has executed, and evaluates their duration.

```
block_resync_counter 308897
block_resync_duration_bucket{le="0.5"} 308892
block_resync_duration_sum 139.64204196100016
block_resync_duration_count 308897
```

#### `block_resync_queue_length` (gauge)

The number of block hashes currently queued for a resync.
This is normal to be nonzero for long periods of time.

```
block_resync_queue_length 0
```

#### `block_resync_errored_blocks` (gauge)

The number of block hashes that we were unable to resync last time we tried.
**THIS SHOULD BE ZERO, OR FALL BACK TO ZERO RAPIDLY, IN A HEALTHY CLUSTER.**
Persistent nonzero values indicate that some data is likely to be lost.

```
block_resync_errored_blocks 0
```


### Metrics related to RPCs (remote procedure calls) between nodes

#### `rpc_netapp_request_counter` (counter)

Number of RPC requests emitted

```
rpc_request_counter{from="<this node>",rpc_endpoint="garage_block/manager.rs/Rpc",to="<remote node>"} 176
```

#### `rpc_netapp_error_counter` (counter)

Number of communication errors (errors in the Netapp library, generally due to disconnected nodes)

```
rpc_netapp_error_counter{from="<this node>",rpc_endpoint="garage_block/manager.rs/Rpc",to="<remote node>"} 354
```

#### `rpc_timeout_counter` (counter)

Number of RPC timeouts, should be close to zero in a healthy cluster.

```
rpc_timeout_counter{from="<this node>",rpc_endpoint="garage_rpc/membership.rs/SystemRpc",to="<remote node>"} 1
```

#### `rpc_duration` (histogram)

The duration of internal RPC calls between Garage nodes.

```
rpc_duration_bucket{from="<this node>",rpc_endpoint="garage_block/manager.rs/Rpc",to="<remote node>",le="0.5"} 166
rpc_duration_sum{from="<this node>",rpc_endpoint="garage_block/manager.rs/Rpc",to="<remote node>"} 35.172253716
rpc_duration_count{from="<this node>",rpc_endpoint="garage_block/manager.rs/Rpc",to="<remote node>"} 174
```


### Metrics of the metadata table manager

#### `table_gc_todo_queue_length` (gauge)

Table garbage collector TODO queue length

```
table_gc_todo_queue_length{table_name="block_ref"} 0
```

#### `table_get_request_counter` (counter), `table_get_request_duration` (histogram)

Number of get/get_range requests internally made on each table, and their duration.

```
table_get_request_counter{table_name="bucket_alias"} 315
table_get_request_duration_bucket{table_name="bucket_alias",le="0.5"} 315
table_get_request_duration_sum{table_name="bucket_alias"} 0.048509778000000024
table_get_request_duration_count{table_name="bucket_alias"} 315
```


#### `table_put_request_counter` (counter), `table_put_request_duration` (histogram)

Number of insert/insert_many requests internally made on this table, and their duration

```
table_put_request_counter{table_name="block_ref"} 677
table_put_request_duration_bucket{table_name="block_ref",le="0.5"} 677
table_put_request_duration_sum{table_name="block_ref"} 61.617528636
table_put_request_duration_count{table_name="block_ref"} 677
```

#### `table_internal_delete_counter` (counter)

Number of value deletions in the tree (due to GC or repartitioning)

```
table_internal_delete_counter{table_name="block_ref"} 2296
```

#### `table_internal_update_counter` (counter)

Number of value updates where the value actually changes (includes creation of new key and update of existing key)

```
table_internal_update_counter{table_name="block_ref"} 5996
```

#### `table_merkle_updater_todo_queue_length` (gauge)

Merkle tree updater TODO queue length (should fall to zero rapidly)

```
table_merkle_updater_todo_queue_length{table_name="block_ref"} 0
```

#### `table_sync_items_received`, `table_sync_items_sent` (counters)

Number of data items sent to/recieved from other nodes during resync procedures

```
table_sync_items_received{from="<remote node>",table_name="bucket_v2"} 3
table_sync_items_sent{table_name="block_ref",to="<remote node>"} 2
```


