
+++
title = "Monitoring"
weight = 60
+++


For information on setting up monitoring, see our [dedicated page](@/documentation/cookbook/monitoring.md) in the Cookbook section.

## List of exported metrics

### Garage system metrics

#### `garage_build_info` (counter)

Exposes the Garage version number running on a node.

```
garage_build_info{version="1.0"} 1
```

#### `garage_replication_factor` (counter)

Exposes the Garage replication factor configured on the node

```
garage_replication_factor 3
```

#### `garage_local_disk_avail` and `garage_local_disk_total` (gauge)

Reports the available and total disk space on each node, for data and metadata separately.

```
garage_local_disk_avail{volume="data"} 540341960704
garage_local_disk_avail{volume="metadata"} 540341960704
garage_local_disk_total{volume="data"} 763063566336
garage_local_disk_total{volume="metadata"} 763063566336
```

### Cluster health status metrics

#### `cluster_healthy` (gauge)

Whether all storage nodes are connected (0 or 1)

```
cluster_healthy 0
```

#### `cluster_available` (gauge)

Whether all requests can be served, even if some storage nodes are disconnected

```
cluster_available 1
```

#### `cluster_connected_nodes` (gauge)

Number of nodes currently connected

```
cluster_connected_nodes 3
```

#### `cluster_known_nodes` (gauge)

Number of nodes already seen once in the cluster

```
cluster_known_nodes 3
```

#### `cluster_layout_node_connected` (gauge)

Connection status for individual nodes of the cluster layout

```
cluster_layout_node_connected{id="62b218d848e86a64",role_capacity="1000000000",role_gateway="0",role_zone="dc1"} 1
cluster_layout_node_connected{id="a11c7cf18af29737",role_capacity="1000000000",role_gateway="0",role_zone="dc1"} 0
cluster_layout_node_connected{id="a235ac7695e0c54d",role_capacity="1000000000",role_gateway="0",role_zone="dc1"} 1
cluster_layout_node_connected{id="b10c110e4e854e5a",role_capacity="1000000000",role_gateway="0",role_zone="dc1"} 1
```

#### `cluster_layout_node_disconnected_time` (gauge)

Time (in seconds) since last connection to individual nodes of the cluster layout

```
cluster_layout_node_disconnected_time{id="62b218d848e86a64",role_capacity="1000000000",role_gateway="0",role_zone="dc1"} 0
cluster_layout_node_disconnected_time{id="a235ac7695e0c54d",role_capacity="1000000000",role_gateway="0",role_zone="dc1"} 0
cluster_layout_node_disconnected_time{id="b10c110e4e854e5a",role_capacity="1000000000",role_gateway="0",role_zone="dc1"} 0
```

#### `cluster_storage_nodes` (gauge)

Number of storage nodes declared in the current layout

```
cluster_storage_nodes 4
```

#### `cluster_storage_nodes_ok` (gauge)

Number of storage nodes currently connected

```
cluster_storage_nodes_ok 3
```

#### `cluster_partitions` (gauge)

Number of partitions in the layout (this is always 256)

```
cluster_partitions 256
```

#### `cluster_partitions_all_ok` (gauge)

Number of partitions for which all storage nodes are connected

```
cluster_partitions_all_ok 64
```

#### `cluster_partitions_quorum` (gauge)

Number of partitions for which we have a quorum of connected nodes and all requests can be served

```
cluster_partitions_quorum 256
```

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

#### `block_ram_buffer_free_kb` (gauge)

Kibibytes available for buffering blocks that have to be sent to remote nodes.
When clients send too much data to this node and a storage node is not receiving
data fast enough due to slower network conditions, this will decrease down to
zero and backpressure will be applied.

```
block_ram_buffer_free_kb 219829
```

#### `block_compression_level` (counter)

Exposes the block compression level configured for the Garage node.

```
block_compression_level 3
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

Number of data items sent to/received from other nodes during resync procedures

```
table_sync_items_received{from="<remote node>",table_name="bucket_v2"} 3
table_sync_items_sent{table_name="block_ref",to="<remote node>"} 2
```


