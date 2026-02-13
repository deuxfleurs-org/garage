
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

#### `garage_cluster_healthy` (gauge)

Whether all storage nodes are connected (0 or 1)

```
garage_cluster_healthy 0
```

#### `garage_cluster_available` (gauge)

Whether all requests can be served, even if some storage nodes are disconnected

```
garage_cluster_available 1
```

#### `garage_cluster_connected_nodes` (gauge)

Number of nodes currently connected

```
garage_cluster_connected_nodes 3
```

#### `garage_cluster_known_nodes` (gauge)

Number of nodes already seen once in the cluster

```
garage_cluster_known_nodes 3
```

#### `garage_cluster_layout_node_connected` (gauge)

Connection status for individual nodes of the cluster layout

```
garage_cluster_layout_node_connected{id="62b218d848e86a64",role_capacity="1000000000",role_gateway="0",role_zone="dc1"} 1
garage_cluster_layout_node_connected{id="a11c7cf18af29737",role_capacity="1000000000",role_gateway="0",role_zone="dc1"} 0
garage_cluster_layout_node_connected{id="a235ac7695e0c54d",role_capacity="1000000000",role_gateway="0",role_zone="dc1"} 1
garage_cluster_layout_node_connected{id="b10c110e4e854e5a",role_capacity="1000000000",role_gateway="0",role_zone="dc1"} 1
```

#### `garage_cluster_layout_node_disconnected_time` (gauge)

Time (in seconds) since last connection to individual nodes of the cluster layout

```
garage_cluster_layout_node_disconnected_time{id="62b218d848e86a64",role_capacity="1000000000",role_gateway="0",role_zone="dc1"} 0
garage_cluster_layout_node_disconnected_time{id="a235ac7695e0c54d",role_capacity="1000000000",role_gateway="0",role_zone="dc1"} 0
garage_cluster_layout_node_disconnected_time{id="b10c110e4e854e5a",role_capacity="1000000000",role_gateway="0",role_zone="dc1"} 0
```

#### `garage_cluster_storage_nodes` (gauge)

Number of storage nodes declared in the current layout

```
garage_cluster_storage_nodes 4
```

#### `garage_cluster_storage_nodes_ok` (gauge)

Number of storage nodes currently connected

```
garage_cluster_storage_nodes_ok 3
```

#### `garage_cluster_partitions` (gauge)

Number of partitions in the layout (this is always 256)

```
garage_cluster_partitions 256
```

#### `garage_cluster_partitions_all_ok` (gauge)

Number of partitions for which all storage nodes are connected

```
garage_cluster_partitions_all_ok 64
```

#### `garage_cluster_partitions_quorum` (gauge)

Number of partitions for which we have a quorum of connected nodes and all requests can be served

```
garage_cluster_partitions_quorum 256
```

### Metrics of the API endpoints

#### `garage_api_admin_request_counter` (counter)

Counts the number of requests to a given endpoint of the administration API. Example:

```
garage_api_admin_request_counter{api_endpoint="Metrics"} 127041
```

#### `garage_api_admin_request_duration` (histogram)

Evaluates the duration of API calls to the various administration API endpoint. Example:

```
garage_api_admin_request_duration_bucket{api_endpoint="Metrics",le="0.5"} 127041
garage_api_admin_request_duration_sum{api_endpoint="Metrics"} 605.250344830999
garage_api_admin_request_duration_count{api_endpoint="Metrics"} 127041
```

#### `garage_api_s3_request_counter` (counter)

Counts the number of requests to a given endpoint of the S3 API. Example:

```
garage_api_s3_request_counter{api_endpoint="CreateMultipartUpload"} 1
```

#### `garage_api_s3_error_counter` (counter)

Counts the number of requests to a given endpoint of the S3 API that returned an error. Example:

```
garage_api_s3_error_counter{api_endpoint="GetObject",status_code="404"} 39
```

#### `garage_api_s3_request_duration` (histogram)

Evaluates the duration of API calls to the various S3 API endpoints. Example:

```
garage_api_s3_request_duration_bucket{api_endpoint="CreateMultipartUpload",le="0.5"} 1
garage_api_s3_request_duration_sum{api_endpoint="CreateMultipartUpload"} 0.046340762
garage_api_s3_request_duration_count{api_endpoint="CreateMultipartUpload"} 1
```

#### `garage_api_k2v_request_counter` (counter), `garage_api_k2v_error_counter` (counter), `garage_api_k2v_error_duration` (histogram)

Same as for S3, for the K2V API.


### Metrics of the Web endpoint


#### `garage_web_request_counter` (counter)

Number of requests to the web endpoint

```
garage_web_request_counter{method="GET"} 80
```

#### `garage_web_request_duration` (histogram)

Duration of requests to the web endpoint

```
garage_web_request_duration_bucket{method="GET",le="0.5"} 80
garage_web_request_duration_sum{method="GET"} 1.0528433229999998
garage_web_request_duration_count{method="GET"} 80
```

#### `garage_web_error_counter` (counter)

Number of requests to the web endpoint resulting in errors

```
garage_web_error_counter{method="GET",status_code="404 Not Found"} 64
```


### Metrics of the data block manager

#### `garage_block_bytes_read`, `garage_block_bytes_written` (counter)

Number of bytes read/written to/from disk in the data storage directory.

```
garage_block_bytes_read 120586322022
garage_block_bytes_written 3386618077
```

#### `garage_block_ram_buffer_free_kb` (gauge)

Kibibytes available for buffering blocks that have to be sent to remote nodes.
When clients send too much data to this node and a storage node is not receiving
data fast enough due to slower network conditions, this will decrease down to
zero and backpressure will be applied.

```
garage_block_ram_buffer_free_kb 219829
```

#### `garage_block_compression_level` (counter)

Exposes the block compression level configured for the Garage node.

```
garage_block_compression_level 3
```

#### `garage_block_read_duration`, `garage_block_write_duration` (histograms)

Evaluates the duration of the reading/writing of individual data blocks in the data storage directory.

```
garage_block_read_duration_bucket{le="0.5"} 169229
garage_block_read_duration_sum 2761.6902550310056
garage_block_read_duration_count 169240
garage_block_write_duration_bucket{le="0.5"} 3559
garage_block_write_duration_sum 195.59170078500006
garage_block_write_duration_count 3571
```

#### `garage_block_delete_counter` (counter)

Counts the number of data blocks that have been deleted from storage.

```
garage_block_delete_counter 122
```

#### `garage_block_resync_counter` (counter), `garage_block_resync_duration` (histogram)

Counts the number of resync operations the node has executed, and evaluates their duration.

```
garage_block_resync_counter 308897
garage_block_resync_duration_bucket{le="0.5"} 308892
garage_block_resync_duration_sum 139.64204196100016
garage_block_resync_duration_count 308897
```

#### `garage_block_resync_queue_length` (gauge)

The number of block hashes currently queued for a resync.
This is normal to be nonzero for long periods of time.

```
garage_block_resync_queue_length 0
```

#### `garage_block_resync_errored_blocks` (gauge)

The number of block hashes that we were unable to resync last time we tried.
**THIS SHOULD BE ZERO, OR FALL BACK TO ZERO RAPIDLY, IN A HEALTHY CLUSTER.**
Persistent nonzero values indicate that some data is likely to be lost.

```
garage_block_resync_errored_blocks 0
```


### Metrics related to RPCs (remote procedure calls) between nodes

#### `garage_rpc_netapp_request_counter` (counter)

Number of RPC requests emitted

```
garage_rpc_request_counter{from="<this node>",rpc_endpoint="garage_block/manager.rs/Rpc",to="<remote node>"} 176
```

#### `garage_rpc_netapp_error_counter` (counter)

Number of communication errors (errors in the Netapp library, generally due to disconnected nodes)

```
garage_rpc_netapp_error_counter{from="<this node>",rpc_endpoint="garage_block/manager.rs/Rpc",to="<remote node>"} 354
```

#### `garage_rpc_timeout_counter` (counter)

Number of RPC timeouts, should be close to zero in a healthy cluster.

```
garage_rpc_timeout_counter{from="<this node>",rpc_endpoint="garage_rpc/membership.rs/SystemRpc",to="<remote node>"} 1
```

#### `garage_rpc_duration` (histogram)

The duration of internal RPC calls between Garage nodes.

```
garage_rpc_duration_bucket{from="<this node>",rpc_endpoint="garage_block/manager.rs/Rpc",to="<remote node>",le="0.5"} 166
garage_rpc_duration_sum{from="<this node>",rpc_endpoint="garage_block/manager.rs/Rpc",to="<remote node>"} 35.172253716
garage_rpc_duration_count{from="<this node>",rpc_endpoint="garage_block/manager.rs/Rpc",to="<remote node>"} 174
```


### Metrics of the metadata table manager

#### `garage_table_gc_todo_queue_length` (gauge)

Table garbage collector TODO queue length

```
garage_table_gc_todo_queue_length{table_name="block_ref"} 0
```

#### `garage_table_get_request_counter` (counter), `garage_table_get_request_duration` (histogram)

Number of get/get_range requests internally made on each table, and their duration.

```
garage_table_get_request_counter{table_name="bucket_alias"} 315
garage_table_get_request_duration_bucket{table_name="bucket_alias",le="0.5"} 315
garage_table_get_request_duration_sum{table_name="bucket_alias"} 0.048509778000000024
garage_table_get_request_duration_count{table_name="bucket_alias"} 315
```


#### `garage_table_put_request_counter` (counter), `garage_table_put_request_duration` (histogram)

Number of insert/insert_many requests internally made on this table, and their duration

```
garage_table_put_request_counter{table_name="block_ref"} 677
garage_table_put_request_duration_bucket{table_name="block_ref",le="0.5"} 677
garage_table_put_request_duration_sum{table_name="block_ref"} 61.617528636
garage_table_put_request_duration_count{table_name="block_ref"} 677
```

#### `garage_table_internal_delete_counter` (counter)

Number of value deletions in the tree (due to GC or repartitioning)

```
garage_table_internal_delete_counter{table_name="block_ref"} 2296
```

#### `garage_table_internal_update_counter` (counter)

Number of value updates where the value actually changes (includes creation of new key and update of existing key)

```
garage_table_internal_update_counter{table_name="block_ref"} 5996
```

#### `garage_table_merkle_updater_todo_queue_length` (gauge)

Merkle tree updater TODO queue length (should fall to zero rapidly)

```
garage_table_merkle_updater_todo_queue_length{table_name="block_ref"} 0
```

#### `garage_table_sync_items_received`, `garage_table_sync_items_sent` (counters)

Number of data items sent to/received from other nodes during resync procedures

```
garage_table_sync_items_received{from="<remote node>",table_name="bucket_v2"} 3
garage_table_sync_items_sent{table_name="block_ref",to="<remote node>"} 2
```
