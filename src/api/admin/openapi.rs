#![allow(dead_code)]
#![allow(non_snake_case)]

use utoipa::{Modify, OpenApi};

use crate::api::*;

// **********************************************
//      Cluster operations
// **********************************************

#[utoipa::path(get,
    path = "/v2/GetClusterStatus",
    tag = "Cluster",
    description = "
Returns the cluster's current status, including:

- ID of the node being queried and its version of the Garage daemon
- Live nodes
- Currently configured cluster layout
- Staged changes to the cluster layout

*Capacity is given in bytes*
    ",
	responses(
            (status = 200, description = "Cluster status report", body = GetClusterStatusResponse),
            (status = 500, description = "Internal server error")
        ),
)]
fn GetClusterStatus() -> () {}

#[utoipa::path(get,
    path = "/v2/GetClusterHealth",
    tag = "Cluster",
    description = "Returns the global status of the cluster, the number of connected nodes (over the number of known ones), the number of healthy storage nodes (over the declared ones), and the number of healthy partitions (over the total).",
	responses(
            (status = 200, description = "Cluster health report", body = GetClusterHealthResponse),
        ),
)]
fn GetClusterHealth() -> () {}

#[utoipa::path(get,
    path = "/v2/GetClusterStatistics",
    tag = "Cluster",
    description = "
Fetch global cluster statistics.

*Note: do not try to parse the `freeform` field of the response, it is given as a string specifically because its format is not stable.*
    ",
	responses(
            (status = 200, description = "Global cluster statistics", body = GetClusterStatisticsResponse),
            (status = 500, description = "Internal server error")
        ),
)]
fn GetClusterStatistics() -> () {}

#[utoipa::path(post,
    path = "/v2/ConnectClusterNodes",
    tag = "Cluster",
    description = "Instructs this Garage node to connect to other Garage nodes at specified `<node_id>@<net_address>`. `node_id` is generated automatically on node start.",
    request_body=ConnectClusterNodesRequest,
	responses(
            (status = 200, description = "The request has been handled correctly but it does not mean that all connection requests succeeded; some might have fail, you need to check the body!", body = ConnectClusterNodesResponse),
            (status = 500, description = "Internal server error")
        ),
)]
fn ConnectClusterNodes() -> () {}

// **********************************************
//      Layout operations
// **********************************************

#[utoipa::path(get,
    path = "/v2/GetClusterLayout",
    tag = "Cluster layout",
    description = "
Returns the cluster's current layout, including:

- Currently configured cluster layout
- Staged changes to the cluster layout

*Capacity is given in bytes*
    ",
	responses(
            (status = 200, description = "Current cluster layout", body = GetClusterLayoutResponse),
            (status = 500, description = "Internal server error")
        ),
)]
fn GetClusterLayout() -> () {}

#[utoipa::path(get,
    path = "/v2/GetClusterLayoutHistory",
    tag = "Cluster layout",
    description = "
Returns the history of layouts in the cluster
    ",
	responses(
            (status = 200, description = "Cluster layout history", body = GetClusterLayoutHistoryResponse),
            (status = 500, description = "Internal server error")
        ),
)]
fn GetClusterLayoutHistory() -> () {}

#[utoipa::path(post,
    path = "/v2/UpdateClusterLayout",
    tag = "Cluster layout",
    description = "
Send modifications to the cluster layout. These modifications will be included in the staged role changes, visible in subsequent calls of `GET /GetClusterHealth`. Once the set of staged changes is satisfactory, the user may call `POST /ApplyClusterLayout` to apply the changed changes, or `POST /RevertClusterLayout` to clear all of the staged changes in the layout.

Setting the capacity to `null` will configure the node as a gateway.
Otherwise, capacity must be now set in bytes (before Garage 0.9 it was arbitrary weights).
For example to declare 100GB, you must set `capacity: 100000000000`.

Garage uses internally the International System of Units (SI), it assumes that 1kB = 1000 bytes, and displays storage as kB, MB, GB (and not KiB, MiB, GiB that assume 1KiB = 1024 bytes).
    ",
    request_body(
        content=UpdateClusterLayoutRequest,
        description="
To add a new node to the layout or to change the configuration of an existing node, simply set the values you want (`zone`, `capacity`, and `tags`).
To remove a node, simply pass the `remove: true` field.
This logic is represented in OpenAPI with a 'One Of' object.

Contrary to the CLI that may update only a subset of the fields capacity, zone and tags, when calling this API all of these values must be specified.
        "
    ),
	responses(
            (status = 200, description = "Proposed changes have been added to the list of pending changes", body = UpdateClusterLayoutResponse),
            (status = 500, description = "Internal server error")
        ),
)]
fn UpdateClusterLayout() -> () {}

#[utoipa::path(post,
    path = "/v2/PreviewClusterLayoutChanges",
    tag = "Cluster layout",
    description = "
Computes a new layout taking into account the staged parameters, and returns it with detailed statistics. The new layout is not applied in the cluster.

*Note: do not try to parse the `message` field of the response, it is given as an array of string specifically because its format is not stable.*
    ",
	responses(
            (status = 200, description = "Information about the new layout", body = PreviewClusterLayoutChangesResponse),
            (status = 500, description = "Internal server error")
        ),
)]
fn PreviewClusterLayoutChanges() -> () {}

#[utoipa::path(post,
    path = "/v2/ApplyClusterLayout",
    tag = "Cluster layout",
    description = "
Applies to the cluster the layout changes currently registered as staged layout changes.

*Note: do not try to parse the `message` field of the response, it is given as an array of string specifically because its format is not stable.*
    ",
    request_body=ApplyClusterLayoutRequest,
	responses(
            (status = 200, description = "The updated cluster layout has been applied in the cluster", body = ApplyClusterLayoutResponse),
            (status = 500, description = "Internal server error")
        ),
)]
fn ApplyClusterLayout() -> () {}

#[utoipa::path(post,
    path = "/v2/RevertClusterLayout",
    tag = "Cluster layout",
    description = "Clear staged layout changes",
	responses(
            (status = 200, description = "All pending changes to the cluster layout have been erased", body = RevertClusterLayoutResponse),
            (status = 500, description = "Internal server error")
        ),
)]
fn RevertClusterLayout() -> () {}

#[utoipa::path(post,
    path = "/v2/ClusterLayoutSkipDeadNodes",
    tag = "Cluster layout",
    description = "Force progress in layout update trackers",
    request_body = ClusterLayoutSkipDeadNodesRequest,
	responses(
            (status = 200, description = "Request has been taken into account", body = ClusterLayoutSkipDeadNodesResponse),
            (status = 500, description = "Internal server error")
        ),
)]
fn ClusterLayoutSkipDeadNodes() -> () {}

// **********************************************
//      Access key operations
// **********************************************

#[utoipa::path(get,
    path = "/v2/ListKeys",
    tag = "Access key",
    description = "Returns all API access keys in the cluster.",
	responses(
            (status = 200, description = "Returns the key identifier (aka `AWS_ACCESS_KEY_ID`) and its associated, human friendly, name if any (otherwise return an empty string)", body = ListKeysResponse),
            (status = 500, description = "Internal server error")
        ),
)]
fn ListKeys() -> () {}

#[utoipa::path(get,
    path = "/v2/GetKeyInfo",
    tag = "Access key",
    description = "
Return information about a specific key like its identifiers, its permissions and buckets on which it has permissions.
You can search by specifying the exact key identifier (`id`) or by specifying a pattern (`search`).

For confidentiality reasons, the secret key is not returned by default: you must pass the `showSecretKey` query parameter to get it.
    ",
    params(
        ("id", description = "Access key ID"),
        ("search", description = "Partial key ID or name to search for"),
        ("showSecretKey", description = "Whether to return the secret access key"),
    ),
	responses(
            (status = 200, description = "Information about the access key", body = GetKeyInfoResponse),
            (status = 500, description = "Internal server error")
        ),
)]
fn GetKeyInfo() -> () {}

#[utoipa::path(post,
    path = "/v2/CreateKey",
    tag = "Access key",
    description = "Creates a new API access key.",
    request_body = CreateKeyRequest,
	responses(
            (status = 200, description = "Access key has been created", body = CreateKeyResponse),
            (status = 500, description = "Internal server error")
        ),
)]
fn CreateKey() -> () {}

#[utoipa::path(post,
    path = "/v2/ImportKey",
    tag = "Access key",
    description = "
Imports an existing API key. This feature must only be used for migrations and backup restore.

**Do not use it to generate custom key identifiers or you will break your Garage cluster.**
    ",
    request_body = ImportKeyRequest,
	responses(
            (status = 200, description = "Access key has been imported", body = ImportKeyResponse),
            (status = 500, description = "Internal server error")
        ),
)]
fn ImportKey() -> () {}

#[utoipa::path(post,
    path = "/v2/UpdateKey",
    tag = "Access key",
    description = "
Updates information about the specified API access key.

*Note: the secret key is not returned in the response, `null` is sent instead.*
    ",
    request_body = UpdateKeyRequestBody,
    params(
        ("id", description = "Access key ID"),
    ),
	responses(
            (status = 200, description = "Access key has been updated", body = UpdateKeyResponse),
            (status = 500, description = "Internal server error")
        ),
)]
fn UpdateKey() -> () {}

#[utoipa::path(post,
    path = "/v2/DeleteKey",
    tag = "Access key",
    description = "Delete a key from the cluster. Its access will be removed from all the buckets. Buckets are not automatically deleted and can be dangling. You should manually delete them before. ",
    params(
        ("id", description = "Access key ID"),
    ),
	responses(
            (status = 200, description = "Access key has been deleted"),
            (status = 500, description = "Internal server error")
        ),
)]
fn DeleteKey() -> () {}

// **********************************************
//      Bucket operations
// **********************************************

#[utoipa::path(get,
    path = "/v2/ListBuckets",
    tag = "Bucket",
    description = "List all the buckets on the cluster with their UUID and their global and local aliases.",
	responses(
            (status = 200, description = "Returns the UUID of all the buckets and all their aliases", body = ListBucketsResponse),
            (status = 500, description = "Internal server error")
        ),
)]
fn ListBuckets() -> () {}

#[utoipa::path(get,
    path = "/v2/GetBucketInfo",
    tag = "Bucket",
    description = "
Given a bucket identifier (`id`) or a global alias (`alias`), get its information.
It includes its aliases, its web configuration, keys that have some permissions
on it, some statistics (number of objects, size), number of dangling multipart uploads,
and its quotas (if any).
    ",
    params(
        ("id", description = "Exact bucket ID to look up"),
        ("globalAlias", description = "Global alias of bucket to look up"),
        ("search", description = "Partial ID or alias to search for"),
    ),
	responses(
            (status = 200, description = "Returns exhaustive information about the bucket", body = GetBucketInfoResponse),
            (status = 500, description = "Internal server error")
        ),
)]
fn GetBucketInfo() -> () {}

#[utoipa::path(post,
    path = "/v2/CreateBucket",
    tag = "Bucket",
    description = "
Creates a new bucket, either with a global alias, a local one, or no alias at all.
Technically, you can also specify both `globalAlias` and `localAlias` and that would create two aliases.
    ",
    request_body = CreateBucketRequest,
	responses(
            (status = 200, description = "Returns exhaustive information about the bucket", body = CreateBucketResponse),
            (status = 500, description = "Internal server error")
        ),
)]
fn CreateBucket() -> () {}

#[utoipa::path(post,
    path = "/v2/UpdateBucket",
    tag = "Bucket",
    description = "
All fields (`websiteAccess` and `quotas`) are optional.
If they are present, the corresponding modifications are applied to the bucket, otherwise nothing is changed.

In `websiteAccess`: if `enabled` is `true`, `indexDocument` must be specified.
The field `errorDocument` is optional, if no error document is set a generic
error message is displayed when errors happen. Conversely, if `enabled` is
`false`, neither `indexDocument` nor `errorDocument` must be specified.

In `quotas`: new values of `maxSize` and `maxObjects` must both be specified, or set to `null`
to remove the quotas. An absent value will be considered the same as a `null`. It is not possible
to change only one of the two quotas.
    ",
    params(
        ("id", description = "ID of the bucket to update"),
    ),
    request_body = UpdateBucketRequestBody,
	responses(
            (status = 200, description = "Bucket has been updated", body = UpdateBucketResponse),
            (status = 404, description = "Bucket not found"),
            (status = 500, description = "Internal server error")
        ),
)]
fn UpdateBucket() -> () {}

#[utoipa::path(post,
    path = "/v2/DeleteBucket",
    tag = "Bucket",
    description = "
Deletes a storage bucket. A bucket cannot be deleted if it is not empty.

**Warning:** this will delete all aliases associated with the bucket!
    ",
    params(
        ("id", description = "ID of the bucket to delete"),
    ),
	responses(
            (status = 200, description = "Bucket has been deleted"),
            (status = 400, description = "Bucket is not empty"),
            (status = 404, description = "Bucket not found"),
            (status = 500, description = "Internal server error")
        ),
)]
fn DeleteBucket() -> () {}

#[utoipa::path(post,
    path = "/v2/CleanupIncompleteUploads",
    tag = "Bucket",
    description = "Removes all incomplete multipart uploads that are older than the specified number of seconds.",
    request_body = CleanupIncompleteUploadsRequest,
	responses(
            (status = 200, description = "The bucket was cleaned up successfully", body = CleanupIncompleteUploadsResponse),
            (status = 500, description = "Internal server error")
        ),
)]
fn CleanupIncompleteUploads() -> () {}

// **********************************************
//      Operations on permissions for keys on buckets
// **********************************************

#[utoipa::path(post,
    path = "/v2/AllowBucketKey",
    tag = "Permission",
    description = "
⚠️ **DISCLAIMER**: Garage's developers are aware that this endpoint has an unconventional semantic. Be extra careful when implementing it, its behavior is not obvious.

Allows a key to do read/write/owner operations on a bucket.

Flags in permissions which have the value true will be activated. Other flags will remain unchanged (ie. they will keep their internal value).

For example, if you set read to true, the key will be allowed to read the bucket.
If you set it to false, the key will keeps its previous read permission.
If you want to disallow read for the key, check the DenyBucketKey operation.
    ",
    request_body = AllowBucketKeyRequest,
	responses(
            (status = 200, description = "Returns exhaustive information about the bucket", body = AllowBucketKeyResponse),
            (status = 500, description = "Internal server error")
        ),
)]
fn AllowBucketKey() -> () {}

#[utoipa::path(post,
    path = "/v2/DenyBucketKey",
    tag = "Permission",
    description = "
⚠️ **DISCLAIMER**: Garage's developers are aware that this endpoint has an unconventional semantic. Be extra careful when implementing it, its behavior is not obvious.

Denies a key from doing read/write/owner operations on a bucket.

Flags in permissions which have the value true will be deactivated. Other flags will remain unchanged.

For example, if you set read to true, the key will be denied from reading.
If you set read to false,  the key will keep its previous permissions.
If you want the key to have the reading permission, check the AllowBucketKey operation.
    ",
    request_body = DenyBucketKeyRequest,
	responses(
            (status = 200, description = "Returns exhaustive information about the bucket", body = DenyBucketKeyResponse),
            (status = 500, description = "Internal server error")
        ),
)]
fn DenyBucketKey() -> () {}

// **********************************************
//      Operations on bucket aliases
// **********************************************

#[utoipa::path(post,
    path = "/v2/AddBucketAlias",
    tag = "Bucket alias",
    description = "Add an alias for the target bucket.  This can be either a global or a local alias, depending on which fields are specified.",
    request_body = AddBucketAliasRequest,
	responses(
            (status = 200, description = "Returns exhaustive information about the bucket", body = AddBucketAliasResponse),
            (status = 500, description = "Internal server error")
        ),
)]
fn AddBucketAlias() -> () {}

#[utoipa::path(post,
    path = "/v2/RemoveBucketAlias",
    tag = "Bucket alias",
    description = "Remove an alias for the target bucket.  This can be either a global or a local alias, depending on which fields are specified.",
    request_body = RemoveBucketAliasRequest,
	responses(
            (status = 200, description = "Returns exhaustive information about the bucket", body = RemoveBucketAliasResponse),
            (status = 500, description = "Internal server error")
        ),
)]
fn RemoveBucketAlias() -> () {}

// **********************************************
//      Node operations
// **********************************************

#[utoipa::path(get,
    path = "/v2/GetNodeInfo",
    tag = "Node",
    description = "
Return information about the Garage daemon running on one or several nodes.
    ",
    params(
        ("node", description = "Node ID to query, or `*` for all nodes, or `self` for the node responding to the request"),
    ),
	responses(
            (status = 200, description = "Responses from individual cluster nodes", body = MultiResponse<LocalGetNodeInfoResponse>),
            (status = 500, description = "Internal server error")
        ),
)]
fn GetNodeInfo() -> () {}

#[utoipa::path(get,
    path = "/v2/GetNodeStatistics",
    tag = "Node",
    description = "
Fetch statistics for one or several Garage nodes.

*Note: do not try to parse the `freeform` field of the response, it is given as a string specifically because its format is not stable.*
    ",
    params(
        ("node", description = "Node ID to query, or `*` for all nodes, or `self` for the node responding to the request"),
    ),
	responses(
            (status = 200, description = "Responses from individual cluster nodes", body = MultiResponse<LocalGetNodeStatisticsResponse>),
            (status = 500, description = "Internal server error")
        ),
)]
fn GetNodeStatistics() -> () {}

#[utoipa::path(post,
    path = "/v2/CreateMetadataSnapshot",
    tag = "Node",
    description = "
Instruct one or several nodes to take a snapshot of their metadata databases.
    ",
    params(
        ("node", description = "Node ID to query, or `*` for all nodes, or `self` for the node responding to the request"),
    ),
	responses(
            (status = 200, description = "Responses from individual cluster nodes", body = MultiResponse<LocalCreateMetadataSnapshotResponse>),
            (status = 500, description = "Internal server error")
        ),
)]
fn CreateMetadataSnapshot() -> () {}

#[utoipa::path(post,
    path = "/v2/LaunchRepairOperation",
    tag = "Node",
    description = "
Launch a repair operation on one or several cluster nodes.
    ",
    params(
        ("node", description = "Node ID to query, or `*` for all nodes, or `self` for the node responding to the request"),
    ),
    request_body = LocalLaunchRepairOperationRequest,
	responses(
            (status = 200, description = "Responses from individual cluster nodes", body = MultiResponse<LocalLaunchRepairOperationResponse>),
            (status = 500, description = "Internal server error")
        ),
)]
fn LaunchRepairOperation() -> () {}

// **********************************************
//      Worker operations
// **********************************************

#[utoipa::path(post,
    path = "/v2/ListWorkers",
    tag = "Worker",
    description = "
List background workers currently running on one or several cluster nodes.
    ",
    params(
        ("node", description = "Node ID to query, or `*` for all nodes, or `self` for the node responding to the request"),
    ),
    request_body = LocalListWorkersRequest,
	responses(
            (status = 200, description = "Responses from individual cluster nodes", body = MultiResponse<LocalListWorkersResponse>),
            (status = 500, description = "Internal server error")
        ),
)]
fn ListWorkers() -> () {}

#[utoipa::path(post,
    path = "/v2/GetWorkerInfo",
    tag = "Worker",
    description = "
Get information about the specified background worker on one or several cluster nodes.
    ",
    params(
        ("node", description = "Node ID to query, or `*` for all nodes, or `self` for the node responding to the request"),
    ),
    request_body = LocalGetWorkerInfoRequest,
	responses(
            (status = 200, description = "Responses from individual cluster nodes", body = MultiResponse<LocalGetWorkerInfoResponse>),
            (status = 500, description = "Internal server error")
        ),
)]
fn GetWorkerInfo() -> () {}

#[utoipa::path(post,
    path = "/v2/GetWorkerVariable",
    tag = "Worker",
    description = "
Fetch values of one or several worker variables, from one or several cluster nodes.
    ",
    params(
        ("node", description = "Node ID to query, or `*` for all nodes, or `self` for the node responding to the request"),
    ),
    request_body = LocalGetWorkerVariableRequest,
	responses(
            (status = 200, description = "Responses from individual cluster nodes", body = MultiResponse<LocalGetWorkerVariableResponse>),
            (status = 500, description = "Internal server error")
        ),
)]
fn GetWorkerVariable() -> () {}

#[utoipa::path(post,
    path = "/v2/SetWorkerVariable",
    tag = "Worker",
    description = "
Set the value for a worker variable, on one or several cluster nodes.
    ",
    params(
        ("node", description = "Node ID to query, or `*` for all nodes, or `self` for the node responding to the request"),
    ),
    request_body = LocalSetWorkerVariableRequest,
	responses(
            (status = 200, description = "Responses from individual cluster nodes", body = MultiResponse<LocalSetWorkerVariableResponse>),
            (status = 500, description = "Internal server error")
        ),
)]
fn SetWorkerVariable() -> () {}

// **********************************************
//      Block operations
// **********************************************

#[utoipa::path(get,
    path = "/v2/ListBlockErrors",
    tag = "Block",
    description = "
List data blocks that are currently in an errored state on one or several Garage nodes.
    ",
    params(
        ("node", description = "Node ID to query, or `*` for all nodes, or `self` for the node responding to the request"),
    ),
	responses(
            (status = 200, description = "Responses from individual cluster nodes", body = MultiResponse<LocalListBlockErrorsResponse>),
            (status = 500, description = "Internal server error")
        ),
)]
fn ListBlockErrors() -> () {}

#[utoipa::path(post,
    path = "/v2/GetBlockInfo",
    tag = "Block",
    description = "
Get detailed information about a data block stored on a Garage node, including all object versions and in-progress multipart uploads that contain a reference to this block.
    ",
    params(
        ("node", description = "Node ID to query, or `*` for all nodes, or `self` for the node responding to the request"),
    ),
    request_body = LocalGetBlockInfoRequest,
	responses(
            (status = 200, description = "Detailed block information", body = MultiResponse<LocalGetBlockInfoResponse>),
            (status = 500, description = "Internal server error")
        ),
)]
fn GetBlockInfo() -> () {}

#[utoipa::path(post,
    path = "/v2/RetryBlockResync",
    tag = "Block",
    description = "
Instruct Garage node(s) to retry the resynchronization of one or several missing data block(s).
    ",
    params(
        ("node", description = "Node ID to query, or `*` for all nodes, or `self` for the node responding to the request"),
    ),
    request_body = LocalRetryBlockResyncRequest,
	responses(
            (status = 200, description = "Responses from individual cluster nodes", body = MultiResponse<LocalRetryBlockResyncResponse>),
            (status = 500, description = "Internal server error")
        ),
)]
fn RetryBlockResync() -> () {}

#[utoipa::path(post,
    path = "/v2/PurgeBlocks",
    tag = "Block",
    description = "
Purge references to one or several missing data blocks.

This will remove all objects and in-progress multipart uploads that contain the specified data block(s). The objects will be permanently deleted from the buckets in which they appear. Use with caution.
    ",
    params(
        ("node", description = "Node ID to query, or `*` for all nodes, or `self` for the node responding to the request"),
    ),
    request_body = LocalPurgeBlocksRequest,
	responses(
            (status = 200, description = "Responses from individual cluster nodes", body = MultiResponse<LocalPurgeBlocksResponse>),
            (status = 500, description = "Internal server error")
        ),
)]
fn PurgeBlocks() -> () {}

// **********************************************
// **********************************************
// **********************************************

struct SecurityAddon;

impl Modify for SecurityAddon {
	fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
		use utoipa::openapi::security::*;
		let components = openapi.components.as_mut().unwrap(); // we can unwrap safely since there already is components registered.
		components.add_security_scheme(
			"bearerAuth",
			SecurityScheme::Http(Http::builder().scheme(HttpAuthScheme::Bearer).build()),
		)
	}
}

#[derive(OpenApi)]
#[openapi(
    info(
        version = "v2.0.0",
        title = "Garage administration API",
        description = "Administrate your Garage cluster programatically, including status, layout, keys, buckets, and maintainance tasks.

*Disclaimer: This API may change in future Garage versions. Read the changelog and upgrade your scripts before upgrading. Additionnaly, this specification is early stage and can contain bugs, so be careful and please report any issues on our issue tracker.*",
        contact(
            name = "The Garage team",
            email = "garagehq@deuxfleurs.fr",
            url = "https://garagehq.deuxfleurs.fr/",
        ),
    ),
    modifiers(&SecurityAddon),
    security(("bearerAuth" = [])),
    paths(
        // Cluster operations
        GetClusterHealth,
        GetClusterStatus,
        GetClusterStatistics,
        ConnectClusterNodes,
        // Layout operations
        GetClusterLayout,
        GetClusterLayoutHistory,
        UpdateClusterLayout,
        PreviewClusterLayoutChanges,
        ApplyClusterLayout,
        RevertClusterLayout,
        ClusterLayoutSkipDeadNodes,
        // Key operations
        ListKeys,
        GetKeyInfo,
        CreateKey,
        ImportKey,
        UpdateKey,
        DeleteKey,
        // Bucket operations
        ListBuckets,
        GetBucketInfo,
        CreateBucket,
        UpdateBucket,
        DeleteBucket,
        CleanupIncompleteUploads,
        // Operations on permissions
        AllowBucketKey,
        DenyBucketKey,
        // Operations on aliases
        AddBucketAlias,
        RemoveBucketAlias,
        // Node operations
        GetNodeInfo,
        GetNodeStatistics,
        CreateMetadataSnapshot,
        LaunchRepairOperation,
        // Worker operations
        ListWorkers,
        GetWorkerInfo,
        GetWorkerVariable,
        SetWorkerVariable,
        // Block operations
        ListBlockErrors,
        GetBlockInfo,
        RetryBlockResync,
        PurgeBlocks,
    ),
    servers(
        (url = "http://localhost:3903/", description = "A local server")
    ),
)]
pub struct ApiDoc;
