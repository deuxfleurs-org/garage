use std::collections::HashMap;
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use paste::paste;
use serde::{Deserialize, Serialize};
use utoipa::{IntoParams, ToSchema};

use garage_rpc::*;

use garage_model::garage::Garage;

use garage_api_common::helpers::is_default;

use crate::api_server::{find_matching_nodes, AdminRpc, AdminRpcResponse};
use crate::error::Error;
use crate::macros::*;
use crate::{Admin, RequestHandler};

// This generates the following:
//
// - An enum AdminApiRequest that contains a variant for all endpoints
//
// - An enum AdminApiResponse that contains a variant for all non-special endpoints.
//   This enum is serialized in api_server.rs, without the enum tag,
//   which gives directly the JSON response corresponding to the API call.
//   This enum does not implement Deserialize as its meaning can be ambiguous.
//
// - An enum TaggedAdminApiResponse that contains the same variants, but
//   serializes as a tagged enum. This allows it to be transmitted through
//   Garage RPC and deserialized correctly upon receival.
//   Conversion from untagged to tagged can be done using the `.tagged()` method.
//
// - AdminApiRequest::name() that returns the name of the endpoint
//
// - impl EndpointHandler for AdminApiHandler, that uses the impl EndpointHandler
//   of each request type below for non-special endpoints
admin_endpoints![
	// Special endpoints of the Admin API
	@special Options,
	@special CheckDomain,
	@special Health,
	@special Metrics,

	// Cluster operations
	GetClusterStatus,
	GetClusterHealth,
	GetClusterStatistics,
	ConnectClusterNodes,

	// Admin tokens operations
	ListAdminTokens,
	GetAdminTokenInfo,
	CreateAdminToken,
	UpdateAdminToken,
	DeleteAdminToken,

	// Layout operations
	GetClusterLayout,
	GetClusterLayoutHistory,
	UpdateClusterLayout,
	PreviewClusterLayoutChanges,
	ApplyClusterLayout,
	RevertClusterLayout,
	ClusterLayoutSkipDeadNodes,

	// Access key operations
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
	InspectObject,

	// Operations on permissions for keys on buckets
	AllowBucketKey,
	DenyBucketKey,

	// Operations on bucket aliases
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
];

local_admin_endpoints![
	// Node operations
	GetNodeInfo,
	GetNodeStatistics,
	CreateMetadataSnapshot,
	LaunchRepairOperation,
	// Background workers
	ListWorkers,
	GetWorkerInfo,
	GetWorkerVariable,
	SetWorkerVariable,
	// Block operations
	ListBlockErrors,
	GetBlockInfo,
	RetryBlockResync,
	PurgeBlocks,
];

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MultiRequest<RB> {
	pub node: String,
	pub body: RB,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct MultiResponse<RB> {
	/// Map of node id to response returned by this node, for nodes that were able to
	/// successfully complete the API call
	pub success: HashMap<String, RB>,
	/// Map of node id to error message, for nodes that were unable to complete the API
	/// call
	pub error: HashMap<String, String>,
}

// **********************************************
//      Special endpoints
//
// These endpoints don't have associated *Response structs
// because they directly produce an http::Response
// **********************************************

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionsRequest;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckDomainRequest {
	pub domain: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthRequest;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsRequest;

// **********************************************
//      Cluster operations
// **********************************************

// ---- GetClusterStatus ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetClusterStatusRequest;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetClusterStatusResponse {
	/// Current version number of the cluster layout
	pub layout_version: u64,
	/// List of nodes that are either currently connected, part of the
	/// current cluster layout, or part of an older cluster layout that
	/// is still active in the cluster (being drained).
	pub nodes: Vec<NodeResp>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeResp {
	/// Full-length node identifier
	pub id: String,
	/// Garage version
	pub garage_version: Option<String>,
	/// Socket address used by other nodes to connect to this node for RPC
	#[schema(value_type = Option<String>)]
	pub addr: Option<SocketAddr>,
	/// Hostname of the node
	pub hostname: Option<String>,
	/// Whether this node is connected in the cluster
	pub is_up: bool,
	/// For disconnected nodes, the number of seconds since last contact,
	/// or `null` if no contact was established since Garage restarted.
	pub last_seen_secs_ago: Option<u64>,
	/// Role assigned to this node in the current cluster layout
	pub role: Option<NodeAssignedRole>,
	/// Whether this node is part of an older layout version and is draining data.
	pub draining: bool,
	/// Total and available space on the disk partition(s) containing the data
	/// directory(ies)
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub data_partition: Option<FreeSpaceResp>,
	/// Total and available space on the disk partition containing the
	/// metadata directory
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub metadata_partition: Option<FreeSpaceResp>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeAssignedRole {
	/// Zone name assigned by the cluster administrator
	pub zone: String,
	/// List of tags assigned by the cluster administrator
	pub tags: Vec<String>,
	/// Capacity (in bytes) assigned by the cluster administrator,
	/// absent for gateway nodes
	pub capacity: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FreeSpaceResp {
	/// Number of bytes available
	pub available: u64,
	/// Total number of bytes
	pub total: u64,
}

// ---- GetClusterHealth ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetClusterHealthRequest;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetClusterHealthResponse {
	/// One of `healthy`, `degraded` or `unavailable`:
	/// - `healthy`: Garage node is connected to all storage nodes
	/// - `degraded`: Garage node is not connected to all storage nodes, but a quorum of write nodes is available for all partitions
	/// - `unavailable`: a quorum of write nodes is not available for some partitions
	pub status: String,
	/// the number of nodes this Garage node has had a TCP connection to since the daemon started
	pub known_nodes: usize,
	/// the nubmer of nodes this Garage node currently has an open connection to
	pub connected_nodes: usize,
	/// the number of storage nodes currently registered in the cluster layout
	pub storage_nodes: usize,
	/// the number of storage nodes to which a connection is currently open
	pub storage_nodes_ok: usize,
	/// the total number of partitions of the data (currently always 256)
	pub partitions: usize,
	/// the number of partitions for which a quorum of write nodes is available
	pub partitions_quorum: usize,
	/// the number of partitions for which we are connected to all storage nodes responsible of storing it
	pub partitions_all_ok: usize,
}

// ---- GetClusterStatistics ----

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct GetClusterStatisticsRequest;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct GetClusterStatisticsResponse {
	pub freeform: String,
}

// ---- ConnectClusterNodes ----

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ConnectClusterNodesRequest(pub Vec<String>);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ConnectClusterNodesResponse(pub Vec<ConnectNodeResponse>);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ConnectNodeResponse {
	/// `true` if Garage managed to connect to this node
	pub success: bool,
	/// An error message if Garage did not manage to connect to this node
	pub error: Option<String>,
}

// **********************************************
//      Admin token operations
// **********************************************

// ---- ListAdminTokens ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListAdminTokensRequest;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListAdminTokensResponse(pub Vec<GetAdminTokenInfoResponse>);

// ---- GetAdminTokenInfo ----

#[derive(Debug, Clone, Serialize, Deserialize, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct GetAdminTokenInfoRequest {
	/// Admin API token ID
	pub id: Option<String>,
	/// Partial token ID or name to search for
	pub search: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetAdminTokenInfoResponse {
	/// Identifier of the admin token (which is also a prefix of the full bearer token)
	pub id: Option<String>,
	/// Creation date
	pub created: Option<DateTime<Utc>>,
	/// Name of the admin API token
	pub name: String,
	/// Expiration time and date, formatted according to RFC 3339
	pub expiration: Option<DateTime<Utc>>,
	/// Whether this admin token is expired already
	pub expired: bool,
	/// Scope of the admin API token, a list of admin endpoint names (such as
	/// `GetClusterStatus`, etc), or the special value `*` to allow all
	/// admin endpoints
	pub scope: Vec<String>,
}

// ---- CreateAdminToken ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateAdminTokenRequest(pub UpdateAdminTokenRequestBody);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateAdminTokenResponse {
	/// The secret bearer token. **CAUTION:** This token will be shown only
	/// ONCE, so this value MUST be remembered somewhere, or the token
	/// will be unusable.
	pub secret_token: String,
	#[serde(flatten)]
	pub info: GetAdminTokenInfoResponse,
}

// ---- UpdateAdminToken ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateAdminTokenRequest {
	pub id: String,
	pub body: UpdateAdminTokenRequestBody,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpdateAdminTokenRequestBody {
	/// Name of the admin API token
	pub name: Option<String>,
	/// Expiration time and date, formatted according to RFC 3339
	pub expiration: Option<DateTime<Utc>>,
	/// Scope of the admin API token, a list of admin endpoint names (such as
	/// `GetClusterStatus`, etc), or the special value `*` to allow all
	/// admin endpoints. **WARNING:** Granting a scope of `CreateAdminToken` or
	/// `UpdateAdminToken` trivially allows for privilege escalation, and is thus
	/// functionnally equivalent to granting a scope of `*`.
	pub scope: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UpdateAdminTokenResponse(pub GetAdminTokenInfoResponse);

// ---- DeleteAdminToken ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteAdminTokenRequest {
	pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteAdminTokenResponse;

// **********************************************
//      Layout operations
// **********************************************

// ---- GetClusterLayout ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetClusterLayoutRequest;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetClusterLayoutResponse {
	/// The current version number of the cluster layout
	pub version: u64,
	/// List of nodes that currently have a role in the cluster layout
	pub roles: Vec<LayoutNodeRole>,
	/// Layout parameters used when the current layout was computed
	pub parameters: LayoutParameters,
	/// The size, in bytes, of one Garage partition (= a shard)
	pub partition_size: u64,
	/// List of nodes that will have a new role or whose role will be
	/// removed in the next version of the cluster layout
	pub staged_role_changes: Vec<NodeRoleChange>,
	/// Layout parameters to use when computing the next version of
	/// the cluster layout
	pub staged_parameters: Option<LayoutParameters>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LayoutNodeRole {
	/// Identifier of the node
	pub id: String,
	/// Zone name assigned by the cluster administrator
	pub zone: String,
	/// List of tags assigned by the cluster administrator
	pub tags: Vec<String>,
	/// Capacity (in bytes) assigned by the cluster administrator,
	/// absent for gateway nodes
	pub capacity: Option<u64>,
	/// Number of partitions stored on this node
	/// (a result of the layout computation)
	pub stored_partitions: Option<u64>,
	/// Capacity (in bytes) that is actually usable on this node in the current
	/// layout, which is equal to `stored_partitions` × `partition_size`
	pub usable_capacity: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeRoleChange {
	/// ID of the node for which this change applies
	pub id: String,
	#[serde(flatten)]
	pub action: NodeRoleChangeEnum,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum NodeRoleChangeEnum {
	#[serde(rename_all = "camelCase")]
	Remove {
		/// Set `remove` to `true` to remove the node from the layout
		remove: bool,
	},
	#[serde(rename_all = "camelCase")]
	Update(NodeAssignedRole),
}

#[derive(Copy, Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LayoutParameters {
	/// Minimum number of zones in which a data partition must be replicated
	pub zone_redundancy: ZoneRedundancy,
}

#[derive(Copy, Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum ZoneRedundancy {
	/// Partitions must be replicated in at least this number of
	/// distinct zones.
	AtLeast(usize),
	/// Partitions must be replicated in as many zones as possible:
	/// as many zones as there are replicas, if there are enough distinct
	/// zones, or at least one in each zone otherwise.
	Maximum,
}

// ---- GetClusterLayoutHistory ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetClusterLayoutHistoryRequest;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetClusterLayoutHistoryResponse {
	/// The current version number of the cluster layout
	pub current_version: u64,
	/// All nodes in the cluster are aware of layout versions up to
	/// this version number (at least)
	pub min_ack: u64,
	/// Layout version history
	pub versions: Vec<ClusterLayoutVersion>,
	/// Detailed update trackers for nodes (see
	/// `https://garagehq.deuxfleurs.fr/blog/2023-12-preserving-read-after-write-consistency/`)
	pub update_trackers: Option<HashMap<String, NodeUpdateTrackers>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterLayoutVersion {
	/// Version number of this layout version
	pub version: u64,
	/// Status of this layout version
	pub status: ClusterLayoutVersionStatus,
	/// Number of nodes with an assigned storage capacity in this layout version
	pub storage_nodes: u64,
	/// Number of nodes with a gateway role in this layout version
	pub gateway_nodes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub enum ClusterLayoutVersionStatus {
	/// This is the most up-to-date layout version
	Current,
	/// This version is still active in the cluster because metadata
	/// is being rebalanced or migrated from old nodes
	Draining,
	/// This version is no longer active in the cluster for metadata
	/// reads and writes. Note that there is still the possibility
	/// that data blocks are being migrated away from nodes in this
	/// layout version.
	Historical,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NodeUpdateTrackers {
	pub ack: u64,
	pub sync: u64,
	pub sync_ack: u64,
}

// ---- UpdateClusterLayout ----

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UpdateClusterLayoutRequest {
	/// New node roles to assign or remove in the cluster layout
	#[serde(default)]
	pub roles: Vec<NodeRoleChange>,
	/// New layout computation parameters to use
	#[serde(default)]
	pub parameters: Option<LayoutParameters>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UpdateClusterLayoutResponse(pub GetClusterLayoutResponse);

// ---- PreviewClusterLayoutChanges ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreviewClusterLayoutChangesRequest;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum PreviewClusterLayoutChangesResponse {
	#[serde(rename_all = "camelCase")]
	Error {
		/// Error message indicating that the layout could not be computed
		/// with the provided configuration
		error: String,
	},
	#[serde(rename_all = "camelCase")]
	Success {
		/// Plain-text information about the layout computation
		/// (do not try to parse this)
		message: Vec<String>,
		/// Details about the new cluster layout
		new_layout: GetClusterLayoutResponse,
	},
}

// ---- ApplyClusterLayout ----

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApplyClusterLayoutRequest {
	/// As a safety measure, the new version number of the layout must
	/// be specified here
	pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApplyClusterLayoutResponse {
	/// Plain-text information about the layout computation
	/// (do not try to parse this)
	pub message: Vec<String>,
	/// Details about the new cluster layout
	pub layout: GetClusterLayoutResponse,
}

// ---- RevertClusterLayout ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevertClusterLayoutRequest;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RevertClusterLayoutResponse(pub GetClusterLayoutResponse);

// ---- ClusterLayoutSkipDeadNodes ----

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterLayoutSkipDeadNodesRequest {
	/// Version number of the layout to assume is currently up-to-date.
	/// This will generally be the current layout version.
	pub version: u64,
	/// Allow the skip even if a quorum of nodes could not be found for
	/// the data among the remaining nodes
	pub allow_missing_data: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ClusterLayoutSkipDeadNodesResponse {
	/// Nodes for which the ACK update tracker has been updated to `version`
	pub ack_updated: Vec<String>,
	/// If `allow_missing_data` is set,
	/// nodes for which the SYNC update tracker has been updated to `version`
	pub sync_updated: Vec<String>,
}

// **********************************************
//      Access key operations
// **********************************************

// ---- ListKeys ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListKeysRequest;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListKeysResponse(pub Vec<ListKeysResponseItem>);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListKeysResponseItem {
	pub id: String,
	pub name: String,
	pub created: Option<DateTime<Utc>>,
	pub expiration: Option<DateTime<Utc>>,
	pub expired: bool,
}

// ---- GetKeyInfo ----

#[derive(Debug, Clone, Serialize, Deserialize, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct GetKeyInfoRequest {
	/// Access key ID
	pub id: Option<String>,
	/// Partial key ID or name to search for
	pub search: Option<String>,
	/// Whether to return the secret access key
	#[serde(default)]
	pub show_secret_key: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetKeyInfoResponse {
	pub access_key_id: String,
	pub created: Option<DateTime<Utc>>,
	pub name: String,
	pub expiration: Option<DateTime<Utc>>,
	pub expired: bool,
	#[serde(default, skip_serializing_if = "is_default")]
	pub secret_access_key: Option<String>,
	pub permissions: KeyPerm,
	pub buckets: Vec<KeyInfoBucketResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct KeyPerm {
	#[serde(default)]
	pub create_bucket: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct KeyInfoBucketResponse {
	pub id: String,
	pub global_aliases: Vec<String>,
	pub local_aliases: Vec<String>,
	pub permissions: ApiBucketKeyPerm,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApiBucketKeyPerm {
	#[serde(default)]
	pub read: bool,
	#[serde(default)]
	pub write: bool,
	#[serde(default)]
	pub owner: bool,
}

// ---- CreateKey ----

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateKeyRequest {
	pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateKeyResponse(pub GetKeyInfoResponse);

// ---- ImportKey ----

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ImportKeyRequest {
	pub access_key_id: String,
	pub secret_access_key: String,
	pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ImportKeyResponse(pub GetKeyInfoResponse);

// ---- UpdateKey ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateKeyRequest {
	pub id: String,
	pub body: UpdateKeyRequestBody,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UpdateKeyResponse(pub GetKeyInfoResponse);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpdateKeyRequestBody {
	pub name: Option<String>,
	pub allow: Option<KeyPerm>,
	pub deny: Option<KeyPerm>,
}

// ---- DeleteKey ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteKeyRequest {
	pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteKeyResponse;

// **********************************************
//      Bucket operations
// **********************************************

// ---- ListBuckets ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListBucketsRequest;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ListBucketsResponse(pub Vec<ListBucketsResponseItem>);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ListBucketsResponseItem {
	pub id: String,
	pub created: DateTime<Utc>,
	pub global_aliases: Vec<String>,
	pub local_aliases: Vec<BucketLocalAlias>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BucketLocalAlias {
	pub access_key_id: String,
	pub alias: String,
}

// ---- GetBucketInfo ----

#[derive(Debug, Clone, Serialize, Deserialize, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct GetBucketInfoRequest {
	/// Exact bucket ID to look up
	pub id: Option<String>,
	/// Global alias of bucket to look up
	pub global_alias: Option<String>,
	/// Partial ID or alias to search for
	pub search: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetBucketInfoResponse {
	/// Identifier of the bucket
	pub id: String,
	/// Bucket creation date
	pub created: DateTime<Utc>,
	/// List of global aliases for this bucket
	pub global_aliases: Vec<String>,
	/// Whether website acces is enabled for this bucket
	pub website_access: bool,
	#[serde(default)]
	/// Website configuration for this bucket
	pub website_config: Option<GetBucketInfoWebsiteResponse>,
	/// List of access keys that have permissions granted on this bucket
	pub keys: Vec<GetBucketInfoKey>,
	/// Number of objects in this bucket
	pub objects: i64,
	/// Total number of bytes used by objects in this bucket
	pub bytes: i64,
	/// Number of unfinished uploads in this bucket
	pub unfinished_uploads: i64,
	/// Number of unfinished multipart uploads in this bucket
	pub unfinished_multipart_uploads: i64,
	/// Number of parts in unfinished multipart uploads in this bucket
	pub unfinished_multipart_upload_parts: i64,
	/// Total number of bytes used by unfinished multipart uploads in this bucket
	pub unfinished_multipart_upload_bytes: i64,
	/// Quotas that apply to this bucket
	pub quotas: ApiBucketQuotas,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetBucketInfoWebsiteResponse {
	pub index_document: String,
	pub error_document: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetBucketInfoKey {
	pub access_key_id: String,
	pub name: String,
	pub permissions: ApiBucketKeyPerm,
	pub bucket_local_aliases: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ApiBucketQuotas {
	pub max_size: Option<u64>,
	pub max_objects: Option<u64>,
}

// ---- CreateBucket ----

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateBucketRequest {
	pub global_alias: Option<String>,
	pub local_alias: Option<CreateBucketLocalAlias>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct CreateBucketResponse(pub GetBucketInfoResponse);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateBucketLocalAlias {
	pub access_key_id: String,
	pub alias: String,
	#[serde(default)]
	pub allow: ApiBucketKeyPerm,
}

// ---- UpdateBucket ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateBucketRequest {
	pub id: String,
	pub body: UpdateBucketRequestBody,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct UpdateBucketResponse(pub GetBucketInfoResponse);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpdateBucketRequestBody {
	pub website_access: Option<UpdateBucketWebsiteAccess>,
	pub quotas: Option<ApiBucketQuotas>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct UpdateBucketWebsiteAccess {
	pub enabled: bool,
	pub index_document: Option<String>,
	pub error_document: Option<String>,
}

// ---- DeleteBucket ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteBucketRequest {
	pub id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteBucketResponse;

// ---- CleanupIncompleteUploads ----

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CleanupIncompleteUploadsRequest {
	pub bucket_id: String,
	pub older_than_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CleanupIncompleteUploadsResponse {
	pub uploads_deleted: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, IntoParams)]
#[serde(rename_all = "camelCase")]
pub struct InspectObjectRequest {
	pub bucket_id: String,
	pub key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct InspectObjectResponse {
	/// ID of the bucket containing the inspected object
	pub bucket_id: String,
	/// Key of the inspected object
	pub key: String,
	/// List of versions currently stored for this object
	pub versions: Vec<InspectObjectVersion>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema, Default)]
#[serde(rename_all = "camelCase")]
pub struct InspectObjectVersion {
	/// Version ID
	pub uuid: String,
	/// Creation timestamp of this object version
	pub timestamp: DateTime<Utc>,
	/// Whether this object version was created with SSE-C encryption
	pub encrypted: bool,
	/// Whether this object version is still uploading
	pub uploading: bool,
	/// Whether this is an aborted upload
	pub aborted: bool,
	/// Whether this version is a delete marker (a tombstone indicating that a previous version of
	/// the object has been deleted)
	pub delete_marker: bool,
	/// Whether the object's data is stored inline (for small objects)
	pub inline: bool,
	/// Size of the object, in bytes
	pub size: Option<u64>,
	/// Etag of this object version
	pub etag: Option<String>,
	/// Metadata (HTTP headers) associated with this object version
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub headers: Vec<(String, String)>,
	/// List of data blocks for this object version
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub blocks: Vec<InspectObjectBlock>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct InspectObjectBlock {
	/// Part number of the part containing this block, for multipart uploads
	pub part_number: u64,
	/// Offset of this block within the part
	pub offset: u64,
	/// Hash (blake2 sum) of the block's data
	pub hash: String,
	/// Length of the blocks's data
	pub size: u64,
}

// **********************************************
//      Operations on permissions for keys on buckets
// **********************************************

// ---- AllowBucketKey ----

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AllowBucketKeyRequest(pub BucketKeyPermChangeRequest);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AllowBucketKeyResponse(pub GetBucketInfoResponse);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BucketKeyPermChangeRequest {
	pub bucket_id: String,
	pub access_key_id: String,
	pub permissions: ApiBucketKeyPerm,
}

// ---- DenyBucketKey ----

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DenyBucketKeyRequest(pub BucketKeyPermChangeRequest);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct DenyBucketKeyResponse(pub GetBucketInfoResponse);

// **********************************************
//      Operations on bucket aliases
// **********************************************

// ---- AddBucketAlias ----

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct AddBucketAliasRequest {
	pub bucket_id: String,
	#[serde(flatten)]
	pub alias: BucketAliasEnum,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct AddBucketAliasResponse(pub GetBucketInfoResponse);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum BucketAliasEnum {
	#[serde(rename_all = "camelCase")]
	Global { global_alias: String },
	#[serde(rename_all = "camelCase")]
	Local {
		local_alias: String,
		access_key_id: String,
	},
}

// ---- RemoveBucketAlias ----

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct RemoveBucketAliasRequest {
	pub bucket_id: String,
	#[serde(flatten)]
	pub alias: BucketAliasEnum,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct RemoveBucketAliasResponse(pub GetBucketInfoResponse);

// **********************************************
//      Node operations
// **********************************************

// ---- GetNodeInfo ----

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LocalGetNodeInfoRequest;

#[derive(Debug, Clone, Serialize, Deserialize, Default, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LocalGetNodeInfoResponse {
	pub node_id: String,
	pub garage_version: String,
	pub garage_features: Option<Vec<String>>,
	pub rust_version: String,
	pub db_engine: String,
}

// ---- GetNodeStatistics ----

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LocalGetNodeStatisticsRequest;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LocalGetNodeStatisticsResponse {
	pub freeform: String,
}

// ---- CreateMetadataSnapshot ----

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LocalCreateMetadataSnapshotRequest;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LocalCreateMetadataSnapshotResponse;

// ---- LaunchRepairOperation ----

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LocalLaunchRepairOperationRequest {
	pub repair_type: RepairType,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum RepairType {
	Tables,
	Blocks,
	Versions,
	MultipartUploads,
	BlockRefs,
	BlockRc,
	Rebalance,
	Scrub(ScrubCommand),
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum ScrubCommand {
	Start,
	Pause,
	Resume,
	Cancel,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LocalLaunchRepairOperationResponse;

// **********************************************
//      Worker operations
// **********************************************

// ---- ListWorkers ----

#[derive(Debug, Clone, Serialize, Deserialize, Default, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LocalListWorkersRequest {
	#[serde(default)]
	pub busy_only: bool,
	#[serde(default)]
	pub error_only: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LocalListWorkersResponse(pub Vec<WorkerInfoResp>);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkerInfoResp {
	pub id: u64,
	pub name: String,
	pub state: WorkerStateResp,
	pub errors: u64,
	pub consecutive_errors: u64,
	pub last_error: Option<WorkerLastError>,
	pub tranquility: Option<u32>,
	pub progress: Option<String>,
	pub queue_length: Option<u64>,
	pub persistent_errors: Option<u64>,
	pub freeform: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum WorkerStateResp {
	Busy,
	#[serde(rename_all = "camelCase")]
	Throttled {
		duration_secs: f32,
	},
	Idle,
	Done,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct WorkerLastError {
	pub message: String,
	pub secs_ago: u64,
}

// ---- GetWorkerInfo ----

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LocalGetWorkerInfoRequest {
	pub id: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LocalGetWorkerInfoResponse(pub WorkerInfoResp);

// ---- GetWorkerVariable ----

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LocalGetWorkerVariableRequest {
	pub variable: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LocalGetWorkerVariableResponse(pub HashMap<String, String>);

// ---- SetWorkerVariable ----

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LocalSetWorkerVariableRequest {
	pub variable: String,
	pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LocalSetWorkerVariableResponse {
	pub variable: String,
	pub value: String,
}

// **********************************************
//      Block operations
// **********************************************

// ---- ListBlockErrors ----

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LocalListBlockErrorsRequest;

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct LocalListBlockErrorsResponse(pub Vec<BlockError>);

#[derive(Serialize, Deserialize, Clone, Debug, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BlockError {
	pub block_hash: String,
	pub refcount: u64,
	pub error_count: u64,
	pub last_try_secs_ago: u64,
	pub next_try_in_secs: u64,
}

// ---- GetBlockInfo ----

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LocalGetBlockInfoRequest {
	pub block_hash: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LocalGetBlockInfoResponse {
	pub block_hash: String,
	pub refcount: u64,
	pub versions: Vec<BlockVersion>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct BlockVersion {
	pub version_id: String,
	pub ref_deleted: bool,
	pub version_deleted: bool,
	pub garbage_collected: bool,
	pub backlink: Option<BlockVersionBacklink>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub enum BlockVersionBacklink {
	#[serde(rename_all = "camelCase")]
	Object { bucket_id: String, key: String },
	#[serde(rename_all = "camelCase")]
	Upload {
		upload_id: String,
		upload_deleted: bool,
		upload_garbage_collected: bool,
		bucket_id: Option<String>,
		key: Option<String>,
	},
}

// ---- RetryBlockResync ----

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(untagged)]
pub enum LocalRetryBlockResyncRequest {
	#[serde(rename_all = "camelCase")]
	All { all: bool },
	#[serde(rename_all = "camelCase")]
	Blocks { block_hashes: Vec<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LocalRetryBlockResyncResponse {
	pub count: u64,
}

// ---- PurgeBlocks ----

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LocalPurgeBlocksRequest(pub Vec<String>);

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct LocalPurgeBlocksResponse {
	pub blocks_purged: u64,
	pub objects_deleted: u64,
	pub uploads_deleted: u64,
	pub versions_deleted: u64,
}
