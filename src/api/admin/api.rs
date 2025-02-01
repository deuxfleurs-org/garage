use std::convert::TryFrom;
use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use paste::paste;
use serde::{Deserialize, Serialize};

use garage_model::garage::Garage;

use garage_api_common::helpers::is_default;

use crate::error::Error;
use crate::macros::*;
use crate::EndpointHandler;

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
	ConnectClusterNodes,
	GetClusterLayout,
	UpdateClusterLayout,
	ApplyClusterLayout,
	RevertClusterLayout,

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

	// Operations on permissions for keys on buckets
	AllowBucketKey,
	DenyBucketKey,

	// Operations on bucket aliases
	AddBucketAlias,
	RemoveBucketAlias,
];

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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetClusterStatusResponse {
	pub node: String,
	pub garage_version: String,
	pub garage_features: Option<Vec<String>>,
	pub rust_version: String,
	pub db_engine: String,
	pub layout_version: u64,
	pub nodes: Vec<NodeResp>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct NodeResp {
	pub id: String,
	pub role: Option<NodeRoleResp>,
	pub addr: Option<SocketAddr>,
	pub hostname: Option<String>,
	pub is_up: bool,
	pub last_seen_secs_ago: Option<u64>,
	pub draining: bool,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub data_partition: Option<FreeSpaceResp>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub metadata_partition: Option<FreeSpaceResp>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeRoleResp {
	pub id: String,
	pub zone: String,
	pub capacity: Option<u64>,
	pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FreeSpaceResp {
	pub available: u64,
	pub total: u64,
}

// ---- GetClusterHealth ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetClusterHealthRequest;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetClusterHealthResponse {
	pub status: String,
	pub known_nodes: usize,
	pub connected_nodes: usize,
	pub storage_nodes: usize,
	pub storage_nodes_ok: usize,
	pub partitions: usize,
	pub partitions_quorum: usize,
	pub partitions_all_ok: usize,
}

// ---- ConnectClusterNodes ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectClusterNodesRequest(pub Vec<String>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConnectClusterNodesResponse(pub Vec<ConnectNodeResponse>);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectNodeResponse {
	pub success: bool,
	pub error: Option<String>,
}

// ---- GetClusterLayout ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetClusterLayoutRequest;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetClusterLayoutResponse {
	pub version: u64,
	pub roles: Vec<NodeRoleResp>,
	pub staged_role_changes: Vec<NodeRoleChange>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeRoleChange {
	pub id: String,
	#[serde(flatten)]
	pub action: NodeRoleChangeEnum,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum NodeRoleChangeEnum {
	#[serde(rename_all = "camelCase")]
	Remove { remove: bool },
	#[serde(rename_all = "camelCase")]
	Update {
		zone: String,
		capacity: Option<u64>,
		tags: Vec<String>,
	},
}

// ---- UpdateClusterLayout ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateClusterLayoutRequest(pub Vec<NodeRoleChange>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateClusterLayoutResponse(pub GetClusterLayoutResponse);

// ---- ApplyClusterLayout ----

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplyClusterLayoutRequest {
	pub version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplyClusterLayoutResponse {
	pub message: Vec<String>,
	pub layout: GetClusterLayoutResponse,
}

// ---- RevertClusterLayout ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevertClusterLayoutRequest;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevertClusterLayoutResponse(pub GetClusterLayoutResponse);

// **********************************************
//      Access key operations
// **********************************************

// ---- ListKeys ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListKeysRequest;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListKeysResponse(pub Vec<ListKeysResponseItem>);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListKeysResponseItem {
	pub id: String,
	pub name: String,
}

// ---- GetKeyInfo ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetKeyInfoRequest {
	pub id: Option<String>,
	pub search: Option<String>,
	pub show_secret_key: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetKeyInfoResponse {
	pub name: String,
	pub access_key_id: String,
	#[serde(skip_serializing_if = "is_default")]
	pub secret_access_key: Option<String>,
	pub permissions: KeyPerm,
	pub buckets: Vec<KeyInfoBucketResponse>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyPerm {
	#[serde(default)]
	pub create_bucket: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyInfoBucketResponse {
	pub id: String,
	pub global_aliases: Vec<String>,
	pub local_aliases: Vec<String>,
	pub permissions: ApiBucketKeyPerm,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateKeyRequest {
	pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateKeyResponse(pub GetKeyInfoResponse);

// ---- ImportKey ----

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportKeyRequest {
	pub access_key_id: String,
	pub secret_access_key: String,
	pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportKeyResponse(pub GetKeyInfoResponse);

// ---- UpdateKey ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateKeyRequest {
	pub id: String,
	pub body: UpdateKeyRequestBody,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateKeyResponse(pub GetKeyInfoResponse);

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ListBucketsResponse(pub Vec<ListBucketsResponseItem>);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ListBucketsResponseItem {
	pub id: String,
	pub global_aliases: Vec<String>,
	pub local_aliases: Vec<BucketLocalAlias>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BucketLocalAlias {
	pub access_key_id: String,
	pub alias: String,
}

// ---- GetBucketInfo ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GetBucketInfoRequest {
	pub id: Option<String>,
	pub global_alias: Option<String>,
	pub search: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetBucketInfoResponse {
	pub id: String,
	pub global_aliases: Vec<String>,
	pub website_access: bool,
	#[serde(default)]
	pub website_config: Option<GetBucketInfoWebsiteResponse>,
	pub keys: Vec<GetBucketInfoKey>,
	pub objects: i64,
	pub bytes: i64,
	pub unfinished_uploads: i64,
	pub unfinished_multipart_uploads: i64,
	pub unfinished_multipart_upload_parts: i64,
	pub unfinished_multipart_upload_bytes: i64,
	pub quotas: ApiBucketQuotas,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetBucketInfoWebsiteResponse {
	pub index_document: String,
	pub error_document: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GetBucketInfoKey {
	pub access_key_id: String,
	pub name: String,
	pub permissions: ApiBucketKeyPerm,
	pub bucket_local_aliases: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiBucketQuotas {
	pub max_size: Option<u64>,
	pub max_objects: Option<u64>,
}

// ---- CreateBucket ----

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateBucketRequest {
	pub global_alias: Option<String>,
	pub local_alias: Option<CreateBucketLocalAlias>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateBucketResponse(pub GetBucketInfoResponse);

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateBucketResponse(pub GetBucketInfoResponse);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateBucketRequestBody {
	pub website_access: Option<UpdateBucketWebsiteAccess>,
	pub quotas: Option<ApiBucketQuotas>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

// **********************************************
//      Operations on permissions for keys on buckets
// **********************************************

// ---- AllowBucketKey ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllowBucketKeyRequest(pub BucketKeyPermChangeRequest);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllowBucketKeyResponse(pub GetBucketInfoResponse);

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BucketKeyPermChangeRequest {
	pub bucket_id: String,
	pub access_key_id: String,
	pub permissions: ApiBucketKeyPerm,
}

// ---- DenyBucketKey ----

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DenyBucketKeyRequest(pub BucketKeyPermChangeRequest);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DenyBucketKeyResponse(pub GetBucketInfoResponse);

// **********************************************
//      Operations on bucket aliases
// **********************************************

// ---- AddBucketAlias ----

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AddBucketAliasRequest {
	pub bucket_id: String,
	#[serde(flatten)]
	pub alias: BucketAliasEnum,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AddBucketAliasResponse(pub GetBucketInfoResponse);

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoveBucketAliasRequest {
	pub bucket_id: String,
	#[serde(flatten)]
	pub alias: BucketAliasEnum,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoveBucketAliasResponse(pub GetBucketInfoResponse);
