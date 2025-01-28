use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use garage_model::garage::Garage;

use crate::admin::error::Error;
use crate::admin::EndpointHandler;
use crate::helpers::is_default;

pub enum AdminApiRequest {
	// Special endpoints of the Admin API
	Options(OptionsRequest),
	CheckDomain(CheckDomainRequest),
	Health(HealthRequest),
	Metrics(MetricsRequest),

	// Cluster operations
	GetClusterStatus(GetClusterStatusRequest),
	GetClusterHealth(GetClusterHealthRequest),
	ConnectClusterNodes(ConnectClusterNodesRequest),
	GetClusterLayout(GetClusterLayoutRequest),
	UpdateClusterLayout(UpdateClusterLayoutRequest),
	ApplyClusterLayout(ApplyClusterLayoutRequest),
	RevertClusterLayout(RevertClusterLayoutRequest),

	// Access key operations
	ListKeys(ListKeysRequest),
	GetKeyInfo(GetKeyInfoRequest),
	CreateKey(CreateKeyRequest),
	ImportKey(ImportKeyRequest),
	UpdateKey(UpdateKeyRequest),
	DeleteKey(DeleteKeyRequest),

	// Bucket operations
	ListBuckets(ListBucketsRequest),
	GetBucketInfo(GetBucketInfoRequest),
	CreateBucket(CreateBucketRequest),
	UpdateBucket(UpdateBucketRequest),
	DeleteBucket(DeleteBucketRequest),

	// Operations on permissions for keys on buckets
	BucketAllowKey(BucketAllowKeyRequest),
	BucketDenyKey(BucketDenyKeyRequest),

	// Operations on bucket aliases
	GlobalAliasBucket(GlobalAliasBucketRequest),
	GlobalUnaliasBucket(GlobalUnaliasBucketRequest),
	LocalAliasBucket(LocalAliasBucketRequest),
	LocalUnaliasBucket(LocalUnaliasBucketRequest),
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum AdminApiResponse {
	// Cluster operations
	GetClusterStatus(GetClusterStatusResponse),
	GetClusterHealth(GetClusterHealthResponse),
	ConnectClusterNodes(ConnectClusterNodesResponse),
	GetClusterLayout(GetClusterLayoutResponse),
	UpdateClusterLayout(UpdateClusterLayoutResponse),
	ApplyClusterLayout(ApplyClusterLayoutResponse),
	RevertClusterLayout(RevertClusterLayoutResponse),

	// Access key operations
	ListKeys(ListKeysResponse),
	GetKeyInfo(GetKeyInfoResponse),
	CreateKey(CreateKeyResponse),
	ImportKey(ImportKeyResponse),
	UpdateKey(UpdateKeyResponse),
	DeleteKey(DeleteKeyResponse),

	// Bucket operations
	ListBuckets(ListBucketsResponse),
	GetBucketInfo(GetBucketInfoResponse),
	CreateBucket(CreateBucketResponse),
	UpdateBucket(UpdateBucketResponse),
	DeleteBucket(DeleteBucketResponse),

	// Operations on permissions for keys on buckets
	BucketAllowKey(BucketAllowKeyResponse),
	BucketDenyKey(BucketDenyKeyResponse),

	// Operations on bucket aliases
	GlobalAliasBucket(GlobalAliasBucketResponse),
	GlobalUnaliasBucket(GlobalUnaliasBucketResponse),
	LocalAliasBucket(LocalAliasBucketResponse),
	LocalUnaliasBucket(LocalUnaliasBucketResponse),
}

#[async_trait]
impl EndpointHandler for AdminApiRequest {
	type Response = AdminApiResponse;

	async fn handle(self, garage: &Arc<Garage>) -> Result<AdminApiResponse, Error> {
		Ok(match self {
			Self::Options | Self::CheckDomain | Self::Health | Self::Metrics => unreachable!(),
			// Cluster operations
			Self::GetClusterStatus(req) => {
				AdminApiResponse::GetClusterStatus(req.handle(garage).await?)
			}
			Self::GetClusterHealth(req) => {
				AdminApiResponse::GetClusterHealth(req.handle(garage).await?)
			}
			Self::ConnectClusterNodes(req) => {
				AdminApiResponse::ConnectClusterNodes(req.handle(garage).await?)
			}
			Self::GetClusterLayout(req) => {
				AdminApiResponse::GetClusterLayout(req.handle(garage).await?)
			}
			Self::UpdateClusterLayout(req) => {
				AdminApiResponse::UpdateClusterLayout(req.handle(garage).await?)
			}
			Self::ApplyClusterLayout(req) => {
				AdminApiResponse::ApplyClusterLayout(req.handle(garage).await?)
			}
			Self::RevertClusterLayout(req) => {
				AdminApiResponse::RevertClusterLayout(req.handle(garage).await?)
			}

			// Access key operations
			Self::ListKeys(req) => AdminApiResponse::ListKeys(req.handle(garage).await?),
			Self::GetKeyInfo(req) => AdminApiResponse::GetKeyInfo(req.handle(garage).await?),
			Self::CreateKey(req) => AdminApiResponse::CreateKey(req.handle(garage).await?),
			Self::ImportKey(req) => AdminApiResponse::ImportKey(req.handle(garage).await?),
			Self::UpdateKey(req) => AdminApiResponse::UpdateKey(req.handle(garage).await?),
			Self::DeleteKey(req) => AdminApiResponse::DeleteKey(req.handle(garage).await?),

			// Bucket operations
			Self::ListBuckets(req) => AdminApiResponse::ListBuckets(req.handle(garage).await?),
			Self::GetBucketInfo(req) => AdminApiResponse::GetBucketInfo(req.handle(garage).await?),
			Self::CreateBucket(req) => AdminApiResponse::CreateBucket(req.handle(garage).await?),
			Self::UpdateBucket(req) => AdminApiResponse::UpdateBucket(req.handle(garage).await?),
			Self::DeleteBucket(req) => AdminApiResponse::DeleteBucket(req.handle(garage).await?),

			// Operations on permissions for keys on buckets
			Self::BucketAllowKey(req) => {
				AdminApiResponse::BucketAllowKey(req.handle(garage).await?)
			}
			Self::BucketDenyKey(req) => AdminApiResponse::BucketDenyKey(req.handle(garage).await?),

			// Operations on bucket aliases
			Self::GlobalAliasBucket(req) => {
				AdminApiResponse::GlobalAliasBucket(req.handle(garage).await?)
			}
			Self::GlobalUnaliasBucket(req) => {
				AdminApiResponse::GlobalUnaliasBucket(req.handle(garage).await?)
			}
			Self::LocalAliasBucket(req) => {
				AdminApiResponse::LocalAliasBucket(req.handle(garage).await?)
			}
			Self::LocalUnaliasBucket(req) => {
				AdminApiResponse::LocalUnaliasBucket(req.handle(garage).await?)
			}
		})
	}
}

// **********************************************
//      Special endpoints
// **********************************************

pub struct OptionsRequest;

pub struct CheckDomainRequest {
	pub domain: String,
}

pub struct HealthRequest;

pub struct MetricsRequest;

// **********************************************
//      Cluster operations
// **********************************************

// ---- GetClusterStatus ----

pub struct GetClusterStatusRequest;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetClusterStatusResponse {
	pub node: String,
	pub garage_version: &'static str,
	pub garage_features: Option<&'static [&'static str]>,
	pub rust_version: &'static str,
	pub db_engine: String,
	pub layout_version: u64,
	pub nodes: Vec<NodeResp>,
}

#[derive(Serialize, Default)]
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

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeRoleResp {
	pub id: String,
	pub zone: String,
	pub capacity: Option<u64>,
	pub tags: Vec<String>,
}

#[derive(Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct FreeSpaceResp {
	pub available: u64,
	pub total: u64,
}

// ---- GetClusterHealth ----

pub struct GetClusterHealthRequest;

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetClusterHealthResponse {
	pub status: &'static str,
	pub known_nodes: usize,
	pub connected_nodes: usize,
	pub storage_nodes: usize,
	pub storage_nodes_ok: usize,
	pub partitions: usize,
	pub partitions_quorum: usize,
	pub partitions_all_ok: usize,
}

// ---- ConnectClusterNodes ----

#[derive(Debug, Clone, Deserialize)]
pub struct ConnectClusterNodesRequest(pub Vec<String>);

#[derive(Serialize)]
pub struct ConnectClusterNodesResponse(pub Vec<ConnectClusterNodeResponse>);

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ConnectClusterNodeResponse {
	pub success: bool,
	pub error: Option<String>,
}

// ---- GetClusterLayout ----

pub struct GetClusterLayoutRequest;

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetClusterLayoutResponse {
	pub version: u64,
	pub roles: Vec<NodeRoleResp>,
	pub staged_role_changes: Vec<NodeRoleChange>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeRoleChange {
	pub id: String,
	#[serde(flatten)]
	pub action: NodeRoleChangeEnum,
}

#[derive(Serialize, Deserialize)]
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

#[derive(Deserialize)]
pub struct UpdateClusterLayoutRequest(pub Vec<NodeRoleChange>);

#[derive(Serialize)]
pub struct UpdateClusterLayoutResponse(pub GetClusterLayoutResponse);

// ---- ApplyClusterLayout ----

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplyClusterLayoutRequest {
	pub version: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApplyClusterLayoutResponse {
	pub message: Vec<String>,
	pub layout: GetClusterLayoutResponse,
}

// ---- RevertClusterLayout ----

pub struct RevertClusterLayoutRequest;

#[derive(Serialize)]
pub struct RevertClusterLayoutResponse(pub GetClusterLayoutResponse);

// **********************************************
//      Access key operations
// **********************************************

// ---- ListKeys ----

pub struct ListKeysRequest;

#[derive(Serialize)]
pub struct ListKeysResponse(pub Vec<ListKeysResponseItem>);

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListKeysResponseItem {
	pub id: String,
	pub name: String,
}

// ---- GetKeyInfo ----

pub struct GetKeyInfoRequest {
	pub id: Option<String>,
	pub search: Option<String>,
	pub show_secret_key: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetKeyInfoResponse {
	pub name: String,
	pub access_key_id: String,
	#[serde(skip_serializing_if = "is_default")]
	pub secret_access_key: Option<String>,
	pub permissions: KeyPerm,
	pub buckets: Vec<KeyInfoBucketResponse>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyPerm {
	#[serde(default)]
	pub create_bucket: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyInfoBucketResponse {
	pub id: String,
	pub global_aliases: Vec<String>,
	pub local_aliases: Vec<String>,
	pub permissions: ApiBucketKeyPerm,
}

#[derive(Serialize, Deserialize, Default)]
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

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateKeyRequest {
	pub name: Option<String>,
}

#[derive(Serialize)]
pub struct CreateKeyResponse(pub GetKeyInfoResponse);

// ---- ImportKey ----

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ImportKeyRequest {
	pub access_key_id: String,
	pub secret_access_key: String,
	pub name: Option<String>,
}

#[derive(Serialize)]
pub struct ImportKeyResponse(pub GetKeyInfoResponse);

// ---- UpdateKey ----

pub struct UpdateKeyRequest {
	pub id: String,
	pub body: UpdateKeyRequestBody,
}

#[derive(Serialize)]
pub struct UpdateKeyResponse(pub GetKeyInfoResponse);

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateKeyRequestBody {
	// TODO: id (get parameter) goes here
	pub name: Option<String>,
	pub allow: Option<KeyPerm>,
	pub deny: Option<KeyPerm>,
}

// ---- DeleteKey ----

pub struct DeleteKeyRequest {
	pub id: String,
}

#[derive(Serialize)]
pub struct DeleteKeyResponse;

// **********************************************
//      Bucket operations
// **********************************************

// ---- ListBuckets ----

pub struct ListBucketsRequest;

#[derive(Serialize)]
pub struct ListBucketsResponse(pub Vec<ListBucketsResponseItem>);

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListBucketsResponseItem {
	pub id: String,
	pub global_aliases: Vec<String>,
	pub local_aliases: Vec<BucketLocalAlias>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BucketLocalAlias {
	pub access_key_id: String,
	pub alias: String,
}

// ---- GetBucketInfo ----

pub struct GetBucketInfoRequest {
	pub id: Option<String>,
	pub global_alias: Option<String>,
}

#[derive(Serialize)]
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

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetBucketInfoWebsiteResponse {
	pub index_document: String,
	pub error_document: Option<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GetBucketInfoKey {
	pub access_key_id: String,
	pub name: String,
	pub permissions: ApiBucketKeyPerm,
	pub bucket_local_aliases: Vec<String>,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiBucketQuotas {
	pub max_size: Option<u64>,
	pub max_objects: Option<u64>,
}

// ---- CreateBucket ----

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateBucketRequest {
	pub global_alias: Option<String>,
	pub local_alias: Option<CreateBucketLocalAlias>,
}

#[derive(Serialize)]
pub struct CreateBucketResponse(pub GetBucketInfoResponse);

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateBucketLocalAlias {
	pub access_key_id: String,
	pub alias: String,
	#[serde(default)]
	pub allow: ApiBucketKeyPerm,
}

// ---- UpdateBucket ----

pub struct UpdateBucketRequest {
	pub id: String,
	pub body: UpdateBucketRequestBody,
}

#[derive(Serialize)]
pub struct UpdateBucketResponse(pub GetBucketInfoResponse);

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateBucketRequestBody {
	pub website_access: Option<UpdateBucketWebsiteAccess>,
	pub quotas: Option<ApiBucketQuotas>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateBucketWebsiteAccess {
	pub enabled: bool,
	pub index_document: Option<String>,
	pub error_document: Option<String>,
}

// ---- DeleteBucket ----

pub struct DeleteBucketRequest {
	pub id: String,
}

#[derive(Serialize)]
pub struct DeleteBucketResponse;

// **********************************************
//      Operations on permissions for keys on buckets
// **********************************************

// ---- BucketAllowKey ----

#[derive(Deserialize)]
pub struct BucketAllowKeyRequest(pub BucketKeyPermChangeRequest);

#[derive(Serialize)]
pub struct BucketAllowKeyResponse(pub GetBucketInfoResponse);

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BucketKeyPermChangeRequest {
	pub bucket_id: String,
	pub access_key_id: String,
	pub permissions: ApiBucketKeyPerm,
}

// ---- BucketDenyKey ----

#[derive(Deserialize)]
pub struct BucketDenyKeyRequest(pub BucketKeyPermChangeRequest);

#[derive(Serialize)]
pub struct BucketDenyKeyResponse(pub GetBucketInfoResponse);

// **********************************************
//      Operations on bucket aliases
// **********************************************

// ---- GlobalAliasBucket ----

pub struct GlobalAliasBucketRequest {
	pub id: String,
	pub alias: String,
}

#[derive(Serialize)]
pub struct GlobalAliasBucketResponse(pub GetBucketInfoResponse);

// ---- GlobalUnaliasBucket ----

pub struct GlobalUnaliasBucketRequest {
	pub id: String,
	pub alias: String,
}

#[derive(Serialize)]
pub struct GlobalUnaliasBucketResponse(pub GetBucketInfoResponse);

// ---- LocalAliasBucket ----

pub struct LocalAliasBucketRequest {
	pub id: String,
	pub access_key_id: String,
	pub alias: String,
}

#[derive(Serialize)]
pub struct LocalAliasBucketResponse(pub GetBucketInfoResponse);

// ---- LocalUnaliasBucket ----

pub struct LocalUnaliasBucketRequest {
	pub id: String,
	pub access_key_id: String,
	pub alias: String,
}

#[derive(Serialize)]
pub struct LocalUnaliasBucketResponse(pub GetBucketInfoResponse);
