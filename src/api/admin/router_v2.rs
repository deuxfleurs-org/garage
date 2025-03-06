use std::borrow::Cow;

use hyper::body::Incoming as IncomingBody;
use hyper::{Method, Request};
use paste::paste;

use garage_api_common::helpers::*;
use garage_api_common::router_macros::*;

use crate::api::*;
use crate::error::*;
use crate::router_v1;
use crate::Authorization;

impl AdminApiRequest {
	/// Determine which S3 endpoint a request is for using the request, and a bucket which was
	/// possibly extracted from the Host header.
	/// Returns Self plus bucket name, if endpoint is not Endpoint::ListBuckets
	pub async fn from_request(req: Request<IncomingBody>) -> Result<Self, Error> {
		let uri = req.uri().clone();
		let path = uri.path();
		let query = uri.query();

		let method = req.method().clone();

		let mut query = QueryParameters::from_query(query.unwrap_or_default())?;

		let res = router_match!(@gen_path_parser_v2 (&method, path, "/v2/", query, req) [
			@special OPTIONS _ => Options (),
			@special GET "/check" => CheckDomain (query::domain),
			@special GET "/health" => Health (),
			@special GET "/metrics" => Metrics (),
			// Cluster endpoints
			GET GetClusterStatus (),
			GET GetClusterHealth (),
			POST ConnectClusterNodes (body),
			// Layout endpoints
			GET GetClusterLayout (),
			GET GetClusterLayoutHistory (),
			POST UpdateClusterLayout (body),
			POST PreviewClusterLayoutChanges (),
			POST ApplyClusterLayout (body),
			POST RevertClusterLayout (),
			// API key endpoints
			GET GetKeyInfo (query_opt::id, query_opt::search, parse_default(false)::show_secret_key),
			POST UpdateKey (body_field, query::id),
			POST CreateKey (body),
			POST ImportKey (body),
			POST DeleteKey (query::id),
			GET ListKeys (),
			// Bucket endpoints
			GET GetBucketInfo (query_opt::id, query_opt::global_alias, query_opt::search),
			GET ListBuckets (),
			POST CreateBucket (body),
			POST DeleteBucket (query::id),
			POST UpdateBucket (body_field, query::id),
			POST CleanupIncompleteUploads (body),
			// Bucket-key permissions
			POST AllowBucketKey (body),
			POST DenyBucketKey (body),
			// Bucket aliases
			POST AddBucketAlias (body),
			POST RemoveBucketAlias (body),
			// Node APIs
			GET GetNodeInfo (default::body, query::node),
			POST CreateMetadataSnapshot (default::body, query::node),
			GET GetNodeStatistics (default::body, query::node),
			GET GetClusterStatistics (),
			POST LaunchRepairOperation (body_field, query::node),
			// Worker APIs
			POST ListWorkers (body_field, query::node),
			POST GetWorkerInfo (body_field, query::node),
			POST GetWorkerVariable (body_field, query::node),
			POST SetWorkerVariable (body_field, query::node),
			// Block APIs
			GET ListBlockErrors (default::body, query::node),
			POST GetBlockInfo (body_field, query::node),
			POST RetryBlockResync (body_field, query::node),
			POST PurgeBlocks (body_field, query::node),
		]);

		if let Some(message) = query.nonempty_message() {
			debug!("Unused query parameter: {}", message)
		}

		Ok(res)
	}

	/// Some endpoints work exactly the same in their v2/ version as they did in their v1/ version.
	/// For these endpoints, we can convert a v1/ call to its equivalent as if it was made using
	/// its v2/ URL.
	pub async fn from_v1(
		v1_endpoint: router_v1::Endpoint,
		req: Request<IncomingBody>,
	) -> Result<Self, Error> {
		use router_v1::Endpoint;

		match v1_endpoint {
			// GetClusterStatus semantics changed:
			// info about local node is no longer returned
			Endpoint::GetClusterHealth => {
				Ok(AdminApiRequest::GetClusterHealth(GetClusterHealthRequest))
			}
			Endpoint::ConnectClusterNodes => {
				let req = parse_json_body::<ConnectClusterNodesRequest, _, Error>(req).await?;
				Ok(AdminApiRequest::ConnectClusterNodes(req))
			}

			// Layout
			Endpoint::GetClusterLayout => {
				Ok(AdminApiRequest::GetClusterLayout(GetClusterLayoutRequest))
			}
			// UpdateClusterLayout semantics changed
			Endpoint::ApplyClusterLayout => {
				let param = parse_json_body::<ApplyClusterLayoutRequest, _, Error>(req).await?;
				Ok(AdminApiRequest::ApplyClusterLayout(param))
			}
			Endpoint::RevertClusterLayout => Ok(AdminApiRequest::RevertClusterLayout(
				RevertClusterLayoutRequest,
			)),

			// Keys
			Endpoint::ListKeys => Ok(AdminApiRequest::ListKeys(ListKeysRequest)),
			Endpoint::GetKeyInfo {
				id,
				search,
				show_secret_key,
			} => {
				let show_secret_key = show_secret_key.map(|x| x == "true").unwrap_or(false);
				Ok(AdminApiRequest::GetKeyInfo(GetKeyInfoRequest {
					id,
					search,
					show_secret_key,
				}))
			}
			Endpoint::CreateKey => {
				let req = parse_json_body::<CreateKeyRequest, _, Error>(req).await?;
				Ok(AdminApiRequest::CreateKey(req))
			}
			Endpoint::ImportKey => {
				let req = parse_json_body::<ImportKeyRequest, _, Error>(req).await?;
				Ok(AdminApiRequest::ImportKey(req))
			}
			Endpoint::UpdateKey { id } => {
				let body = parse_json_body::<UpdateKeyRequestBody, _, Error>(req).await?;
				Ok(AdminApiRequest::UpdateKey(UpdateKeyRequest { id, body }))
			}

			// DeleteKey semantics changed:
			// - in v1/ : HTTP DELETE => HTTP 204 No Content
			// - in v2/ : HTTP POST => HTTP 200 Ok
			// Endpoint::DeleteKey { id } => Ok(AdminApiRequest::DeleteKey(DeleteKeyRequest { id })),

			// Buckets
			Endpoint::ListBuckets => Ok(AdminApiRequest::ListBuckets(ListBucketsRequest)),
			Endpoint::GetBucketInfo { id, global_alias } => {
				Ok(AdminApiRequest::GetBucketInfo(GetBucketInfoRequest {
					id,
					global_alias,
					search: None,
				}))
			}
			Endpoint::CreateBucket => {
				let req = parse_json_body::<CreateBucketRequest, _, Error>(req).await?;
				Ok(AdminApiRequest::CreateBucket(req))
			}

			// DeleteBucket semantics changed::
			// - in v1/ : HTTP DELETE => HTTP 204 No Content
			// - in v2/ : HTTP POST => HTTP 200 Ok
			// Endpoint::DeleteBucket { id } => {
			// 	Ok(AdminApiRequest::DeleteBucket(DeleteBucketRequest { id }))
			// }
			Endpoint::UpdateBucket { id } => {
				let body = parse_json_body::<UpdateBucketRequestBody, _, Error>(req).await?;
				Ok(AdminApiRequest::UpdateBucket(UpdateBucketRequest {
					id,
					body,
				}))
			}

			// Bucket-key permissions
			Endpoint::BucketAllowKey => {
				let req = parse_json_body::<BucketKeyPermChangeRequest, _, Error>(req).await?;
				Ok(AdminApiRequest::AllowBucketKey(AllowBucketKeyRequest(req)))
			}
			Endpoint::BucketDenyKey => {
				let req = parse_json_body::<BucketKeyPermChangeRequest, _, Error>(req).await?;
				Ok(AdminApiRequest::DenyBucketKey(DenyBucketKeyRequest(req)))
			}
			// Bucket aliasing
			Endpoint::GlobalAliasBucket { id, alias } => {
				Ok(AdminApiRequest::AddBucketAlias(AddBucketAliasRequest {
					bucket_id: id,
					alias: BucketAliasEnum::Global {
						global_alias: alias,
					},
				}))
			}
			Endpoint::GlobalUnaliasBucket { id, alias } => Ok(AdminApiRequest::RemoveBucketAlias(
				RemoveBucketAliasRequest {
					bucket_id: id,
					alias: BucketAliasEnum::Global {
						global_alias: alias,
					},
				},
			)),
			Endpoint::LocalAliasBucket {
				id,
				access_key_id,
				alias,
			} => Ok(AdminApiRequest::AddBucketAlias(AddBucketAliasRequest {
				bucket_id: id,
				alias: BucketAliasEnum::Local {
					local_alias: alias,
					access_key_id,
				},
			})),
			Endpoint::LocalUnaliasBucket {
				id,
				access_key_id,
				alias,
			} => Ok(AdminApiRequest::RemoveBucketAlias(
				RemoveBucketAliasRequest {
					bucket_id: id,
					alias: BucketAliasEnum::Local {
						local_alias: alias,
						access_key_id,
					},
				},
			)),

			// For endpoints that have different body content syntax, issue
			// deprecation warning
			_ => Err(Error::bad_request(format!(
				"v1/ endpoint is no longer supported: {}",
				v1_endpoint.name()
			))),
		}
	}

	/// Get the kind of authorization which is required to perform the operation.
	pub fn authorization_type(&self) -> Authorization {
		match self {
			Self::Options(_) => Authorization::None,
			Self::Health(_) => Authorization::None,
			Self::CheckDomain(_) => Authorization::None,
			Self::Metrics(_) => Authorization::MetricsToken,
			_ => Authorization::AdminToken,
		}
	}
}

generateQueryParameters! {
	keywords: [],
	fields: [
		"node" => node,
		"domain" => domain,
		"format" => format,
		"id" => id,
		"search" => search,
		"globalAlias" => global_alias,
		"alias" => alias,
		"accessKeyId" => access_key_id,
		"showSecretKey" => show_secret_key
	]
}
