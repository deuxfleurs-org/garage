use std::borrow::Cow;

use hyper::{Method, Request};

use garage_api_common::router_macros::*;

use crate::error::*;
use crate::router_v0;

router_match! {@func

/// List of all Admin API endpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
	Options,
	CheckDomain,
	Health,
	Metrics,
	GetClusterStatus,
	GetClusterHealth,
	ConnectClusterNodes,
	// Layout
	GetClusterLayout,
	UpdateClusterLayout,
	ApplyClusterLayout,
	RevertClusterLayout,
	// Keys
	ListKeys,
	CreateKey,
	ImportKey,
	GetKeyInfo {
		id: Option<String>,
		search: Option<String>,
		show_secret_key: Option<String>,
	},
	DeleteKey {
		id: String,
	},
	UpdateKey {
		id: String,
	},
	// Buckets
	ListBuckets,
	CreateBucket,
	GetBucketInfo {
		id: Option<String>,
		global_alias: Option<String>,
	},
	DeleteBucket {
		id: String,
	},
	UpdateBucket {
		id: String,
	},
	// Bucket-Key Permissions
	BucketAllowKey,
	BucketDenyKey,
	// Bucket aliases
	GlobalAliasBucket {
		id: String,
		alias: String,
	},
	GlobalUnaliasBucket {
		id: String,
		alias: String,
	},
	LocalAliasBucket {
		id: String,
		access_key_id: String,
		alias: String,
	},
	LocalUnaliasBucket {
		id: String,
		access_key_id: String,
		alias: String,
	},
}}

impl Endpoint {
	/// Determine which S3 endpoint a request is for using the request, and a bucket which was
	/// possibly extracted from the Host header.
	/// Returns Self plus bucket name, if endpoint is not Endpoint::ListBuckets
	pub fn from_request<T>(req: &Request<T>) -> Result<Self, Error> {
		let uri = req.uri();
		let path = uri.path();
		let query = uri.query();

		let mut query = QueryParameters::from_query(query.unwrap_or_default())?;

		let res = router_match!(@gen_path_parser (req.method(), path, query) [
			OPTIONS _ => Options,
			GET "/check" => CheckDomain,
			GET "/health" => Health,
			GET "/metrics" => Metrics,
			GET "/v1/status" => GetClusterStatus,
			GET "/v1/health" => GetClusterHealth,
			POST "/v1/connect" => ConnectClusterNodes,
			// Layout endpoints
			GET "/v1/layout" => GetClusterLayout,
			POST "/v1/layout" => UpdateClusterLayout,
			POST "/v1/layout/apply" => ApplyClusterLayout,
			POST "/v1/layout/revert" => RevertClusterLayout,
			// API key endpoints
			GET "/v1/key" if id => GetKeyInfo (query_opt::id, query_opt::search, query_opt::show_secret_key),
			GET "/v1/key" if search => GetKeyInfo (query_opt::id, query_opt::search, query_opt::show_secret_key),
			POST "/v1/key" if id => UpdateKey (query::id),
			POST "/v1/key" => CreateKey,
			POST "/v1/key/import" => ImportKey,
			DELETE "/v1/key" if id => DeleteKey (query::id),
			GET "/v1/key" => ListKeys,
			// Bucket endpoints
			GET "/v1/bucket" if id => GetBucketInfo (query_opt::id, query_opt::global_alias),
			GET "/v1/bucket" if global_alias => GetBucketInfo (query_opt::id, query_opt::global_alias),
			GET "/v1/bucket" => ListBuckets,
			POST "/v1/bucket" => CreateBucket,
			DELETE "/v1/bucket" if id => DeleteBucket (query::id),
			PUT "/v1/bucket" if id => UpdateBucket (query::id),
			// Bucket-key permissions
			POST "/v1/bucket/allow" => BucketAllowKey,
			POST "/v1/bucket/deny" => BucketDenyKey,
			// Bucket aliases
			PUT "/v1/bucket/alias/global" => GlobalAliasBucket (query::id, query::alias),
			DELETE "/v1/bucket/alias/global" => GlobalUnaliasBucket (query::id, query::alias),
			PUT "/v1/bucket/alias/local" => LocalAliasBucket (query::id, query::access_key_id, query::alias),
			DELETE "/v1/bucket/alias/local" => LocalUnaliasBucket (query::id, query::access_key_id, query::alias),
		]);

		if let Some(message) = query.nonempty_message() {
			debug!("Unused query parameter: {}", message)
		}

		Ok(res)
	}
	/// Some endpoints work exactly the same in their v1/ version as they did in their v0/ version.
	/// For these endpoints, we can convert a v0/ call to its equivalent as if it was made using
	/// its v1/ URL.
	pub fn from_v0(v0_endpoint: router_v0::Endpoint) -> Result<Self, Error> {
		match v0_endpoint {
			// Cluster endpoints
			router_v0::Endpoint::ConnectClusterNodes => Ok(Self::ConnectClusterNodes),
			// - GetClusterStatus: response format changed
			// - GetClusterHealth: response format changed

			// Layout endpoints
			router_v0::Endpoint::RevertClusterLayout => Ok(Self::RevertClusterLayout),
			// - GetClusterLayout: response format changed
			// - UpdateClusterLayout: query format changed
			// - ApplyCusterLayout: response format changed

			// Key endpoints
			router_v0::Endpoint::ListKeys => Ok(Self::ListKeys),
			router_v0::Endpoint::CreateKey => Ok(Self::CreateKey),
			router_v0::Endpoint::GetKeyInfo { id, search } => Ok(Self::GetKeyInfo {
				id,
				search,
				show_secret_key: Some("true".into()),
			}),
			router_v0::Endpoint::DeleteKey { id } => Ok(Self::DeleteKey { id }),
			// - UpdateKey: response format changed (secret key no longer returned)

			// Bucket endpoints
			router_v0::Endpoint::GetBucketInfo { id, global_alias } => {
				Ok(Self::GetBucketInfo { id, global_alias })
			}
			router_v0::Endpoint::ListBuckets => Ok(Self::ListBuckets),
			router_v0::Endpoint::CreateBucket => Ok(Self::CreateBucket),
			router_v0::Endpoint::DeleteBucket { id } => Ok(Self::DeleteBucket { id }),
			router_v0::Endpoint::UpdateBucket { id } => Ok(Self::UpdateBucket { id }),

			// Bucket-key permissions
			router_v0::Endpoint::BucketAllowKey => Ok(Self::BucketAllowKey),
			router_v0::Endpoint::BucketDenyKey => Ok(Self::BucketDenyKey),

			// Bucket alias endpoints
			router_v0::Endpoint::GlobalAliasBucket { id, alias } => {
				Ok(Self::GlobalAliasBucket { id, alias })
			}
			router_v0::Endpoint::GlobalUnaliasBucket { id, alias } => {
				Ok(Self::GlobalUnaliasBucket { id, alias })
			}
			router_v0::Endpoint::LocalAliasBucket {
				id,
				access_key_id,
				alias,
			} => Ok(Self::LocalAliasBucket {
				id,
				access_key_id,
				alias,
			}),
			router_v0::Endpoint::LocalUnaliasBucket {
				id,
				access_key_id,
				alias,
			} => Ok(Self::LocalUnaliasBucket {
				id,
				access_key_id,
				alias,
			}),

			// For endpoints that have different body content syntax, issue
			// deprecation warning
			_ => Err(Error::bad_request(format!(
				"v0/ endpoint is no longer supported: {}",
				v0_endpoint.name()
			))),
		}
	}
}

generateQueryParameters! {
	keywords: [],
	fields: [
		"format" => format,
		"id" => id,
		"search" => search,
		"globalAlias" => global_alias,
		"alias" => alias,
		"accessKeyId" => access_key_id,
		"showSecretKey" => show_secret_key
	]
}
