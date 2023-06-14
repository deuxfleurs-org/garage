use std::borrow::Cow;

use hyper::{Method, Request};

use crate::admin::error::*;
use crate::router_macros::*;

pub enum Authorization {
	None,
	MetricsToken,
	AdminToken,
}

router_match! {@func

/// List of all Admin API endpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
	Options,
	CheckWebsiteEnabled,
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
			GET "/check" => CheckWebsiteEnabled,
			GET "/health" => Health,
			GET "/metrics" => Metrics,
			GET "/v1/status" => GetClusterStatus,
			GET "/v1/health" => GetClusterHealth,
			POST "/v0/connect" => ConnectClusterNodes,
			// Layout endpoints
			GET "/v1/layout" => GetClusterLayout,
			POST "/v1/layout" => UpdateClusterLayout,
			POST "/v0/layout/apply" => ApplyClusterLayout,
			POST "/v0/layout/revert" => RevertClusterLayout,
			// API key endpoints
			GET "/v0/key" if id => GetKeyInfo (query_opt::id, query_opt::search),
			GET "/v0/key" if search => GetKeyInfo (query_opt::id, query_opt::search),
			POST "/v0/key" if id => UpdateKey (query::id),
			POST "/v0/key" => CreateKey,
			POST "/v0/key/import" => ImportKey,
			DELETE "/v0/key" if id => DeleteKey (query::id),
			GET "/v0/key" => ListKeys,
			// Bucket endpoints
			GET "/v0/bucket" if id => GetBucketInfo (query_opt::id, query_opt::global_alias),
			GET "/v0/bucket" if global_alias => GetBucketInfo (query_opt::id, query_opt::global_alias),
			GET "/v0/bucket" => ListBuckets,
			POST "/v0/bucket" => CreateBucket,
			DELETE "/v0/bucket" if id => DeleteBucket (query::id),
			PUT "/v0/bucket" if id => UpdateBucket (query::id),
			// Bucket-key permissions
			POST "/v0/bucket/allow" => BucketAllowKey,
			POST "/v0/bucket/deny" => BucketDenyKey,
			// Bucket aliases
			PUT "/v0/bucket/alias/global" => GlobalAliasBucket (query::id, query::alias),
			DELETE "/v0/bucket/alias/global" => GlobalUnaliasBucket (query::id, query::alias),
			PUT "/v0/bucket/alias/local" => LocalAliasBucket (query::id, query::access_key_id, query::alias),
			DELETE "/v0/bucket/alias/local" => LocalUnaliasBucket (query::id, query::access_key_id, query::alias),
		]);

		if let Some(message) = query.nonempty_message() {
			debug!("Unused query parameter: {}", message)
		}

		Ok(res)
	}
	/// Get the kind of authorization which is required to perform the operation.
	pub fn authorization_type(&self) -> Authorization {
		match self {
			Self::Health => Authorization::None,
			Self::CheckWebsiteEnabled => Authorization::None,
			Self::Metrics => Authorization::MetricsToken,
			_ => Authorization::AdminToken,
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
		"accessKeyId" => access_key_id
	]
}
