use std::borrow::Cow;

use hyper::{Method, Request};

use garage_api_common::router_macros::*;

use crate::error::*;

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
			GET "/v0/status" => GetClusterStatus,
			GET "/v0/health" => GetClusterHealth,
			POST "/v0/connect" => ConnectClusterNodes,
			// Layout endpoints
			GET "/v0/layout" => GetClusterLayout,
			POST "/v0/layout" => UpdateClusterLayout,
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
