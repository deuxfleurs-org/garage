use std::borrow::Cow;

use hyper::{Method, Request};

use crate::admin::error::*;
use crate::router_macros::*;

pub enum Authorization {
	MetricsToken,
	AdminToken,
}

router_match! {@func

/// List of all Admin API endpoints.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpoint {
	Options,
	Metrics,
	GetClusterStatus,
	// Layout
	GetClusterLayout,
	UpdateClusterLayout,
	ApplyClusterLayout,
	RevertClusterLayout,
	// Keys
	ListKeys,
	CreateKey,
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
	// Bucket-Key Permissions
	BucketAllowKey,
	BucketDenyKey,
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
			GET "/metrics" => Metrics,
			GET "/status" => GetClusterStatus,
			// Layout endpoints
			GET "/layout" => GetClusterLayout,
			POST "/layout" => UpdateClusterLayout,
			POST "/layout/apply" => ApplyClusterLayout,
			POST "/layout/revert" => RevertClusterLayout,
			// API key endpoints
			GET "/key" if id => GetKeyInfo (query_opt::id, query_opt::search),
			GET "/key" if search => GetKeyInfo (query_opt::id, query_opt::search),
			POST "/key" if id => UpdateKey (query::id),
			POST "/key" => CreateKey,
			DELETE "/key" if id => DeleteKey (query::id),
			GET "/key" => ListKeys,
			// Bucket endpoints
			GET "/bucket" if id => GetBucketInfo (query_opt::id, query_opt::global_alias),
			GET "/bucket" if global_alias => GetBucketInfo (query_opt::id, query_opt::global_alias),
			GET "/bucket" => ListBuckets,
			POST "/bucket" => CreateBucket,
			DELETE "/bucket" if id => DeleteBucket (query::id),
			// Bucket-key permissions
			POST "/bucket/allow" => BucketAllowKey,
			POST "/bucket/deny" => BucketDenyKey,
		]);

		if let Some(message) = query.nonempty_message() {
			debug!("Unused query parameter: {}", message)
		}

		Ok(res)
	}
	/// Get the kind of authorization which is required to perform the operation.
	pub fn authorization_type(&self) -> Authorization {
		match self {
			Self::Metrics => Authorization::MetricsToken,
			_ => Authorization::AdminToken,
		}
	}
}

generateQueryParameters! {
	"id" => id,
	"search" => search,
	"globalAlias" => global_alias
}
