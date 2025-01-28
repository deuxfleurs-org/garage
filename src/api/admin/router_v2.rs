use std::borrow::Cow;

use hyper::body::Incoming as IncomingBody;
use hyper::{Method, Request};
use paste::paste;

use crate::admin::api::*;
use crate::admin::error::*;
//use crate::admin::router_v1;
use crate::admin::Authorization;
use crate::helpers::*;
use crate::router_macros::*;

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
			POST UpdateClusterLayout (body),
			POST ApplyClusterLayout (body),
			POST RevertClusterLayout (),
			// API key endpoints
			GET GetKeyInfo (query_opt::id, query_opt::search, parse_default(false)::show_secret_key),
			POST UpdateKey (body_field, query::id),
			POST CreateKey (body),
			POST ImportKey (body),
			DELETE DeleteKey (query::id),
			GET ListKeys (),
			// Bucket endpoints
			GET GetBucketInfo (query_opt::id, query_opt::global_alias),
			GET ListBuckets (),
			POST CreateBucket (body),
			DELETE DeleteBucket (query::id),
			PUT UpdateBucket (body_field, query::id),
			// Bucket-key permissions
			POST BucketAllowKey (body),
			POST BucketDenyKey (body),
			// Bucket aliases
			PUT GlobalAliasBucket (query::id, query::alias),
			DELETE GlobalUnaliasBucket (query::id, query::alias),
			PUT LocalAliasBucket (query::id, query::access_key_id, query::alias),
			DELETE LocalUnaliasBucket (query::id, query::access_key_id, query::alias),
		]);

		if let Some(message) = query.nonempty_message() {
			debug!("Unused query parameter: {}", message)
		}

		Ok(res)
	}
	/*
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
	*/
	/// Get the kind of authorization which is required to perform the operation.
	pub fn authorization_type(&self) -> Authorization {
		match self {
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
