use std::sync::Arc;

use http::header::{
	ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
	ACCESS_CONTROL_EXPOSE_HEADERS, ACCESS_CONTROL_REQUEST_HEADERS, ACCESS_CONTROL_REQUEST_METHOD,
};
use hyper::{body::Body, body::Incoming as IncomingBody, Request, Response, StatusCode};

use garage_model::bucket_table::{BucketParams, CorsRule as GarageCorsRule};
use garage_model::garage::Garage;

use crate::common_error::{
	helper_error_as_internal, CommonError, OkOrBadRequest, OkOrInternalError,
};
use crate::helpers::*;

pub fn find_matching_cors_rule<'a, B>(
	bucket_params: &'a BucketParams,
	req: &Request<B>,
) -> Result<Option<&'a GarageCorsRule>, CommonError> {
	if let Some(cors_config) = bucket_params.cors_config.get() {
		if let Some(origin) = req.headers().get("Origin") {
			let origin = origin.to_str()?;
			let request_headers = match req.headers().get(ACCESS_CONTROL_REQUEST_HEADERS) {
				Some(h) => h.to_str()?.split(',').map(|h| h.trim()).collect::<Vec<_>>(),
				None => vec![],
			};
			return Ok(cors_config.iter().find(|rule| {
				cors_rule_matches(rule, origin, req.method().as_ref(), request_headers.iter())
			}));
		}
	}
	Ok(None)
}

pub fn cors_rule_matches<'a, HI, S>(
	rule: &GarageCorsRule,
	origin: &'a str,
	method: &'a str,
	mut request_headers: HI,
) -> bool
where
	HI: Iterator<Item = S>,
	S: AsRef<str>,
{
	rule.allow_origins.iter().any(|x| x == "*" || x == origin)
		&& rule.allow_methods.iter().any(|x| x == "*" || x == method)
		&& request_headers.all(|h| {
			rule.allow_headers
				.iter()
				.any(|x| x == "*" || x == h.as_ref())
		})
}

pub fn add_cors_headers(
	resp: &mut Response<impl Body>,
	rule: &GarageCorsRule,
) -> Result<(), http::header::InvalidHeaderValue> {
	let h = resp.headers_mut();
	h.insert(
		ACCESS_CONTROL_ALLOW_ORIGIN,
		rule.allow_origins.join(", ").parse()?,
	);
	h.insert(
		ACCESS_CONTROL_ALLOW_METHODS,
		rule.allow_methods.join(", ").parse()?,
	);
	h.insert(
		ACCESS_CONTROL_ALLOW_HEADERS,
		rule.allow_headers.join(", ").parse()?,
	);
	h.insert(
		ACCESS_CONTROL_EXPOSE_HEADERS,
		rule.expose_headers.join(", ").parse()?,
	);
	Ok(())
}

pub async fn handle_options_api(
	garage: Arc<Garage>,
	req: &Request<IncomingBody>,
	bucket_name: Option<String>,
) -> Result<Response<EmptyBody>, CommonError> {
	// FIXME: CORS rules of buckets with local aliases are
	// not taken into account.

	// If the bucket name is a global bucket name,
	// we try to apply the CORS rules of that bucket.
	// If a user has a local bucket name that has
	// the same name, its CORS rules won't be applied
	// and will be shadowed by the rules of the globally
	// existing bucket (but this is inevitable because
	// OPTIONS calls are not auhtenticated).
	if let Some(bn) = bucket_name {
		let helper = garage.bucket_helper();
		let bucket_id = helper
			.resolve_global_bucket_name(&bn)
			.await
			.map_err(helper_error_as_internal)?;
		if let Some(id) = bucket_id {
			let bucket = garage
				.bucket_helper()
				.get_existing_bucket(id)
				.await
				.map_err(helper_error_as_internal)?;
			let bucket_params = bucket.state.into_option().unwrap();
			handle_options_for_bucket(req, &bucket_params)
		} else {
			// If there is a bucket name in the request, but that name
			// does not correspond to a global alias for a bucket,
			// then it's either a non-existing bucket or a local bucket.
			// We have no way of knowing, because the request is not
			// authenticated and thus we can't resolve local aliases.
			// We take the permissive approach of allowing everything,
			// because we don't want to prevent web apps that use
			// local bucket names from making API calls.
			Ok(Response::builder()
				.header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
				.header(ACCESS_CONTROL_ALLOW_METHODS, "*")
				.status(StatusCode::OK)
				.body(EmptyBody::new())?)
		}
	} else {
		// If there is no bucket name in the request,
		// we are doing a ListBuckets call, which we want to allow
		// for all origins.
		Ok(Response::builder()
			.header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
			.header(ACCESS_CONTROL_ALLOW_METHODS, "GET")
			.status(StatusCode::OK)
			.body(EmptyBody::new())?)
	}
}

pub fn handle_options_for_bucket<B>(
	req: &Request<B>,
	bucket_params: &BucketParams,
) -> Result<Response<EmptyBody>, CommonError> {
	let origin = req
		.headers()
		.get("Origin")
		.ok_or_bad_request("Missing Origin header")?
		.to_str()?;
	let request_method = req
		.headers()
		.get(ACCESS_CONTROL_REQUEST_METHOD)
		.ok_or_bad_request("Missing Access-Control-Request-Method header")?
		.to_str()?;
	let request_headers = match req.headers().get(ACCESS_CONTROL_REQUEST_HEADERS) {
		Some(h) => h.to_str()?.split(',').map(|h| h.trim()).collect::<Vec<_>>(),
		None => vec![],
	};

	if let Some(cors_config) = bucket_params.cors_config.get() {
		let matching_rule = cors_config
			.iter()
			.find(|rule| cors_rule_matches(rule, origin, request_method, request_headers.iter()));
		if let Some(rule) = matching_rule {
			let mut resp = Response::builder()
				.status(StatusCode::OK)
				.body(EmptyBody::new())?;
			add_cors_headers(&mut resp, rule).ok_or_internal_error("Invalid CORS configuration")?;
			return Ok(resp);
		}
	}

	Err(CommonError::Forbidden(
		"This CORS request is not allowed.".into(),
	))
}
