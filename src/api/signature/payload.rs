use std::collections::HashMap;
use std::convert::TryFrom;

use chrono::{DateTime, Duration, NaiveDateTime, TimeZone, Utc};
use hmac::Mac;
use hyper::header::{HeaderMap, HeaderName, AUTHORIZATION, CONTENT_TYPE, HOST};
use hyper::{body::Incoming as IncomingBody, Method, Request};
use sha2::{Digest, Sha256};

use garage_table::*;
use garage_util::data::Hash;

use garage_model::garage::Garage;
use garage_model::key_table::*;

use super::LONG_DATETIME;
use super::{compute_scope, signing_hmac};

use crate::encoding::uri_encode;
use crate::signature::error::*;

pub const X_AMZ_ALGORITHM: HeaderName = HeaderName::from_static("x-amz-algorithm");
pub const X_AMZ_CREDENTIAL: HeaderName = HeaderName::from_static("x-amz-credential");
pub const X_AMZ_DATE: HeaderName = HeaderName::from_static("x-amz-date");
pub const X_AMZ_EXPIRES: HeaderName = HeaderName::from_static("x-amz-expires");
pub const X_AMZ_SIGNEDHEADERS: HeaderName = HeaderName::from_static("x-amz-signedheaders");
pub const X_AMZ_SIGNATURE: HeaderName = HeaderName::from_static("x-amz-signature");
pub const X_AMZ_CONTENT_SH256: HeaderName = HeaderName::from_static("x-amz-content-sha256");

pub const AWS4_HMAC_SHA256: &str = "AWS4-HMAC-SHA256";
pub const UNSIGNED_PAYLOAD: &str = "UNSIGNED-PAYLOAD";
pub const STREAMING_AWS4_HMAC_SHA256_PAYLOAD: &str = "STREAMING-AWS4-HMAC-SHA256-PAYLOAD";

pub type QueryMap = HashMap<String, String>;

pub async fn check_payload_signature(
	garage: &Garage,
	service: &'static str,
	request: &Request<IncomingBody>,
) -> Result<(Option<Key>, Option<Hash>), Error> {
	let query = parse_query_map(request.uri())?;

	let res = if query.contains_key(X_AMZ_ALGORITHM.as_str()) {
		check_presigned_signature(garage, service, request, query).await
	} else {
		check_standard_signature(garage, service, request, query).await
	};

	if let Err(e) = &res {
		error!("ERROR IN SIGNATURE\n{:?}\n{}", request, e);
	}

	res
}

async fn check_standard_signature(
	garage: &Garage,
	service: &'static str,
	request: &Request<IncomingBody>,
	query: QueryMap,
) -> Result<(Option<Key>, Option<Hash>), Error> {
	let authorization = Authorization::parse(request.headers())?;

	// Verify that all necessary request headers are signed
	let signed_headers = split_signed_headers(&authorization)?;
	for (name, _) in request.headers().iter() {
		if name.as_str().starts_with("x-amz-") || name == CONTENT_TYPE {
			if !signed_headers.contains(name) {
				return Err(Error::bad_request(format!(
					"Header `{}` should be signed",
					name
				)));
			}
		}
	}
	if !signed_headers.contains(&HOST) {
		return Err(Error::bad_request("Header `Host` should be signed"));
	}

	let canonical_request = canonical_request(
		service,
		request.method(),
		request.uri().path(),
		&query,
		request.headers(),
		signed_headers,
		&authorization.content_sha256,
	)?;
	let string_to_sign = string_to_sign(
		&authorization.date,
		&authorization.scope,
		&canonical_request,
	);

	trace!("canonical request:\n{}", canonical_request);
	trace!("string to sign:\n{}", string_to_sign);

	let key = verify_v4(garage, service, &authorization, string_to_sign.as_bytes()).await?;

	let content_sha256 = if authorization.content_sha256 == UNSIGNED_PAYLOAD {
		None
	} else if authorization.content_sha256 == STREAMING_AWS4_HMAC_SHA256_PAYLOAD {
		let bytes = hex::decode(authorization.signature).ok_or_bad_request("Invalid signature")?;
		Some(Hash::try_from(&bytes).ok_or_bad_request("Invalid signature")?)
	} else {
		let bytes = hex::decode(authorization.content_sha256)
			.ok_or_bad_request("Invalid content sha256 hash")?;
		Some(Hash::try_from(&bytes).ok_or_bad_request("Invalid content sha256 hash")?)
	};

	Ok((Some(key), content_sha256))
}

async fn check_presigned_signature(
	garage: &Garage,
	service: &'static str,
	request: &Request<IncomingBody>,
	mut query: QueryMap,
) -> Result<(Option<Key>, Option<Hash>), Error> {
	let algorithm = query.get(X_AMZ_ALGORITHM.as_str()).unwrap();
	let authorization = Authorization::parse_presigned(algorithm, &query)?;

	// Check that all mandatory signed headers are included
	let signed_headers = split_signed_headers(&authorization)?;
	for (name, _) in request.headers().iter() {
		if name.as_str().starts_with("x-amz-") {
			if !signed_headers.contains(name) {
				return Err(Error::bad_request(format!(
					"Header `{}` should be signed",
					name
				)));
			}
		}
	}
	if !signed_headers.contains(&HOST) {
		return Err(Error::bad_request("Header `Host` should be signed"));
	}

	query.remove(X_AMZ_SIGNATURE.as_str());
	let canonical_request = canonical_request(
		service,
		request.method(),
		request.uri().path(),
		&query,
		request.headers(),
		signed_headers,
		&authorization.content_sha256,
	)?;
	let string_to_sign = string_to_sign(
		&authorization.date,
		&authorization.scope,
		&canonical_request,
	);

	trace!("canonical request:\n{}", canonical_request);
	trace!("string to sign:\n{}", string_to_sign);

	let key = verify_v4(garage, service, &authorization, string_to_sign.as_bytes()).await?;

	Ok((Some(key), None))
}

pub fn parse_query_map(uri: &http::uri::Uri) -> Result<QueryMap, Error> {
	let mut query = QueryMap::new();
	if let Some(query_str) = uri.query() {
		let query_pairs = url::form_urlencoded::parse(query_str.as_bytes());
		for (key, val) in query_pairs {
			if query.insert(key.to_string(), val.into_owned()).is_some() {
				return Err(Error::bad_request(format!(
					"duplicate query parameter: `{}`",
					key
				)));
			}
		}
	}
	Ok(query)
}

fn split_signed_headers(authorization: &Authorization) -> Result<Vec<HeaderName>, Error> {
	let signed_headers = authorization
		.signed_headers
		.split(';')
		.map(HeaderName::try_from)
		.collect::<Result<Vec<HeaderName>, _>>()
		.ok_or_bad_request("invalid header name")?;
	Ok(signed_headers)
}

pub fn string_to_sign(datetime: &DateTime<Utc>, scope_string: &str, canonical_req: &str) -> String {
	let mut hasher = Sha256::default();
	hasher.update(canonical_req.as_bytes());
	[
		AWS4_HMAC_SHA256,
		&datetime.format(LONG_DATETIME).to_string(),
		scope_string,
		&hex::encode(hasher.finalize().as_slice()),
	]
	.join("\n")
}

pub fn canonical_request(
	service: &'static str,
	method: &Method,
	canonical_uri: &str,
	query: &QueryMap,
	headers: &HeaderMap,
	mut signed_headers: Vec<HeaderName>,
	content_sha256: &str,
) -> Result<String, Error> {
	// There seems to be evidence that in AWSv4 signatures, the path component is url-encoded
	// a second time when building the canonical request, as specified in this documentation page:
	// -> https://docs.aws.amazon.com/rolesanywhere/latest/userguide/authentication-sign-process.html
	// However this documentation page is for a specific service ("roles anywhere"), and
	// in the S3 service we know for a fact that there is no double-urlencoding, because all of
	// the tests we made with external software work without it.
	//
	// The theory is that double-urlencoding occurs for all services except S3,
	// which is what is implemented in rusoto_signature:
	// -> https://docs.rs/rusoto_signature/latest/src/rusoto_signature/signature.rs.html#464
	//
	// Digging into the code of the official AWS Rust SDK, we learn that double-URI-encoding can
	// be set or unset on a per-request basis (the signature crates, aws-sigv4 and aws-sig-auth,
	// are agnostic to this). Grepping the codebase confirms that S3 is the only API for which
	// double_uri_encode is set to false, meaning it is true (its default value) for all other
	// AWS services. We will therefore implement this behavior in Garage as well.
	//
	// Note that this documentation page, which is touted as the "authoritative reference" on
	// AWSv4 signatures, makes no mention of either single- or double-urlencoding:
	// -> https://docs.aws.amazon.com/IAM/latest/UserGuide/create-signed-request.html
	// This page of the S3 documentation does also not mention anything specific:
	// -> https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
	//
	// Note that there is also the issue of path normalization, which I hope is unrelated to the
	// one of URI-encoding. At least in aws-sigv4 both parameters can be set independently,
	// and rusoto_signature does not seem to do any effective path normalization, even though
	// it mentions it in the comments (same link to the souce code as above).
	// We make the explicit choice of NOT normalizing paths in the K2V API because doing so
	// would make non-normalized paths invalid K2V partition keys, and we don't want that.
	let canonical_uri: std::borrow::Cow<str> = if service != "s3" {
		uri_encode(canonical_uri, false).into()
	} else {
		canonical_uri.into()
	};

	// Canonical query string from passed HeaderMap
	let canonical_query_string = {
		let mut items = Vec::with_capacity(query.len());
		for (key, value) in query.iter() {
			items.push(uri_encode(&key, true) + "=" + &uri_encode(&value, true));
		}
		items.sort();
		items.join("&")
	};

	// Canonical header string calculated from signed headers
	signed_headers.sort_by(|h1, h2| h1.as_str().cmp(h2.as_str()));
	let canonical_header_string = {
		let mut items = Vec::with_capacity(signed_headers.len());
		for name in signed_headers.iter() {
			let value = headers
				.get(name)
				.ok_or_bad_request(format!("signed header `{}` is not present", name))?
				.to_str()?;
			items.push((name, value));
		}
		items
			.iter()
			.map(|(key, value)| format!("{}:{}", key.as_str(), value.trim()))
			.collect::<Vec<_>>()
			.join("\n")
	};
	let signed_headers = signed_headers.join(";");

	let list = [
		method.as_str(),
		&canonical_uri,
		&canonical_query_string,
		&canonical_header_string,
		"",
		&signed_headers,
		content_sha256,
	];
	Ok(list.join("\n"))
}

pub fn parse_date(date: &str) -> Result<DateTime<Utc>, Error> {
	let date: NaiveDateTime =
		NaiveDateTime::parse_from_str(date, LONG_DATETIME).ok_or_bad_request("Invalid date")?;
	Ok(Utc.from_utc_datetime(&date))
}

pub async fn verify_v4(
	garage: &Garage,
	service: &str,
	auth: &Authorization,
	payload: &[u8],
) -> Result<Key, Error> {
	let scope_expected = compute_scope(&auth.date, &garage.config.s3_api.s3_region, service);
	if auth.scope != scope_expected {
		return Err(Error::AuthorizationHeaderMalformed(auth.scope.to_string()));
	}

	let key = garage
		.key_table
		.get(&EmptyKey, &auth.key_id)
		.await?
		.filter(|k| !k.state.is_deleted())
		.ok_or_else(|| Error::forbidden(format!("No such key: {}", &auth.key_id)))?;
	let key_p = key.params().unwrap();

	let mut hmac = signing_hmac(
		&auth.date,
		&key_p.secret_key,
		&garage.config.s3_api.s3_region,
		service,
	)
	.ok_or_internal_error("Unable to build signing HMAC")?;
	hmac.update(payload);
	let our_signature = hex::encode(hmac.finalize().into_bytes());
	if auth.signature != our_signature {
		return Err(Error::forbidden("Invalid signature".to_string()));
	}

	Ok(key)
}

// ============ Authorization header, or X-Amz-* query params =========

pub struct Authorization {
	key_id: String,
	scope: String,
	signed_headers: String,
	signature: String,
	content_sha256: String,
	date: DateTime<Utc>,
}

impl Authorization {
	fn parse(headers: &HeaderMap) -> Result<Self, Error> {
		let authorization = headers
			.get(AUTHORIZATION)
			.ok_or_bad_request("Missing authorization header")?
			.to_str()?;

		let (auth_kind, rest) = authorization
			.split_once(' ')
			.ok_or_bad_request("Authorization field to short")?;

		if auth_kind != AWS4_HMAC_SHA256 {
			return Err(Error::bad_request("Unsupported authorization method"));
		}

		let mut auth_params = HashMap::new();
		for auth_part in rest.split(',') {
			let auth_part = auth_part.trim();
			let eq = auth_part
				.find('=')
				.ok_or_bad_request("Field without value in authorization header")?;
			let (key, value) = auth_part.split_at(eq);
			auth_params.insert(key.to_string(), value.trim_start_matches('=').to_string());
		}

		let cred = auth_params
			.get("Credential")
			.ok_or_bad_request("Could not find Credential in Authorization field")?;
		let signed_headers = auth_params
			.get("SignedHeaders")
			.ok_or_bad_request("Could not find SignedHeaders in Authorization field")?
			.to_string();
		let signature = auth_params
			.get("Signature")
			.ok_or_bad_request("Could not find Signature in Authorization field")?
			.to_string();

		let content_sha256 = headers
			.get(X_AMZ_CONTENT_SH256)
			.ok_or_bad_request("Missing X-Amz-Content-Sha256 field")?;

		let date = headers
			.get(X_AMZ_DATE)
			.ok_or_bad_request("Missing X-Amz-Date field")
			.map_err(Error::from)?
			.to_str()?;
		let date = parse_date(date)?;

		if Utc::now() - date > Duration::hours(24) {
			return Err(Error::bad_request("Date is too old".to_string()));
		}

		let (key_id, scope) = parse_credential(cred)?;
		let auth = Authorization {
			key_id,
			scope,
			signed_headers,
			signature,
			content_sha256: content_sha256.to_str()?.to_string(),
			date,
		};
		Ok(auth)
	}

	fn parse_presigned(algorithm: &str, query: &QueryMap) -> Result<Self, Error> {
		if algorithm != AWS4_HMAC_SHA256 {
			return Err(Error::bad_request(
				"Unsupported authorization method".to_string(),
			));
		}

		let cred = query
			.get(X_AMZ_CREDENTIAL.as_str())
			.ok_or_bad_request("X-Amz-Credential not found in query parameters")?;
		let signed_headers = query
			.get(X_AMZ_SIGNEDHEADERS.as_str())
			.ok_or_bad_request("X-Amz-SignedHeaders not found in query parameters")?;
		let signature = query
			.get(X_AMZ_SIGNATURE.as_str())
			.ok_or_bad_request("X-Amz-Signature not found in query parameters")?;

		let duration = query
			.get(X_AMZ_EXPIRES.as_str())
			.ok_or_bad_request("X-Amz-Expires not found in query parameters")?
			.parse()
			.map_err(|_| Error::bad_request("X-Amz-Expires is not a number".to_string()))?;

		if duration > 7 * 24 * 3600 {
			return Err(Error::bad_request(
				"X-Amz-Expires may not exceed a week".to_string(),
			));
		}

		let date = query
			.get(X_AMZ_DATE.as_str())
			.ok_or_bad_request("Missing X-Amz-Date field")?;
		let date = parse_date(date)?;

		if Utc::now() - date > Duration::seconds(duration) {
			return Err(Error::bad_request("Date is too old".to_string()));
		}

		let (key_id, scope) = parse_credential(cred)?;
		Ok(Authorization {
			key_id,
			scope,
			signed_headers: signed_headers.to_string(),
			signature: signature.to_string(),
			content_sha256: UNSIGNED_PAYLOAD.to_string(),
			date,
		})
	}

	pub(crate) fn parse_form(params: &HeaderMap) -> Result<Self, Error> {
		let credential = params
			.get(X_AMZ_CREDENTIAL)
			.ok_or_else(|| Error::forbidden("Garage does not support anonymous access yet"))?
			.to_str()?;
		let signature = params
			.get(X_AMZ_SIGNATURE)
			.ok_or_bad_request("No signature was provided")?
			.to_str()?
			.to_string();
		let date = params
			.get(X_AMZ_DATE)
			.ok_or_bad_request("No date was provided")?
			.to_str()?;
		let date = parse_date(date)?;

		if Utc::now() - date > Duration::hours(24) {
			return Err(Error::bad_request("Date is too old".to_string()));
		}

		let (key_id, scope) = parse_credential(credential)?;
		let auth = Authorization {
			key_id,
			scope,
			signed_headers: "".to_string(),
			signature,
			content_sha256: UNSIGNED_PAYLOAD.to_string(),
			date,
		};
		Ok(auth)
	}
}

fn parse_credential(cred: &str) -> Result<(String, String), Error> {
	let first_slash = cred
		.find('/')
		.ok_or_bad_request("Credentials does not contain '/' in authorization field")?;
	let (key_id, scope) = cred.split_at(first_slash);
	Ok((
		key_id.to_string(),
		scope.trim_start_matches('/').to_string(),
	))
}
