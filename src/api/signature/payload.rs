use std::collections::HashMap;

use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use hmac::Mac;
use hyper::{Body, Method, Request};
use sha2::{Digest, Sha256};

use garage_table::*;
use garage_util::data::Hash;

use garage_model::garage::Garage;
use garage_model::key_table::*;

use super::signing_hmac;
use super::{LONG_DATETIME, SHORT_DATE};

use crate::encoding::uri_encode;
use crate::error::*;

pub async fn check_payload_signature(
	garage: &Garage,
	request: &Request<Body>,
) -> Result<(Key, Option<Hash>), Error> {
	let mut headers = HashMap::new();
	for (key, val) in request.headers() {
		headers.insert(key.to_string(), val.to_str()?.to_string());
	}
	if let Some(query) = request.uri().query() {
		let query_pairs = url::form_urlencoded::parse(query.as_bytes());
		for (key, val) in query_pairs {
			headers.insert(key.to_lowercase(), val.to_string());
		}
	}

	let authorization = if let Some(authorization) = headers.get("authorization") {
		parse_authorization(authorization, &headers)?
	} else {
		parse_query_authorization(&headers)?
	};

	let date = headers
		.get("x-amz-date")
		.ok_or_bad_request("Missing X-Amz-Date field")?;
	let date: NaiveDateTime =
		NaiveDateTime::parse_from_str(date, LONG_DATETIME).ok_or_bad_request("Invalid date")?;
	let date: DateTime<Utc> = DateTime::from_utc(date, Utc);

	if Utc::now() - date > Duration::hours(24) {
		return Err(Error::BadRequest("Date is too old".to_string()));
	}

	let scope = format!(
		"{}/{}/s3/aws4_request",
		date.format(SHORT_DATE),
		garage.config.s3_api.s3_region
	);
	if authorization.scope != scope {
		return Err(Error::AuthorizationHeaderMalformed(scope.to_string()));
	}

	let key = garage
		.key_table
		.get(&EmptyKey, &authorization.key_id)
		.await?
		.filter(|k| !k.state.is_deleted())
		.ok_or_else(|| Error::Forbidden(format!("No such key: {}", authorization.key_id)))?;
	let key_p = key.params().unwrap();

	let canonical_request = canonical_request(
		request.method(),
		&request.uri().path().to_string(),
		&canonical_query_string(request.uri()),
		&headers,
		&authorization.signed_headers,
		&authorization.content_sha256,
	);
	let string_to_sign = string_to_sign(&date, &scope, &canonical_request);

	let mut hmac = signing_hmac(
		&date,
		&key_p.secret_key,
		&garage.config.s3_api.s3_region,
		"s3",
	)
	.ok_or_internal_error("Unable to build signing HMAC")?;
	hmac.update(string_to_sign.as_bytes());
	let signature = hex::encode(hmac.finalize().into_bytes());

	if authorization.signature != signature {
		trace!("Canonical request: ``{}``", canonical_request);
		trace!("String to sign: ``{}``", string_to_sign);
		trace!("Expected: {}, got: {}", signature, authorization.signature);
		return Err(Error::Forbidden("Invalid signature".to_string()));
	}

	let content_sha256 = if authorization.content_sha256 == "UNSIGNED-PAYLOAD" {
		None
	} else if authorization.content_sha256 == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" {
		let bytes = hex::decode(authorization.signature).ok_or_bad_request("Invalid signature")?;
		Some(Hash::try_from(&bytes).ok_or_bad_request("Invalid signature")?)
	} else {
		let bytes = hex::decode(authorization.content_sha256)
			.ok_or_bad_request("Invalid content sha256 hash")?;
		Some(Hash::try_from(&bytes).ok_or_bad_request("Invalid content sha256 hash")?)
	};

	Ok((key, content_sha256))
}

struct Authorization {
	key_id: String,
	scope: String,
	signed_headers: String,
	signature: String,
	content_sha256: String,
}

fn parse_authorization(
	authorization: &str,
	headers: &HashMap<String, String>,
) -> Result<Authorization, Error> {
	let first_space = authorization
		.find(' ')
		.ok_or_bad_request("Authorization field to short")?;
	let (auth_kind, rest) = authorization.split_at(first_space);

	if auth_kind != "AWS4-HMAC-SHA256" {
		return Err(Error::BadRequest("Unsupported authorization method".into()));
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
	let (key_id, scope) = parse_credential(cred)?;

	let content_sha256 = headers
		.get("x-amz-content-sha256")
		.ok_or_bad_request("Missing X-Amz-Content-Sha256 field")?;

	let auth = Authorization {
		key_id,
		scope,
		signed_headers: auth_params
			.get("SignedHeaders")
			.ok_or_bad_request("Could not find SignedHeaders in Authorization field")?
			.to_string(),
		signature: auth_params
			.get("Signature")
			.ok_or_bad_request("Could not find Signature in Authorization field")?
			.to_string(),
		content_sha256: content_sha256.to_string(),
	};
	Ok(auth)
}

fn parse_query_authorization(headers: &HashMap<String, String>) -> Result<Authorization, Error> {
	let algo = headers
		.get("x-amz-algorithm")
		.ok_or_bad_request("X-Amz-Algorithm not found in query parameters")?;
	if algo != "AWS4-HMAC-SHA256" {
		return Err(Error::BadRequest(
			"Unsupported authorization method".to_string(),
		));
	}

	let cred = headers
		.get("x-amz-credential")
		.ok_or_bad_request("X-Amz-Credential not found in query parameters")?;
	let (key_id, scope) = parse_credential(cred)?;
	let signed_headers = headers
		.get("x-amz-signedheaders")
		.ok_or_bad_request("X-Amz-SignedHeaders not found in query parameters")?;
	let signature = headers
		.get("x-amz-signature")
		.ok_or_bad_request("X-Amz-Signature not found in query parameters")?;
	let content_sha256 = headers
		.get("x-amz-content-sha256")
		.map(|x| x.as_str())
		.unwrap_or("UNSIGNED-PAYLOAD");

	Ok(Authorization {
		key_id,
		scope,
		signed_headers: signed_headers.to_string(),
		signature: signature.to_string(),
		content_sha256: content_sha256.to_string(),
	})
}

fn parse_credential(cred: &str) -> Result<(String, String), Error> {
	let first_slash = cred
		.find('/')
		.ok_or_bad_request("Credentials does not contain / in authorization field")?;
	let (key_id, scope) = cred.split_at(first_slash);
	Ok((
		key_id.to_string(),
		scope.trim_start_matches('/').to_string(),
	))
}

fn string_to_sign(datetime: &DateTime<Utc>, scope_string: &str, canonical_req: &str) -> String {
	let mut hasher = Sha256::default();
	hasher.update(canonical_req.as_bytes());
	[
		"AWS4-HMAC-SHA256",
		&datetime.format(LONG_DATETIME).to_string(),
		scope_string,
		&hex::encode(hasher.finalize().as_slice()),
	]
	.join("\n")
}

fn canonical_request(
	method: &Method,
	url_path: &str,
	canonical_query_string: &str,
	headers: &HashMap<String, String>,
	signed_headers: &str,
	content_sha256: &str,
) -> String {
	[
		method.as_str(),
		url_path,
		canonical_query_string,
		&canonical_header_string(headers, signed_headers),
		"",
		signed_headers,
		content_sha256,
	]
	.join("\n")
}

fn canonical_header_string(headers: &HashMap<String, String>, signed_headers: &str) -> String {
	let signed_headers_vec = signed_headers.split(';').collect::<Vec<_>>();
	let mut items = headers
		.iter()
		.filter(|(key, _)| signed_headers_vec.contains(&key.as_str()))
		.collect::<Vec<_>>();
	items.sort_by(|(k1, _), (k2, _)| k1.cmp(k2));
	items
		.iter()
		.map(|(key, value)| key.to_lowercase() + ":" + value.trim())
		.collect::<Vec<_>>()
		.join("\n")
}

fn canonical_query_string(uri: &hyper::Uri) -> String {
	if let Some(query) = uri.query() {
		let query_pairs = url::form_urlencoded::parse(query.as_bytes());
		let mut items = query_pairs
			.filter(|(key, _)| key != "X-Amz-Signature")
			.map(|(key, value)| uri_encode(&key, true) + "=" + &uri_encode(&value, true))
			.collect::<Vec<_>>();
		items.sort();
		items.join("&")
	} else {
		"".to_string()
	}
}
