use std::sync::Arc;

use quick_xml::de::from_reader;

use http::header::{
	ACCESS_CONTROL_ALLOW_HEADERS, ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN,
	ACCESS_CONTROL_EXPOSE_HEADERS, ACCESS_CONTROL_REQUEST_HEADERS, ACCESS_CONTROL_REQUEST_METHOD,
};
use hyper::{
	body::Body, body::Incoming as IncomingBody, header::HeaderName, Method, Request, Response,
	StatusCode,
};

use http_body_util::BodyExt;

use serde::{Deserialize, Serialize};

use garage_model::bucket_table::{Bucket, BucketParams, CorsRule as GarageCorsRule};
use garage_model::garage::Garage;
use garage_util::data::*;

use garage_api_common::common_error::{helper_error_as_internal, CommonError};
use garage_api_common::helpers::*;
use garage_api_common::signature::verify_signed_content;

use crate::api_server::{ReqBody, ResBody};
use crate::error::*;
use crate::xml::{to_xml_with_header, xmlns_tag, IntValue, Value};

pub async fn handle_get_cors(ctx: ReqCtx) -> Result<Response<ResBody>, Error> {
	let ReqCtx { bucket_params, .. } = ctx;
	if let Some(cors) = bucket_params.cors_config.get() {
		let wc = CorsConfiguration {
			xmlns: (),
			cors_rules: cors
				.iter()
				.map(CorsRule::from_garage_cors_rule)
				.collect::<Vec<_>>(),
		};
		let xml = to_xml_with_header(&wc)?;
		Ok(Response::builder()
			.status(StatusCode::OK)
			.header(http::header::CONTENT_TYPE, "application/xml")
			.body(string_body(xml))?)
	} else {
		Ok(Response::builder()
			.status(StatusCode::NO_CONTENT)
			.body(empty_body())?)
	}
}

pub async fn handle_delete_cors(ctx: ReqCtx) -> Result<Response<ResBody>, Error> {
	let ReqCtx {
		garage,
		bucket_id,
		mut bucket_params,
		..
	} = ctx;
	bucket_params.cors_config.update(None);
	garage
		.bucket_table
		.insert(&Bucket::present(bucket_id, bucket_params))
		.await?;

	Ok(Response::builder()
		.status(StatusCode::NO_CONTENT)
		.body(empty_body())?)
}

pub async fn handle_put_cors(
	ctx: ReqCtx,
	req: Request<ReqBody>,
	content_sha256: Option<Hash>,
) -> Result<Response<ResBody>, Error> {
	let ReqCtx {
		garage,
		bucket_id,
		mut bucket_params,
		..
	} = ctx;

	let body = BodyExt::collect(req.into_body()).await?.to_bytes();

	if let Some(content_sha256) = content_sha256 {
		verify_signed_content(content_sha256, &body[..])?;
	}

	let conf: CorsConfiguration = from_reader(&body as &[u8])?;
	conf.validate()?;

	bucket_params
		.cors_config
		.update(Some(conf.into_garage_cors_config()?));
	garage
		.bucket_table
		.insert(&Bucket::present(bucket_id, bucket_params))
		.await?;

	Ok(Response::builder()
		.status(StatusCode::OK)
		.body(empty_body())?)
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

pub fn handle_options_for_bucket(
	req: &Request<IncomingBody>,
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

pub fn find_matching_cors_rule<'a>(
	bucket_params: &'a BucketParams,
	req: &Request<impl Body>,
) -> Result<Option<&'a GarageCorsRule>, Error> {
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

fn cors_rule_matches<'a, HI, S>(
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

// ---- SERIALIZATION AND DESERIALIZATION TO/FROM S3 XML ----

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename = "CORSConfiguration")]
pub struct CorsConfiguration {
	#[serde(serialize_with = "xmlns_tag", skip_deserializing)]
	pub xmlns: (),
	#[serde(rename = "CORSRule")]
	pub cors_rules: Vec<CorsRule>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct CorsRule {
	#[serde(rename = "ID")]
	pub id: Option<Value>,
	#[serde(rename = "MaxAgeSeconds")]
	pub max_age_seconds: Option<IntValue>,
	#[serde(rename = "AllowedOrigin")]
	pub allowed_origins: Vec<Value>,
	#[serde(rename = "AllowedMethod")]
	pub allowed_methods: Vec<Value>,
	#[serde(rename = "AllowedHeader", default)]
	pub allowed_headers: Vec<Value>,
	#[serde(rename = "ExposeHeader", default)]
	pub expose_headers: Vec<Value>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct AllowedMethod {
	#[serde(rename = "AllowedMethod")]
	pub allowed_method: Value,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct AllowedHeader {
	#[serde(rename = "AllowedHeader")]
	pub allowed_header: Value,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExposeHeader {
	#[serde(rename = "ExposeHeader")]
	pub expose_header: Value,
}

impl CorsConfiguration {
	pub fn validate(&self) -> Result<(), Error> {
		for r in self.cors_rules.iter() {
			r.validate()?;
		}
		Ok(())
	}

	pub fn into_garage_cors_config(self) -> Result<Vec<GarageCorsRule>, Error> {
		Ok(self
			.cors_rules
			.iter()
			.map(CorsRule::to_garage_cors_rule)
			.collect())
	}
}

impl CorsRule {
	pub fn validate(&self) -> Result<(), Error> {
		for method in self.allowed_methods.iter() {
			method
				.0
				.parse::<Method>()
				.ok_or_bad_request("Invalid CORSRule method")?;
		}
		for header in self
			.allowed_headers
			.iter()
			.chain(self.expose_headers.iter())
		{
			header
				.0
				.parse::<HeaderName>()
				.ok_or_bad_request("Invalid HTTP header name")?;
		}
		Ok(())
	}

	pub fn to_garage_cors_rule(&self) -> GarageCorsRule {
		let convert_vec =
			|vval: &[Value]| vval.iter().map(|x| x.0.to_owned()).collect::<Vec<String>>();
		GarageCorsRule {
			id: self.id.as_ref().map(|x| x.0.to_owned()),
			max_age_seconds: self.max_age_seconds.as_ref().map(|x| x.0 as u64),
			allow_origins: convert_vec(&self.allowed_origins),
			allow_methods: convert_vec(&self.allowed_methods),
			allow_headers: convert_vec(&self.allowed_headers),
			expose_headers: convert_vec(&self.expose_headers),
		}
	}

	pub fn from_garage_cors_rule(rule: &GarageCorsRule) -> Self {
		let convert_vec = |vval: &[String]| {
			vval.iter()
				.map(|x| Value(x.clone()))
				.collect::<Vec<Value>>()
		};
		Self {
			id: rule.id.as_ref().map(|x| Value(x.clone())),
			max_age_seconds: rule.max_age_seconds.map(|x| IntValue(x as i64)),
			allowed_origins: convert_vec(&rule.allow_origins),
			allowed_methods: convert_vec(&rule.allow_methods),
			allowed_headers: convert_vec(&rule.allow_headers),
			expose_headers: convert_vec(&rule.expose_headers),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	use quick_xml::de::from_str;

	#[test]
	fn test_deserialize() -> Result<(), Error> {
		let message = r#"<?xml version="1.0" encoding="UTF-8"?>
<CORSConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
 <CORSRule>
   <AllowedOrigin>http://www.example.com</AllowedOrigin>

   <AllowedMethod>PUT</AllowedMethod>
   <AllowedMethod>POST</AllowedMethod>
   <AllowedMethod>DELETE</AllowedMethod>

   <AllowedHeader>*</AllowedHeader>
 </CORSRule>
 <CORSRule>
   <AllowedOrigin>*</AllowedOrigin>
   <AllowedMethod>GET</AllowedMethod>
 </CORSRule>
 <CORSRule>
   <ID>qsdfjklm</ID>
   <MaxAgeSeconds>12345</MaxAgeSeconds>
   <AllowedOrigin>https://perdu.com</AllowedOrigin>

   <AllowedMethod>GET</AllowedMethod>
   <AllowedMethod>DELETE</AllowedMethod>
   <AllowedHeader>*</AllowedHeader>
   <ExposeHeader>*</ExposeHeader>
 </CORSRule>
</CORSConfiguration>"#;
		let conf: CorsConfiguration = from_str(message).unwrap();
		let ref_value = CorsConfiguration {
			xmlns: (),
			cors_rules: vec![
				CorsRule {
					id: None,
					max_age_seconds: None,
					allowed_origins: vec!["http://www.example.com".into()],
					allowed_methods: vec!["PUT".into(), "POST".into(), "DELETE".into()],
					allowed_headers: vec!["*".into()],
					expose_headers: vec![],
				},
				CorsRule {
					id: None,
					max_age_seconds: None,
					allowed_origins: vec!["*".into()],
					allowed_methods: vec!["GET".into()],
					allowed_headers: vec![],
					expose_headers: vec![],
				},
				CorsRule {
					id: Some("qsdfjklm".into()),
					max_age_seconds: Some(IntValue(12345)),
					allowed_origins: vec!["https://perdu.com".into()],
					allowed_methods: vec!["GET".into(), "DELETE".into()],
					allowed_headers: vec!["*".into()],
					expose_headers: vec!["*".into()],
				},
			],
		};
		assert_eq! {
			ref_value,
			conf
		};

		let message2 = to_xml_with_header(&ref_value)?;

		let cleanup = |c: &str| c.replace(char::is_whitespace, "");
		assert_eq!(cleanup(message), cleanup(&message2));

		Ok(())
	}
}
