use std::collections::HashMap;
use std::convert::TryInto;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::Bytes;
use chrono::{DateTime, Duration, Utc};
use futures::{Stream, StreamExt};
use hyper::header::{self, HeaderMap, HeaderName, HeaderValue};
use hyper::{Body, Request, Response, StatusCode};
use multer::{Constraints, Multipart, SizeLimit};
use serde::Deserialize;

use garage_model::garage::Garage;

use crate::s3::error::*;
use crate::s3::put::{get_headers, save_stream};
use crate::s3::xml as s3_xml;
use crate::signature::payload::{parse_date, verify_v4};

pub async fn handle_post_object(
	garage: Arc<Garage>,
	req: Request<Body>,
	bucket_name: String,
) -> Result<Response<Body>, Error> {
	let boundary = req
		.headers()
		.get(header::CONTENT_TYPE)
		.and_then(|ct| ct.to_str().ok())
		.and_then(|ct| multer::parse_boundary(ct).ok())
		.ok_or_bad_request("Counld not get multipart boundary")?;

	// 16k seems plenty for a header. 5G is the max size of a single part, so it seems reasonable
	// for a PostObject
	let constraints = Constraints::new().size_limit(
		SizeLimit::new()
			.per_field(16 * 1024)
			.for_field("file", 5 * 1024 * 1024 * 1024),
	);

	let (head, body) = req.into_parts();
	let mut multipart = Multipart::with_constraints(body, boundary, constraints);

	let mut params = HeaderMap::new();
	let field = loop {
		let field = if let Some(field) = multipart.next_field().await? {
			field
		} else {
			return Err(Error::bad_request("Request did not contain a file"));
		};
		let name: HeaderName = if let Some(Ok(name)) = field.name().map(TryInto::try_into) {
			name
		} else {
			continue;
		};
		if name == "file" {
			break field;
		}

		if let Ok(content) = HeaderValue::from_str(&field.text().await?) {
			match name.as_str() {
				"tag" => (/* tag need to be reencoded, but we don't support them yet anyway */),
				"acl" => {
					if params.insert("x-amz-acl", content).is_some() {
						return Err(Error::bad_request(
							"Field 'acl' provided more than one time",
						));
					}
				}
				_ => {
					if params.insert(&name, content).is_some() {
						return Err(Error::bad_request(format!(
							"Field '{}' provided more than one time",
							name
						)));
					}
				}
			}
		}
	};

	// Current part is file. Do some checks before handling to PutObject code
	let key = params
		.get("key")
		.ok_or_bad_request("No key was provided")?
		.to_str()?;
	let credential = params
		.get("x-amz-credential")
		.ok_or_else(|| Error::forbidden("Garage does not support anonymous access yet"))?
		.to_str()?;
	let policy = params
		.get("policy")
		.ok_or_bad_request("No policy was provided")?
		.to_str()?;
	let signature = params
		.get("x-amz-signature")
		.ok_or_bad_request("No signature was provided")?
		.to_str()?;
	let date = params
		.get("x-amz-date")
		.ok_or_bad_request("No date was provided")?
		.to_str()?;

	let key = if key.contains("${filename}") {
		// if no filename is provided, don't replace. This matches the behavior of AWS.
		if let Some(filename) = field.file_name() {
			key.replace("${filename}", filename)
		} else {
			key.to_owned()
		}
	} else {
		key.to_owned()
	};

	let date = parse_date(date)?;
	let api_key = verify_v4(
		&garage,
		"s3",
		credential,
		&date,
		signature,
		policy.as_bytes(),
	)
	.await?;

	let bucket_id = garage
		.bucket_helper()
		.resolve_bucket(&bucket_name, &api_key)
		.await?;

	if !api_key.allow_write(&bucket_id) {
		return Err(Error::forbidden("Operation is not allowed for this key."));
	}

	let bucket = garage
		.bucket_helper()
		.get_existing_bucket(bucket_id)
		.await?;

	let decoded_policy = base64::decode(&policy).ok_or_bad_request("Invalid policy")?;
	let decoded_policy: Policy =
		serde_json::from_slice(&decoded_policy).ok_or_bad_request("Invalid policy")?;

	let expiration: DateTime<Utc> = DateTime::parse_from_rfc3339(&decoded_policy.expiration)
		.ok_or_bad_request("Invalid expiration date")?
		.into();
	if Utc::now() - expiration > Duration::zero() {
		return Err(Error::bad_request("Expiration date is in the paste"));
	}

	let mut conditions = decoded_policy.into_conditions()?;

	for (param_key, value) in params.iter() {
		let mut param_key = param_key.to_string();
		param_key.make_ascii_lowercase();
		match param_key.as_str() {
			"policy" | "x-amz-signature" => (), // this is always accepted, as it's required to validate other fields
			"content-type" => {
				let conds = conditions.params.remove("content-type").ok_or_else(|| {
					Error::bad_request(format!("Key '{}' is not allowed in policy", param_key))
				})?;
				for cond in conds {
					let ok = match cond {
						Operation::Equal(s) => s.as_str() == value,
						Operation::StartsWith(s) => {
							value.to_str()?.split(',').all(|v| v.starts_with(&s))
						}
					};
					if !ok {
						return Err(Error::bad_request(format!(
							"Key '{}' has value not allowed in policy",
							param_key
						)));
					}
				}
			}
			"key" => {
				let conds = conditions.params.remove("key").ok_or_else(|| {
					Error::bad_request(format!("Key '{}' is not allowed in policy", param_key))
				})?;
				for cond in conds {
					let ok = match cond {
						Operation::Equal(s) => s == key,
						Operation::StartsWith(s) => key.starts_with(&s),
					};
					if !ok {
						return Err(Error::bad_request(format!(
							"Key '{}' has value not allowed in policy",
							param_key
						)));
					}
				}
			}
			_ => {
				if param_key.starts_with("x-ignore-") {
					// if a x-ignore is provided in policy, it's not removed here, so it will be
					// rejected as provided in policy but not in the request. As odd as it is, it's
					// how aws seems to behave.
					continue;
				}
				let conds = conditions.params.remove(&param_key).ok_or_else(|| {
					Error::bad_request(format!("Key '{}' is not allowed in policy", param_key))
				})?;
				for cond in conds {
					let ok = match cond {
						Operation::Equal(s) => s.as_str() == value,
						Operation::StartsWith(s) => value.to_str()?.starts_with(s.as_str()),
					};
					if !ok {
						return Err(Error::bad_request(format!(
							"Key '{}' has value not allowed in policy",
							param_key
						)));
					}
				}
			}
		}
	}

	if let Some((param_key, _)) = conditions.params.iter().next() {
		return Err(Error::bad_request(format!(
			"Key '{}' is required in policy, but no value was provided",
			param_key
		)));
	}

	let headers = get_headers(&params)?;

	let stream = field.map(|r| r.map_err(Into::into));
	let (_, md5) = save_stream(
		garage,
		headers,
		StreamLimiter::new(stream, conditions.content_length),
		&bucket,
		&key,
		None,
		None,
	)
	.await?;

	let etag = format!("\"{}\"", md5);

	let resp = if let Some(mut target) = params
		.get("success_action_redirect")
		.and_then(|h| h.to_str().ok())
		.and_then(|u| url::Url::parse(u).ok())
		.filter(|u| u.scheme() == "https" || u.scheme() == "http")
	{
		target
			.query_pairs_mut()
			.append_pair("bucket", &bucket_name)
			.append_pair("key", &key)
			.append_pair("etag", &etag);
		let target = target.to_string();
		Response::builder()
			.status(StatusCode::SEE_OTHER)
			.header(header::LOCATION, target.clone())
			.header(header::ETAG, etag)
			.body(target.into())?
	} else {
		let path = head
			.uri
			.into_parts()
			.path_and_query
			.map(|paq| paq.path().to_string())
			.unwrap_or_else(|| "/".to_string());
		let authority = head
			.headers
			.get(header::HOST)
			.and_then(|h| h.to_str().ok())
			.unwrap_or_default();
		let proto = if !authority.is_empty() {
			"https://"
		} else {
			""
		};

		let url_key: String = form_urlencoded::byte_serialize(key.as_bytes())
			.flat_map(str::chars)
			.collect();
		let location = format!("{}{}{}{}", proto, authority, path, url_key);

		let action = params
			.get("success_action_status")
			.and_then(|h| h.to_str().ok())
			.unwrap_or("204");
		let builder = Response::builder()
			.header(header::LOCATION, location.clone())
			.header(header::ETAG, etag.clone());
		match action {
			"200" => builder.status(StatusCode::OK).body(Body::empty())?,
			"201" => {
				let xml = s3_xml::PostObject {
					xmlns: (),
					location: s3_xml::Value(location),
					bucket: s3_xml::Value(bucket_name),
					key: s3_xml::Value(key),
					etag: s3_xml::Value(etag),
				};
				let body = s3_xml::to_xml_with_header(&xml)?;
				builder
					.status(StatusCode::CREATED)
					.body(Body::from(body.into_bytes()))?
			}
			_ => builder.status(StatusCode::NO_CONTENT).body(Body::empty())?,
		}
	};

	Ok(resp)
}

#[derive(Deserialize)]
struct Policy {
	expiration: String,
	conditions: Vec<PolicyCondition>,
}

impl Policy {
	fn into_conditions(self) -> Result<Conditions, Error> {
		let mut params = HashMap::<_, Vec<_>>::new();

		let mut length = (0, u64::MAX);
		for condition in self.conditions {
			match condition {
				PolicyCondition::Equal(map) => {
					if map.len() != 1 {
						return Err(Error::bad_request("Invalid policy item"));
					}
					let (mut k, v) = map.into_iter().next().expect("size was verified");
					k.make_ascii_lowercase();
					params.entry(k).or_default().push(Operation::Equal(v));
				}
				PolicyCondition::OtherOp([cond, mut key, value]) => {
					if key.remove(0) != '$' {
						return Err(Error::bad_request("Invalid policy item"));
					}
					key.make_ascii_lowercase();
					match cond.as_str() {
						"eq" => {
							params.entry(key).or_default().push(Operation::Equal(value));
						}
						"starts-with" => {
							params
								.entry(key)
								.or_default()
								.push(Operation::StartsWith(value));
						}
						_ => return Err(Error::bad_request("Invalid policy item")),
					}
				}
				PolicyCondition::SizeRange(key, min, max) => {
					if key == "content-length-range" {
						length.0 = length.0.max(min);
						length.1 = length.1.min(max);
					} else {
						return Err(Error::bad_request("Invalid policy item"));
					}
				}
			}
		}
		Ok(Conditions {
			params,
			content_length: RangeInclusive::new(length.0, length.1),
		})
	}
}

/// A single condition from a policy
#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum PolicyCondition {
	// will contain a single key-value pair
	Equal(HashMap<String, String>),
	OtherOp([String; 3]),
	SizeRange(String, u64, u64),
}

#[derive(Debug)]
struct Conditions {
	params: HashMap<String, Vec<Operation>>,
	content_length: RangeInclusive<u64>,
}

#[derive(Debug, PartialEq, Eq)]
enum Operation {
	Equal(String),
	StartsWith(String),
}

struct StreamLimiter<T> {
	inner: T,
	length: RangeInclusive<u64>,
	read: u64,
}

impl<T> StreamLimiter<T> {
	fn new(stream: T, length: RangeInclusive<u64>) -> Self {
		StreamLimiter {
			inner: stream,
			length,
			read: 0,
		}
	}
}

impl<T> Stream for StreamLimiter<T>
where
	T: Stream<Item = Result<Bytes, Error>> + Unpin,
{
	type Item = Result<Bytes, Error>;
	fn poll_next(
		mut self: std::pin::Pin<&mut Self>,
		ctx: &mut Context<'_>,
	) -> Poll<Option<Self::Item>> {
		let res = std::pin::Pin::new(&mut self.inner).poll_next(ctx);
		match &res {
			Poll::Ready(Some(Ok(bytes))) => {
				self.read += bytes.len() as u64;
				// optimization to fail early when we know before the end it's too long
				if self.length.end() < &self.read {
					return Poll::Ready(Some(Err(Error::bad_request(
						"File size does not match policy",
					))));
				}
			}
			Poll::Ready(None) => {
				if !self.length.contains(&self.read) {
					return Poll::Ready(Some(Err(Error::bad_request(
						"File size does not match policy",
					))));
				}
			}
			_ => {}
		}
		res
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_policy_1() {
		let policy_json = br#"
{ "expiration": "2007-12-01T12:00:00.000Z",
  "conditions": [
    {"acl": "public-read" },
    {"bucket": "johnsmith" },
    ["starts-with", "$key", "user/eric/"]
  ]
}
		"#;
		let policy_2: Policy = serde_json::from_slice(&policy_json[..]).unwrap();
		let mut conditions = policy_2.into_conditions().unwrap();

		assert_eq!(
			conditions.params.remove(&"acl".to_string()),
			Some(vec![Operation::Equal("public-read".into())])
		);
		assert_eq!(
			conditions.params.remove(&"bucket".to_string()),
			Some(vec![Operation::Equal("johnsmith".into())])
		);
		assert_eq!(
			conditions.params.remove(&"key".to_string()),
			Some(vec![Operation::StartsWith("user/eric/".into())])
		);
		assert!(conditions.params.is_empty());
		assert_eq!(conditions.content_length, 0..=u64::MAX);
	}

	#[test]
	fn test_policy_2() {
		let policy_json = br#"
{ "expiration": "2007-12-01T12:00:00.000Z",
  "conditions": [
	[ "eq", "$acl", "public-read" ],
	["starts-with", "$Content-Type", "image/"],
	["starts-with", "$success_action_redirect", ""],
	["content-length-range", 1048576, 10485760]
  ]
}
		"#;
		let policy_2: Policy = serde_json::from_slice(&policy_json[..]).unwrap();
		let mut conditions = policy_2.into_conditions().unwrap();

		assert_eq!(
			conditions.params.remove(&"acl".to_string()),
			Some(vec![Operation::Equal("public-read".into())])
		);
		assert_eq!(
			conditions.params.remove("content-type").unwrap(),
			vec![Operation::StartsWith("image/".into())]
		);
		assert_eq!(
			conditions
				.params
				.remove(&"success_action_redirect".to_string()),
			Some(vec![Operation::StartsWith("".into())])
		);
		assert!(conditions.params.is_empty());
		assert_eq!(conditions.content_length, 1048576..=10485760);
	}
}
