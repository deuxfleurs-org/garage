#![allow(dead_code)]

use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};

use chrono::{offset::Utc, DateTime};
use hmac::{Hmac, Mac};
use http_body_util::BodyExt;
use http_body_util::Full as FullBody;
use hyper::header::{
	HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, CONTENT_ENCODING, CONTENT_LENGTH, HOST,
};
use hyper::{Method, Request, Response, Uri};
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::TokioExecutor;

use super::garage::{Instance, Key};
use garage_api_common::signature;

pub type Body = FullBody<hyper::body::Bytes>;

/// You should ever only use this to send requests AWS sdk won't send,
/// like to reproduce behavior of unusual implementations found to be
/// problematic.
#[derive(Clone)]
pub struct CustomRequester {
	key: Key,
	uri: Uri,
	service: &'static str,
	client: Client<HttpConnector, Body>,
}

impl CustomRequester {
	pub fn new_s3(instance: &Instance, key: &Key) -> Self {
		CustomRequester {
			key: key.clone(),
			uri: instance.s3_uri(),
			service: "s3",
			client: Client::builder(TokioExecutor::new()).build_http(),
		}
	}

	pub fn new_k2v(instance: &Instance, key: &Key) -> Self {
		CustomRequester {
			key: key.clone(),
			uri: instance.k2v_uri(),
			service: "k2v",
			client: Client::builder(TokioExecutor::new()).build_http(),
		}
	}

	pub fn builder(&self, bucket: String) -> RequestBuilder<'_> {
		RequestBuilder {
			requester: self,
			service: self.service,
			bucket,
			method: Method::GET,
			path: String::new(),
			query_params: HashMap::new(),
			signed_headers: HashMap::new(),
			unsigned_headers: HashMap::new(),
			body: Vec::new(),
			body_signature: BodySignature::Classic,
			vhost_style: false,
		}
	}

	pub fn client(&self) -> &Client<HttpConnector, Body> {
		&self.client
	}
}

pub struct RequestBuilder<'a> {
	requester: &'a CustomRequester,
	service: &'static str,
	bucket: String,
	method: Method,
	path: String,
	query_params: HashMap<String, Option<String>>,
	signed_headers: HashMap<String, String>,
	unsigned_headers: HashMap<String, String>,
	body: Vec<u8>,
	body_signature: BodySignature,
	vhost_style: bool,
}

impl<'a> RequestBuilder<'a> {
	pub fn service(&mut self, service: &'static str) -> &mut Self {
		self.service = service;
		self
	}
	pub fn method(&mut self, method: Method) -> &mut Self {
		self.method = method;
		self
	}

	pub fn path(&mut self, path: impl ToString) -> &mut Self {
		self.path = path.to_string();
		self
	}

	pub fn query_params(&mut self, query_params: HashMap<String, Option<String>>) -> &mut Self {
		self.query_params = query_params;
		self
	}

	pub fn query_param<T, U>(&mut self, param: T, value: Option<U>) -> &mut Self
	where
		T: ToString,
		U: ToString,
	{
		self.query_params
			.insert(param.to_string(), value.as_ref().map(ToString::to_string));
		self
	}

	pub fn signed_headers(&mut self, signed_headers: HashMap<String, String>) -> &mut Self {
		self.signed_headers = signed_headers;
		self
	}

	pub fn signed_header(&mut self, name: impl ToString, value: impl ToString) -> &mut Self {
		self.signed_headers
			.insert(name.to_string(), value.to_string());
		self
	}

	pub fn unsigned_headers(&mut self, unsigned_headers: HashMap<String, String>) -> &mut Self {
		self.unsigned_headers = unsigned_headers;
		self
	}

	pub fn unsigned_header(&mut self, name: impl ToString, value: impl ToString) -> &mut Self {
		self.unsigned_headers
			.insert(name.to_string(), value.to_string());
		self
	}

	pub fn body(&mut self, body: Vec<u8>) -> &mut Self {
		self.body = body;
		self
	}

	pub fn body_signature(&mut self, body_signature: BodySignature) -> &mut Self {
		self.body_signature = body_signature;
		self
	}

	pub fn vhost_style(&mut self, vhost_style: bool) -> &mut Self {
		self.vhost_style = vhost_style;
		self
	}

	pub async fn send(&mut self) -> Result<Response<Body>, String> {
		// TODO this is a bit incorrect in that path and query params should be url-encoded and
		// aren't, but this is good enough for now.

		let query = query_param_to_string(&self.query_params);
		let (host, path) = if self.vhost_style {
			(
				format!("{}.{}.garage", self.bucket, self.service),
				format!("{}{}", self.path, query),
			)
		} else {
			(
				format!("{}.garage", self.service),
				format!("{}/{}{}", self.bucket, self.path, query),
			)
		};
		let uri = format!("{}{}", self.requester.uri, path);

		let now = Utc::now();
		let scope = signature::compute_scope(&now, super::REGION.as_ref(), self.service);
		let mut signer = signature::signing_hmac(
			&now,
			&self.requester.key.secret,
			super::REGION.as_ref(),
			self.service,
		)
		.unwrap();
		let streaming_signer = signer.clone();

		let mut all_headers = self
			.signed_headers
			.iter()
			.map(|(k, v)| {
				(
					HeaderName::try_from(k).expect("invalid header name"),
					HeaderValue::try_from(v).expect("invalid header value"),
				)
			})
			.collect::<HeaderMap>();

		let date = now.format(signature::LONG_DATETIME).to_string();
		all_headers.insert(signature::X_AMZ_DATE, HeaderValue::from_str(&date).unwrap());
		all_headers.insert(HOST, HeaderValue::from_str(&host).unwrap());

		let body_sha = match &self.body_signature {
			BodySignature::Unsigned => "UNSIGNED-PAYLOAD".to_owned(),
			BodySignature::Classic => hex::encode(garage_util::data::sha256sum(&self.body)),
			BodySignature::Streaming { chunk_size } => {
				all_headers.insert(
					CONTENT_ENCODING,
					HeaderValue::from_str("aws-chunked").unwrap(),
				);
				all_headers.insert(
					HeaderName::from_static("x-amz-decoded-content-length"),
					HeaderValue::from_str(&self.body.len().to_string()).unwrap(),
				);
				// Get length of body by doing the conversion to a streaming body with an
				// invalid signature (we don't know the seed) just to get its length. This
				// is a pretty lazy and inefficient way to do it, but it's enough for test
				// code.
				all_headers.insert(
					CONTENT_LENGTH,
					to_streaming_body(
						&self.body,
						*chunk_size,
						String::new(),
						signer.clone(),
						now,
						"",
					)
					.len()
					.to_string()
					.try_into()
					.unwrap(),
				);

				"STREAMING-AWS4-HMAC-SHA256-PAYLOAD".to_owned()
			}
			BodySignature::StreamingUnsignedTrailer {
				chunk_size,
				trailer_algorithm,
				trailer_value,
			} => {
				all_headers.insert(
					CONTENT_ENCODING,
					HeaderValue::from_str("aws-chunked").unwrap(),
				);
				all_headers.insert(
					HeaderName::from_static("x-amz-decoded-content-length"),
					HeaderValue::from_str(&self.body.len().to_string()).unwrap(),
				);
				all_headers.insert(
					HeaderName::from_static("x-amz-trailer"),
					HeaderValue::from_str(&trailer_algorithm).unwrap(),
				);

				all_headers.insert(
					CONTENT_LENGTH,
					to_streaming_unsigned_trailer_body(
						&self.body,
						*chunk_size,
						&trailer_algorithm,
						&trailer_value,
					)
					.len()
					.to_string()
					.try_into()
					.unwrap(),
				);

				"STREAMING-UNSIGNED-PAYLOAD-TRAILER".to_owned()
			}
		};
		all_headers.insert(
			signature::X_AMZ_CONTENT_SHA256,
			HeaderValue::from_str(&body_sha).unwrap(),
		);

		let mut signed_headers = all_headers.keys().cloned().collect::<Vec<_>>();
		signed_headers.sort_by(|h1, h2| h1.as_str().cmp(h2.as_str()));
		let signed_headers_str = signed_headers
			.iter()
			.map(ToString::to_string)
			.collect::<Vec<_>>()
			.join(";");

		all_headers.extend(self.unsigned_headers.iter().map(|(k, v)| {
			(
				HeaderName::try_from(k).expect("invalid header name"),
				HeaderValue::try_from(v).expect("invalid header value"),
			)
		}));

		let uri = Uri::try_from(&uri).unwrap();
		let query = signature::payload::parse_query_map(&uri).unwrap();

		let canonical_request = signature::payload::canonical_request(
			self.service,
			&self.method,
			uri.path(),
			&query,
			&all_headers,
			&signed_headers,
			&body_sha,
		)
		.unwrap();

		let string_to_sign = signature::payload::string_to_sign(&now, &scope, &canonical_request);

		signer.update(string_to_sign.as_bytes());
		let signature = hex::encode(signer.finalize().into_bytes());
		let authorization = format!(
			"AWS4-HMAC-SHA256 Credential={}/{},SignedHeaders={},Signature={}",
			self.requester.key.id, scope, signed_headers_str, signature
		);
		all_headers.insert(
			AUTHORIZATION,
			HeaderValue::from_str(&authorization).unwrap(),
		);

		let mut request = Request::builder();
		*request.headers_mut().unwrap() = all_headers;

		let body = match &self.body_signature {
			BodySignature::Streaming { chunk_size } => to_streaming_body(
				&self.body,
				*chunk_size,
				signature,
				streaming_signer,
				now,
				&scope,
			),
			BodySignature::StreamingUnsignedTrailer {
				chunk_size,
				trailer_algorithm,
				trailer_value,
			} => to_streaming_unsigned_trailer_body(
				&self.body,
				*chunk_size,
				&trailer_algorithm,
				&trailer_value,
			),
			_ => self.body.clone(),
		};
		let request = request
			.uri(uri)
			.method(self.method.clone())
			.body(Body::from(body))
			.unwrap();

		let result = self
			.requester
			.client
			.request(request)
			.await
			.map_err(|err| format!("hyper client error: {}", err))?;

		let (head, body) = result.into_parts();
		let body = Body::new(
			body.collect()
				.await
				.map_err(|err| format!("hyper client error in body.collect: {}", err))?
				.to_bytes(),
		);
		Ok(Response::from_parts(head, body))
	}
}

pub enum BodySignature {
	Unsigned,
	Classic,
	Streaming {
		chunk_size: usize,
	},
	StreamingUnsignedTrailer {
		chunk_size: usize,
		trailer_algorithm: String,
		trailer_value: String,
	},
}

fn query_param_to_string(params: &HashMap<String, Option<String>>) -> String {
	if params.is_empty() {
		return String::new();
	}

	"?".to_owned()
		+ &params
			.iter()
			.map(|(k, v)| {
				if let Some(v) = v {
					format!("{}={}", k, v)
				} else {
					k.clone()
				}
			})
			.collect::<Vec<String>>()
			.join("&")
}

fn to_streaming_body(
	body: &[u8],
	chunk_size: usize,
	mut seed: String,
	hasher: Hmac<sha2::Sha256>,
	now: DateTime<Utc>,
	scope: &str,
) -> Vec<u8> {
	const SHA_NULL: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
	let now = now.format(signature::LONG_DATETIME).to_string();
	let mut res = Vec::with_capacity(body.len());
	for chunk in body.chunks(chunk_size).chain(std::iter::once(&[][..])) {
		let to_sign = format!(
			"AWS4-HMAC-SHA256-PAYLOAD\n{}\n{}\n{}\n{}\n{}",
			now,
			scope,
			seed,
			SHA_NULL,
			hex::encode(garage_util::data::sha256sum(chunk))
		);

		let mut hasher = hasher.clone();
		hasher.update(to_sign.as_bytes());
		seed = hex::encode(hasher.finalize().into_bytes());

		let header = format!("{:x};chunk-signature={}\r\n", chunk.len(), seed);
		res.extend_from_slice(header.as_bytes());
		res.extend_from_slice(chunk);
		res.extend_from_slice(b"\r\n");
	}

	res
}

fn to_streaming_unsigned_trailer_body(
	body: &[u8],
	chunk_size: usize,
	trailer_algorithm: &str,
	trailer_value: &str,
) -> Vec<u8> {
	let mut res = Vec::with_capacity(body.len());
	for chunk in body.chunks(chunk_size) {
		let header = format!("{:x}\r\n", chunk.len());
		res.extend_from_slice(header.as_bytes());
		res.extend_from_slice(chunk);
		res.extend_from_slice(b"\r\n");
	}

	res.extend_from_slice(b"0\r\n");
	res.extend_from_slice(trailer_algorithm.as_bytes());
	res.extend_from_slice(b":");
	res.extend_from_slice(trailer_value.as_bytes());
	res.extend_from_slice(b"\n\r\n\r\n");

	res
}
