#![allow(dead_code)]

use std::collections::HashMap;
use std::convert::TryFrom;

use chrono::{offset::Utc, DateTime};
use hmac::{Hmac, Mac};
use hyper::client::HttpConnector;
use hyper::{Body, Client, Method, Request, Response, Uri};

use super::garage::{Instance, Key};
use garage_api::signature;

/// You should ever only use this to send requests AWS sdk won't send,
/// like to reproduce behavior of unusual implementations found to be
/// problematic.
pub struct CustomRequester {
	key: Key,
	uri: Uri,
	client: Client<HttpConnector>,
}

impl CustomRequester {
	pub fn new(instance: &Instance) -> Self {
		/*
		let credentials = Credentials::new(
			&instance.key.id,
			&instance.key.secret,
			None,
			None,
			"garage-integ-test",
		);
		let endpoint = Endpoint::immutable(instance.uri());
		*/
		CustomRequester {
			key: instance.key.clone(),
			uri: instance.uri(),
			client: Client::new(),
		}
	}

	pub fn builder(&self, bucket: String) -> RequestBuilder<'_> {
		RequestBuilder {
			requester: self,
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
	/*
	pub async fn request(&self, method: &str, path: String, headers: &HashMap<String, (bool, String)>, body: &[u8], vhost_style: bool) -> hyper::Result<Response<Body>> {
		let request = Request::builder()
			.method(
		self.client.request(todo!()).await
	}
	*/
}

pub struct RequestBuilder<'a> {
	requester: &'a CustomRequester,
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
	pub fn method(&mut self, method: Method) -> &mut Self {
		self.method = method;
		self
	}

	pub fn path(&mut self, path: String) -> &mut Self {
		self.path = path;
		self
	}

	pub fn query_params(&mut self, query_params: HashMap<String, Option<String>>) -> &mut Self {
		self.query_params = query_params;
		self
	}

	pub fn signed_headers(&mut self, signed_headers: HashMap<String, String>) -> &mut Self {
		self.signed_headers = signed_headers;
		self
	}

	pub fn unsigned_headers(&mut self, unsigned_headers: HashMap<String, String>) -> &mut Self {
		self.unsigned_headers = unsigned_headers;
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

	pub async fn send(&mut self) -> hyper::Result<Response<Body>> {
		// TODO this is a bit incorrect in that path and query params should be url-encoded and
		// aren't, but this is good enought for now.

		let query = query_param_to_string(&self.query_params);
		let (host, path) = if self.vhost_style {
			(
				format!("{}.s3.garage", self.bucket),
				format!("{}{}", self.path, query),
			)
		} else {
			(
				"s3.garage".to_owned(),
				format!("{}/{}{}", self.bucket, self.path, query),
			)
		};
		let uri = format!("{}{}", self.requester.uri, path);

		let now = Utc::now();
		let scope = signature::compute_scope(&now, super::REGION.as_ref());
		let mut signer = signature::signing_hmac(
			&now,
			&self.requester.key.secret,
			super::REGION.as_ref(),
			"s3",
		)
		.unwrap();
		let streaming_signer = signer.clone();

		let mut all_headers = self.signed_headers.clone();

		let date = now.format(signature::LONG_DATETIME).to_string();
		all_headers.insert("x-amz-date".to_owned(), date);
		all_headers.insert("host".to_owned(), host);

		let body_sha = match self.body_signature {
			BodySignature::Unsigned => "UNSIGNED-PAYLOAD".to_owned(),
			BodySignature::Classic => hex::encode(garage_util::data::sha256sum(&self.body)),
			BodySignature::Streaming(size) => {
				all_headers.insert("content-encoding".to_owned(), "aws-chunked".to_owned());
				all_headers.insert(
					"x-amz-decoded-content-length".to_owned(),
					self.body.len().to_string(),
				);
				// this is a pretty lazy and inefficient way to do it, but it's enought for
				// test code.
				all_headers.insert(
					"content-length".to_owned(),
					to_streaming_body(&self.body, size, String::new(), signer.clone(), now, "")
						.len()
						.to_string(),
				);

				"STREAMING-AWS4-HMAC-SHA256-PAYLOAD".to_owned()
			}
		};
		all_headers.insert("x-amz-content-sha256".to_owned(), body_sha.clone());

		let mut signed_headers = all_headers
			.iter()
			.map(|(k, _)| k.as_ref())
			.collect::<Vec<&str>>();
		signed_headers.sort();
		let signed_headers = signed_headers.join(";");

		all_headers.extend(self.unsigned_headers.clone());

		let canonical_request = signature::payload::canonical_request(
			&self.method,
			&Uri::try_from(&uri).unwrap(),
			&all_headers,
			&signed_headers,
			&body_sha,
		);

		let string_to_sign = signature::payload::string_to_sign(&now, &scope, &canonical_request);

		signer.update(string_to_sign.as_bytes());
		let signature = hex::encode(signer.finalize().into_bytes());
		let authorization = format!(
			"AWS4-HMAC-SHA256 Credential={}/{},SignedHeaders={},Signature={}",
			self.requester.key.id, scope, signed_headers, signature
		);
		all_headers.insert("authorization".to_owned(), authorization);

		let mut request = Request::builder();
		for (k, v) in all_headers {
			request = request.header(k, v);
		}

		let body = if let BodySignature::Streaming(size) = self.body_signature {
			to_streaming_body(&self.body, size, signature, streaming_signer, now, &scope)
		} else {
			self.body.clone()
		};
		let request = request
			.uri(uri)
			.method(self.method.clone())
			.body(Body::from(body))
			.unwrap();
		self.requester.client.request(request).await
	}
}

pub enum BodySignature {
	Unsigned,
	Classic,
	Streaming(usize),
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
