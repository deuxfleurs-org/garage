use std::collections::BTreeMap;
use std::convert::TryInto;
use std::time::{Duration, SystemTime};

use base64::prelude::*;
use log::{debug, error};
use percent_encoding::{utf8_percent_encode, AsciiSet, NON_ALPHANUMERIC};

use http::header::{ACCEPT, CONTENT_TYPE};
use http::status::StatusCode;
use http::{HeaderName, HeaderValue, Request};
use http_body_util::{BodyExt, Full as FullBody};
use hyper::body::Bytes;
use hyper_rustls::HttpsConnector;
use hyper_util::client::legacy::{connect::HttpConnector, Client as HttpClient};
use hyper_util::rt::TokioExecutor;

use aws_sdk_config::config::Credentials;
use aws_sigv4::http_request::{sign, SignableBody, SignableRequest, SigningSettings};
use aws_sigv4::sign::v4::SigningParams;

use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

mod error;

pub use error::Error;

pub type Body = FullBody<Bytes>;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_secs(300);
const SERVICE: &str = "k2v";
const AMZ_CONTENT_SHA256: HeaderName = HeaderName::from_static("x-amz-content-sha256");
const GARAGE_CAUSALITY_TOKEN: HeaderName = HeaderName::from_static("x-garage-causality-token");

const STRICT_ENCODE_SET: AsciiSet = NON_ALPHANUMERIC
	.remove(b'_')
	.remove(b'-')
	.remove(b'.')
	.remove(b'~');
const PATH_ENCODE_SET: AsciiSet = NON_ALPHANUMERIC
	.remove(b'/')
	.remove(b'_')
	.remove(b'-')
	.remove(b'.')
	.remove(b'~');

pub struct K2vClientConfig {
	pub endpoint: String,
	pub region: String,
	pub aws_access_key_id: String,
	pub aws_secret_access_key: String,
	pub bucket: String,
	pub user_agent: Option<String>,
}

/// Client used to query a K2V server.
pub struct K2vClient {
	config: K2vClientConfig,
	user_agent: HeaderValue,
	client: HttpClient<HttpsConnector<HttpConnector>, Body>,
}

impl K2vClient {
	/// Create a new K2V client.
	pub fn new(config: K2vClientConfig) -> Result<Self, Error> {
		let connector = hyper_rustls::HttpsConnectorBuilder::new()
			.with_native_roots()?
			.https_or_http()
			.enable_http1()
			.enable_http2()
			.build();
		let client = HttpClient::builder(TokioExecutor::new()).build(connector);
		let user_agent: std::borrow::Cow<str> = match &config.user_agent {
			Some(ua) => ua.into(),
			None => format!("k2v/{}", env!("CARGO_PKG_VERSION")).into(),
		};
		let user_agent = HeaderValue::from_str(&user_agent)
			.map_err(|_| Error::Message("invalid user agent".into()))?;
		Ok(K2vClient {
			config,
			client,
			user_agent,
		})
	}

	/// Perform a ReadItem request, reading the value(s) stored for a single pk+sk.
	pub async fn read_item(
		&self,
		partition_key: &str,
		sort_key: &str,
	) -> Result<CausalValue, Error> {
		let url = self.build_url(Some(partition_key), &[("sort_key", sort_key)]);
		let req = Request::get(url)
			.header(ACCEPT, "application/octet-stream, application/json")
			.body(Bytes::new())?;
		let res = self.dispatch(req, None).await?;

		let causality = res
			.causality_token
			.ok_or_else(|| Error::InvalidResponse("missing causality token".into()))?;

		if res.status == StatusCode::NO_CONTENT {
			return Ok(CausalValue {
				causality,
				value: vec![K2vValue::Tombstone],
			});
		}

		match res.content_type.as_deref() {
			Some("application/octet-stream") => Ok(CausalValue {
				causality,
				value: vec![K2vValue::Value(res.body.to_vec())],
			}),
			Some("application/json") => {
				let value = serde_json::from_slice(&res.body)?;
				Ok(CausalValue { causality, value })
			}
			Some(ct) => Err(Error::InvalidResponse(
				format!("invalid content type: {}", ct).into(),
			)),
			None => Err(Error::InvalidResponse("missing content type".into())),
		}
	}

	/// Perform a PollItem request, waiting for the value(s) stored for a single pk+sk to be
	/// updated.
	pub async fn poll_item(
		&self,
		partition_key: &str,
		sort_key: &str,
		causality: CausalityToken,
		timeout: Option<Duration>,
	) -> Result<Option<CausalValue>, Error> {
		let timeout = timeout.unwrap_or(DEFAULT_POLL_TIMEOUT);

		let url = self.build_url(
			Some(partition_key),
			&[
				("sort_key", sort_key),
				("causality_token", &causality.0),
				("timeout", &timeout.as_secs().to_string()),
			],
		);
		let req = Request::get(url)
			.header(ACCEPT, "application/octet-stream, application/json")
			.body(Bytes::new())?;

		let res = self.dispatch(req, Some(timeout + DEFAULT_TIMEOUT)).await?;

		if res.status == StatusCode::NOT_MODIFIED {
			return Ok(None);
		}

		let causality = res
			.causality_token
			.ok_or_else(|| Error::InvalidResponse("missing causality token".into()))?;

		if res.status == StatusCode::NO_CONTENT {
			return Ok(Some(CausalValue {
				causality,
				value: vec![K2vValue::Tombstone],
			}));
		}

		match res.content_type.as_deref() {
			Some("application/octet-stream") => Ok(Some(CausalValue {
				causality,
				value: vec![K2vValue::Value(res.body.to_vec())],
			})),
			Some("application/json") => {
				let value = serde_json::from_slice(&res.body)?;
				Ok(Some(CausalValue { causality, value }))
			}
			Some(ct) => Err(Error::InvalidResponse(
				format!("invalid content type: {}", ct).into(),
			)),
			None => Err(Error::InvalidResponse("missing content type".into())),
		}
	}

	/// Perform a PollRange request, waiting for any change in a given range of keys
	/// to occur
	pub async fn poll_range(
		&self,
		partition_key: &str,
		filter: Option<PollRangeFilter<'_>>,
		seen_marker: Option<&str>,
		timeout: Option<Duration>,
	) -> Result<Option<PollRangeResult>, Error> {
		let timeout = timeout.unwrap_or(DEFAULT_POLL_TIMEOUT);

		let request = PollRangeRequest {
			filter: filter.unwrap_or_default(),
			seen_marker,
			timeout: timeout.as_secs(),
		};

		let url = self.build_url(Some(partition_key), &[("poll_range", "")]);
		let payload = serde_json::to_vec(&request)?;
		let req = Request::post(url).body(Bytes::from(payload))?;

		let res = self.dispatch(req, Some(timeout + DEFAULT_TIMEOUT)).await?;

		if res.status == StatusCode::NOT_MODIFIED {
			return Ok(None);
		}

		let resp: PollRangeResponse = serde_json::from_slice(&res.body)?;

		let items = resp
			.items
			.into_iter()
			.map(|BatchReadItem { sk, ct, v }| {
				(
					sk,
					CausalValue {
						causality: ct,
						value: v,
					},
				)
			})
			.collect::<BTreeMap<_, _>>();

		Ok(Some(PollRangeResult {
			items,
			seen_marker: resp.seen_marker,
		}))
	}

	/// Perform an InsertItem request, inserting a value for a single pk+sk.
	pub async fn insert_item(
		&self,
		partition_key: &str,
		sort_key: &str,
		value: Vec<u8>,
		causality: Option<CausalityToken>,
	) -> Result<(), Error> {
		let url = self.build_url(Some(partition_key), &[("sort_key", sort_key)]);
		let mut req = Request::put(url);
		if let Some(causality) = causality {
			req = req.header(GARAGE_CAUSALITY_TOKEN, &causality.0);
		}
		let req = req.body(Bytes::from(value))?;

		self.dispatch(req, None).await?;
		Ok(())
	}

	/// Perform a DeleteItem request, deleting the value(s) stored for a single pk+sk.
	pub async fn delete_item(
		&self,
		partition_key: &str,
		sort_key: &str,
		causality: CausalityToken,
	) -> Result<(), Error> {
		let url = self.build_url(Some(partition_key), &[("sort_key", sort_key)]);
		let req = Request::delete(url)
			.header(GARAGE_CAUSALITY_TOKEN, &causality.0)
			.body(Bytes::new())?;

		self.dispatch(req, None).await?;
		Ok(())
	}

	/// Perform a ReadIndex request, listing partition key which have at least one associated
	/// sort key, and which matches the filter.
	pub async fn read_index(
		&self,
		filter: Filter<'_>,
	) -> Result<PaginatedRange<PartitionInfo>, Error> {
		let params = filter.query_params();
		let url = self.build_url(None, &params);
		let req = Request::get(url).body(Bytes::new())?;

		let res = self.dispatch(req, None).await?;

		let resp: ReadIndexResponse = serde_json::from_slice(&res.body)?;

		let items = resp
			.partition_keys
			.into_iter()
			.map(|ReadIndexItem { pk, info }| (pk, info))
			.collect();

		Ok(PaginatedRange {
			items,
			next_start: resp.next_start,
		})
	}

	/// Perform an InsertBatch request, inserting multiple values at once. Note: this operation is
	/// *not* atomic: it is possible for some sub-operations to fails and others to success. In
	/// that case, failure is reported.
	pub async fn insert_batch(&self, operations: &[BatchInsertOp<'_>]) -> Result<(), Error> {
		let url = self.build_url::<&str>(None, &[]);
		let payload = serde_json::to_vec(operations)?;
		let req = Request::post(url).body(payload.into())?;

		self.dispatch(req, None).await?;
		Ok(())
	}

	/// Perform a ReadBatch request, reading multiple values or range of values at once.
	pub async fn read_batch(
		&self,
		operations: &[BatchReadOp<'_>],
	) -> Result<Vec<PaginatedRange<CausalValue>>, Error> {
		let url = self.build_url(None, &[("search", "")]);
		let payload = serde_json::to_vec(operations)?;
		let req = Request::post(url).body(payload.into())?;

		let res = self.dispatch(req, None).await?;

		let resp: Vec<BatchReadResponse> = serde_json::from_slice(&res.body)?;

		Ok(resp
			.into_iter()
			.map(|e| PaginatedRange {
				items: e
					.items
					.into_iter()
					.map(|BatchReadItem { sk, ct, v }| {
						(
							sk,
							CausalValue {
								causality: ct,
								value: v,
							},
						)
					})
					.collect(),
				next_start: e.next_start,
			})
			.collect())
	}

	/// Perform a DeleteBatch request, deleting multiple values or range of values at once, without
	/// providing causality information.
	pub async fn delete_batch(&self, operations: &[BatchDeleteOp<'_>]) -> Result<Vec<u64>, Error> {
		let url = self.build_url(None, &[("delete", "")]);
		let payload = serde_json::to_vec(operations)?;
		let req = Request::post(url).body(payload.into())?;

		let res = self.dispatch(req, None).await?;

		let resp: Vec<BatchDeleteResponse> = serde_json::from_slice(&res.body)?;

		Ok(resp.into_iter().map(|r| r.deleted_items).collect())
	}

	async fn dispatch(
		&self,
		mut req: Request<Bytes>,
		timeout: Option<Duration>,
	) -> Result<Response, Error> {
		req.headers_mut()
			.insert(http::header::USER_AGENT, self.user_agent.clone());

		use sha2::{Digest, Sha256};
		let mut hasher = Sha256::new();
		hasher.update(req.body());
		let hash = hex::encode(&hasher.finalize());
		req.headers_mut()
			.insert(AMZ_CONTENT_SHA256, hash.try_into().unwrap());

		debug!("request uri: {:?}", req.uri());

		// Sign request
		let signing_settings = SigningSettings::default();
		let identity = Credentials::new(
			&self.config.aws_access_key_id,
			&self.config.aws_secret_access_key,
			None,
			None,
			"k2v-client",
		)
		.into();
		let signing_params = SigningParams::builder()
			.identity(&identity)
			.region(&self.config.region)
			.name(SERVICE)
			.time(SystemTime::now())
			.settings(signing_settings)
			.build()?
			.into();
		// Convert the HTTP request into a signable request
		let signable_request = SignableRequest::new(
			req.method().as_str(),
			req.uri().to_string(),
			// TODO: get rid of Unwrap
			req.headers()
				.iter()
				.map(|(x, y)| (x.as_str(), y.to_str().unwrap())),
			SignableBody::Bytes(req.body().as_ref()),
		)?;

		// Sign and then apply the signature to the request
		let (signing_instructions, _signature) =
			sign(signable_request, &signing_params)?.into_parts();
		signing_instructions.apply_to_request_http1x(&mut req);

		// Send and wait for timeout
		let res = tokio::select! {
			res = self.client.request(req.map(Body::from)) => res?,
			_ = tokio::time::sleep(timeout.unwrap_or(DEFAULT_TIMEOUT)) => {
				return Err(Error::Timeout);
			}
		};

		let (mut res, body) = res.into_parts();
		let causality_token = match res.headers.remove(GARAGE_CAUSALITY_TOKEN) {
			Some(v) => Some(CausalityToken(v.to_str()?.to_string())),
			None => None,
		};
		let content_type = match res.headers.remove(CONTENT_TYPE) {
			Some(v) => Some(v.to_str()?.to_string()),
			None => None,
		};

		let body = match res.status {
			StatusCode::OK => BodyExt::collect(body).await?.to_bytes(),
			StatusCode::NO_CONTENT => Bytes::new(),
			StatusCode::NOT_FOUND => return Err(Error::NotFound),
			StatusCode::NOT_MODIFIED => Bytes::new(),
			s => {
				let err_body = body
					.collect()
					.await
					.map(|x| x.to_bytes())
					.unwrap_or_default();
				let err_body_str = std::str::from_utf8(&err_body)
					.map(String::from)
					.unwrap_or_else(|_| BASE64_STANDARD.encode(&err_body));

				if s.is_client_error() || s.is_server_error() {
					error!("Error response {}: {}", res.status, err_body_str);
					let err = match serde_json::from_slice::<ErrorResponse>(&err_body) {
						Ok(err) => Error::Remote(
							res.status,
							err.code.into(),
							err.message.into(),
							err.path.into(),
						),
						Err(_) => Error::Remote(
							res.status,
							"unknown".into(),
							err_body_str.into(),
							"?".into(),
						),
					};
					return Err(err);
				} else {
					let msg = format!(
						"Unexpected response code {}. Response body: {}",
						res.status, err_body_str
					);
					error!("{}", msg);
					return Err(Error::InvalidResponse(msg.into()));
				}
			}
		};
		debug!(
			"Response body: {}",
			std::str::from_utf8(&body)
				.map(String::from)
				.unwrap_or_else(|_| BASE64_STANDARD.encode(&body))
		);

		Ok(Response {
			body,
			status: res.status,
			causality_token,
			content_type,
		})
	}

	fn build_url<V: AsRef<str>>(&self, partition_key: Option<&str>, query: &[(&str, V)]) -> String {
		let mut url = format!(
			"{}/{}",
			self.config.endpoint.trim_end_matches('/'),
			self.config.bucket
		);
		if let Some(pk) = partition_key {
			url.push('/');
			url.extend(utf8_percent_encode(pk, &PATH_ENCODE_SET));
		}
		if !query.is_empty() {
			url.push('?');
			for (i, (k, v)) in query.iter().enumerate() {
				if i > 0 {
					url.push('&');
				}
				url.extend(utf8_percent_encode(k, &STRICT_ENCODE_SET));
				url.push('=');
				url.extend(utf8_percent_encode(v.as_ref(), &STRICT_ENCODE_SET));
			}
		}
		url
	}
}

/// An opaque token used to convey causality between operations.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(transparent)]
pub struct CausalityToken(String);

impl From<String> for CausalityToken {
	fn from(v: String) -> Self {
		CausalityToken(v)
	}
}

impl From<CausalityToken> for String {
	fn from(v: CausalityToken) -> Self {
		v.0
	}
}

impl AsRef<str> for CausalityToken {
	fn as_ref(&self) -> &str {
		&self.0
	}
}

/// A value in K2V. can be either a binary value, or a tombstone.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum K2vValue {
	Tombstone,
	Value(Vec<u8>),
}

impl From<Vec<u8>> for K2vValue {
	fn from(v: Vec<u8>) -> Self {
		K2vValue::Value(v)
	}
}

impl From<Option<Vec<u8>>> for K2vValue {
	fn from(v: Option<Vec<u8>>) -> Self {
		match v {
			Some(v) => K2vValue::Value(v),
			None => K2vValue::Tombstone,
		}
	}
}

impl<'de> Deserialize<'de> for K2vValue {
	fn deserialize<D>(d: D) -> Result<Self, D::Error>
	where
		D: Deserializer<'de>,
	{
		let val: Option<&str> = Option::deserialize(d)?;
		Ok(match val {
			Some(s) => K2vValue::Value(
				BASE64_STANDARD
					.decode(s)
					.map_err(|_| DeError::custom("invalid base64"))?,
			),
			None => K2vValue::Tombstone,
		})
	}
}

impl Serialize for K2vValue {
	fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
	where
		S: Serializer,
	{
		match self {
			K2vValue::Tombstone => serializer.serialize_none(),
			K2vValue::Value(v) => {
				let b64 = BASE64_STANDARD.encode(v);
				serializer.serialize_str(&b64)
			}
		}
	}
}

/// A set of K2vValue and associated causality information.
#[derive(Debug, Clone, Serialize)]
pub struct CausalValue {
	pub causality: CausalityToken,
	pub value: Vec<K2vValue>,
}

/// Result of paginated requests.
#[derive(Debug, Clone)]
pub struct PaginatedRange<V> {
	pub items: BTreeMap<String, V>,
	pub next_start: Option<String>,
}

/// Filter for batch operations.
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
pub struct Filter<'a> {
	pub start: Option<&'a str>,
	pub end: Option<&'a str>,
	pub prefix: Option<&'a str>,
	pub limit: Option<u64>,
	#[serde(default)]
	pub reverse: bool,
}

/// Filter for a poll range operations.
#[derive(Debug, Default, Clone, Serialize)]
pub struct PollRangeFilter<'a> {
	pub start: Option<&'a str>,
	pub end: Option<&'a str>,
	pub prefix: Option<&'a str>,
}

/// Response to a poll_range query
#[derive(Debug, Default, Clone, Serialize)]
pub struct PollRangeResult {
	/// List of items that have changed since last PollRange call.
	pub items: BTreeMap<String, CausalValue>,
	/// opaque string representing items already seen for future PollRange calls.
	pub seen_marker: String,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct PollRangeRequest<'a> {
	#[serde(flatten)]
	filter: PollRangeFilter<'a>,
	seen_marker: Option<&'a str>,
	timeout: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PollRangeResponse {
	items: Vec<BatchReadItem>,
	seen_marker: String,
}

impl<'a> Filter<'a> {
	fn query_params(&self) -> Vec<(&'static str, std::borrow::Cow<str>)> {
		let mut res = Vec::<(&'static str, std::borrow::Cow<str>)>::with_capacity(8);
		if let Some(start) = self.start.as_deref() {
			res.push(("start", start.into()));
		}
		if let Some(end) = self.end.as_deref() {
			res.push(("end", end.into()));
		}
		if let Some(prefix) = self.prefix.as_deref() {
			res.push(("prefix", prefix.into()));
		}
		if let Some(limit) = &self.limit {
			res.push(("limit", limit.to_string().into()));
		}
		if self.reverse {
			res.push(("reverse", "true".into()));
		}
		res
	}
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ReadIndexResponse<'a> {
	#[serde(flatten, borrow)]
	#[allow(dead_code)]
	filter: Filter<'a>,
	partition_keys: Vec<ReadIndexItem>,
	#[allow(dead_code)]
	more: bool,
	next_start: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ReadIndexItem {
	pk: String,
	#[serde(flatten)]
	info: PartitionInfo,
}

/// Information about data stored with a given partition key.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PartitionInfo {
	pub entries: u64,
	pub conflicts: u64,
	pub values: u64,
	pub bytes: u64,
}

/// Single sub-operation of an InsertBatch.
#[derive(Debug, Clone, Serialize)]
pub struct BatchInsertOp<'a> {
	#[serde(rename = "pk")]
	pub partition_key: &'a str,
	#[serde(rename = "sk")]
	pub sort_key: &'a str,
	#[serde(rename = "ct")]
	pub causality: Option<CausalityToken>,
	#[serde(rename = "v")]
	pub value: K2vValue,
}

/// Single sub-operation of a ReadBatch.
#[derive(Debug, Default, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchReadOp<'a> {
	pub partition_key: &'a str,
	#[serde(flatten, borrow)]
	pub filter: Filter<'a>,
	#[serde(default)]
	pub single_item: bool,
	#[serde(default)]
	pub conflicts_only: bool,
	#[serde(default)]
	pub tombstones: bool,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BatchReadResponse<'a> {
	#[serde(flatten, borrow)]
	#[allow(dead_code)]
	op: BatchReadOp<'a>,
	items: Vec<BatchReadItem>,
	#[allow(dead_code)]
	more: bool,
	next_start: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct BatchReadItem {
	sk: String,
	ct: CausalityToken,
	v: Vec<K2vValue>,
}

/// Single sub-operation of a DeleteBatch
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchDeleteOp<'a> {
	pub partition_key: &'a str,
	pub prefix: Option<&'a str>,
	pub start: Option<&'a str>,
	pub end: Option<&'a str>,
	#[serde(default)]
	pub single_item: bool,
}

impl<'a> BatchDeleteOp<'a> {
	pub fn new(partition_key: &'a str) -> Self {
		BatchDeleteOp {
			partition_key,
			prefix: None,
			start: None,
			end: None,
			single_item: false,
		}
	}
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct BatchDeleteResponse<'a> {
	#[serde(flatten, borrow)]
	#[allow(dead_code)]
	filter: BatchDeleteOp<'a>,
	deleted_items: u64,
}

#[derive(Deserialize)]
struct ErrorResponse {
	code: String,
	message: String,
	#[allow(dead_code)]
	region: String,
	path: String,
}

struct Response {
	body: Bytes,
	status: StatusCode,
	causality_token: Option<CausalityToken>,
	content_type: Option<String>,
}
