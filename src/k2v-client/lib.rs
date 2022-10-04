use std::collections::BTreeMap;
use std::time::Duration;

use http::header::{ACCEPT, CONTENT_LENGTH, CONTENT_TYPE};
use http::status::StatusCode;
use http::HeaderMap;
use log::{debug, error};

use rusoto_core::{ByteStream, DispatchSignedRequest, HttpClient};
use rusoto_credential::AwsCredentials;
use rusoto_signature::region::Region;
use rusoto_signature::signature::SignedRequest;
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use tokio::io::AsyncReadExt;

mod error;

pub use error::Error;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(5);
const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_secs(300);
const SERVICE: &str = "k2v";
const GARAGE_CAUSALITY_TOKEN: &str = "X-Garage-Causality-Token";

/// Client used to query a K2V server.
pub struct K2vClient {
	region: Region,
	bucket: String,
	creds: AwsCredentials,
	client: HttpClient,
}

impl K2vClient {
	/// Create a new K2V client.
	pub fn new(
		region: Region,
		bucket: String,
		creds: AwsCredentials,
		user_agent: Option<String>,
	) -> Result<Self, Error> {
		let mut client = HttpClient::new()?;
		if let Some(ua) = user_agent {
			client.local_agent_prepend(ua);
		} else {
			client.local_agent_prepend(format!("k2v/{}", env!("CARGO_PKG_VERSION")));
		}
		Ok(K2vClient {
			region,
			bucket,
			creds,
			client,
		})
	}

	/// Perform a ReadItem request, reading the value(s) stored for a single pk+sk.
	pub async fn read_item(
		&self,
		partition_key: &str,
		sort_key: &str,
	) -> Result<CausalValue, Error> {
		let mut req = SignedRequest::new(
			"GET",
			SERVICE,
			&self.region,
			&format!("/{}/{}", self.bucket, partition_key),
		);
		req.add_param("sort_key", sort_key);
		req.add_header(ACCEPT, "application/octet-stream, application/json");

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
				value: vec![K2vValue::Value(res.body)],
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

		let mut req = SignedRequest::new(
			"GET",
			SERVICE,
			&self.region,
			&format!("/{}/{}", self.bucket, partition_key),
		);
		req.add_param("sort_key", sort_key);
		req.add_param("causality_token", &causality.0);
		req.add_param("timeout", &timeout.as_secs().to_string());
		req.add_header(ACCEPT, "application/octet-stream, application/json");

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
				value: vec![K2vValue::Value(res.body)],
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

	/// Perform an InsertItem request, inserting a value for a single pk+sk.
	pub async fn insert_item(
		&self,
		partition_key: &str,
		sort_key: &str,
		value: Vec<u8>,
		causality: Option<CausalityToken>,
	) -> Result<(), Error> {
		let mut req = SignedRequest::new(
			"PUT",
			SERVICE,
			&self.region,
			&format!("/{}/{}", self.bucket, partition_key),
		);
		req.add_param("sort_key", sort_key);
		req.set_payload(Some(value));

		if let Some(causality) = causality {
			req.add_header(GARAGE_CAUSALITY_TOKEN, &causality.0);
		}

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
		let mut req = SignedRequest::new(
			"DELETE",
			SERVICE,
			&self.region,
			&format!("/{}/{}", self.bucket, partition_key),
		);
		req.add_param("sort_key", sort_key);
		req.add_header(GARAGE_CAUSALITY_TOKEN, &causality.0);

		self.dispatch(req, None).await?;
		Ok(())
	}

	/// Perform a ReadIndex request, listing partition key which have at least one associated
	/// sort key, and which matches the filter.
	pub async fn read_index(
		&self,
		filter: Filter<'_>,
	) -> Result<PaginatedRange<PartitionInfo>, Error> {
		let mut req =
			SignedRequest::new("GET", SERVICE, &self.region, &format!("/{}", self.bucket));
		filter.insert_params(&mut req);

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
		let mut req =
			SignedRequest::new("POST", SERVICE, &self.region, &format!("/{}", self.bucket));

		let payload = serde_json::to_vec(operations)?;
		req.set_payload(Some(payload));
		self.dispatch(req, None).await?;
		Ok(())
	}

	/// Perform a ReadBatch request, reading multiple values or range of values at once.
	pub async fn read_batch(
		&self,
		operations: &[BatchReadOp<'_>],
	) -> Result<Vec<PaginatedRange<CausalValue>>, Error> {
		let mut req =
			SignedRequest::new("POST", SERVICE, &self.region, &format!("/{}", self.bucket));
		req.add_param("search", "");

		let payload = serde_json::to_vec(operations)?;
		req.set_payload(Some(payload));
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

	/// Perform a DeleteBatch request, deleting mutiple values or range of values at once, without
	/// providing causality information.
	pub async fn delete_batch(&self, operations: &[BatchDeleteOp<'_>]) -> Result<Vec<u64>, Error> {
		let mut req =
			SignedRequest::new("POST", SERVICE, &self.region, &format!("/{}", self.bucket));
		req.add_param("delete", "");

		let payload = serde_json::to_vec(operations)?;
		req.set_payload(Some(payload));
		let res = self.dispatch(req, None).await?;

		let resp: Vec<BatchDeleteResponse> = serde_json::from_slice(&res.body)?;

		Ok(resp.into_iter().map(|r| r.deleted_items).collect())
	}

	async fn dispatch(
		&self,
		mut req: SignedRequest,
		timeout: Option<Duration>,
	) -> Result<Response, Error> {
		req.sign(&self.creds);
		let mut res = self
			.client
			.dispatch(req, Some(timeout.unwrap_or(DEFAULT_TIMEOUT)))
			.await?;

		let causality_token = res
			.headers
			.remove(GARAGE_CAUSALITY_TOKEN)
			.map(CausalityToken);
		let content_type = res.headers.remove(CONTENT_TYPE);

		let body = match res.status {
			StatusCode::OK => read_body(&mut res.headers, res.body).await?,
			StatusCode::NO_CONTENT => Vec::new(),
			StatusCode::NOT_FOUND => return Err(Error::NotFound),
			StatusCode::NOT_MODIFIED => Vec::new(),
			s => {
				let err_body = read_body(&mut res.headers, res.body)
					.await
					.unwrap_or_default();
				let err_body_str = std::str::from_utf8(&err_body)
					.map(String::from)
					.unwrap_or_else(|_| base64::encode(&err_body));

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
				.unwrap_or_else(|_| base64::encode(&body))
		);

		Ok(Response {
			body,
			status: res.status,
			causality_token,
			content_type,
		})
	}
}

async fn read_body(headers: &mut HeaderMap<String>, body: ByteStream) -> Result<Vec<u8>, Error> {
	let body_len = headers
		.get(CONTENT_LENGTH)
		.and_then(|h| h.parse().ok())
		.unwrap_or(0);
	let mut res = Vec::with_capacity(body_len);
	body.into_async_read().read_to_end(&mut res).await?;
	Ok(res)
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
			Some(s) => {
				K2vValue::Value(base64::decode(s).map_err(|_| DeError::custom("invalid base64"))?)
			}
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
				let b64 = base64::encode(v);
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

impl<'a> Filter<'a> {
	fn insert_params(&self, req: &mut SignedRequest) {
		if let Some(start) = &self.start {
			req.add_param("start", start);
		}
		if let Some(end) = &self.end {
			req.add_param("end", end);
		}
		if let Some(prefix) = &self.prefix {
			req.add_param("prefix", prefix);
		}
		if let Some(limit) = &self.limit {
			req.add_param("limit", &limit.to_string());
		}
		if self.reverse {
			req.add_param("reverse", "true");
		}
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
	body: Vec<u8>,
	status: StatusCode,
	causality_token: Option<CausalityToken>,
	content_type: Option<String>,
}
