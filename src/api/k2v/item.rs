use base64::prelude::*;
use http::header;

use hyper::{Request, Response, StatusCode};

use garage_model::k2v::causality::*;
use garage_model::k2v::item_table::*;

use garage_api_common::helpers::*;

use crate::api_server::{ReqBody, ResBody};
use crate::error::*;

pub const X_GARAGE_CAUSALITY_TOKEN: &str = "X-Garage-Causality-Token";

pub enum ReturnFormat {
	Json,
	Binary,
	Either,
}

pub(crate) fn parse_causality_token(s: &str) -> Result<CausalContext, Error> {
	CausalContext::parse(s).ok_or(Error::InvalidCausalityToken)
}

impl ReturnFormat {
	pub fn from(req: &Request<ReqBody>) -> Result<Self, Error> {
		let accept = match req.headers().get(header::ACCEPT) {
			Some(a) => a.to_str()?,
			None => return Ok(Self::Json),
		};

		let accept = accept.split(',').map(|s| s.trim()).collect::<Vec<_>>();
		let accept_json = accept.contains(&"application/json") || accept.contains(&"*/*");
		let accept_binary = accept.contains(&"application/octet-stream") || accept.contains(&"*/*");

		match (accept_json, accept_binary) {
			(true, true) => Ok(Self::Either),
			(true, false) => Ok(Self::Json),
			(false, true) => Ok(Self::Binary),
			(false, false) => Err(Error::NotAcceptable("Invalid Accept: header value, must contain either application/json or application/octet-stream (or both)".into())),
		}
	}

	pub fn make_response(&self, item: &K2VItem) -> Result<Response<ResBody>, Error> {
		let vals = item.values();

		if vals.is_empty() {
			return Err(Error::NoSuchKey);
		}

		let ct = item.causal_context().serialize();
		match self {
			Self::Binary if vals.len() > 1 => Ok(Response::builder()
				.header(X_GARAGE_CAUSALITY_TOKEN, ct)
				.status(StatusCode::CONFLICT)
				.body(empty_body())?),
			Self::Binary => {
				assert!(vals.len() == 1);
				Self::make_binary_response(ct, vals[0])
			}
			Self::Either if vals.len() == 1 => Self::make_binary_response(ct, vals[0]),
			_ => Self::make_json_response(ct, &vals[..]),
		}
	}

	fn make_binary_response(ct: String, v: &DvvsValue) -> Result<Response<ResBody>, Error> {
		match v {
			DvvsValue::Deleted => Ok(Response::builder()
				.header(X_GARAGE_CAUSALITY_TOKEN, ct)
				.header(header::CONTENT_TYPE, "application/octet-stream")
				.status(StatusCode::NO_CONTENT)
				.body(empty_body())?),
			DvvsValue::Value(v) => Ok(Response::builder()
				.header(X_GARAGE_CAUSALITY_TOKEN, ct)
				.header(header::CONTENT_TYPE, "application/octet-stream")
				.status(StatusCode::OK)
				.body(bytes_body(v.to_vec().into()))?),
		}
	}

	fn make_json_response(ct: String, v: &[&DvvsValue]) -> Result<Response<ResBody>, Error> {
		let items = v
			.iter()
			.map(|v| match v {
				DvvsValue::Deleted => serde_json::Value::Null,
				DvvsValue::Value(v) => serde_json::Value::String(BASE64_STANDARD.encode(v)),
			})
			.collect::<Vec<_>>();
		let json_body =
			serde_json::to_string_pretty(&items).ok_or_internal_error("JSON encoding error")?;
		Ok(Response::builder()
			.header(X_GARAGE_CAUSALITY_TOKEN, ct)
			.header(header::CONTENT_TYPE, "application/json")
			.status(StatusCode::OK)
			.body(string_body(json_body))?)
	}
}

/// Handle ReadItem request
#[allow(clippy::ptr_arg)]
pub async fn handle_read_item(
	ctx: ReqCtx,
	req: &Request<ReqBody>,
	partition_key: &str,
	sort_key: &String,
) -> Result<Response<ResBody>, Error> {
	let ReqCtx {
		garage, bucket_id, ..
	} = &ctx;

	let format = ReturnFormat::from(req)?;

	let item = garage
		.k2v
		.item_table
		.get(
			&K2VItemPartition {
				bucket_id: *bucket_id,
				partition_key: partition_key.to_string(),
			},
			sort_key,
		)
		.await?
		.ok_or(Error::NoSuchKey)?;

	format.make_response(&item)
}

pub async fn handle_insert_item(
	ctx: ReqCtx,
	req: Request<ReqBody>,
	partition_key: &str,
	sort_key: &str,
) -> Result<Response<ResBody>, Error> {
	let ReqCtx {
		garage, bucket_id, ..
	} = &ctx;
	let causal_context = req
		.headers()
		.get(X_GARAGE_CAUSALITY_TOKEN)
		.map(|s| s.to_str())
		.transpose()?
		.map(parse_causality_token)
		.transpose()?;

	let body = req.into_body().collect().await?;

	let value = DvvsValue::Value(body.to_vec());

	garage
		.k2v
		.rpc
		.insert(
			*bucket_id,
			partition_key.to_string(),
			sort_key.to_string(),
			causal_context,
			value,
		)
		.await?;

	Ok(Response::builder()
		.status(StatusCode::NO_CONTENT)
		.body(empty_body())?)
}

pub async fn handle_delete_item(
	ctx: ReqCtx,
	req: Request<ReqBody>,
	partition_key: &str,
	sort_key: &str,
) -> Result<Response<ResBody>, Error> {
	let ReqCtx {
		garage, bucket_id, ..
	} = &ctx;
	let causal_context = req
		.headers()
		.get(X_GARAGE_CAUSALITY_TOKEN)
		.map(|s| s.to_str())
		.transpose()?
		.map(parse_causality_token)
		.transpose()?;

	let value = DvvsValue::Deleted;

	garage
		.k2v
		.rpc
		.insert(
			*bucket_id,
			partition_key.to_string(),
			sort_key.to_string(),
			causal_context,
			value,
		)
		.await?;

	Ok(Response::builder()
		.status(StatusCode::NO_CONTENT)
		.body(empty_body())?)
}

/// Handle ReadItem request
#[allow(clippy::ptr_arg)]
pub async fn handle_poll_item(
	ctx: ReqCtx,
	req: &Request<ReqBody>,
	partition_key: String,
	sort_key: String,
	causality_token: String,
	timeout_secs: Option<u64>,
) -> Result<Response<ResBody>, Error> {
	let ReqCtx {
		garage, bucket_id, ..
	} = &ctx;
	let format = ReturnFormat::from(req)?;

	let causal_context =
		CausalContext::parse(&causality_token).ok_or_bad_request("Invalid causality token")?;

	let timeout_msec = timeout_secs.unwrap_or(300).clamp(1, 600) * 1000;

	let item = garage
		.k2v
		.rpc
		.poll_item(
			*bucket_id,
			partition_key,
			sort_key,
			causal_context,
			timeout_msec,
		)
		.await?;

	if let Some(item) = item {
		format.make_response(&item)
	} else {
		Ok(Response::builder()
			.status(StatusCode::NOT_MODIFIED)
			.body(empty_body())?)
	}
}
