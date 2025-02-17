use base64::prelude::*;
use hyper::{Request, Response, StatusCode};
use serde::{Deserialize, Serialize};

use garage_table::{EnumerationOrder, TableSchema};

use garage_model::k2v::item_table::*;

use garage_api_common::helpers::*;

use crate::api_server::{ReqBody, ResBody};
use crate::error::*;
use crate::item::parse_causality_token;
use crate::range::read_range;

pub async fn handle_insert_batch(
	ctx: ReqCtx,
	req: Request<ReqBody>,
) -> Result<Response<ResBody>, Error> {
	let ReqCtx {
		garage, bucket_id, ..
	} = &ctx;
	let items = req.into_body().json::<Vec<InsertBatchItem>>().await?;

	let mut items2 = vec![];
	for it in items {
		let ct = it.ct.map(|s| parse_causality_token(&s)).transpose()?;
		let v = match it.v {
			Some(vs) => DvvsValue::Value(
				BASE64_STANDARD
					.decode(vs)
					.ok_or_bad_request("Invalid base64 value")?,
			),
			None => DvvsValue::Deleted,
		};
		items2.push((it.pk, it.sk, ct, v));
	}

	garage.k2v.rpc.insert_batch(*bucket_id, items2).await?;

	Ok(Response::builder()
		.status(StatusCode::NO_CONTENT)
		.body(empty_body())?)
}

pub async fn handle_read_batch(
	ctx: ReqCtx,
	req: Request<ReqBody>,
) -> Result<Response<ResBody>, Error> {
	let queries = req.into_body().json::<Vec<ReadBatchQuery>>().await?;

	let resp_results = futures::future::join_all(
		queries
			.into_iter()
			.map(|q| handle_read_batch_query(&ctx, q)),
	)
	.await;

	let mut resps: Vec<ReadBatchResponse> = vec![];
	for resp in resp_results {
		resps.push(resp?);
	}

	Ok(json_ok_response(&resps)?)
}

async fn handle_read_batch_query(
	ctx: &ReqCtx,
	query: ReadBatchQuery,
) -> Result<ReadBatchResponse, Error> {
	let ReqCtx {
		garage, bucket_id, ..
	} = ctx;

	let partition = K2VItemPartition {
		bucket_id: *bucket_id,
		partition_key: query.partition_key.clone(),
	};

	let filter = ItemFilter {
		exclude_only_tombstones: !query.tombstones,
		conflicts_only: query.conflicts_only,
	};

	let (items, more, next_start) = if query.single_item {
		if query.prefix.is_some() || query.end.is_some() || query.limit.is_some() || query.reverse {
			return Err(Error::bad_request("Batch query parameters 'prefix', 'end', 'limit' and 'reverse' must not be set when singleItem is true."));
		}
		let sk = query
			.start
			.as_ref()
			.ok_or_bad_request("start should be specified if single_item is set")?;
		let item = garage
			.k2v
			.item_table
			.get(&partition, sk)
			.await?
			.filter(|e| K2VItemTable::matches_filter(e, &filter));
		match item {
			Some(i) => (vec![ReadBatchResponseItem::from(i)], false, None),
			None => (vec![], false, None),
		}
	} else {
		let (items, more, next_start) = read_range(
			&garage.k2v.item_table,
			&partition,
			&query.prefix,
			&query.start,
			&query.end,
			query.limit,
			Some(filter),
			EnumerationOrder::from_reverse(query.reverse),
		)
		.await?;

		let items = items
			.into_iter()
			.map(ReadBatchResponseItem::from)
			.collect::<Vec<_>>();

		(items, more, next_start)
	};

	Ok(ReadBatchResponse {
		partition_key: query.partition_key,
		prefix: query.prefix,
		start: query.start,
		end: query.end,
		limit: query.limit,
		reverse: query.reverse,
		single_item: query.single_item,
		conflicts_only: query.conflicts_only,
		tombstones: query.tombstones,
		items,
		more,
		next_start,
	})
}

pub async fn handle_delete_batch(
	ctx: ReqCtx,
	req: Request<ReqBody>,
) -> Result<Response<ResBody>, Error> {
	let queries = req.into_body().json::<Vec<DeleteBatchQuery>>().await?;

	let resp_results = futures::future::join_all(
		queries
			.into_iter()
			.map(|q| handle_delete_batch_query(&ctx, q)),
	)
	.await;

	let mut resps: Vec<DeleteBatchResponse> = vec![];
	for resp in resp_results {
		resps.push(resp?);
	}

	Ok(json_ok_response(&resps)?)
}

async fn handle_delete_batch_query(
	ctx: &ReqCtx,
	query: DeleteBatchQuery,
) -> Result<DeleteBatchResponse, Error> {
	let ReqCtx {
		garage, bucket_id, ..
	} = &ctx;

	let partition = K2VItemPartition {
		bucket_id: *bucket_id,
		partition_key: query.partition_key.clone(),
	};

	let filter = ItemFilter {
		exclude_only_tombstones: true,
		conflicts_only: false,
	};

	let deleted_items = if query.single_item {
		if query.prefix.is_some() || query.end.is_some() {
			return Err(Error::bad_request("Batch query parameters 'prefix' and 'end' must not be set when singleItem is true."));
		}
		let sk = query
			.start
			.as_ref()
			.ok_or_bad_request("start should be specified if single_item is set")?;
		let item = garage
			.k2v
			.item_table
			.get(&partition, sk)
			.await?
			.filter(|e| K2VItemTable::matches_filter(e, &filter));
		match item {
			Some(i) => {
				let cc = i.causal_context();
				garage
					.k2v
					.rpc
					.insert(
						*bucket_id,
						i.partition.partition_key,
						i.sort_key,
						Some(cc),
						DvvsValue::Deleted,
					)
					.await?;
				1
			}
			None => 0,
		}
	} else {
		let (items, more, _next_start) = read_range(
			&garage.k2v.item_table,
			&partition,
			&query.prefix,
			&query.start,
			&query.end,
			None,
			Some(filter),
			EnumerationOrder::Forward,
		)
		.await?;
		assert!(!more);

		// TODO delete items
		let items = items
			.into_iter()
			.map(|i| {
				let cc = i.causal_context();
				(
					i.partition.partition_key,
					i.sort_key,
					Some(cc),
					DvvsValue::Deleted,
				)
			})
			.collect::<Vec<_>>();
		let n = items.len();

		garage.k2v.rpc.insert_batch(*bucket_id, items).await?;

		n
	};

	Ok(DeleteBatchResponse {
		partition_key: query.partition_key,
		prefix: query.prefix,
		start: query.start,
		end: query.end,
		single_item: query.single_item,
		deleted_items,
	})
}

pub(crate) async fn handle_poll_range(
	ctx: ReqCtx,
	partition_key: &str,
	req: Request<ReqBody>,
) -> Result<Response<ResBody>, Error> {
	let ReqCtx {
		garage, bucket_id, ..
	} = ctx;
	use garage_model::k2v::sub::PollRange;

	let query = req.into_body().json::<PollRangeQuery>().await?;

	let timeout_msec = query.timeout.unwrap_or(300).clamp(1, 600) * 1000;

	let resp = garage
		.k2v
		.rpc
		.poll_range(
			PollRange {
				partition: K2VItemPartition {
					bucket_id,
					partition_key: partition_key.to_string(),
				},
				start: query.start,
				end: query.end,
				prefix: query.prefix,
			},
			query.seen_marker,
			timeout_msec,
		)
		.await
		.map_err(pass_helper_error)?;

	if let Some((items, seen_marker)) = resp {
		let resp = PollRangeResponse {
			items: items
				.into_values()
				.map(ReadBatchResponseItem::from)
				.collect::<Vec<_>>(),
			seen_marker,
		};

		Ok(json_ok_response(&resp)?)
	} else {
		Ok(Response::builder()
			.status(StatusCode::NOT_MODIFIED)
			.body(empty_body())?)
	}
}

#[derive(Deserialize)]
struct InsertBatchItem {
	pk: String,
	sk: String,
	ct: Option<String>,
	v: Option<String>,
}

#[derive(Deserialize)]
struct ReadBatchQuery {
	#[serde(rename = "partitionKey")]
	partition_key: String,
	#[serde(default)]
	prefix: Option<String>,
	#[serde(default)]
	start: Option<String>,
	#[serde(default)]
	end: Option<String>,
	#[serde(default)]
	limit: Option<u64>,
	#[serde(default)]
	reverse: bool,
	#[serde(default, rename = "singleItem")]
	single_item: bool,
	#[serde(default, rename = "conflictsOnly")]
	conflicts_only: bool,
	#[serde(default)]
	tombstones: bool,
}

#[derive(Serialize)]
struct ReadBatchResponse {
	#[serde(rename = "partitionKey")]
	partition_key: String,
	prefix: Option<String>,
	start: Option<String>,
	end: Option<String>,
	limit: Option<u64>,
	reverse: bool,
	#[serde(rename = "singleItem")]
	single_item: bool,
	#[serde(rename = "conflictsOnly")]
	conflicts_only: bool,
	tombstones: bool,

	items: Vec<ReadBatchResponseItem>,
	more: bool,
	#[serde(rename = "nextStart")]
	next_start: Option<String>,
}

#[derive(Serialize)]
struct ReadBatchResponseItem {
	sk: String,
	ct: String,
	v: Vec<Option<String>>,
}

impl ReadBatchResponseItem {
	fn from(i: K2VItem) -> Self {
		let ct = i.causal_context().serialize();
		let v = i
			.values()
			.iter()
			.map(|v| match v {
				DvvsValue::Value(x) => Some(BASE64_STANDARD.encode(x)),
				DvvsValue::Deleted => None,
			})
			.collect::<Vec<_>>();
		Self {
			sk: i.sort_key,
			ct,
			v,
		}
	}
}

#[derive(Deserialize)]
struct DeleteBatchQuery {
	#[serde(rename = "partitionKey")]
	partition_key: String,
	#[serde(default)]
	prefix: Option<String>,
	#[serde(default)]
	start: Option<String>,
	#[serde(default)]
	end: Option<String>,
	#[serde(default, rename = "singleItem")]
	single_item: bool,
}

#[derive(Serialize)]
struct DeleteBatchResponse {
	#[serde(rename = "partitionKey")]
	partition_key: String,
	prefix: Option<String>,
	start: Option<String>,
	end: Option<String>,
	#[serde(rename = "singleItem")]
	single_item: bool,

	#[serde(rename = "deletedItems")]
	deleted_items: usize,
}

#[derive(Deserialize)]
struct PollRangeQuery {
	#[serde(default)]
	prefix: Option<String>,
	#[serde(default)]
	start: Option<String>,
	#[serde(default)]
	end: Option<String>,
	#[serde(default)]
	timeout: Option<u64>,
	#[serde(default, rename = "seenMarker")]
	seen_marker: Option<String>,
}

#[derive(Serialize)]
struct PollRangeResponse {
	items: Vec<ReadBatchResponseItem>,
	#[serde(rename = "seenMarker")]
	seen_marker: String,
}
