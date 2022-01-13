//! Function related to GET and HEAD requests
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use futures::stream::*;
use http::header::{
	ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_MODIFIED_SINCE,
	IF_NONE_MATCH, LAST_MODIFIED, RANGE,
};
use hyper::body::Bytes;
use hyper::{Body, Request, Response, StatusCode};

use garage_table::EmptyKey;
use garage_util::data::*;

use garage_model::garage::Garage;
use garage_model::object_table::*;
use garage_model::version_table::*;

use crate::error::*;

fn object_headers(
	version: &ObjectVersion,
	version_meta: &ObjectVersionMeta,
) -> http::response::Builder {
	debug!("Version meta: {:?}", version_meta);

	let date = UNIX_EPOCH + Duration::from_millis(version.timestamp);
	let date_str = httpdate::fmt_http_date(date);

	let mut resp = Response::builder()
		.header(CONTENT_TYPE, version_meta.headers.content_type.to_string())
		.header(LAST_MODIFIED, date_str)
		.header(ACCEPT_RANGES, "bytes".to_string());

	if !version_meta.etag.is_empty() {
		resp = resp.header(ETAG, format!("\"{}\"", version_meta.etag));
	}

	for (k, v) in version_meta.headers.other.iter() {
		resp = resp.header(k, v.to_string());
	}

	resp
}

fn try_answer_cached(
	version: &ObjectVersion,
	version_meta: &ObjectVersionMeta,
	req: &Request<Body>,
) -> Option<Response<Body>> {
	// <trinity> It is possible, and is even usually the case, [that both If-None-Match and
	// If-Modified-Since] are present in a request. In this situation If-None-Match takes
	// precedence and If-Modified-Since is ignored (as per 6.Precedence from rfc7232). The rational
	// being that etag based matching is more accurate, it has no issue with sub-second precision
	// for instance (in case of very fast updates)
	let cached = if let Some(none_match) = req.headers().get(IF_NONE_MATCH) {
		let none_match = none_match.to_str().ok()?;
		let expected = format!("\"{}\"", version_meta.etag);
		let found = none_match
			.split(',')
			.map(str::trim)
			.any(|etag| etag == expected || etag == "\"*\"");
		found
	} else if let Some(modified_since) = req.headers().get(IF_MODIFIED_SINCE) {
		let modified_since = modified_since.to_str().ok()?;
		let client_date = httpdate::parse_http_date(modified_since).ok()?;
		let server_date = UNIX_EPOCH + Duration::from_millis(version.timestamp);
		client_date >= server_date
	} else {
		false
	};

	if cached {
		Some(
			Response::builder()
				.status(StatusCode::NOT_MODIFIED)
				.body(Body::empty())
				.unwrap(),
		)
	} else {
		None
	}
}

/// Handle HEAD request
pub async fn handle_head(
	garage: Arc<Garage>,
	req: &Request<Body>,
	bucket_id: Uuid,
	key: &str,
	part_number: Option<u64>,
) -> Result<Response<Body>, Error> {
	let object = garage
		.object_table
		.get(&bucket_id, &key.to_string())
		.await?
		.ok_or(Error::NoSuchKey)?;

	let object_version = object
		.versions()
		.iter()
		.rev()
		.find(|v| v.is_data())
		.ok_or(Error::NoSuchKey)?;

	let version_data = match &object_version.state {
		ObjectVersionState::Complete(c) => c,
		_ => unreachable!(),
	};

	let version_meta = match version_data {
		ObjectVersionData::Inline(meta, _) => meta,
		ObjectVersionData::FirstBlock(meta, _) => meta,
		_ => unreachable!(),
	};

	if let Some(cached) = try_answer_cached(object_version, version_meta, req) {
		return Ok(cached);
	}

	if let Some(pn) = part_number {
		if let ObjectVersionData::Inline(_, _) = version_data {
			// Not a multipart upload
			return Err(Error::BadRequest(
				"Cannot process part_number argument: not a multipart upload".into(),
			));
		}

		let version = garage
			.version_table
			.get(&object_version.uuid, &EmptyKey)
			.await?
			.ok_or(Error::NoSuchKey)?;
		if !version.has_part_number(pn) {
			return Err(Error::BadRequest(format!(
				"Part number {} does not exist",
				pn
			)));
		}

		let part_size: u64 = version
			.blocks
			.items()
			.iter()
			.filter(|(k, _)| k.part_number == pn)
			.map(|(_, b)| b.size)
			.sum();
		let n_parts = version.parts_etags.items().len();

		Ok(object_headers(object_version, version_meta)
			.header(CONTENT_LENGTH, format!("{}", part_size))
			.header("x-amz-mp-parts-count", format!("{}", n_parts))
			.status(StatusCode::OK)
			.body(Body::empty())?)
	} else {
		Ok(object_headers(object_version, version_meta)
			.header(CONTENT_LENGTH, format!("{}", version_meta.size))
			.status(StatusCode::OK)
			.body(Body::empty())?)
	}
}

/// Handle GET request
pub async fn handle_get(
	garage: Arc<Garage>,
	req: &Request<Body>,
	bucket_id: Uuid,
	key: &str,
	part_number: Option<u64>,
) -> Result<Response<Body>, Error> {
	let object = garage
		.object_table
		.get(&bucket_id, &key.to_string())
		.await?
		.ok_or(Error::NoSuchKey)?;

	let last_v = object
		.versions()
		.iter()
		.rev()
		.find(|v| v.is_complete())
		.ok_or(Error::NoSuchKey)?;

	let last_v_data = match &last_v.state {
		ObjectVersionState::Complete(x) => x,
		_ => unreachable!(),
	};
	let last_v_meta = match last_v_data {
		ObjectVersionData::DeleteMarker => return Err(Error::NoSuchKey),
		ObjectVersionData::Inline(meta, _) => meta,
		ObjectVersionData::FirstBlock(meta, _) => meta,
	};

	if let Some(cached) = try_answer_cached(last_v, last_v_meta, req) {
		return Ok(cached);
	}

	if let Some(pn) = part_number {
		return handle_get_part(garage, req, last_v, last_v_data, last_v_meta, pn).await;
	}

	// No part_number specified, it's a normal get object

	if let Some(range) = parse_range_header(req, last_v_meta.size)? {
		return handle_get_range(
			garage,
			last_v,
			last_v_data,
			last_v_meta,
			range.start,
			range.start + range.length,
		)
		.await;
	}

	let resp_builder = object_headers(last_v, last_v_meta)
		.header(CONTENT_LENGTH, format!("{}", last_v_meta.size))
		.status(StatusCode::OK);

	match &last_v_data {
		ObjectVersionData::DeleteMarker => unreachable!(),
		ObjectVersionData::Inline(_, bytes) => {
			let body: Body = Body::from(bytes.to_vec());
			Ok(resp_builder.body(body)?)
		}
		ObjectVersionData::FirstBlock(_, first_block_hash) => {
			let read_first_block = garage.block_manager.rpc_get_block(first_block_hash);
			let get_next_blocks = garage.version_table.get(&last_v.uuid, &EmptyKey);

			let (first_block, version) = futures::try_join!(read_first_block, get_next_blocks)?;
			let version = version.ok_or(Error::NoSuchKey)?;

			let mut blocks = version
				.blocks
				.items()
				.iter()
				.map(|(_, vb)| (vb.hash, None))
				.collect::<Vec<_>>();
			blocks[0].1 = Some(first_block);

			let body_stream = futures::stream::iter(blocks)
				.map(move |(hash, data_opt)| {
					let garage = garage.clone();
					async move {
						if let Some(data) = data_opt {
							Ok(Bytes::from(data))
						} else {
							garage
								.block_manager
								.rpc_get_block(&hash)
								.await
								.map(Bytes::from)
						}
					}
				})
				.buffered(2);

			let body = hyper::body::Body::wrap_stream(body_stream);
			Ok(resp_builder.body(body)?)
		}
	}
}

async fn handle_get_range(
	garage: Arc<Garage>,
	version: &ObjectVersion,
	version_data: &ObjectVersionData,
	version_meta: &ObjectVersionMeta,
	begin: u64,
	end: u64,
) -> Result<Response<Body>, Error> {
	let resp_builder = object_headers(version, version_meta)
		.header(CONTENT_LENGTH, format!("{}", end - begin))
		.header(
			CONTENT_RANGE,
			format!("bytes {}-{}/{}", begin, end - 1, version_meta.size),
		)
		.status(StatusCode::PARTIAL_CONTENT);

	match &version_data {
		ObjectVersionData::DeleteMarker => unreachable!(),
		ObjectVersionData::Inline(_meta, bytes) => {
			if end as usize <= bytes.len() {
				let body: Body = Body::from(bytes[begin as usize..end as usize].to_vec());
				Ok(resp_builder.body(body)?)
			} else {
				None.ok_or_internal_error(
					"Requested range not present in inline bytes when it should have been",
				)
			}
		}
		ObjectVersionData::FirstBlock(_meta, _first_block_hash) => {
			let version = garage
				.version_table
				.get(&version.uuid, &EmptyKey)
				.await?
				.ok_or(Error::NoSuchKey)?;

			let body = body_from_blocks_range(garage, version.blocks.items(), begin, end);
			Ok(resp_builder.body(body)?)
		}
	}
}

async fn handle_get_part(
	garage: Arc<Garage>,
	req: &Request<Body>,
	object_version: &ObjectVersion,
	version_data: &ObjectVersionData,
	version_meta: &ObjectVersionMeta,
	part_number: u64,
) -> Result<Response<Body>, Error> {
	let version = if let ObjectVersionData::FirstBlock(_, _) = version_data {
		garage
			.version_table
			.get(&object_version.uuid, &EmptyKey)
			.await?
			.ok_or(Error::NoSuchKey)?
	} else {
		return Err(Error::BadRequest(
			"Cannot handle part_number: not a multipart upload.".into(),
		));
	};

	let blocks = version
		.blocks
		.items()
		.iter()
		.filter(|(k, _)| k.part_number == part_number)
		.cloned()
		.collect::<Vec<_>>();

	if blocks.is_empty() {
		return Err(Error::BadRequest(format!("No such part: {}", part_number)));
	}

	let part_size = blocks.iter().map(|(_, b)| b.size).sum();

	if let Some(r) = parse_range_header(req, part_size)? {
		let range_begin = r.start;
		let range_end = r.start + r.length;
		let body = body_from_blocks_range(garage, &blocks[..], range_begin, range_end);

		Ok(object_headers(object_version, version_meta)
			.header(CONTENT_LENGTH, format!("{}", range_end - range_begin))
			.header(
				CONTENT_RANGE,
				format!("bytes {}-{}/{}", range_begin, range_end - 1, part_size),
			)
			.status(StatusCode::PARTIAL_CONTENT)
			.body(body)?)
	} else {
		let body = body_from_blocks_range(garage, &blocks[..], 0, part_size);

		Ok(object_headers(object_version, version_meta)
			.header(CONTENT_LENGTH, format!("{}", part_size))
			.status(StatusCode::OK)
			.body(body)?)
	}
}

fn parse_range_header(
	req: &Request<Body>,
	total_size: u64,
) -> Result<Option<http_range::HttpRange>, Error> {
	let range = match req.headers().get(RANGE) {
		Some(range) => {
			let range_str = range.to_str()?;
			let mut ranges =
				http_range::HttpRange::parse(range_str, total_size).map_err(|e| (e, total_size))?;
			if ranges.len() > 1 {
				// garage does not support multi-range requests yet, so we respond with the entire
				// object when multiple ranges are requested
				None
			} else {
				ranges.pop()
			}
		}
		None => None,
	};
	Ok(range)
}

fn body_from_blocks_range(
	garage: Arc<Garage>,
	all_blocks: &[(VersionBlockKey, VersionBlock)],
	begin: u64,
	end: u64,
) -> Body {
	// We will store here the list of blocks that have an intersection with the requested
	// range, as well as their "true offset", which is their actual offset in the complete
	// file (whereas block.offset designates the offset of the block WITHIN THE PART
	// block.part_number, which is not the same in the case of a multipart upload)
	let mut blocks: Vec<(VersionBlock, u64)> = Vec::with_capacity(std::cmp::min(
		all_blocks.len(),
		4 + ((end - begin) / std::cmp::max(all_blocks[0].1.size as u64, 1024)) as usize,
	));
	let mut true_offset = 0;
	for (_, b) in all_blocks.iter() {
		if true_offset >= end {
			break;
		}
		// Keep only blocks that have an intersection with the requested range
		if true_offset < end && true_offset + b.size > begin {
			blocks.push((*b, true_offset));
		}
		true_offset += b.size;
	}

	let body_stream = futures::stream::iter(blocks)
		.map(move |(block, true_offset)| {
			let garage = garage.clone();
			async move {
				let data = garage.block_manager.rpc_get_block(&block.hash).await?;
				let data = Bytes::from(data);
				let start_in_block = if true_offset > begin {
					0
				} else {
					begin - true_offset
				};
				let end_in_block = if true_offset + block.size < end {
					block.size
				} else {
					end - true_offset
				};
				Result::<Bytes, Error>::Ok(
					data.slice(start_in_block as usize..end_in_block as usize),
				)
			}
		})
		.buffered(2);

	hyper::body::Body::wrap_stream(body_stream)
}
