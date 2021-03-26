//! Function related to GET and HEAD requests
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use futures::stream::*;
use hyper::body::Bytes;
use hyper::{Body, Request, Response, StatusCode};

use garage_table::EmptyKey;

use garage_model::garage::Garage;
use garage_model::object_table::*;

use crate::error::*;

fn object_headers(
	version: &ObjectVersion,
	version_meta: &ObjectVersionMeta,
) -> http::response::Builder {
	debug!("Version meta: {:?}", version_meta);

	let date = UNIX_EPOCH + Duration::from_millis(version.timestamp);
	let date_str = httpdate::fmt_http_date(date);

	let mut resp = Response::builder()
		.header(
			"Content-Type",
			version_meta.headers.content_type.to_string(),
		)
		.header("Last-Modified", date_str)
		.header("Accept-Ranges", format!("bytes"));

	if !version_meta.etag.is_empty() {
		resp = resp.header("ETag", format!("\"{}\"", version_meta.etag));
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
	let cached = if let Some(none_match) = req.headers().get(http::header::IF_NONE_MATCH) {
		let none_match = none_match.to_str().ok()?;
		let expected = format!("\"{}\"", version_meta.etag);
		let found = none_match
			.split(',')
			.map(str::trim)
			.any(|etag| etag == expected || etag == "\"*\"");
		found
	} else if let Some(modified_since) = req.headers().get(http::header::IF_MODIFIED_SINCE) {
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
	bucket: &str,
	key: &str,
) -> Result<Response<Body>, Error> {
	let object = garage
		.object_table
		.get(&bucket.to_string(), &key.to_string())
		.await?
		.ok_or(Error::NotFound)?;

	let version = object
		.versions()
		.iter()
		.rev()
		.filter(|v| v.is_data())
		.next()
		.ok_or(Error::NotFound)?;

	let version_meta = match &version.state {
		ObjectVersionState::Complete(ObjectVersionData::Inline(meta, _)) => meta,
		ObjectVersionState::Complete(ObjectVersionData::FirstBlock(meta, _)) => meta,
		_ => unreachable!(),
	};

	if let Some(cached) = try_answer_cached(&version, version_meta, req) {
		return Ok(cached);
	}

	let body: Body = Body::empty();
	let response = object_headers(&version, version_meta)
		.header("Content-Length", format!("{}", version_meta.size))
		.status(StatusCode::OK)
		.body(body)
		.unwrap();
	Ok(response)
}

/// Handle GET request
pub async fn handle_get(
	garage: Arc<Garage>,
	req: &Request<Body>,
	bucket: &str,
	key: &str,
) -> Result<Response<Body>, Error> {
	let object = garage
		.object_table
		.get(&bucket.to_string(), &key.to_string())
		.await?
		.ok_or(Error::NotFound)?;

	let last_v = object
		.versions()
		.iter()
		.rev()
		.filter(|v| v.is_complete())
		.next()
		.ok_or(Error::NotFound)?;

	let last_v_data = match &last_v.state {
		ObjectVersionState::Complete(x) => x,
		_ => unreachable!(),
	};
	let last_v_meta = match last_v_data {
		ObjectVersionData::DeleteMarker => return Err(Error::NotFound),
		ObjectVersionData::Inline(meta, _) => meta,
		ObjectVersionData::FirstBlock(meta, _) => meta,
	};

	if let Some(cached) = try_answer_cached(&last_v, last_v_meta, req) {
		return Ok(cached);
	}

	let range = match req.headers().get("range") {
		Some(range) => {
			let range_str = range.to_str()?;
			let mut ranges = http_range::HttpRange::parse(range_str, last_v_meta.size)?;
			if ranges.len() > 1 {
				return Err(Error::BadRequest(format!("Multiple ranges not supported")));
			} else {
				ranges.pop()
			}
		}
		None => None,
	};
	if let Some(range) = range {
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

	let resp_builder = object_headers(&last_v, last_v_meta)
		.header("Content-Length", format!("{}", last_v_meta.size))
		.status(StatusCode::OK);

	match &last_v_data {
		ObjectVersionData::DeleteMarker => unreachable!(),
		ObjectVersionData::Inline(_, bytes) => {
			let body: Body = Body::from(bytes.to_vec());
			Ok(resp_builder.body(body)?)
		}
		ObjectVersionData::FirstBlock(_, first_block_hash) => {
			let read_first_block = garage.block_manager.rpc_get_block(&first_block_hash);
			let get_next_blocks = garage.version_table.get(&last_v.uuid, &EmptyKey);

			let (first_block, version) = futures::try_join!(read_first_block, get_next_blocks)?;
			let version = version.ok_or(Error::NotFound)?;

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
	if end > version_meta.size {
		return Err(Error::BadRequest(format!("Range not included in file")));
	}

	let resp_builder = object_headers(version, version_meta)
		.header("Content-Length", format!("{}", end - begin))
		.header(
			"Content-Range",
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
			let version = garage.version_table.get(&version.uuid, &EmptyKey).await?;
			let version = match version {
				Some(v) => v,
				None => return Err(Error::NotFound),
			};

			// We will store here the list of blocks that have an intersection with the requested
			// range, as well as their "true offset", which is their actual offset in the complete
			// file (whereas block.offset designates the offset of the block WITHIN THE PART
			// block.part_number, which is not the same in the case of a multipart upload)
			let mut blocks = Vec::with_capacity(std::cmp::min(
				version.blocks.len(),
				4 + ((end - begin) / std::cmp::max(version.blocks.items()[0].1.size as u64, 1024))
					as usize,
			));
			let mut true_offset = 0;
			for (_, b) in version.blocks.items().iter() {
				if true_offset >= end {
					break;
				}
				// Keep only blocks that have an intersection with the requested range
				if true_offset < end && true_offset + b.size > begin {
					blocks.push((b.clone(), true_offset));
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
						Result::<Bytes, Error>::Ok(Bytes::from(
							data.slice(start_in_block as usize..end_in_block as usize),
						))
					}
				})
				.buffered(2);

			let body = hyper::body::Body::wrap_stream(body_stream);
			Ok(resp_builder.body(body)?)
		}
	}
}
