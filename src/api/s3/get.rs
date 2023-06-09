//! Function related to GET and HEAD requests
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use futures::future;
use futures::stream::{self, StreamExt};
use http::header::{
	ACCEPT_RANGES, CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, IF_MODIFIED_SINCE,
	IF_NONE_MATCH, LAST_MODIFIED, RANGE,
};
use hyper::{Body, Request, Response, StatusCode};
use tokio::sync::mpsc;

use garage_rpc::rpc_helper::{netapp::stream::ByteStream, OrderTag};
use garage_table::EmptyKey;
use garage_util::data::*;
use garage_util::error::OkOrMessage;

use garage_model::garage::Garage;
use garage_model::s3::object_table::*;
use garage_model::s3::version_table::*;

use crate::s3::error::*;

const X_AMZ_MP_PARTS_COUNT: &str = "x-amz-mp-parts-count";

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
		match version_data {
			ObjectVersionData::Inline(_, bytes) => {
				if pn != 1 {
					return Err(Error::InvalidPart);
				}
				Ok(object_headers(object_version, version_meta)
					.header(CONTENT_LENGTH, format!("{}", bytes.len()))
					.header(
						CONTENT_RANGE,
						format!("bytes 0-{}/{}", bytes.len() - 1, bytes.len()),
					)
					.header(X_AMZ_MP_PARTS_COUNT, "1")
					.status(StatusCode::PARTIAL_CONTENT)
					.body(Body::empty())?)
			}
			ObjectVersionData::FirstBlock(_, _) => {
				let version = garage
					.version_table
					.get(&object_version.uuid, &EmptyKey)
					.await?
					.ok_or(Error::NoSuchKey)?;

				let (part_offset, part_end) =
					calculate_part_bounds(&version, pn).ok_or(Error::InvalidPart)?;

				Ok(object_headers(object_version, version_meta)
					.header(CONTENT_LENGTH, format!("{}", part_end - part_offset))
					.header(
						CONTENT_RANGE,
						format!(
							"bytes {}-{}/{}",
							part_offset,
							part_end - 1,
							version_meta.size
						),
					)
					.header(X_AMZ_MP_PARTS_COUNT, format!("{}", version.n_parts()?))
					.status(StatusCode::PARTIAL_CONTENT)
					.body(Body::empty())?)
			}
			_ => unreachable!(),
		}
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

	match (part_number, parse_range_header(req, last_v_meta.size)?) {
		(Some(_), Some(_)) => {
			return Err(Error::bad_request(
				"Cannot specify both partNumber and Range header",
			));
		}
		(Some(pn), None) => {
			return handle_get_part(garage, last_v, last_v_data, last_v_meta, pn).await;
		}
		(None, Some(range)) => {
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
		(None, None) => (),
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
			let (tx, rx) = mpsc::channel(2);

			let order_stream = OrderTag::stream();
			let first_block_hash = *first_block_hash;
			let version_uuid = last_v.uuid;

			tokio::spawn(async move {
				match async {
					let garage2 = garage.clone();
					let version_fut = tokio::spawn(async move {
						garage2.version_table.get(&version_uuid, &EmptyKey).await
					});

					let stream_block_0 = garage
						.block_manager
						.rpc_get_block_streaming(&first_block_hash, Some(order_stream.order(0)))
						.await?;
					tx.send(stream_block_0)
						.await
						.ok_or_message("channel closed")?;

					let version = version_fut.await.unwrap()?.ok_or(Error::NoSuchKey)?;
					for (i, (_, vb)) in version.blocks.items().iter().enumerate().skip(1) {
						let stream_block_i = garage
							.block_manager
							.rpc_get_block_streaming(&vb.hash, Some(order_stream.order(i as u64)))
							.await?;
						tx.send(stream_block_i)
							.await
							.ok_or_message("channel closed")?;
					}

					Ok::<(), Error>(())
				}
				.await
				{
					Ok(()) => (),
					Err(e) => {
						let err = std::io::Error::new(
							std::io::ErrorKind::Other,
							format!("Error while getting object data: {}", e),
						);
						let _ = tx
							.send(Box::pin(stream::once(future::ready(Err(err)))))
							.await;
					}
				}
			});

			let body_stream = tokio_stream::wrappers::ReceiverStream::new(rx).flatten();

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
				Err(Error::internal_error(
					"Requested range not present in inline bytes when it should have been",
				))
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
	object_version: &ObjectVersion,
	version_data: &ObjectVersionData,
	version_meta: &ObjectVersionMeta,
	part_number: u64,
) -> Result<Response<Body>, Error> {
	let resp_builder =
		object_headers(object_version, version_meta).status(StatusCode::PARTIAL_CONTENT);

	match version_data {
		ObjectVersionData::Inline(_, bytes) => {
			if part_number != 1 {
				return Err(Error::InvalidPart);
			}
			Ok(resp_builder
				.header(CONTENT_LENGTH, format!("{}", bytes.len()))
				.header(
					CONTENT_RANGE,
					format!("bytes {}-{}/{}", 0, bytes.len() - 1, bytes.len()),
				)
				.header(X_AMZ_MP_PARTS_COUNT, "1")
				.body(Body::from(bytes.to_vec()))?)
		}
		ObjectVersionData::FirstBlock(_, _) => {
			let version = garage
				.version_table
				.get(&object_version.uuid, &EmptyKey)
				.await?
				.ok_or(Error::NoSuchKey)?;

			let (begin, end) =
				calculate_part_bounds(&version, part_number).ok_or(Error::InvalidPart)?;

			let body = body_from_blocks_range(garage, version.blocks.items(), begin, end);

			Ok(resp_builder
				.header(CONTENT_LENGTH, format!("{}", end - begin))
				.header(
					CONTENT_RANGE,
					format!("bytes {}-{}/{}", begin, end - 1, version_meta.size),
				)
				.header(X_AMZ_MP_PARTS_COUNT, format!("{}", version.n_parts()?))
				.body(body)?)
		}
		_ => unreachable!(),
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

fn calculate_part_bounds(v: &Version, part_number: u64) -> Option<(u64, u64)> {
	let mut offset = 0;
	for (i, (bk, bv)) in v.blocks.items().iter().enumerate() {
		if bk.part_number == part_number {
			let size: u64 = v.blocks.items()[i..]
				.iter()
				.take_while(|(k, _)| k.part_number == part_number)
				.map(|(_, v)| v.size)
				.sum();
			return Some((offset, offset + size));
		}
		offset += bv.size;
	}
	None
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
		4 + ((end - begin) / std::cmp::max(all_blocks[0].1.size, 1024)) as usize,
	));
	let mut block_offset: u64 = 0;
	for (_, b) in all_blocks.iter() {
		if block_offset >= end {
			break;
		}
		// Keep only blocks that have an intersection with the requested range
		if block_offset < end && block_offset + b.size > begin {
			blocks.push((*b, block_offset));
		}
		block_offset += b.size;
	}

	let order_stream = OrderTag::stream();
	let body_stream = futures::stream::iter(blocks)
		.enumerate()
		.map(move |(i, (block, block_offset))| {
			let garage = garage.clone();
			async move {
				garage
					.block_manager
					.rpc_get_block_streaming(&block.hash, Some(order_stream.order(i as u64)))
					.await
					.unwrap_or_else(|e| error_stream(i, e))
					.scan(block_offset, move |chunk_offset, chunk| {
						let r = match chunk {
							Ok(chunk_bytes) => {
								let chunk_len = chunk_bytes.len() as u64;
								let r = if *chunk_offset >= end {
									// The current chunk is after the part we want to read.
									// Returning None here will stop the scan, the rest of the
									// stream will be ignored
									None
								} else if *chunk_offset + chunk_len <= begin {
									// The current chunk is before the part we want to read.
									// We return a None that will be removed by the filter_map
									// below.
									Some(None)
								} else {
									// The chunk has an intersection with the requested range
									let start_in_chunk = if *chunk_offset > begin {
										0
									} else {
										begin - *chunk_offset
									};
									let end_in_chunk = if *chunk_offset + chunk_len < end {
										chunk_len
									} else {
										end - *chunk_offset
									};
									Some(Some(Ok(chunk_bytes
										.slice(start_in_chunk as usize..end_in_chunk as usize))))
								};
								*chunk_offset += chunk_bytes.len() as u64;
								r
							}
							Err(e) => Some(Some(Err(e))),
						};
						futures::future::ready(r)
					})
					.filter_map(futures::future::ready)
			}
		})
		.buffered(2)
		.flatten();

	hyper::body::Body::wrap_stream(body_stream)
}

fn error_stream(i: usize, e: garage_util::error::Error) -> ByteStream {
	Box::pin(futures::stream::once(async move {
		Err(std::io::Error::new(
			std::io::ErrorKind::Other,
			format!("Could not get block {}: {}", i, e),
		))
	}))
}
