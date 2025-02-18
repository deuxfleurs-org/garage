//! Function related to GET and HEAD requests
use std::collections::BTreeMap;
use std::convert::TryInto;
use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use bytes::Bytes;
use futures::future;
use futures::stream::{self, Stream, StreamExt};
use http::header::{
	ACCEPT_RANGES, CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LANGUAGE,
	CONTENT_LENGTH, CONTENT_RANGE, CONTENT_TYPE, ETAG, EXPIRES, IF_MODIFIED_SINCE, IF_NONE_MATCH,
	LAST_MODIFIED, RANGE,
};
use hyper::{Request, Response, StatusCode};
use tokio::sync::mpsc;

use garage_net::stream::ByteStream;
use garage_rpc::rpc_helper::OrderTag;
use garage_table::EmptyKey;
use garage_util::data::*;
use garage_util::error::OkOrMessage;

use garage_model::garage::Garage;
use garage_model::s3::object_table::*;
use garage_model::s3::version_table::*;

use garage_api_common::helpers::*;
use garage_api_common::signature::checksum::{add_checksum_response_headers, X_AMZ_CHECKSUM_MODE};

use crate::api_server::ResBody;
use crate::encryption::EncryptionParams;
use crate::error::*;

const X_AMZ_MP_PARTS_COUNT: &str = "x-amz-mp-parts-count";

#[derive(Default)]
pub struct GetObjectOverrides {
	pub(crate) response_cache_control: Option<String>,
	pub(crate) response_content_disposition: Option<String>,
	pub(crate) response_content_encoding: Option<String>,
	pub(crate) response_content_language: Option<String>,
	pub(crate) response_content_type: Option<String>,
	pub(crate) response_expires: Option<String>,
}

fn object_headers(
	version: &ObjectVersion,
	version_meta: &ObjectVersionMeta,
	meta_inner: &ObjectVersionMetaInner,
	encryption: EncryptionParams,
	checksum_mode: ChecksumMode,
) -> http::response::Builder {
	debug!("Version meta: {:?}", version_meta);

	let date = UNIX_EPOCH + Duration::from_millis(version.timestamp);
	let date_str = httpdate::fmt_http_date(date);

	let mut resp = Response::builder()
		.header(LAST_MODIFIED, date_str)
		.header(ACCEPT_RANGES, "bytes".to_string());

	if !version_meta.etag.is_empty() {
		resp = resp.header(ETAG, format!("\"{}\"", version_meta.etag));
	}

	// When metadata is retrieved through the REST API, Amazon S3 combines headers that
	// have the same name (ignoring case) into a comma-delimited list.
	// See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingMetadata.html
	let mut headers_by_name = BTreeMap::new();
	for (name, value) in meta_inner.headers.iter() {
		let name_lower = name.to_ascii_lowercase();
		headers_by_name
			.entry(name_lower)
			.or_insert(vec![])
			.push(value.as_str());
	}

	for (name, values) in headers_by_name {
		resp = resp.header(name, values.join(","));
	}

	if checksum_mode.enabled {
		resp = add_checksum_response_headers(&meta_inner.checksum, resp);
	}

	encryption.add_response_headers(&mut resp);

	resp
}

/// Override headers according to specific query parameters, see
/// section "Overriding response header values through the request" in
/// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
fn getobject_override_headers(
	overrides: GetObjectOverrides,
	resp: &mut http::response::Builder,
) -> Result<(), Error> {
	// TODO: this only applies for signed requests, so when we support
	// anonymous access in the future we will have to do a permission check here
	let overrides = [
		(CACHE_CONTROL, overrides.response_cache_control),
		(CONTENT_DISPOSITION, overrides.response_content_disposition),
		(CONTENT_ENCODING, overrides.response_content_encoding),
		(CONTENT_LANGUAGE, overrides.response_content_language),
		(CONTENT_TYPE, overrides.response_content_type),
		(EXPIRES, overrides.response_expires),
	];
	for (hdr, val_opt) in overrides {
		if let Some(val) = val_opt {
			let val = val.try_into().ok_or_bad_request("invalid header value")?;
			resp.headers_mut().unwrap().insert(hdr, val);
		}
	}
	Ok(())
}

fn try_answer_cached(
	version: &ObjectVersion,
	version_meta: &ObjectVersionMeta,
	req: &Request<()>,
) -> Option<Response<ResBody>> {
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
				.body(empty_body())
				.unwrap(),
		)
	} else {
		None
	}
}

/// Handle HEAD request
pub async fn handle_head(
	ctx: ReqCtx,
	req: &Request<()>,
	key: &str,
	part_number: Option<u64>,
) -> Result<Response<ResBody>, Error> {
	handle_head_without_ctx(ctx.garage, req, ctx.bucket_id, key, part_number).await
}

/// Handle HEAD request for website
pub async fn handle_head_without_ctx(
	garage: Arc<Garage>,
	req: &Request<()>,
	bucket_id: Uuid,
	key: &str,
	part_number: Option<u64>,
) -> Result<Response<ResBody>, Error> {
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

	let (encryption, headers) =
		EncryptionParams::check_decrypt(&garage, req.headers(), &version_meta.encryption)?;

	let checksum_mode = checksum_mode(&req);

	if let Some(pn) = part_number {
		match version_data {
			ObjectVersionData::Inline(_, _) => {
				if pn != 1 {
					return Err(Error::InvalidPart);
				}
				let bytes_len = version_meta.size;
				Ok(object_headers(
					object_version,
					version_meta,
					&headers,
					encryption,
					checksum_mode,
				)
				.header(CONTENT_LENGTH, format!("{}", bytes_len))
				.header(
					CONTENT_RANGE,
					format!("bytes 0-{}/{}", bytes_len - 1, bytes_len),
				)
				.header(X_AMZ_MP_PARTS_COUNT, "1")
				.status(StatusCode::PARTIAL_CONTENT)
				.body(empty_body())?)
			}
			ObjectVersionData::FirstBlock(_, _) => {
				let version = garage
					.version_table
					.get(&object_version.uuid, &EmptyKey)
					.await?
					.ok_or(Error::NoSuchKey)?;

				let (part_offset, part_end) =
					calculate_part_bounds(&version, pn).ok_or(Error::InvalidPart)?;

				Ok(object_headers(
					object_version,
					version_meta,
					&headers,
					encryption,
					checksum_mode,
				)
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
				.body(empty_body())?)
			}
			_ => unreachable!(),
		}
	} else {
		Ok(object_headers(
			object_version,
			version_meta,
			&headers,
			encryption,
			checksum_mode,
		)
		.header(CONTENT_LENGTH, format!("{}", version_meta.size))
		.status(StatusCode::OK)
		.body(empty_body())?)
	}
}

/// Handle GET request
pub async fn handle_get(
	ctx: ReqCtx,
	req: &Request<()>,
	key: &str,
	part_number: Option<u64>,
	overrides: GetObjectOverrides,
) -> Result<Response<ResBody>, Error> {
	handle_get_without_ctx(ctx.garage, req, ctx.bucket_id, key, part_number, overrides).await
}

/// Handle GET request
pub async fn handle_get_without_ctx(
	garage: Arc<Garage>,
	req: &Request<()>,
	bucket_id: Uuid,
	key: &str,
	part_number: Option<u64>,
	overrides: GetObjectOverrides,
) -> Result<Response<ResBody>, Error> {
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

	let (enc, headers) =
		EncryptionParams::check_decrypt(&garage, req.headers(), &last_v_meta.encryption)?;

	let checksum_mode = checksum_mode(&req);

	match (part_number, parse_range_header(req, last_v_meta.size)?) {
		(Some(_), Some(_)) => Err(Error::bad_request(
			"Cannot specify both partNumber and Range header",
		)),
		(Some(pn), None) => {
			handle_get_part(
				garage,
				last_v,
				last_v_data,
				last_v_meta,
				enc,
				&headers,
				pn,
				ChecksumMode {
					// TODO: for multipart uploads, checksums of each part should be stored
					// so that we can return the corresponding checksum here
					// https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html
					enabled: false,
				},
			)
			.await
		}
		(None, Some(range)) => {
			handle_get_range(
				garage,
				last_v,
				last_v_data,
				last_v_meta,
				enc,
				&headers,
				range.start,
				range.start + range.length,
				ChecksumMode {
					// TODO: for range queries that align with part boundaries,
					// we should return the saved checksum of the part
					// https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html
					enabled: false,
				},
			)
			.await
		}
		(None, None) => {
			handle_get_full(
				garage,
				last_v,
				last_v_data,
				last_v_meta,
				enc,
				&headers,
				overrides,
				checksum_mode,
			)
			.await
		}
	}
}

async fn handle_get_full(
	garage: Arc<Garage>,
	version: &ObjectVersion,
	version_data: &ObjectVersionData,
	version_meta: &ObjectVersionMeta,
	encryption: EncryptionParams,
	meta_inner: &ObjectVersionMetaInner,
	overrides: GetObjectOverrides,
	checksum_mode: ChecksumMode,
) -> Result<Response<ResBody>, Error> {
	let mut resp_builder = object_headers(
		version,
		version_meta,
		&meta_inner,
		encryption,
		checksum_mode,
	)
	.header(CONTENT_LENGTH, format!("{}", version_meta.size))
	.status(StatusCode::OK);
	getobject_override_headers(overrides, &mut resp_builder)?;

	let stream = full_object_byte_stream(garage, version, version_data, encryption);

	Ok(resp_builder.body(response_body_from_stream(stream))?)
}

pub fn full_object_byte_stream(
	garage: Arc<Garage>,
	version: &ObjectVersion,
	version_data: &ObjectVersionData,
	encryption: EncryptionParams,
) -> ByteStream {
	match &version_data {
		ObjectVersionData::DeleteMarker => unreachable!(),
		ObjectVersionData::Inline(_, bytes) => {
			let bytes = bytes.to_vec();
			Box::pin(futures::stream::once(async move {
				encryption
					.decrypt_blob(&bytes)
					.map(|x| Bytes::from(x.to_vec()))
					.map_err(std_error_from_read_error)
			}))
		}
		ObjectVersionData::FirstBlock(_, first_block_hash) => {
			let (tx, rx) = mpsc::channel::<ByteStream>(2);

			let order_stream = OrderTag::stream();
			let first_block_hash = *first_block_hash;
			let version_uuid = version.uuid;

			tokio::spawn(async move {
				match async {
					let garage2 = garage.clone();
					let version_fut = tokio::spawn(async move {
						garage2.version_table.get(&version_uuid, &EmptyKey).await
					});

					let stream_block_0 = encryption
						.get_block(&garage, &first_block_hash, Some(order_stream.order(0)))
						.await?;

					tx.send(stream_block_0)
						.await
						.ok_or_message("channel closed")?;

					let version = version_fut.await.unwrap()?.ok_or(Error::NoSuchKey)?;
					for (i, (_, vb)) in version.blocks.items().iter().enumerate().skip(1) {
						let stream_block_i = encryption
							.get_block(&garage, &vb.hash, Some(order_stream.order(i as u64)))
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
						let _ = tx.send(error_stream_item(e)).await;
					}
				}
			});

			Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx).flatten())
		}
	}
}

async fn handle_get_range(
	garage: Arc<Garage>,
	version: &ObjectVersion,
	version_data: &ObjectVersionData,
	version_meta: &ObjectVersionMeta,
	encryption: EncryptionParams,
	meta_inner: &ObjectVersionMetaInner,
	begin: u64,
	end: u64,
	checksum_mode: ChecksumMode,
) -> Result<Response<ResBody>, Error> {
	// Here we do not use getobject_override_headers because we don't
	// want to add any overridden headers (those should not be added
	// when returning PARTIAL_CONTENT)
	let resp_builder = object_headers(version, version_meta, meta_inner, encryption, checksum_mode)
		.header(CONTENT_LENGTH, format!("{}", end - begin))
		.header(
			CONTENT_RANGE,
			format!("bytes {}-{}/{}", begin, end - 1, version_meta.size),
		)
		.status(StatusCode::PARTIAL_CONTENT);

	match &version_data {
		ObjectVersionData::DeleteMarker => unreachable!(),
		ObjectVersionData::Inline(_meta, bytes) => {
			let bytes = encryption.decrypt_blob(&bytes)?;
			if end as usize <= bytes.len() {
				let body = bytes_body(bytes[begin as usize..end as usize].to_vec().into());
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

			let body =
				body_from_blocks_range(garage, encryption, version.blocks.items(), begin, end);
			Ok(resp_builder.body(body)?)
		}
	}
}

async fn handle_get_part(
	garage: Arc<Garage>,
	object_version: &ObjectVersion,
	version_data: &ObjectVersionData,
	version_meta: &ObjectVersionMeta,
	encryption: EncryptionParams,
	meta_inner: &ObjectVersionMetaInner,
	part_number: u64,
	checksum_mode: ChecksumMode,
) -> Result<Response<ResBody>, Error> {
	// Same as for get_range, no getobject_override_headers
	let resp_builder = object_headers(
		object_version,
		version_meta,
		meta_inner,
		encryption,
		checksum_mode,
	)
	.status(StatusCode::PARTIAL_CONTENT);

	match version_data {
		ObjectVersionData::Inline(_, bytes) => {
			if part_number != 1 {
				return Err(Error::InvalidPart);
			}
			let bytes = encryption.decrypt_blob(&bytes)?;
			assert_eq!(bytes.len() as u64, version_meta.size);
			Ok(resp_builder
				.header(CONTENT_LENGTH, format!("{}", bytes.len()))
				.header(
					CONTENT_RANGE,
					format!("bytes {}-{}/{}", 0, bytes.len() - 1, bytes.len()),
				)
				.header(X_AMZ_MP_PARTS_COUNT, "1")
				.body(bytes_body(bytes.into_owned().into()))?)
		}
		ObjectVersionData::FirstBlock(_, _) => {
			let version = garage
				.version_table
				.get(&object_version.uuid, &EmptyKey)
				.await?
				.ok_or(Error::NoSuchKey)?;

			let (begin, end) =
				calculate_part_bounds(&version, part_number).ok_or(Error::InvalidPart)?;

			let body =
				body_from_blocks_range(garage, encryption, version.blocks.items(), begin, end);

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
	req: &Request<()>,
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

struct ChecksumMode {
	enabled: bool,
}

fn checksum_mode(req: &Request<()>) -> ChecksumMode {
	ChecksumMode {
		enabled: req
			.headers()
			.get(X_AMZ_CHECKSUM_MODE)
			.map(|x| x == "ENABLED")
			.unwrap_or(false),
	}
}

fn body_from_blocks_range(
	garage: Arc<Garage>,
	encryption: EncryptionParams,
	all_blocks: &[(VersionBlockKey, VersionBlock)],
	begin: u64,
	end: u64,
) -> ResBody {
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
	let (tx, rx) = mpsc::channel::<ByteStream>(2);

	tokio::spawn(async move {
		match async {
			for (i, (block, block_offset)) in blocks.iter().enumerate() {
				let block_stream = encryption
					.get_block(&garage, &block.hash, Some(order_stream.order(i as u64)))
					.await?;
				let block_stream = block_stream
					.scan(*block_offset, move |chunk_offset, chunk| {
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
					.filter_map(futures::future::ready);

				let block_stream: ByteStream = Box::pin(block_stream);
				tx.send(Box::pin(block_stream))
					.await
					.ok_or_message("channel closed")?;
			}

			Ok::<(), Error>(())
		}
		.await
		{
			Ok(()) => (),
			Err(e) => {
				let _ = tx.send(error_stream_item(e)).await;
			}
		}
	});

	response_body_from_block_stream(rx)
}

fn response_body_from_block_stream(rx: mpsc::Receiver<ByteStream>) -> ResBody {
	let body_stream = tokio_stream::wrappers::ReceiverStream::new(rx).flatten();
	response_body_from_stream(body_stream)
}

fn response_body_from_stream<S>(stream: S) -> ResBody
where
	S: Stream<Item = Result<Bytes, std::io::Error>> + Send + Sync + 'static,
{
	let body_stream = stream.map(|x| {
		x.map(hyper::body::Frame::data)
			.map_err(|e| Error::from(garage_util::error::Error::from(e)))
	});
	ResBody::new(http_body_util::StreamBody::new(body_stream))
}

fn error_stream_item<E: std::fmt::Display>(e: E) -> ByteStream {
	Box::pin(stream::once(future::ready(Err(std_error_from_read_error(
		e,
	)))))
}

fn std_error_from_read_error<E: std::fmt::Display>(e: E) -> std::io::Error {
	std::io::Error::new(
		std::io::ErrorKind::Other,
		format!("Error while reading object data: {}", e),
	)
}
