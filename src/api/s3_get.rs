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
	let date = UNIX_EPOCH + Duration::from_millis(version.timestamp);
	let date_str = httpdate::fmt_http_date(date);

	let mut resp = Response::builder()
		.header(
			"Content-Type",
			version_meta.headers.content_type.to_string(),
		)
		.header("Content-Length", format!("{}", version_meta.size))
		.header("ETag", version_meta.etag.to_string())
		.header("Last-Modified", date_str)
		.header("Accept-Ranges", format!("bytes"));

	for (k, v) in version_meta.headers.other.iter() {
		resp = resp.header(k, v.to_string());
	}

	resp
}

pub async fn handle_head(
	garage: Arc<Garage>,
	bucket: &str,
	key: &str,
) -> Result<Response<Body>, Error> {
	let object = match garage
		.object_table
		.get(&bucket.to_string(), &key.to_string())
		.await?
	{
		None => return Err(Error::NotFound),
		Some(o) => o,
	};

	let version = match object
		.versions()
		.iter()
		.rev()
		.filter(|v| v.is_data())
		.next()
	{
		Some(v) => v,
		None => return Err(Error::NotFound),
	};
	let version_meta = match &version.state {
		ObjectVersionState::Complete(ObjectVersionData::Inline(meta, _)) => meta,
		ObjectVersionState::Complete(ObjectVersionData::FirstBlock(meta, _)) => meta,
		_ => unreachable!(),
	};

	let body: Body = Body::from(vec![]);
	let response = object_headers(&version, version_meta)
		.status(StatusCode::OK)
		.body(body)
		.unwrap();
	Ok(response)
}

pub async fn handle_get(
	garage: Arc<Garage>,
	req: &Request<Body>,
	bucket: &str,
	key: &str,
) -> Result<Response<Body>, Error> {
	let object = match garage
		.object_table
		.get(&bucket.to_string(), &key.to_string())
		.await?
	{
		None => return Err(Error::NotFound),
		Some(o) => o,
	};

	let last_v = match object
		.versions()
		.iter()
		.rev()
		.filter(|v| v.is_complete())
		.next()
	{
		Some(v) => v,
		None => return Err(Error::NotFound),
	};
	let last_v_data = match &last_v.state {
		ObjectVersionState::Complete(x) => x,
		_ => unreachable!(),
	};
	let last_v_meta = match last_v_data {
		ObjectVersionData::DeleteMarker => return Err(Error::NotFound),
		ObjectVersionData::Inline(meta, _) => meta,
		ObjectVersionData::FirstBlock(meta, _) => meta,
	};

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

	let resp_builder = object_headers(&last_v, last_v_meta).status(StatusCode::OK);

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
			let version = match version {
				Some(v) => v,
				None => return Err(Error::NotFound),
			};

			let mut blocks = version
				.blocks()
				.iter()
				.map(|vb| (vb.hash, None))
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
			//let body: Body = Box::new(StreamBody::new(Box::pin(body_stream)));
			let body = hyper::body::Body::wrap_stream(body_stream);
			Ok(resp_builder.body(body)?)
		}
	}
}

pub async fn handle_get_range(
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
		.header(
			"Content-Range",
			format!("bytes {}-{}/{}", begin, end, version_meta.size),
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

			let blocks = version
				.blocks()
				.iter()
				.cloned()
				.filter(|block| block.offset + block.size > begin && block.offset < end)
				.collect::<Vec<_>>();

			let body_stream = futures::stream::iter(blocks)
				.map(move |block| {
					let garage = garage.clone();
					async move {
						let data = garage.block_manager.rpc_get_block(&block.hash).await?;
						let start_in_block = if block.offset > begin {
							0
						} else {
							begin - block.offset
						};
						let end_in_block = if block.offset + block.size < end {
							block.size
						} else {
							end - block.offset
						};
						Result::<Bytes, Error>::Ok(Bytes::from(
							data[start_in_block as usize..end_in_block as usize].to_vec(),
						))
					}
				})
				.buffered(2);
			//let body: Body = Box::new(StreamBody::new(Box::pin(body_stream)));
			let body = hyper::body::Body::wrap_stream(body_stream);
			Ok(resp_builder.body(body)?)
		}
	}
}
