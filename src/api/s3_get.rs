use std::sync::Arc;
use std::time::{Duration, UNIX_EPOCH};

use futures::stream::*;
use hyper::body::Bytes;
use hyper::{Response, StatusCode};

use garage_util::error::Error;

use garage_table::EmptyKey;

use garage_core::garage::Garage;
use garage_core::object_table::*;

use crate::api_server::BodyType;
use crate::http_util::*;

fn object_headers(version: &ObjectVersion) -> http::response::Builder {
	let date = UNIX_EPOCH + Duration::from_millis(version.timestamp);
	let date_str = httpdate::fmt_http_date(date);

	Response::builder()
		.header("Content-Type", version.mime_type.to_string())
		.header("Content-Length", format!("{}", version.size))
		.header("Last-Modified", date_str)
}

pub async fn handle_head(
	garage: Arc<Garage>,
	bucket: &str,
	key: &str,
) -> Result<Response<BodyType>, Error> {
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
		.filter(|v| v.is_complete && v.data != ObjectVersionData::DeleteMarker)
		.next()
	{
		Some(v) => v,
		None => return Err(Error::NotFound),
	};

	let body: BodyType = Box::new(BytesBody::from(vec![]));
	let response = object_headers(&version)
		.status(StatusCode::OK)
		.body(body)
		.unwrap();
	Ok(response)
}

pub async fn handle_get(
	garage: Arc<Garage>,
	bucket: &str,
	key: &str,
) -> Result<Response<BodyType>, Error> {
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
		.filter(|v| v.is_complete)
		.next()
	{
		Some(v) => v,
		None => return Err(Error::NotFound),
	};

	let resp_builder = object_headers(&last_v).status(StatusCode::OK);

	match &last_v.data {
		ObjectVersionData::DeleteMarker => Err(Error::NotFound),
		ObjectVersionData::Inline(bytes) => {
			let body: BodyType = Box::new(BytesBody::from(bytes.to_vec()));
			Ok(resp_builder.body(body)?)
		}
		ObjectVersionData::FirstBlock(first_block_hash) => {
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
			let body: BodyType = Box::new(StreamBody::new(Box::pin(body_stream)));
			Ok(resp_builder.body(body)?)
		}
	}
}
