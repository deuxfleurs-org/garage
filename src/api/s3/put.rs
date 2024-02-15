use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use base64::prelude::*;
use futures::prelude::*;
use futures::try_join;
use md5::{digest::generic_array::*, Digest as Md5Digest, Md5};
use sha2::Sha256;

use hyper::body::Bytes;
use hyper::header::{HeaderMap, HeaderValue};
use hyper::{Request, Response};

use opentelemetry::{
	trace::{FutureExt as OtelFutureExt, TraceContextExt, Tracer},
	Context,
};

use garage_net::bytes_buf::BytesBuf;
use garage_table::*;
use garage_util::async_hash::*;
use garage_util::data::*;
use garage_util::error::Error as GarageError;
use garage_util::time::*;

use garage_block::manager::INLINE_THRESHOLD;
use garage_model::bucket_table::Bucket;
use garage_model::garage::Garage;
use garage_model::index_counter::CountedItem;
use garage_model::s3::block_ref_table::*;
use garage_model::s3::object_table::*;
use garage_model::s3::version_table::*;

use crate::helpers::*;
use crate::s3::api_server::{ReqBody, ResBody};
use crate::s3::error::*;

pub async fn handle_put(
	garage: Arc<Garage>,
	req: Request<ReqBody>,
	bucket: &Bucket,
	key: &String,
	content_sha256: Option<Hash>,
) -> Result<Response<ResBody>, Error> {
	// Retrieve interesting headers from request
	let headers = get_headers(req.headers())?;
	debug!("Object headers: {:?}", headers);

	let content_md5 = match req.headers().get("content-md5") {
		Some(x) => Some(x.to_str()?.to_string()),
		None => None,
	};

	let stream = body_stream(req.into_body());

	save_stream(
		garage,
		headers,
		stream,
		bucket,
		key,
		content_md5,
		content_sha256,
	)
	.await
	.map(|(uuid, md5)| put_response(uuid, md5))
}

pub(crate) async fn save_stream<S: Stream<Item = Result<Bytes, Error>> + Unpin>(
	garage: Arc<Garage>,
	headers: ObjectVersionHeaders,
	body: S,
	bucket: &Bucket,
	key: &String,
	content_md5: Option<String>,
	content_sha256: Option<FixedBytes32>,
) -> Result<(Uuid, String), Error> {
	let mut chunker = StreamChunker::new(body, garage.config.block_size);
	let (first_block_opt, existing_object) = try_join!(
		chunker.next(),
		garage
			.object_table
			.get(&bucket.id, key)
			.map_err(Error::from),
	)?;

	let first_block = first_block_opt.unwrap_or_default();

	// Generate identity of new version
	let version_uuid = gen_uuid();
	let version_timestamp = next_timestamp(existing_object.as_ref());

	// If body is small enough, store it directly in the object table
	// as "inline data". We can then return immediately.
	if first_block.len() < INLINE_THRESHOLD {
		let mut md5sum = Md5::new();
		md5sum.update(&first_block[..]);
		let data_md5sum = md5sum.finalize();
		let data_md5sum_hex = hex::encode(data_md5sum);

		let data_sha256sum = sha256sum(&first_block[..]);
		let size = first_block.len() as u64;

		ensure_checksum_matches(
			data_md5sum.as_slice(),
			data_sha256sum,
			content_md5.as_deref(),
			content_sha256,
		)?;

		check_quotas(&garage, bucket, size, existing_object.as_ref()).await?;

		let object_version = ObjectVersion {
			uuid: version_uuid,
			timestamp: version_timestamp,
			state: ObjectVersionState::Complete(ObjectVersionData::Inline(
				ObjectVersionMeta {
					headers,
					size,
					etag: data_md5sum_hex.clone(),
				},
				first_block.to_vec(),
			)),
		};

		let object = Object::new(bucket.id, key.into(), vec![object_version]);
		garage.object_table.insert(&object).await?;

		return Ok((version_uuid, data_md5sum_hex));
	}

	// The following consists in many steps that can each fail.
	// Keep track that some cleanup will be needed if things fail
	// before everything is finished (cleanup is done using the Drop trait).
	let mut interrupted_cleanup = InterruptedCleanup(Some(InterruptedCleanupInner {
		garage: garage.clone(),
		bucket_id: bucket.id,
		key: key.into(),
		version_uuid,
		version_timestamp,
	}));

	// Write version identifier in object table so that we have a trace
	// that we are uploading something
	let mut object_version = ObjectVersion {
		uuid: version_uuid,
		timestamp: version_timestamp,
		state: ObjectVersionState::Uploading {
			headers: headers.clone(),
			multipart: false,
		},
	};
	let object = Object::new(bucket.id, key.into(), vec![object_version.clone()]);
	garage.object_table.insert(&object).await?;

	// Initialize corresponding entry in version table
	// Write this entry now, even with empty block list,
	// to prevent block_ref entries from being deleted (they can be deleted
	// if the reference a version that isn't found in the version table)
	let version = Version::new(
		version_uuid,
		VersionBacklink::Object {
			bucket_id: bucket.id,
			key: key.into(),
		},
		false,
	);
	garage.version_table.insert(&version).await?;

	// Transfer data and verify checksum
	let first_block_hash = async_blake2sum(first_block.clone()).await;

	let (total_size, data_md5sum, data_sha256sum) = read_and_put_blocks(
		&garage,
		&version,
		1,
		first_block,
		first_block_hash,
		&mut chunker,
	)
	.await?;

	ensure_checksum_matches(
		data_md5sum.as_slice(),
		data_sha256sum,
		content_md5.as_deref(),
		content_sha256,
	)?;

	check_quotas(&garage, bucket, total_size, existing_object.as_ref()).await?;

	// Save final object state, marked as Complete
	let md5sum_hex = hex::encode(data_md5sum);
	object_version.state = ObjectVersionState::Complete(ObjectVersionData::FirstBlock(
		ObjectVersionMeta {
			headers,
			size: total_size,
			etag: md5sum_hex.clone(),
		},
		first_block_hash,
	));
	let object = Object::new(bucket.id, key.into(), vec![object_version]);
	garage.object_table.insert(&object).await?;

	// We were not interrupted, everything went fine.
	// We won't have to clean up on drop.
	interrupted_cleanup.cancel();

	Ok((version_uuid, md5sum_hex))
}

/// Validate MD5 sum against content-md5 header
/// and sha256sum against signed content-sha256
pub(crate) fn ensure_checksum_matches(
	data_md5sum: &[u8],
	data_sha256sum: garage_util::data::FixedBytes32,
	content_md5: Option<&str>,
	content_sha256: Option<garage_util::data::FixedBytes32>,
) -> Result<(), Error> {
	if let Some(expected_sha256) = content_sha256 {
		if expected_sha256 != data_sha256sum {
			return Err(Error::bad_request(
				"Unable to validate x-amz-content-sha256",
			));
		} else {
			trace!("Successfully validated x-amz-content-sha256");
		}
	}
	if let Some(expected_md5) = content_md5 {
		if expected_md5.trim_matches('"') != BASE64_STANDARD.encode(data_md5sum) {
			return Err(Error::bad_request("Unable to validate content-md5"));
		} else {
			trace!("Successfully validated content-md5");
		}
	}
	Ok(())
}

/// Check that inserting this object with this size doesn't exceed bucket quotas
pub(crate) async fn check_quotas(
	garage: &Arc<Garage>,
	bucket: &Bucket,
	size: u64,
	prev_object: Option<&Object>,
) -> Result<(), Error> {
	let quotas = bucket.state.as_option().unwrap().quotas.get();
	if quotas.max_objects.is_none() && quotas.max_size.is_none() {
		return Ok(());
	};

	let counters = garage
		.object_counter_table
		.table
		.get(&bucket.id, &EmptyKey)
		.await?;

	let counters = counters
		.map(|x| x.filtered_values(&garage.system.ring.borrow()))
		.unwrap_or_default();

	let (prev_cnt_obj, prev_cnt_size) = match prev_object {
		Some(o) => {
			let prev_cnt = o.counts().into_iter().collect::<HashMap<_, _>>();
			(
				prev_cnt.get(OBJECTS).cloned().unwrap_or_default(),
				prev_cnt.get(BYTES).cloned().unwrap_or_default(),
			)
		}
		None => (0, 0),
	};
	let cnt_obj_diff = 1 - prev_cnt_obj;
	let cnt_size_diff = size as i64 - prev_cnt_size;

	if let Some(mo) = quotas.max_objects {
		let current_objects = counters.get(OBJECTS).cloned().unwrap_or_default();
		if cnt_obj_diff > 0 && current_objects + cnt_obj_diff > mo as i64 {
			return Err(Error::forbidden(format!(
				"Object quota is reached, maximum objects for this bucket: {}",
				mo
			)));
		}
	}

	if let Some(ms) = quotas.max_size {
		let current_size = counters.get(BYTES).cloned().unwrap_or_default();
		if cnt_size_diff > 0 && current_size + cnt_size_diff > ms as i64 {
			return Err(Error::forbidden(format!(
				"Bucket size quota is reached, maximum total size of objects for this bucket: {}. The bucket is already {} bytes, and this object would add {} bytes.",
				ms, current_size, cnt_size_diff
			)));
		}
	}

	Ok(())
}

pub(crate) async fn read_and_put_blocks<S: Stream<Item = Result<Bytes, Error>> + Unpin>(
	garage: &Garage,
	version: &Version,
	part_number: u64,
	first_block: Bytes,
	first_block_hash: Hash,
	chunker: &mut StreamChunker<S>,
) -> Result<(u64, GenericArray<u8, typenum::U16>, Hash), Error> {
	let tracer = opentelemetry::global::tracer("garage");

	let md5hasher = AsyncHasher::<Md5>::new();
	let sha256hasher = AsyncHasher::<Sha256>::new();

	futures::future::join(
		md5hasher.update(first_block.clone()),
		sha256hasher.update(first_block.clone()),
	)
	.with_context(Context::current_with_span(
		tracer.start("Hash first block (md5, sha256)"),
	))
	.await;

	let mut next_offset = first_block.len();
	let mut put_curr_version_block = put_block_meta(
		garage,
		version,
		part_number,
		0,
		first_block_hash,
		first_block.len() as u64,
	);
	let mut put_curr_block = garage
		.block_manager
		.rpc_put_block(first_block_hash, first_block);

	loop {
		let (_, _, next_block) = futures::try_join!(
			put_curr_block.map_err(Error::from),
			put_curr_version_block.map_err(Error::from),
			chunker.next(),
		)?;
		if let Some(block) = next_block {
			let (_, _, block_hash) = futures::future::join3(
				md5hasher.update(block.clone()),
				sha256hasher.update(block.clone()),
				async_blake2sum(block.clone()),
			)
			.with_context(Context::current_with_span(
				tracer.start("Hash block (md5, sha256, blake2)"),
			))
			.await;
			let block_len = block.len();
			put_curr_version_block = put_block_meta(
				garage,
				version,
				part_number,
				next_offset as u64,
				block_hash,
				block_len as u64,
			);
			put_curr_block = garage.block_manager.rpc_put_block(block_hash, block);
			next_offset += block_len;
		} else {
			break;
		}
	}

	let total_size = next_offset as u64;
	let data_md5sum = md5hasher.finalize().await;

	let data_sha256sum = sha256hasher.finalize().await;
	let data_sha256sum = Hash::try_from(&data_sha256sum[..]).unwrap();

	Ok((total_size, data_md5sum, data_sha256sum))
}

async fn put_block_meta(
	garage: &Garage,
	version: &Version,
	part_number: u64,
	offset: u64,
	hash: Hash,
	size: u64,
) -> Result<(), GarageError> {
	let mut version = version.clone();
	version.blocks.put(
		VersionBlockKey {
			part_number,
			offset,
		},
		VersionBlock { hash, size },
	);

	let block_ref = BlockRef {
		block: hash,
		version: version.uuid,
		deleted: false.into(),
	};

	futures::try_join!(
		garage.version_table.insert(&version),
		garage.block_ref_table.insert(&block_ref),
	)?;
	Ok(())
}

pub(crate) struct StreamChunker<S: Stream<Item = Result<Bytes, Error>>> {
	stream: S,
	read_all: bool,
	block_size: usize,
	buf: BytesBuf,
}

impl<S: Stream<Item = Result<Bytes, Error>> + Unpin> StreamChunker<S> {
	pub(crate) fn new(stream: S, block_size: usize) -> Self {
		Self {
			stream,
			read_all: false,
			block_size,
			buf: BytesBuf::new(),
		}
	}

	pub(crate) async fn next(&mut self) -> Result<Option<Bytes>, Error> {
		while !self.read_all && self.buf.len() < self.block_size {
			if let Some(block) = self.stream.next().await {
				let bytes = block?;
				trace!("Body next: {} bytes", bytes.len());
				self.buf.extend(bytes);
			} else {
				self.read_all = true;
			}
		}

		if self.buf.is_empty() {
			Ok(None)
		} else {
			Ok(Some(self.buf.take_max(self.block_size)))
		}
	}
}

pub fn put_response(version_uuid: Uuid, md5sum_hex: String) -> Response<ResBody> {
	Response::builder()
		.header("x-amz-version-id", hex::encode(version_uuid))
		.header("ETag", format!("\"{}\"", md5sum_hex))
		.body(empty_body())
		.unwrap()
}

struct InterruptedCleanup(Option<InterruptedCleanupInner>);
struct InterruptedCleanupInner {
	garage: Arc<Garage>,
	bucket_id: Uuid,
	key: String,
	version_uuid: Uuid,
	version_timestamp: u64,
}

impl InterruptedCleanup {
	fn cancel(&mut self) {
		drop(self.0.take());
	}
}
impl Drop for InterruptedCleanup {
	fn drop(&mut self) {
		if let Some(info) = self.0.take() {
			tokio::spawn(async move {
				let object_version = ObjectVersion {
					uuid: info.version_uuid,
					timestamp: info.version_timestamp,
					state: ObjectVersionState::Aborted,
				};
				let object = Object::new(info.bucket_id, info.key, vec![object_version]);
				if let Err(e) = info.garage.object_table.insert(&object).await {
					warn!("Cannot cleanup after aborted PutObject: {}", e);
				}
			});
		}
	}
}

// ============ helpers ============

pub(crate) fn get_mime_type(headers: &HeaderMap<HeaderValue>) -> Result<String, Error> {
	Ok(headers
		.get(hyper::header::CONTENT_TYPE)
		.map(|x| x.to_str())
		.unwrap_or(Ok("blob"))?
		.to_string())
}

pub(crate) fn get_headers(headers: &HeaderMap<HeaderValue>) -> Result<ObjectVersionHeaders, Error> {
	let content_type = get_mime_type(headers)?;
	let mut other = BTreeMap::new();

	// Preserve standard headers
	let standard_header = vec![
		hyper::header::CACHE_CONTROL,
		hyper::header::CONTENT_DISPOSITION,
		hyper::header::CONTENT_ENCODING,
		hyper::header::CONTENT_LANGUAGE,
		hyper::header::EXPIRES,
	];
	for h in standard_header.iter() {
		if let Some(v) = headers.get(h) {
			match v.to_str() {
				Ok(v_str) => {
					other.insert(h.to_string(), v_str.to_string());
				}
				Err(e) => {
					warn!("Discarding header {}, error in .to_str(): {}", h, e);
				}
			}
		}
	}

	// Preserve x-amz-meta- headers
	for (k, v) in headers.iter() {
		if k.as_str().starts_with("x-amz-meta-") {
			match v.to_str() {
				Ok(v_str) => {
					other.insert(k.to_string(), v_str.to_string());
				}
				Err(e) => {
					warn!("Discarding header {}, error in .to_str(): {}", k, e);
				}
			}
		}
	}

	Ok(ObjectVersionHeaders {
		content_type,
		other,
	})
}

pub(crate) fn next_timestamp(existing_object: Option<&Object>) -> u64 {
	existing_object
		.as_ref()
		.and_then(|obj| obj.versions().iter().map(|v| v.timestamp).max())
		.map(|t| std::cmp::max(t + 1, now_msec()))
		.unwrap_or_else(now_msec)
}
