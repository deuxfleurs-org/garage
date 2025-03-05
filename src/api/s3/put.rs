use std::collections::HashMap;
use std::sync::Arc;

use futures::prelude::*;
use futures::stream::FuturesOrdered;
use futures::try_join;

use tokio::sync::mpsc;

use hyper::body::Bytes;
use hyper::header::{HeaderMap, HeaderValue};
use hyper::{Request, Response};

use opentelemetry::{
	trace::{FutureExt as OtelFutureExt, TraceContextExt, Tracer},
	Context,
};

use garage_net::bytes_buf::BytesBuf;
use garage_rpc::rpc_helper::OrderTag;
use garage_table::*;
use garage_util::data::*;
use garage_util::error::Error as GarageError;
use garage_util::time::*;

use garage_block::manager::INLINE_THRESHOLD;
use garage_model::garage::Garage;
use garage_model::index_counter::CountedItem;
use garage_model::s3::block_ref_table::*;
use garage_model::s3::object_table::*;
use garage_model::s3::version_table::*;

use garage_api_common::helpers::*;
use garage_api_common::signature::body::StreamingChecksumReceiver;
use garage_api_common::signature::checksum::*;

use crate::api_server::{ReqBody, ResBody};
use crate::encryption::EncryptionParams;
use crate::error::*;
use crate::website::X_AMZ_WEBSITE_REDIRECT_LOCATION;

const PUT_BLOCKS_MAX_PARALLEL: usize = 3;

pub(crate) struct SaveStreamResult {
	pub(crate) version_uuid: Uuid,
	pub(crate) version_timestamp: u64,
	/// Etag WITHOUT THE QUOTES (just the hex value)
	pub(crate) etag: String,
}

pub(crate) enum ChecksumMode<'a> {
	Verify(&'a ExpectedChecksums),
	VerifyFrom {
		checksummer: StreamingChecksumReceiver,
		trailer_algo: Option<ChecksumAlgorithm>,
	},
	Calculate(Option<ChecksumAlgorithm>),
}

pub async fn handle_put(
	ctx: ReqCtx,
	req: Request<ReqBody>,
	key: &String,
) -> Result<Response<ResBody>, Error> {
	// Retrieve interesting headers from request
	let headers = extract_metadata_headers(req.headers())?;
	debug!("Object headers: {:?}", headers);

	let expected_checksums = ExpectedChecksums {
		md5: match req.headers().get("content-md5") {
			Some(x) => Some(x.to_str()?.to_string()),
			None => None,
		},
		sha256: None,
		extra: request_checksum_value(req.headers())?,
	};
	let trailer_checksum_algorithm = request_trailer_checksum_algorithm(req.headers())?;

	let meta = ObjectVersionMetaInner {
		headers,
		checksum: expected_checksums.extra,
	};

	// Determine whether object should be encrypted, and if so the key
	let encryption = EncryptionParams::new_from_headers(&ctx.garage, req.headers())?;

	// The request body is a special ReqBody object (see garage_api_common::signature::body)
	// which supports calculating checksums while streaming the data.
	// Before we start streaming, we configure it to calculate all the checksums we need.
	let mut req_body = req.into_body();
	req_body.add_expected_checksums(expected_checksums.clone());
	if !encryption.is_encrypted() {
		// For non-encrypted objects, we need to compute the md5sum in all cases
		// (even if content-md5 is not set), because it is used as the object etag
		req_body.add_md5();
	}

	let (stream, checksummer) = req_body.streaming_with_checksums();
	let stream = stream.map_err(Error::from);

	let res = save_stream(
		&ctx,
		meta,
		encryption,
		stream,
		key,
		ChecksumMode::VerifyFrom {
			checksummer,
			trailer_algo: trailer_checksum_algorithm,
		},
	)
	.await?;

	let mut resp = Response::builder()
		.header("x-amz-version-id", hex::encode(res.version_uuid))
		.header("ETag", format!("\"{}\"", res.etag));
	encryption.add_response_headers(&mut resp);
	let resp = add_checksum_response_headers(&expected_checksums.extra, resp);
	Ok(resp.body(empty_body())?)
}

pub(crate) async fn save_stream<S: Stream<Item = Result<Bytes, Error>> + Unpin>(
	ctx: &ReqCtx,
	mut meta: ObjectVersionMetaInner,
	encryption: EncryptionParams,
	body: S,
	key: &String,
	checksum_mode: ChecksumMode<'_>,
) -> Result<SaveStreamResult, Error> {
	let ReqCtx {
		garage, bucket_id, ..
	} = ctx;

	let mut chunker = StreamChunker::new(body, garage.config.block_size);
	let (first_block_opt, existing_object) = try_join!(
		chunker.next(),
		garage.object_table.get(bucket_id, key).map_err(Error::from),
	)?;

	let first_block = first_block_opt.unwrap_or_default();

	// Generate identity of new version
	let version_uuid = gen_uuid();
	let version_timestamp = next_timestamp(existing_object.as_ref());

	let mut checksummer = match &checksum_mode {
		ChecksumMode::Verify(expected) => Checksummer::init(expected, !encryption.is_encrypted()),
		ChecksumMode::Calculate(algo) => {
			Checksummer::init(&Default::default(), !encryption.is_encrypted()).add(*algo)
		}
		ChecksumMode::VerifyFrom { .. } => {
			// Checksums are calculated by the garage_api_common::signature module
			// so here we can just have an empty checksummer that does nothing
			Checksummer::new()
		}
	};

	// If body is small enough, store it directly in the object table
	// as "inline data". We can then return immediately.
	if first_block.len() < INLINE_THRESHOLD {
		checksummer.update(&first_block);
		let mut checksums = checksummer.finalize();

		match checksum_mode {
			ChecksumMode::Verify(expected) => {
				checksums.verify(&expected)?;
			}
			ChecksumMode::Calculate(algo) => {
				meta.checksum = checksums.extract(algo);
			}
			ChecksumMode::VerifyFrom {
				checksummer,
				trailer_algo,
			} => {
				drop(chunker);
				checksums = checksummer
					.await
					.ok_or_internal_error("checksum calculation")??;
				if let Some(algo) = trailer_algo {
					meta.checksum = checksums.extract(Some(algo));
				}
			}
		};

		let size = first_block.len() as u64;
		check_quotas(ctx, size, existing_object.as_ref()).await?;

		let etag = encryption.etag_from_md5(&checksums.md5);
		let inline_data = encryption.encrypt_blob(&first_block)?.to_vec();

		let object_version = ObjectVersion {
			uuid: version_uuid,
			timestamp: version_timestamp,
			state: ObjectVersionState::Complete(ObjectVersionData::Inline(
				ObjectVersionMeta {
					encryption: encryption.encrypt_meta(meta)?,
					size,
					etag: etag.clone(),
				},
				inline_data,
			)),
		};

		let object = Object::new(*bucket_id, key.into(), vec![object_version]);
		garage.object_table.insert(&object).await?;

		return Ok(SaveStreamResult {
			version_uuid,
			version_timestamp,
			etag,
		});
	}

	// The following consists in many steps that can each fail.
	// Keep track that some cleanup will be needed if things fail
	// before everything is finished (cleanup is done using the Drop trait).
	let mut interrupted_cleanup = InterruptedCleanup(Some(InterruptedCleanupInner {
		garage: garage.clone(),
		bucket_id: *bucket_id,
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
			encryption: encryption.encrypt_meta(meta.clone())?,
			checksum_algorithm: None, // don't care; overwritten later
			multipart: false,
		},
	};
	let object = Object::new(*bucket_id, key.into(), vec![object_version.clone()]);
	garage.object_table.insert(&object).await?;

	// Initialize corresponding entry in version table
	// Write this entry now, even with empty block list,
	// to prevent block_ref entries from being deleted (they can be deleted
	// if the reference a version that isn't found in the version table)
	let version = Version::new(
		version_uuid,
		VersionBacklink::Object {
			bucket_id: *bucket_id,
			key: key.into(),
		},
		false,
	);
	garage.version_table.insert(&version).await?;

	// Transfer data
	let (total_size, mut checksums, first_block_hash) = read_and_put_blocks(
		ctx,
		&version,
		encryption,
		1,
		first_block,
		chunker,
		checksummer,
	)
	.await?;

	// Verify checksums are ok / add calculated checksum to metadata
	match checksum_mode {
		ChecksumMode::Verify(expected) => {
			checksums.verify(&expected)?;
		}
		ChecksumMode::Calculate(algo) => {
			meta.checksum = checksums.extract(algo);
		}
		ChecksumMode::VerifyFrom {
			checksummer,
			trailer_algo,
		} => {
			checksums = checksummer
				.await
				.ok_or_internal_error("checksum calculation")??;
			if let Some(algo) = trailer_algo {
				meta.checksum = checksums.extract(Some(algo));
			}
		}
	};

	// Verify quotas are respsected
	check_quotas(ctx, total_size, existing_object.as_ref()).await?;

	// Save final object state, marked as Complete
	let etag = encryption.etag_from_md5(&checksums.md5);

	object_version.state = ObjectVersionState::Complete(ObjectVersionData::FirstBlock(
		ObjectVersionMeta {
			encryption: encryption.encrypt_meta(meta)?,
			size: total_size,
			etag: etag.clone(),
		},
		first_block_hash,
	));
	let object = Object::new(*bucket_id, key.into(), vec![object_version]);
	garage.object_table.insert(&object).await?;

	// We were not interrupted, everything went fine.
	// We won't have to clean up on drop.
	interrupted_cleanup.cancel();

	Ok(SaveStreamResult {
		version_uuid,
		version_timestamp,
		etag,
	})
}

/// Check that inserting this object with this size doesn't exceed bucket quotas
pub(crate) async fn check_quotas(
	ctx: &ReqCtx,
	size: u64,
	prev_object: Option<&Object>,
) -> Result<(), Error> {
	let ReqCtx {
		garage,
		bucket_id,
		bucket_params,
		..
	} = ctx;

	let quotas = bucket_params.quotas.get();
	if quotas.max_objects.is_none() && quotas.max_size.is_none() {
		return Ok(());
	};

	let counters = garage
		.object_counter_table
		.table
		.get(bucket_id, &EmptyKey)
		.await?;

	let counters = counters
		.map(|x| x.filtered_values(&garage.system.cluster_layout()))
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
	ctx: &ReqCtx,
	version: &Version,
	encryption: EncryptionParams,
	part_number: u64,
	first_block: Bytes,
	mut chunker: StreamChunker<S>,
	checksummer: Checksummer,
) -> Result<(u64, Checksums, Hash), Error> {
	let tracer = opentelemetry::global::tracer("garage");

	let (block_tx, mut block_rx) = mpsc::channel::<Result<Bytes, Error>>(2);
	let read_blocks = async {
		block_tx.send(Ok(first_block)).await?;
		loop {
			let res = chunker
				.next()
				.with_context(Context::current_with_span(
					tracer.start("Read block from client"),
				))
				.await;
			match res {
				Ok(Some(block)) => block_tx.send(Ok(block)).await?,
				Ok(None) => break,
				Err(e) => {
					block_tx.send(Err(e)).await?;
					break;
				}
			}
		}
		drop(block_tx);
		Ok::<_, mpsc::error::SendError<_>>(())
	};

	let (block_tx2, mut block_rx2) = mpsc::channel::<Result<Bytes, Error>>(1);
	let hash_stream = async {
		let mut checksummer = checksummer;
		while let Some(next) = block_rx.recv().await {
			match next {
				Ok(block) => {
					block_tx2.send(Ok(block.clone())).await?;
					checksummer = tokio::task::spawn_blocking(move || {
						checksummer.update(&block);
						checksummer
					})
					.with_context(Context::current_with_span(
						tracer.start("Hash block (md5, sha256)"),
					))
					.await
					.unwrap()
				}
				Err(e) => {
					block_tx2.send(Err(e)).await?;
					break;
				}
			}
		}
		drop(block_tx2);
		Ok::<_, mpsc::error::SendError<_>>(checksummer)
	};

	let (block_tx3, mut block_rx3) = mpsc::channel::<Result<(Bytes, u64, Hash), Error>>(1);
	let encrypt_hash_blocks = async {
		let mut first_block_hash = None;
		while let Some(next) = block_rx2.recv().await {
			match next {
				Ok(block) => {
					let unencrypted_len = block.len() as u64;
					let res = tokio::task::spawn_blocking(move || {
						let block = encryption.encrypt_block(block)?;
						let hash = blake2sum(&block);
						Ok((block, hash))
					})
					.with_context(Context::current_with_span(
						tracer.start("Encrypt and hash (blake2) block"),
					))
					.await
					.unwrap();
					match res {
						Ok((block, hash)) => {
							if first_block_hash.is_none() {
								first_block_hash = Some(hash);
							}
							block_tx3.send(Ok((block, unencrypted_len, hash))).await?;
						}
						Err(e) => {
							block_tx3.send(Err(e)).await?;
							break;
						}
					}
				}
				Err(e) => {
					block_tx3.send(Err(e)).await?;
					break;
				}
			}
		}
		drop(block_tx3);
		Ok::<_, mpsc::error::SendError<_>>(first_block_hash.unwrap())
	};

	let put_blocks = async {
		// Structure for handling several concurrent writes to storage nodes
		let order_stream = OrderTag::stream();
		let mut write_futs = FuturesOrdered::new();
		let mut written_bytes = 0u64;
		loop {
			// Simultaneously write blocks to storage nodes & await for next block to be written
			let currently_running = write_futs.len();
			let write_futs_next = async {
				if write_futs.is_empty() {
					futures::future::pending().await
				} else {
					write_futs.next().await.unwrap()
				}
			};
			let recv_next = async {
				// If more than a maximum number of writes are in progress, don't add more for now
				if currently_running >= PUT_BLOCKS_MAX_PARALLEL {
					futures::future::pending().await
				} else {
					block_rx3.recv().await
				}
			};
			let (block, unencrypted_len, hash) = tokio::select! {
				result = write_futs_next => {
					result?;
					continue;
				},
				recv = recv_next => match recv {
					Some(next) => next?,
					None => break,
				},
			};

			// For next block to be written: count its size and spawn future to write it
			write_futs.push_back(put_block_and_meta(
				ctx,
				version,
				part_number,
				written_bytes,
				hash,
				block,
				unencrypted_len,
				encryption.is_encrypted(),
				order_stream.order(written_bytes),
			));
			written_bytes += unencrypted_len;
		}
		while let Some(res) = write_futs.next().await {
			res?;
		}
		Ok::<_, Error>(written_bytes)
	};

	let (_, stream_hash_result, block_hash_result, final_result) =
		futures::join!(read_blocks, hash_stream, encrypt_hash_blocks, put_blocks);

	let total_size = final_result?;
	// unwrap here is ok, because if hasher failed, it is because something failed
	// later in the pipeline which already caused a return at the ? on previous line
	let first_block_hash = block_hash_result.unwrap();
	let checksums = stream_hash_result.unwrap().finalize();

	Ok((total_size, checksums, first_block_hash))
}

async fn put_block_and_meta(
	ctx: &ReqCtx,
	version: &Version,
	part_number: u64,
	offset: u64,
	hash: Hash,
	block: Bytes,
	size: u64,
	is_encrypted: bool,
	order_tag: OrderTag,
) -> Result<(), GarageError> {
	let ReqCtx { garage, .. } = ctx;

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
		garage
			.block_manager
			.rpc_put_block(hash, block, is_encrypted, Some(order_tag)),
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

pub(crate) fn extract_metadata_headers(
	headers: &HeaderMap<HeaderValue>,
) -> Result<HeaderList, Error> {
	let mut ret = Vec::new();

	// Preserve standard headers
	let standard_header = vec![
		hyper::header::CONTENT_TYPE,
		hyper::header::CACHE_CONTROL,
		hyper::header::CONTENT_DISPOSITION,
		hyper::header::CONTENT_ENCODING,
		hyper::header::CONTENT_LANGUAGE,
		hyper::header::EXPIRES,
	];
	for name in standard_header.iter() {
		if let Some(value) = headers.get(name) {
			ret.push((name.to_string(), value.to_str()?.to_string()));
		}
	}

	// Preserve x-amz-meta- headers
	for (name, value) in headers.iter() {
		if name.as_str().starts_with("x-amz-meta-") {
			ret.push((
				name.as_str().to_ascii_lowercase(),
				std::str::from_utf8(value.as_bytes())?.to_string(),
			));
		}
		if name == X_AMZ_WEBSITE_REDIRECT_LOCATION {
			let value = std::str::from_utf8(value.as_bytes())?.to_string();
			if !(value.starts_with("/")
				|| value.starts_with("http://")
				|| value.starts_with("https://"))
			{
				return Err(Error::bad_request(format!(
					"Invalid {X_AMZ_WEBSITE_REDIRECT_LOCATION} header",
				)));
			}
			ret.push((X_AMZ_WEBSITE_REDIRECT_LOCATION.to_string(), value));
		}
	}

	Ok(ret)
}

pub(crate) fn next_timestamp(existing_object: Option<&Object>) -> u64 {
	existing_object
		.as_ref()
		.and_then(|obj| obj.versions().iter().map(|v| v.timestamp).max())
		.map(|t| std::cmp::max(t + 1, now_msec()))
		.unwrap_or_else(now_msec)
}
