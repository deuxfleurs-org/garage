use std::pin::Pin;

use futures::{stream, stream::Stream, StreamExt, TryStreamExt};

use bytes::Bytes;
use http::header::HeaderName;
use hyper::{Request, Response};
use serde::Serialize;

use garage_net::bytes_buf::BytesBuf;
use garage_net::stream::read_stream_to_end;
use garage_rpc::rpc_helper::OrderTag;
use garage_table::*;
use garage_util::data::*;
use garage_util::error::Error as GarageError;
use garage_util::time::*;

use garage_model::s3::block_ref_table::*;
use garage_model::s3::mpu_table::*;
use garage_model::s3::object_table::*;
use garage_model::s3::version_table::*;

use garage_api_common::helpers::*;
use garage_api_common::signature::checksum::*;

use crate::api_server::{ReqBody, ResBody};
use crate::encryption::EncryptionParams;
use crate::error::*;
use crate::get::{check_version_not_deleted, full_object_byte_stream, PreconditionHeaders};
use crate::multipart;
use crate::put::{extract_metadata_headers, save_stream, ChecksumMode, SaveStreamResult};
use crate::website::X_AMZ_WEBSITE_REDIRECT_LOCATION;
use crate::xml::{self as s3_xml, xmlns_tag};

pub const X_AMZ_COPY_SOURCE_IF_MATCH: HeaderName =
	HeaderName::from_static("x-amz-copy-source-if-match");
pub const X_AMZ_COPY_SOURCE_IF_NONE_MATCH: HeaderName =
	HeaderName::from_static("x-amz-copy-source-if-none-match");
pub const X_AMZ_COPY_SOURCE_IF_MODIFIED_SINCE: HeaderName =
	HeaderName::from_static("x-amz-copy-source-if-modified-since");
pub const X_AMZ_COPY_SOURCE_IF_UNMODIFIED_SINCE: HeaderName =
	HeaderName::from_static("x-amz-copy-source-if-unmodified-since");

// -------- CopyObject ---------

pub async fn handle_copy(
	ctx: ReqCtx,
	req: &Request<ReqBody>,
	dest_key: &str,
) -> Result<Response<ResBody>, Error> {
	let copy_precondition = PreconditionHeaders::parse_copy_source(req)?;

	let checksum_algorithm = request_checksum_algorithm(req.headers())?;

	let source_object = get_copy_source(&ctx, req).await?;

	let (source_version, source_version_data, source_version_meta) =
		extract_source_info(&source_object)?;

	// Check precondition, e.g. x-amz-copy-source-if-match
	copy_precondition.check_copy_source(source_version, &source_version_meta.etag)?;

	// Determine encryption parameters
	let (source_encryption, source_object_meta_inner) =
		EncryptionParams::check_decrypt_for_copy_source(
			&ctx.garage,
			req.headers(),
			&source_version_meta.encryption,
		)?;
	let dest_encryption = EncryptionParams::new_from_headers(&ctx.garage, req.headers())?;

	// Extract source checksum info before source_object_meta_inner is consumed
	let source_checksum = source_object_meta_inner.checksum;
	let source_checksum_algorithm = source_checksum.map(|x| x.algorithm());

	// If source object has a checksum, the destination object must as well.
	// The x-amz-checksum-algorithm header allows to change that algorithm,
	// but if it is absent, we must use the same as before
	let checksum_algorithm = checksum_algorithm.or(source_checksum_algorithm);

	// Determine metadata of destination object
	let was_multipart = source_version_meta.etag.contains('-');
	let dest_object_meta = ObjectVersionMetaInner {
		headers: match req.headers().get("x-amz-metadata-directive") {
			Some(v) if v == hyper::header::HeaderValue::from_static("REPLACE") => {
				extract_metadata_headers(req.headers())?
			}
			_ => {
				// The x-amz-website-redirect-location header is not copied, instead
				// it is replaced by the value from the request (or removed if no
				// value was specified)
				let is_redirect =
					|(key, _): &(String, String)| key == X_AMZ_WEBSITE_REDIRECT_LOCATION.as_str();
				let mut headers: Vec<_> = source_object_meta_inner.headers.clone();
				headers.retain(|h| !is_redirect(h));
				let new_headers = extract_metadata_headers(req.headers())?;
				headers.extend(new_headers.into_iter().filter(is_redirect));
				headers
			}
		},
		checksum: source_checksum,
	};

	// Do actual object copying
	//
	// In any of the following scenarios, we need to read the whole object
	// data and re-write it again:
	//
	// - the data needs to be decrypted or encrypted
	// - the requested checksum algorithm requires us to recompute a checksum
	// - the original object was a multipart upload and a checksum algorithm
	//   is defined (AWS specifies that in this case, we must recompute the
	//   checksum from scratch as if this was a single big object and not
	//   a multipart object, as the checksums are not computed in the same way)
	//
	// In other cases, we can just copy the metadata and reference the same blocks.
	//
	// See: https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html

	let must_recopy = !EncryptionParams::is_same(&source_encryption, &dest_encryption)
		|| source_checksum_algorithm != checksum_algorithm
		|| (was_multipart && checksum_algorithm.is_some());

	let res = if !must_recopy {
		// In most cases, we can just copy the metadata and link blocks of the
		// old object from the new object.
		handle_copy_metaonly(
			ctx,
			dest_key,
			dest_object_meta,
			dest_encryption,
			source_version,
			source_version_data,
			source_version_meta,
		)
		.await?
	} else {
		let expected_checksum = ExpectedChecksums {
			md5: None,
			sha256: None,
			extra: source_checksum,
		};
		let checksum_mode = if was_multipart || source_checksum_algorithm != checksum_algorithm {
			ChecksumMode::Calculate(checksum_algorithm)
		} else {
			ChecksumMode::Verify(&expected_checksum)
		};
		// If source and dest encryption use different keys,
		// we must decrypt content and re-encrypt, so rewrite all data blocks.
		handle_copy_reencrypt(
			ctx,
			dest_key,
			dest_object_meta,
			dest_encryption,
			source_version,
			source_version_data,
			source_encryption,
			checksum_mode,
		)
		.await?
	};

	let last_modified = msec_to_rfc3339(res.version_timestamp);
	let result = CopyObjectResult {
		last_modified: s3_xml::Value(last_modified),
		etag: s3_xml::Value(format!("\"{}\"", res.etag)),
	};
	let xml = s3_xml::to_xml_with_header(&result)?;

	let mut resp = Response::builder()
		.header("Content-Type", "application/xml")
		.header("x-amz-version-id", hex::encode(res.version_uuid))
		.header(
			"x-amz-copy-source-version-id",
			hex::encode(source_version.uuid),
		);
	dest_encryption.add_response_headers(&mut resp);
	Ok(resp.body(string_body(xml))?)
}

async fn handle_copy_metaonly(
	ctx: ReqCtx,
	dest_key: &str,
	dest_object_meta: ObjectVersionMetaInner,
	dest_encryption: EncryptionParams,
	source_version: &ObjectVersion,
	source_version_data: &ObjectVersionData,
	source_version_meta: &ObjectVersionMeta,
) -> Result<SaveStreamResult, Error> {
	let ReqCtx {
		garage,
		bucket_id: dest_bucket_id,
		..
	} = ctx;

	// Generate parameters for copied object
	let new_uuid = gen_uuid();
	let new_timestamp = now_msec();

	let new_meta = ObjectVersionMeta {
		encryption: dest_encryption.encrypt_meta(dest_object_meta)?,
		size: source_version_meta.size,
		etag: source_version_meta.etag.clone(),
	};

	let res = SaveStreamResult {
		version_uuid: new_uuid,
		version_timestamp: new_timestamp,
		etag: new_meta.etag.clone(),
	};

	// Save object copy
	match source_version_data {
		ObjectVersionData::DeleteMarker => unreachable!(),
		ObjectVersionData::Inline(_meta, bytes) => {
			// bytes is either plaintext before&after or encrypted with the
			// same keys, so it's ok to just copy it as is
			let dest_object_version = ObjectVersion {
				uuid: new_uuid,
				timestamp: new_timestamp,
				state: ObjectVersionState::Complete(ObjectVersionData::Inline(
					new_meta,
					bytes.clone(),
				)),
			};
			let dest_object = Object::new(
				dest_bucket_id,
				dest_key.to_string(),
				vec![dest_object_version],
			);
			garage.object_table.insert(&dest_object).await?;
		}
		ObjectVersionData::FirstBlock(_meta, first_block_hash) => {
			// Get block list from source version
			let source_version = garage
				.version_table
				.get(&source_version.uuid, &EmptyKey)
				.await?;
			let source_version = source_version.ok_or(Error::NoSuchKey)?;
			check_version_not_deleted(&source_version)?;

			// Write an "uploading" marker in Object table
			// This holds a reference to the object in the Version table
			// so that it won't be deleted, e.g. by repair_versions.
			let tmp_dest_object_version = ObjectVersion {
				uuid: new_uuid,
				timestamp: new_timestamp,
				state: ObjectVersionState::Uploading {
					encryption: new_meta.encryption.clone(),
					checksum_algorithm: None,
					multipart: false,
				},
			};
			let tmp_dest_object = Object::new(
				dest_bucket_id,
				dest_key.to_string(),
				vec![tmp_dest_object_version],
			);
			garage.object_table.insert(&tmp_dest_object).await?;

			// Write version in the version table. Even with empty block list,
			// this means that the BlockRef entries linked to this version cannot be
			// marked as deleted (they are marked as deleted only if the Version
			// doesn't exist or is marked as deleted).
			let mut dest_version = Version::new(
				new_uuid,
				VersionBacklink::Object {
					bucket_id: dest_bucket_id,
					key: dest_key.to_string(),
				},
				false,
			);
			garage.version_table.insert(&dest_version).await?;

			// Fill in block list for version and insert block refs
			for (bk, bv) in source_version.blocks.items().iter() {
				dest_version.blocks.put(*bk, *bv);
			}
			let dest_block_refs = dest_version
				.blocks
				.items()
				.iter()
				.map(|b| BlockRef {
					block: b.1.hash,
					version: new_uuid,
					deleted: false.into(),
				})
				.collect::<Vec<_>>();
			futures::try_join!(
				garage.version_table.insert(&dest_version),
				garage.block_ref_table.insert_many(&dest_block_refs[..]),
			)?;

			// Insert final object
			// We do this last because otherwise there is a race condition in the case where
			// the copy call has the same source and destination (this happens, rclone does
			// it to update the modification timestamp for instance). If we did this concurrently
			// with the stuff before, the block's reference counts could be decremented before
			// they are incremented again for the new version, leading to data being deleted.
			let dest_object_version = ObjectVersion {
				uuid: new_uuid,
				timestamp: new_timestamp,
				state: ObjectVersionState::Complete(ObjectVersionData::FirstBlock(
					new_meta,
					*first_block_hash,
				)),
			};
			let dest_object = Object::new(
				dest_bucket_id,
				dest_key.to_string(),
				vec![dest_object_version],
			);
			garage.object_table.insert(&dest_object).await?;
		}
	}

	Ok(res)
}

async fn handle_copy_reencrypt(
	ctx: ReqCtx,
	dest_key: &str,
	dest_object_meta: ObjectVersionMetaInner,
	dest_encryption: EncryptionParams,
	source_version: &ObjectVersion,
	source_version_data: &ObjectVersionData,
	source_encryption: EncryptionParams,
	checksum_mode: ChecksumMode<'_>,
) -> Result<SaveStreamResult, Error> {
	// basically we will read the source data (decrypt if necessary)
	// and save that in a new object (encrypt if necessary),
	// by combining the code used in getobject and putobject
	let source_stream = full_object_byte_stream(
		ctx.garage.clone(),
		source_version,
		source_version_data,
		source_encryption,
	);

	save_stream(
		&ctx,
		dest_object_meta,
		dest_encryption,
		source_stream.map_err(|e| Error::from(GarageError::from(e))),
		&dest_key.to_string(),
		checksum_mode,
	)
	.await
}

// -------- UploadPartCopy ---------

pub async fn handle_upload_part_copy(
	ctx: ReqCtx,
	req: &Request<ReqBody>,
	dest_key: &str,
	part_number: u64,
	upload_id: &str,
) -> Result<Response<ResBody>, Error> {
	let copy_precondition = PreconditionHeaders::parse_copy_source(req)?;

	let dest_upload_id = multipart::decode_upload_id(upload_id)?;

	let dest_key = dest_key.to_string();
	let (source_object, (_, dest_version, mut dest_mpu)) = futures::try_join!(
		get_copy_source(&ctx, req),
		multipart::get_upload(&ctx, &dest_key, &dest_upload_id)
	)?;

	let ReqCtx { garage, .. } = ctx;

	let (source_object_version, source_version_data, source_version_meta) =
		extract_source_info(&source_object)?;

	// Check precondition on source, e.g. x-amz-copy-source-if-match
	copy_precondition.check_copy_source(source_object_version, &source_version_meta.etag)?;

	// Determine encryption parameters
	let (source_encryption, _) = EncryptionParams::check_decrypt_for_copy_source(
		&garage,
		req.headers(),
		&source_version_meta.encryption,
	)?;
	let (dest_object_encryption, dest_object_checksum_algorithm) = match dest_version.state {
		ObjectVersionState::Uploading {
			encryption,
			checksum_algorithm,
			..
		} => (encryption, checksum_algorithm),
		_ => unreachable!(),
	};
	let (dest_encryption, _) =
		EncryptionParams::check_decrypt(&garage, req.headers(), &dest_object_encryption)?;
	let same_encryption = EncryptionParams::is_same(&source_encryption, &dest_encryption);

	// Check source range is valid
	let source_range = match req.headers().get("x-amz-copy-source-range") {
		Some(range) => {
			let range_str = range.to_str()?;
			let mut ranges = http_range::HttpRange::parse(range_str, source_version_meta.size)
				.map_err(|e| (e, source_version_meta.size))?;
			if ranges.len() != 1 {
				return Err(Error::bad_request(
					"Invalid x-amz-copy-source-range header: exactly 1 range must be given",
				));
			} else {
				ranges.pop().unwrap()
			}
		}
		None => http_range::HttpRange {
			start: 0,
			length: source_version_meta.size,
		},
	};

	// Check source version is not inlined
	if matches!(source_version_data, ObjectVersionData::Inline(_, _)) {
		// This is only for small files, we don't bother handling this.
		// (in AWS UploadPartCopy works for parts at least 5MB which
		// is never the case of an inline object)
		return Err(Error::bad_request(
			"Source object is too small (minimum part size is 5Mb)",
		));
	}

	// Fetch source version with its block list
	let source_version = garage
		.version_table
		.get(&source_object_version.uuid, &EmptyKey)
		.await?
		.ok_or(Error::NoSuchKey)?;
	check_version_not_deleted(&source_version)?;

	// We want to reuse blocks from the source version as much as possible.
	// However, we still need to get the data from these blocks
	// because we need to know it to calculate the MD5sum of the part
	// which is used as its ETag. For encrypted sources or destinations,
	// we must always read(+decrypt) and then write(+encrypt), so we
	// can never reuse data blocks as is.

	// First, calculate what blocks we want to keep,
	// and the subrange of the block to take, if the bounds of the
	// requested range are in the middle.
	let (range_begin, range_end) = (source_range.start, source_range.start + source_range.length);

	let mut blocks_to_copy = vec![];
	let mut current_offset = 0;
	for (_bk, block) in source_version.blocks.items().iter() {
		let (block_begin, block_end) = (current_offset, current_offset + block.size);

		if block_begin < range_end && block_end > range_begin {
			let subrange_begin = if block_begin < range_begin {
				Some(range_begin - block_begin)
			} else {
				None
			};
			let subrange_end = if block_end > range_end {
				Some(range_end - block_begin)
			} else {
				None
			};
			let range_to_copy = match (subrange_begin, subrange_end) {
				(Some(b), Some(e)) => Some(b as usize..e as usize),
				(None, Some(e)) => Some(0..e as usize),
				(Some(b), None) => Some(b as usize..block.size as usize),
				(None, None) => None,
			};

			blocks_to_copy.push((block.hash, range_to_copy));
		}

		current_offset = block_end;
	}

	// Calculate the identity of destination part: timestamp, version id
	let dest_version_id = gen_uuid();
	let dest_mpu_part_key = MpuPartKey {
		part_number,
		timestamp: dest_mpu.next_timestamp(part_number),
	};

	// Create the uploaded part
	dest_mpu.parts.clear();
	dest_mpu.parts.put(
		dest_mpu_part_key,
		MpuPart {
			version: dest_version_id,
			// These are all filled in later (bottom of this function)
			etag: None,
			checksum: None,
			size: None,
		},
	);
	garage.mpu_table.insert(&dest_mpu).await?;

	let mut dest_version = Version::new(
		dest_version_id,
		VersionBacklink::MultipartUpload {
			upload_id: dest_upload_id,
		},
		false,
	);
	// write an empty version now to be the parent of the block_ref entries
	garage.version_table.insert(&dest_version).await?;

	// Now, actually copy the blocks
	let mut checksummer = Checksummer::init(&Default::default(), !dest_encryption.is_encrypted())
		.add(dest_object_checksum_algorithm);

	// First, create a stream that is able to read the source blocks
	// and extract the subrange if necessary.
	// The second returned value is an Option<Hash>, that is Some
	// if and only if the block returned is a block that already existed
	// in the Garage data store and can be reused as-is instead of having
	// to save it again. This excludes encrypted source blocks that we had
	// to decrypt.
	let garage2 = garage.clone();
	let order_stream = OrderTag::stream();
	let source_blocks = stream::iter(blocks_to_copy)
		.enumerate()
		.map(|(i, (block_hash, range_to_copy))| {
			let garage3 = garage2.clone();
			async move {
				let stream = source_encryption
					.get_block(&garage3, &block_hash, Some(order_stream.order(i as u64)))
					.await?;
				let data = read_stream_to_end(stream).await?.into_bytes();
				// For each item, we return a tuple of:
				// 1. the full data block (decrypted)
				// 2. an Option<Hash> that indicates the hash of the block in the block store,
				//    only if it can be re-used as-is in the copied object
				match range_to_copy {
					Some(r) => {
						// If we are taking a subslice of the data, we cannot reuse the block as-is
						Ok((data.slice(r), None))
					}
					None if same_encryption => {
						// If the data is unencrypted before & after, or if we are using
						// the same encryption key, we can reuse the stored block, no need
						// to re-send it to storage nodes.
						Ok((data, Some(block_hash)))
					}
					None => {
						// If we are decrypting / (re)encrypting with different keys,
						// we cannot reuse the block as-is
						Ok((data, None))
					}
				}
			}
		})
		.buffered(2)
		.peekable();

	// The defragmenter is a custom stream (defined below) that concatenates
	// consecutive block parts when they are too small.
	// It returns a series of (Vec<u8>, Option<Hash>).
	// When it is done, it returns an empty vec.
	// Same as the previous iterator, the Option is Some(_) if and only if
	// it's an existing block of the Garage data store that can be reused.
	let mut defragmenter = Defragmenter::new(garage.config.block_size, Box::pin(source_blocks));

	let mut current_offset = 0;
	let mut next_block = defragmenter.next().await?;
	let mut blocks_to_dup = dest_version.clone();

	// TODO this could be optimized similarly to read_and_put_blocks
	// low priority because uploadpartcopy is rarely used
	loop {
		let (data, existing_block_hash) = next_block;
		if data.is_empty() {
			break;
		}

		let data_len = data.len() as u64;

		let (checksummer_updated, (data_to_upload, final_hash)) =
			tokio::task::spawn_blocking(move || {
				checksummer.update(&data[..]);

				let tup = match existing_block_hash {
					Some(hash) if same_encryption => (None, hash),
					_ => {
						let data_enc = dest_encryption.encrypt_block(data)?;
						let hash = blake2sum(&data_enc);
						(Some(data_enc), hash)
					}
				};
				Ok::<_, Error>((checksummer, tup))
			})
			.await
			.unwrap()?;
		checksummer = checksummer_updated;

		let (version_block_key, version_block) = (
			VersionBlockKey {
				part_number,
				offset: current_offset,
			},
			VersionBlock {
				hash: final_hash,
				size: data_len,
			},
		);
		current_offset += data_len;

		let next = if let Some(final_data) = data_to_upload {
			dest_version.blocks.clear();
			dest_version.blocks.put(version_block_key, version_block);
			let block_ref = BlockRef {
				block: final_hash,
				version: dest_version_id,
				deleted: false.into(),
			};
			let (_, _, _, next) = futures::try_join!(
				// Thing 1: if the block is not exactly a block that existed before,
				// we need to insert that data as a new block.
				garage.block_manager.rpc_put_block(
					final_hash,
					final_data,
					dest_encryption.is_encrypted(),
					None
				),
				// Thing 2: we need to insert the block in the version
				garage.version_table.insert(&dest_version),
				// Thing 3: we need to add a block reference
				garage.block_ref_table.insert(&block_ref),
				// Thing 4: we need to read the next block
				defragmenter.next(),
			)?;
			next
		} else {
			blocks_to_dup.blocks.put(version_block_key, version_block);
			defragmenter.next().await?
		};
		next_block = next;
	}

	assert_eq!(current_offset, source_range.length);

	// Put the duplicated blocks into the version & block_refs tables
	let block_refs_to_put = blocks_to_dup
		.blocks
		.items()
		.iter()
		.map(|b| BlockRef {
			block: b.1.hash,
			version: dest_version_id,
			deleted: false.into(),
		})
		.collect::<Vec<_>>();
	futures::try_join!(
		garage.version_table.insert(&blocks_to_dup),
		garage.block_ref_table.insert_many(&block_refs_to_put[..]),
	)?;

	let checksums = checksummer.finalize();
	let etag = dest_encryption.etag_from_md5(&checksums.md5);
	let checksum = checksums.extract(dest_object_checksum_algorithm);

	// Put the part's ETag in the Versiontable
	dest_mpu.parts.put(
		dest_mpu_part_key,
		MpuPart {
			version: dest_version_id,
			etag: Some(etag.clone()),
			checksum,
			size: Some(current_offset),
		},
	);
	garage.mpu_table.insert(&dest_mpu).await?;

	// LGTM
	let resp_xml = s3_xml::to_xml_with_header(&CopyPartResult {
		xmlns: (),
		etag: s3_xml::Value(format!("\"{}\"", etag)),
		last_modified: s3_xml::Value(msec_to_rfc3339(source_object_version.timestamp)),
	})?;

	let mut resp = Response::builder()
		.header("Content-Type", "application/xml")
		.header(
			"x-amz-copy-source-version-id",
			hex::encode(source_object_version.uuid),
		);
	dest_encryption.add_response_headers(&mut resp);
	Ok(resp.body(string_body(resp_xml))?)
}

async fn get_copy_source(ctx: &ReqCtx, req: &Request<ReqBody>) -> Result<Object, Error> {
	let ReqCtx {
		garage, api_key, ..
	} = ctx;

	let copy_source = req.headers().get("x-amz-copy-source").unwrap().to_str()?;
	let copy_source = percent_encoding::percent_decode_str(copy_source).decode_utf8()?;

	let (source_bucket, source_key) = parse_bucket_key(&copy_source, None)?;
	let source_bucket_id = garage
		.bucket_helper()
		.resolve_bucket(&source_bucket.to_string(), api_key)
		.await
		.map_err(pass_helper_error)?;

	if !api_key.allow_read(&source_bucket_id) {
		return Err(Error::forbidden(format!(
			"Reading from bucket {} not allowed for this key",
			source_bucket
		)));
	}

	let source_key = source_key.ok_or_bad_request("No source key specified")?;

	let source_object = garage
		.object_table
		.get(&source_bucket_id, &source_key.to_string())
		.await?
		.ok_or(Error::NoSuchKey)?;

	Ok(source_object)
}

fn extract_source_info(
	source_object: &Object,
) -> Result<(&ObjectVersion, &ObjectVersionData, &ObjectVersionMeta), Error> {
	let source_version = source_object
		.versions()
		.iter()
		.rev()
		.find(|v| v.is_complete())
		.ok_or(Error::NoSuchKey)?;

	let source_version_data = match &source_version.state {
		ObjectVersionState::Complete(x) => x,
		_ => unreachable!(),
	};

	let source_version_meta = match source_version_data {
		ObjectVersionData::DeleteMarker => {
			return Err(Error::NoSuchKey);
		}
		ObjectVersionData::Inline(meta, _bytes) => meta,
		ObjectVersionData::FirstBlock(meta, _fbh) => meta,
	};

	Ok((source_version, source_version_data, source_version_meta))
}

type BlockStreamItemOk = (Bytes, Option<Hash>);
type BlockStreamItem = Result<BlockStreamItemOk, garage_util::error::Error>;

struct Defragmenter<S: Stream<Item = BlockStreamItem>> {
	block_size: usize,
	block_stream: Pin<Box<stream::Peekable<S>>>,
	buffer: BytesBuf,
	hash: Option<Hash>,
}

impl<S: Stream<Item = BlockStreamItem>> Defragmenter<S> {
	fn new(block_size: usize, block_stream: Pin<Box<stream::Peekable<S>>>) -> Self {
		Self {
			block_size,
			block_stream,
			buffer: BytesBuf::new(),
			hash: None,
		}
	}

	async fn next(&mut self) -> BlockStreamItem {
		// Fill buffer while we can
		while let Some(res) = self.block_stream.as_mut().peek().await {
			let (peeked_next_block, _) = match res {
				Ok(t) => t,
				Err(_) => {
					self.block_stream.next().await.unwrap()?;
					unreachable!()
				}
			};

			if self.buffer.is_empty() {
				let (next_block, next_block_hash) = self.block_stream.next().await.unwrap()?;
				self.buffer.extend(next_block);
				self.hash = next_block_hash;
			} else if self.buffer.len() + peeked_next_block.len() > self.block_size {
				break;
			} else {
				let (next_block, _) = self.block_stream.next().await.unwrap()?;
				self.buffer.extend(next_block);
				self.hash = None;
			}
		}

		Ok((self.buffer.take_all(), self.hash.take()))
	}
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct CopyObjectResult {
	#[serde(rename = "LastModified")]
	pub last_modified: s3_xml::Value,
	#[serde(rename = "ETag")]
	pub etag: s3_xml::Value,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct CopyPartResult {
	#[serde(serialize_with = "xmlns_tag")]
	pub xmlns: (),
	#[serde(rename = "LastModified")]
	pub last_modified: s3_xml::Value,
	#[serde(rename = "ETag")]
	pub etag: s3_xml::Value,
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::xml::to_xml_with_header;

	#[test]
	fn copy_object_result() -> Result<(), Error> {
		let copy_result = CopyObjectResult {
			last_modified: s3_xml::Value(msec_to_rfc3339(0)),
			etag: s3_xml::Value("\"9b2cf535f27731c974343645a3985328\"".to_string()),
		};
		assert_eq!(
			to_xml_with_header(&copy_result)?,
			"<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<CopyObjectResult>\
    <LastModified>1970-01-01T00:00:00.000Z</LastModified>\
    <ETag>&quot;9b2cf535f27731c974343645a3985328&quot;</ETag>\
</CopyObjectResult>\
			"
		);
		Ok(())
	}

	#[test]
	fn serialize_copy_part_result() -> Result<(), Error> {
		let expected_retval = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\
<CopyPartResult xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">\
	<LastModified>2011-04-11T20:34:56.000Z</LastModified>\
	<ETag>&quot;9b2cf535f27731c974343645a3985328&quot;</ETag>\
</CopyPartResult>";
		let v = CopyPartResult {
			xmlns: (),
			last_modified: s3_xml::Value("2011-04-11T20:34:56.000Z".into()),
			etag: s3_xml::Value("\"9b2cf535f27731c974343645a3985328\"".into()),
		};

		assert_eq!(to_xml_with_header(&v)?, expected_retval);

		Ok(())
	}
}
