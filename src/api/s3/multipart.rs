use std::collections::HashMap;
use std::convert::{TryFrom, TryInto};
use std::hash::Hasher;
use std::sync::Arc;

use base64::prelude::*;
use crc32c::Crc32cHasher as Crc32c;
use crc32fast::Hasher as Crc32;
use futures::prelude::*;
use hyper::{Request, Response};
use md5::{Digest, Md5};
use sha1::Sha1;
use sha2::Sha256;

use garage_table::*;
use garage_util::data::*;
use garage_util::error::OkOrMessage;

use garage_model::garage::Garage;
use garage_model::s3::block_ref_table::*;
use garage_model::s3::mpu_table::*;
use garage_model::s3::object_table::*;
use garage_model::s3::version_table::*;

use garage_api_common::helpers::*;
use garage_api_common::signature::checksum::*;

use crate::api_server::{ReqBody, ResBody};
use crate::encryption::EncryptionParams;
use crate::error::*;
use crate::put::*;
use crate::xml as s3_xml;

// ----

pub async fn handle_create_multipart_upload(
	ctx: ReqCtx,
	req: &Request<ReqBody>,
	key: &String,
) -> Result<Response<ResBody>, Error> {
	let ReqCtx {
		garage,
		bucket_id,
		bucket_name,
		..
	} = &ctx;
	let existing_object = garage.object_table.get(&bucket_id, &key).await?;

	let upload_id = gen_uuid();
	let timestamp = next_timestamp(existing_object.as_ref());

	let headers = extract_metadata_headers(req.headers())?;
	let meta = ObjectVersionMetaInner {
		headers,
		checksum: None,
	};

	// Determine whether object should be encrypted, and if so the key
	let encryption = EncryptionParams::new_from_headers(&garage, req.headers())?;
	let object_encryption = encryption.encrypt_meta(meta)?;

	let checksum_algorithm = request_checksum_algorithm(req.headers())?;

	// Create object in object table
	let object_version = ObjectVersion {
		uuid: upload_id,
		timestamp,
		state: ObjectVersionState::Uploading {
			multipart: true,
			encryption: object_encryption,
			checksum_algorithm,
		},
	};
	let object = Object::new(*bucket_id, key.to_string(), vec![object_version]);
	garage.object_table.insert(&object).await?;

	// Create multipart upload in mpu table
	// This multipart upload will hold references to uploaded parts
	// (which are entries in the Version table)
	let mpu = MultipartUpload::new(upload_id, timestamp, *bucket_id, key.into(), false);
	garage.mpu_table.insert(&mpu).await?;

	// Send success response
	let result = s3_xml::InitiateMultipartUploadResult {
		xmlns: (),
		bucket: s3_xml::Value(bucket_name.to_string()),
		key: s3_xml::Value(key.to_string()),
		upload_id: s3_xml::Value(hex::encode(upload_id)),
	};
	let xml = s3_xml::to_xml_with_header(&result)?;

	let mut resp = Response::builder();
	encryption.add_response_headers(&mut resp);
	Ok(resp.body(string_body(xml))?)
}

pub async fn handle_put_part(
	ctx: ReqCtx,
	req: Request<ReqBody>,
	key: &str,
	part_number: u64,
	upload_id: &str,
) -> Result<Response<ResBody>, Error> {
	let ReqCtx { garage, .. } = &ctx;

	let upload_id = decode_upload_id(upload_id)?;

	let expected_checksums = ExpectedChecksums {
		md5: match req.headers().get("content-md5") {
			Some(x) => Some(x.to_str()?.to_string()),
			None => None,
		},
		sha256: None,
		extra: request_checksum_value(req.headers())?,
	};

	let key = key.to_string();

	let (req_head, mut req_body) = req.into_parts();

	// Before we stream the body, configure the needed checksums.
	req_body.add_expected_checksums(expected_checksums.clone());
	// TODO: avoid parsing encryption headers twice...
	if !EncryptionParams::new_from_headers(&garage, &req_head.headers)?.is_encrypted() {
		// For non-encrypted objects, we need to compute the md5sum in all cases
		// (even if content-md5 is not set), because it is used as an etag of the
		// part, which is in turn used in the etag computation of the whole object
		req_body.add_md5();
	}

	let (stream, stream_checksums) = req_body.streaming_with_checksums();
	let stream = stream.map_err(Error::from);

	let mut chunker = StreamChunker::new(stream, garage.config.block_size);

	// Read first chuck, and at the same time try to get object to see if it exists
	let ((_, object_version, mut mpu), first_block) =
		futures::try_join!(get_upload(&ctx, &key, &upload_id), chunker.next(),)?;

	// Check encryption params
	let (object_encryption, checksum_algorithm) = match object_version.state {
		ObjectVersionState::Uploading {
			encryption,
			checksum_algorithm,
			..
		} => (encryption, checksum_algorithm),
		_ => unreachable!(),
	};
	let (encryption, _) =
		EncryptionParams::check_decrypt(&garage, &req_head.headers, &object_encryption)?;

	// Check object is valid and part can be accepted
	let first_block = first_block.ok_or_bad_request("Empty body")?;

	// Calculate part identity: timestamp, version id
	let version_uuid = gen_uuid();
	let mpu_part_key = MpuPartKey {
		part_number,
		timestamp: mpu.next_timestamp(part_number),
	};

	// The following consists in many steps that can each fail.
	// Keep track that some cleanup will be needed if things fail
	// before everything is finished (cleanup is done using the Drop trait).
	let mut interrupted_cleanup = InterruptedCleanup(Some(InterruptedCleanupInner {
		garage: garage.clone(),
		upload_id,
		version_uuid,
	}));

	// Create version and link version from MPU
	mpu.parts.clear();
	mpu.parts.put(
		mpu_part_key,
		MpuPart {
			version: version_uuid,
			// all these are filled in later, at the end of this function
			etag: None,
			checksum: None,
			size: None,
		},
	);
	garage.mpu_table.insert(&mpu).await?;

	let version = Version::new(
		version_uuid,
		VersionBacklink::MultipartUpload { upload_id },
		false,
	);
	garage.version_table.insert(&version).await?;

	// Copy data to version
	let (total_size, _, _) = read_and_put_blocks(
		&ctx,
		&version,
		encryption,
		part_number,
		first_block,
		chunker,
		Checksummer::new(),
	)
	.await?;

	// Verify that checksums match
	let checksums = stream_checksums
		.await
		.ok_or_internal_error("checksum calculation")??;

	// Store part etag in version
	let etag = encryption.etag_from_md5(&checksums.md5);

	mpu.parts.put(
		mpu_part_key,
		MpuPart {
			version: version_uuid,
			etag: Some(etag.clone()),
			checksum: checksums.extract(checksum_algorithm),
			size: Some(total_size),
		},
	);
	garage.mpu_table.insert(&mpu).await?;

	// We were not interrupted, everything went fine.
	// We won't have to clean up on drop.
	interrupted_cleanup.cancel();

	let mut resp = Response::builder().header("ETag", format!("\"{}\"", etag));
	encryption.add_response_headers(&mut resp);
	let resp = add_checksum_response_headers(&expected_checksums.extra, resp);
	Ok(resp.body(empty_body())?)
}

struct InterruptedCleanup(Option<InterruptedCleanupInner>);
struct InterruptedCleanupInner {
	garage: Arc<Garage>,
	upload_id: Uuid,
	version_uuid: Uuid,
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
				let version = Version::new(
					info.version_uuid,
					VersionBacklink::MultipartUpload {
						upload_id: info.upload_id,
					},
					true,
				);
				if let Err(e) = info.garage.version_table.insert(&version).await {
					warn!("Cannot cleanup after aborted UploadPart: {}", e);
				}
			});
		}
	}
}

pub async fn handle_complete_multipart_upload(
	ctx: ReqCtx,
	req: Request<ReqBody>,
	key: &str,
	upload_id: &str,
) -> Result<Response<ResBody>, Error> {
	let ReqCtx {
		garage,
		bucket_id,
		bucket_name,
		..
	} = &ctx;
	let (req_head, req_body) = req.into_parts();

	let expected_checksum = request_checksum_value(&req_head.headers)?;

	let body = req_body.collect().await?;

	let body_xml = roxmltree::Document::parse(std::str::from_utf8(&body)?)?;
	let body_list_of_parts = parse_complete_multipart_upload_body(&body_xml)
		.ok_or_bad_request("Invalid CompleteMultipartUpload XML")?;
	debug!(
		"CompleteMultipartUpload list of parts: {:?}",
		body_list_of_parts
	);

	let upload_id = decode_upload_id(upload_id)?;

	// Get object and multipart upload
	let key = key.to_string();
	let (object, mut object_version, mpu) = get_upload(&ctx, &key, &upload_id).await?;

	if mpu.parts.is_empty() {
		return Err(Error::bad_request("No data was uploaded"));
	}

	let (object_encryption, checksum_algorithm) = match object_version.state {
		ObjectVersionState::Uploading {
			encryption,
			checksum_algorithm,
			..
		} => (encryption, checksum_algorithm),
		_ => unreachable!(),
	};

	// Check that part numbers are an increasing sequence.
	// (it doesn't need to start at 1 nor to be a continuous sequence,
	// see discussion in #192)
	if body_list_of_parts.is_empty() {
		return Err(Error::EntityTooSmall);
	}
	if !body_list_of_parts
		.iter()
		.zip(body_list_of_parts.iter().skip(1))
		.all(|(p1, p2)| p1.part_number < p2.part_number)
	{
		return Err(Error::InvalidPartOrder);
	}

	// Check that the list of parts they gave us corresponds to parts we have here
	debug!("Parts stored in multipart upload: {:?}", mpu.parts.items());
	let mut have_parts = HashMap::new();
	for (pk, pv) in mpu.parts.items().iter() {
		have_parts.insert(pk.part_number, pv);
	}
	let mut parts = vec![];
	for req_part in body_list_of_parts.iter() {
		match have_parts.get(&req_part.part_number) {
			Some(part) if part.etag.as_ref() == Some(&req_part.etag) && part.size.is_some() => {
				// alternative version: if req_part.checksum.is_some() && part.checksum != req_part.checksum {
				if part.checksum != req_part.checksum {
					return Err(Error::InvalidDigest(format!(
						"Invalid checksum for part {}: in request = {:?}, uploaded part = {:?}",
						req_part.part_number, req_part.checksum, part.checksum
					)));
				}
				parts.push(*part)
			}
			_ => return Err(Error::InvalidPart),
		}
	}

	let grg = &garage;
	let parts_versions = futures::future::try_join_all(parts.iter().map(|p| async move {
		grg.version_table
			.get(&p.version, &EmptyKey)
			.await?
			.ok_or_internal_error("Part version missing from version table")
	}))
	.await?;

	// Create final version and block refs
	let mut final_version = Version::new(
		upload_id,
		VersionBacklink::Object {
			bucket_id: *bucket_id,
			key: key.to_string(),
		},
		false,
	);
	for (part_number, part_version) in parts_versions.iter().enumerate() {
		if part_version.deleted.get() {
			return Err(Error::InvalidPart);
		}
		for (vbk, vb) in part_version.blocks.items().iter() {
			final_version.blocks.put(
				VersionBlockKey {
					part_number: (part_number + 1) as u64,
					offset: vbk.offset,
				},
				*vb,
			);
		}
	}
	garage.version_table.insert(&final_version).await?;

	let block_refs = final_version.blocks.items().iter().map(|(_, b)| BlockRef {
		block: b.hash,
		version: upload_id,
		deleted: false.into(),
	});
	garage.block_ref_table.insert_many(block_refs).await?;

	// Calculate checksum and etag of final object
	// To understand how etags are calculated, read more here:
	// https://docs.aws.amazon.com/AmazonS3/latest/userguide/checking-object-integrity.html
	// https://teppen.io/2018/06/23/aws_s3_etags/
	let mut checksummer = MultipartChecksummer::init(checksum_algorithm);
	for part in parts.iter() {
		checksummer.update(part.etag.as_ref().unwrap(), part.checksum)?;
	}
	let (checksum_md5, checksum_extra) = checksummer.finalize();

	if expected_checksum.is_some() && checksum_extra != expected_checksum {
		return Err(Error::InvalidDigest(
			"Failed to validate x-amz-checksum-*".into(),
		));
	}

	let etag = format!("{}-{}", hex::encode(&checksum_md5[..]), parts.len());

	// Calculate total size of final object
	let total_size = parts.iter().map(|x| x.size.unwrap()).sum();

	if let Err(e) = check_quotas(&ctx, total_size, Some(&object)).await {
		object_version.state = ObjectVersionState::Aborted;
		let final_object = Object::new(*bucket_id, key.clone(), vec![object_version]);
		garage.object_table.insert(&final_object).await?;

		return Err(e);
	}

	// If there is a checksum algorithm, update metadata with checksum
	let object_encryption = match checksum_algorithm {
		None => object_encryption,
		Some(_) => {
			let (encryption, meta) =
				EncryptionParams::check_decrypt(&garage, &req_head.headers, &object_encryption)?;
			let new_meta = ObjectVersionMetaInner {
				headers: meta.into_owned().headers,
				checksum: checksum_extra,
			};
			encryption.encrypt_meta(new_meta)?
		}
	};

	// Write final object version
	object_version.state = ObjectVersionState::Complete(ObjectVersionData::FirstBlock(
		ObjectVersionMeta {
			encryption: object_encryption,
			size: total_size,
			etag: etag.clone(),
		},
		final_version.blocks.items()[0].1.hash,
	));

	let final_object = Object::new(*bucket_id, key.clone(), vec![object_version]);
	garage.object_table.insert(&final_object).await?;

	// Send response saying ok we're done
	let result = s3_xml::CompleteMultipartUploadResult {
		xmlns: (),
		// FIXME: the location returned is not always correct:
		// - we always return https, but maybe some people do http
		// - if root_domain is not specified, a full URL is not returned
		location: garage
			.config
			.s3_api
			.root_domain
			.as_ref()
			.map(|rd| s3_xml::Value(format!("https://{}.{}/{}", bucket_name, rd, key)))
			.or(Some(s3_xml::Value(format!("/{}/{}", bucket_name, key)))),
		bucket: s3_xml::Value(bucket_name.to_string()),
		key: s3_xml::Value(key),
		etag: s3_xml::Value(format!("\"{}\"", etag)),
		checksum_crc32: match &checksum_extra {
			Some(ChecksumValue::Crc32(x)) => Some(s3_xml::Value(BASE64_STANDARD.encode(&x))),
			_ => None,
		},
		checksum_crc32c: match &checksum_extra {
			Some(ChecksumValue::Crc32c(x)) => Some(s3_xml::Value(BASE64_STANDARD.encode(&x))),
			_ => None,
		},
		checksum_sha1: match &checksum_extra {
			Some(ChecksumValue::Sha1(x)) => Some(s3_xml::Value(BASE64_STANDARD.encode(&x))),
			_ => None,
		},
		checksum_sha256: match &checksum_extra {
			Some(ChecksumValue::Sha256(x)) => Some(s3_xml::Value(BASE64_STANDARD.encode(&x))),
			_ => None,
		},
	};
	let xml = s3_xml::to_xml_with_header(&result)?;

	let resp = Response::builder();
	let resp = add_checksum_response_headers(&expected_checksum, resp);
	Ok(resp.body(string_body(xml))?)
}

pub async fn handle_abort_multipart_upload(
	ctx: ReqCtx,
	key: &str,
	upload_id: &str,
) -> Result<Response<ResBody>, Error> {
	let ReqCtx {
		garage, bucket_id, ..
	} = &ctx;

	let upload_id = decode_upload_id(upload_id)?;

	let (_, mut object_version, _) = get_upload(&ctx, &key.to_string(), &upload_id).await?;

	object_version.state = ObjectVersionState::Aborted;
	let final_object = Object::new(*bucket_id, key.to_string(), vec![object_version]);
	garage.object_table.insert(&final_object).await?;

	Ok(Response::new(empty_body()))
}

// ======== helpers ============

#[allow(clippy::ptr_arg)]
pub(crate) async fn get_upload(
	ctx: &ReqCtx,
	key: &String,
	upload_id: &Uuid,
) -> Result<(Object, ObjectVersion, MultipartUpload), Error> {
	let ReqCtx {
		garage, bucket_id, ..
	} = ctx;
	let (object, mpu) = futures::try_join!(
		garage.object_table.get(bucket_id, key).map_err(Error::from),
		garage
			.mpu_table
			.get(upload_id, &EmptyKey)
			.map_err(Error::from),
	)?;

	let object = object.ok_or(Error::NoSuchUpload)?;
	let mpu = mpu.ok_or(Error::NoSuchUpload)?;

	let object_version = object
		.versions()
		.iter()
		.find(|v| v.uuid == *upload_id && v.is_uploading(Some(true)))
		.ok_or(Error::NoSuchUpload)?
		.clone();

	Ok((object, object_version, mpu))
}

pub fn decode_upload_id(id: &str) -> Result<Uuid, Error> {
	let id_bin = hex::decode(id).map_err(|_| Error::NoSuchUpload)?;
	if id_bin.len() != 32 {
		return Err(Error::NoSuchUpload);
	}
	let mut uuid = [0u8; 32];
	uuid.copy_from_slice(&id_bin[..]);
	Ok(Uuid::from(uuid))
}

#[derive(Debug)]
struct CompleteMultipartUploadPart {
	etag: String,
	part_number: u64,
	checksum: Option<ChecksumValue>,
}

fn parse_complete_multipart_upload_body(
	xml: &roxmltree::Document,
) -> Option<Vec<CompleteMultipartUploadPart>> {
	let mut parts = vec![];

	let root = xml.root();
	let cmu = root.first_child()?;
	if !cmu.has_tag_name("CompleteMultipartUpload") {
		return None;
	}

	for item in cmu.children() {
		// Only parse <Part> nodes
		if !item.is_element() {
			continue;
		}

		if item.has_tag_name("Part") {
			let etag = item.children().find(|e| e.has_tag_name("ETag"))?.text()?;
			let part_number = item
				.children()
				.find(|e| e.has_tag_name("PartNumber"))?
				.text()?;
			let checksum = if let Some(crc32) =
				item.children().find(|e| e.has_tag_name("ChecksumCRC32"))
			{
				Some(ChecksumValue::Crc32(
					BASE64_STANDARD.decode(crc32.text()?).ok()?[..]
						.try_into()
						.ok()?,
				))
			} else if let Some(crc32c) = item.children().find(|e| e.has_tag_name("ChecksumCRC32C"))
			{
				Some(ChecksumValue::Crc32c(
					BASE64_STANDARD.decode(crc32c.text()?).ok()?[..]
						.try_into()
						.ok()?,
				))
			} else if let Some(sha1) = item.children().find(|e| e.has_tag_name("ChecksumSHA1")) {
				Some(ChecksumValue::Sha1(
					BASE64_STANDARD.decode(sha1.text()?).ok()?[..]
						.try_into()
						.ok()?,
				))
			} else if let Some(sha256) = item.children().find(|e| e.has_tag_name("ChecksumSHA256"))
			{
				Some(ChecksumValue::Sha256(
					BASE64_STANDARD.decode(sha256.text()?).ok()?[..]
						.try_into()
						.ok()?,
				))
			} else {
				None
			};
			parts.push(CompleteMultipartUploadPart {
				etag: etag.trim_matches('"').to_string(),
				part_number: part_number.parse().ok()?,
				checksum,
			});
		} else {
			return None;
		}
	}

	Some(parts)
}

// ====== checksummer ====

#[derive(Default)]
pub(crate) struct MultipartChecksummer {
	pub md5: Md5,
	pub extra: Option<MultipartExtraChecksummer>,
}

pub(crate) enum MultipartExtraChecksummer {
	Crc32(Crc32),
	Crc32c(Crc32c),
	Sha1(Sha1),
	Sha256(Sha256),
}

impl MultipartChecksummer {
	pub(crate) fn init(algo: Option<ChecksumAlgorithm>) -> Self {
		Self {
			md5: Md5::new(),
			extra: match algo {
				None => None,
				Some(ChecksumAlgorithm::Crc32) => {
					Some(MultipartExtraChecksummer::Crc32(Crc32::new()))
				}
				Some(ChecksumAlgorithm::Crc32c) => {
					Some(MultipartExtraChecksummer::Crc32c(Crc32c::default()))
				}
				Some(ChecksumAlgorithm::Sha1) => Some(MultipartExtraChecksummer::Sha1(Sha1::new())),
				Some(ChecksumAlgorithm::Sha256) => {
					Some(MultipartExtraChecksummer::Sha256(Sha256::new()))
				}
			},
		}
	}

	pub(crate) fn update(
		&mut self,
		etag: &str,
		checksum: Option<ChecksumValue>,
	) -> Result<(), Error> {
		self.md5
			.update(&hex::decode(&etag).ok_or_message("invalid etag hex")?);
		match (&mut self.extra, checksum) {
			(None, _) => (),
			(
				Some(MultipartExtraChecksummer::Crc32(ref mut crc32)),
				Some(ChecksumValue::Crc32(x)),
			) => {
				crc32.update(&x);
			}
			(
				Some(MultipartExtraChecksummer::Crc32c(ref mut crc32c)),
				Some(ChecksumValue::Crc32c(x)),
			) => {
				crc32c.write(&x);
			}
			(Some(MultipartExtraChecksummer::Sha1(ref mut sha1)), Some(ChecksumValue::Sha1(x))) => {
				sha1.update(&x);
			}
			(
				Some(MultipartExtraChecksummer::Sha256(ref mut sha256)),
				Some(ChecksumValue::Sha256(x)),
			) => {
				sha256.update(&x);
			}
			(Some(_), b) => {
				return Err(Error::internal_error(format!(
					"part checksum was not computed correctly, got: {:?}",
					b
				)))
			}
		}
		Ok(())
	}

	pub(crate) fn finalize(self) -> (Md5Checksum, Option<ChecksumValue>) {
		let md5 = self.md5.finalize()[..].try_into().unwrap();
		let extra = match self.extra {
			None => None,
			Some(MultipartExtraChecksummer::Crc32(crc32)) => {
				Some(ChecksumValue::Crc32(u32::to_be_bytes(crc32.finalize())))
			}
			Some(MultipartExtraChecksummer::Crc32c(crc32c)) => Some(ChecksumValue::Crc32c(
				u32::to_be_bytes(u32::try_from(crc32c.finish()).unwrap()),
			)),
			Some(MultipartExtraChecksummer::Sha1(sha1)) => {
				Some(ChecksumValue::Sha1(sha1.finalize()[..].try_into().unwrap()))
			}
			Some(MultipartExtraChecksummer::Sha256(sha256)) => Some(ChecksumValue::Sha256(
				sha256.finalize()[..].try_into().unwrap(),
			)),
		};
		(md5, extra)
	}
}
