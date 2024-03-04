use std::collections::HashMap;
use std::sync::Arc;

use futures::prelude::*;
use hyper::{Request, Response};
use md5::{Digest as Md5Digest, Md5};

use garage_table::*;
use garage_util::data::*;

use garage_model::garage::Garage;
use garage_model::s3::block_ref_table::*;
use garage_model::s3::mpu_table::*;
use garage_model::s3::object_table::*;
use garage_model::s3::version_table::*;

use crate::helpers::*;
use crate::s3::api_server::{ReqBody, ResBody};
use crate::s3::error::*;
use crate::s3::put::*;
use crate::s3::xml as s3_xml;
use crate::signature::verify_signed_content;

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

	let headers = get_headers(req.headers())?;

	// Create object in object table
	let object_version = ObjectVersion {
		uuid: upload_id,
		timestamp,
		state: ObjectVersionState::Uploading {
			multipart: true,
			headers,
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

	Ok(Response::new(string_body(xml)))
}

pub async fn handle_put_part(
	ctx: ReqCtx,
	req: Request<ReqBody>,
	key: &str,
	part_number: u64,
	upload_id: &str,
	content_sha256: Option<Hash>,
) -> Result<Response<ResBody>, Error> {
	let ReqCtx { garage, .. } = &ctx;

	let upload_id = decode_upload_id(upload_id)?;

	let content_md5 = match req.headers().get("content-md5") {
		Some(x) => Some(x.to_str()?.to_string()),
		None => None,
	};

	// Read first chuck, and at the same time try to get object to see if it exists
	let key = key.to_string();

	let stream = body_stream(req.into_body());
	let mut chunker = StreamChunker::new(stream, garage.config.block_size);

	let ((_, _, mut mpu), first_block) =
		futures::try_join!(get_upload(&ctx, &key, &upload_id), chunker.next(),)?;

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
			etag: None,
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
	let (total_size, data_md5sum, data_sha256sum, _) =
		read_and_put_blocks(&ctx, &version, part_number, first_block, &mut chunker).await?;

	// Verify that checksums map
	ensure_checksum_matches(
		data_md5sum.as_slice(),
		data_sha256sum,
		content_md5.as_deref(),
		content_sha256,
	)?;

	// Store part etag in version
	let data_md5sum_hex = hex::encode(data_md5sum);
	mpu.parts.put(
		mpu_part_key,
		MpuPart {
			version: version_uuid,
			etag: Some(data_md5sum_hex.clone()),
			size: Some(total_size),
		},
	);
	garage.mpu_table.insert(&mpu).await?;

	// We were not interrupted, everything went fine.
	// We won't have to clean up on drop.
	interrupted_cleanup.cancel();

	let response = Response::builder()
		.header("ETag", format!("\"{}\"", data_md5sum_hex))
		.body(empty_body())
		.unwrap();
	Ok(response)
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
	content_sha256: Option<Hash>,
) -> Result<Response<ResBody>, Error> {
	let ReqCtx {
		garage,
		bucket_id,
		bucket_name,
		..
	} = &ctx;

	let body = http_body_util::BodyExt::collect(req.into_body())
		.await?
		.to_bytes();

	if let Some(content_sha256) = content_sha256 {
		verify_signed_content(content_sha256, &body[..])?;
	}

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

	let headers = match object_version.state {
		ObjectVersionState::Uploading { headers, .. } => headers,
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

	// Calculate etag of final object
	// To understand how etags are calculated, read more here:
	// https://teppen.io/2018/06/23/aws_s3_etags/
	let mut etag_md5_hasher = Md5::new();
	for part in parts.iter() {
		etag_md5_hasher.update(part.etag.as_ref().unwrap().as_bytes());
	}
	let etag = format!(
		"{}-{}",
		hex::encode(etag_md5_hasher.finalize()),
		parts.len()
	);

	// Calculate total size of final object
	let total_size = parts.iter().map(|x| x.size.unwrap()).sum();

	if let Err(e) = check_quotas(&ctx, total_size, Some(&object)).await {
		object_version.state = ObjectVersionState::Aborted;
		let final_object = Object::new(*bucket_id, key.clone(), vec![object_version]);
		garage.object_table.insert(&final_object).await?;

		return Err(e);
	}

	// Write final object version
	object_version.state = ObjectVersionState::Complete(ObjectVersionData::FirstBlock(
		ObjectVersionMeta {
			headers,
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
		location: None,
		bucket: s3_xml::Value(bucket_name.to_string()),
		key: s3_xml::Value(key),
		etag: s3_xml::Value(format!("\"{}\"", etag)),
	};
	let xml = s3_xml::to_xml_with_header(&result)?;

	Ok(Response::new(string_body(xml)))
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
			parts.push(CompleteMultipartUploadPart {
				etag: etag.trim_matches('"').to_string(),
				part_number: part_number.parse().ok()?,
			});
		} else {
			return None;
		}
	}

	Some(parts)
}
