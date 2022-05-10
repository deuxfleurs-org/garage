use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

use futures::prelude::*;
use hyper::body::{Body, Bytes};
use hyper::header::{HeaderMap, HeaderValue};
use hyper::{Request, Response};
use md5::{digest::generic_array::*, Digest as Md5Digest, Md5};
use sha2::Sha256;

use garage_table::*;
use garage_util::data::*;
use garage_util::error::Error as GarageError;
use garage_util::time::*;

use garage_block::manager::INLINE_THRESHOLD;
use garage_model::garage::Garage;
use garage_model::s3::block_ref_table::*;
use garage_model::s3::object_table::*;
use garage_model::s3::version_table::*;

use crate::error::*;
use crate::s3::xml as s3_xml;
use crate::signature::verify_signed_content;

pub async fn handle_put(
	garage: Arc<Garage>,
	req: Request<Body>,
	bucket_id: Uuid,
	key: &str,
	content_sha256: Option<Hash>,
) -> Result<Response<Body>, Error> {
	// Retrieve interesting headers from request
	let headers = get_headers(req.headers())?;
	debug!("Object headers: {:?}", headers);

	let content_md5 = match req.headers().get("content-md5") {
		Some(x) => Some(x.to_str()?.to_string()),
		None => None,
	};

	let (_head, body) = req.into_parts();
	let body = body.map_err(Error::from);

	save_stream(
		garage,
		headers,
		body,
		bucket_id,
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
	bucket_id: Uuid,
	key: &str,
	content_md5: Option<String>,
	content_sha256: Option<FixedBytes32>,
) -> Result<(Uuid, String), Error> {
	// Generate identity of new version
	let version_uuid = gen_uuid();
	let version_timestamp = now_msec();

	let mut chunker = StreamChunker::new(body, garage.config.block_size);
	let first_block = chunker.next().await?.unwrap_or_default();

	// If body is small enough, store it directly in the object table
	// as "inline data". We can then return immediately.
	if first_block.len() < INLINE_THRESHOLD {
		let mut md5sum = Md5::new();
		md5sum.update(&first_block[..]);
		let data_md5sum = md5sum.finalize();
		let data_md5sum_hex = hex::encode(data_md5sum);

		let data_sha256sum = sha256sum(&first_block[..]);

		ensure_checksum_matches(
			data_md5sum.as_slice(),
			data_sha256sum,
			content_md5.as_deref(),
			content_sha256,
		)?;

		let object_version = ObjectVersion {
			uuid: version_uuid,
			timestamp: version_timestamp,
			state: ObjectVersionState::Complete(ObjectVersionData::Inline(
				ObjectVersionMeta {
					headers,
					size: first_block.len() as u64,
					etag: data_md5sum_hex.clone(),
				},
				first_block,
			)),
		};

		let object = Object::new(bucket_id, key.into(), vec![object_version]);
		garage.object_table.insert(&object).await?;

		return Ok((version_uuid, data_md5sum_hex));
	}

	// Write version identifier in object table so that we have a trace
	// that we are uploading something
	let mut object_version = ObjectVersion {
		uuid: version_uuid,
		timestamp: version_timestamp,
		state: ObjectVersionState::Uploading(headers.clone()),
	};
	let object = Object::new(bucket_id, key.into(), vec![object_version.clone()]);
	garage.object_table.insert(&object).await?;

	// Initialize corresponding entry in version table
	// Write this entry now, even with empty block list,
	// to prevent block_ref entries from being deleted (they can be deleted
	// if the reference a version that isn't found in the version table)
	let version = Version::new(version_uuid, bucket_id, key.into(), false);
	garage.version_table.insert(&version).await?;

	// Transfer data and verify checksum
	let first_block_hash = blake2sum(&first_block[..]);
	let tx_result = read_and_put_blocks(
		&garage,
		&version,
		1,
		first_block,
		first_block_hash,
		&mut chunker,
	)
	.await
	.and_then(|(total_size, data_md5sum, data_sha256sum)| {
		ensure_checksum_matches(
			data_md5sum.as_slice(),
			data_sha256sum,
			content_md5.as_deref(),
			content_sha256,
		)
		.map(|()| (total_size, data_md5sum))
	});

	// If something went wrong, clean up
	let (total_size, md5sum_arr) = match tx_result {
		Ok(rv) => rv,
		Err(e) => {
			// Mark object as aborted, this will free the blocks further down
			object_version.state = ObjectVersionState::Aborted;
			let object = Object::new(bucket_id, key.into(), vec![object_version.clone()]);
			garage.object_table.insert(&object).await?;
			return Err(e);
		}
	};

	// Save final object state, marked as Complete
	let md5sum_hex = hex::encode(md5sum_arr);
	object_version.state = ObjectVersionState::Complete(ObjectVersionData::FirstBlock(
		ObjectVersionMeta {
			headers,
			size: total_size,
			etag: md5sum_hex.clone(),
		},
		first_block_hash,
	));
	let object = Object::new(bucket_id, key.into(), vec![object_version]);
	garage.object_table.insert(&object).await?;

	Ok((version_uuid, md5sum_hex))
}

/// Validate MD5 sum against content-md5 header
/// and sha256sum against signed content-sha256
fn ensure_checksum_matches(
	data_md5sum: &[u8],
	data_sha256sum: garage_util::data::FixedBytes32,
	content_md5: Option<&str>,
	content_sha256: Option<garage_util::data::FixedBytes32>,
) -> Result<(), Error> {
	if let Some(expected_sha256) = content_sha256 {
		if expected_sha256 != data_sha256sum {
			return Err(Error::BadRequest(
				"Unable to validate x-amz-content-sha256".to_string(),
			));
		} else {
			trace!("Successfully validated x-amz-content-sha256");
		}
	}
	if let Some(expected_md5) = content_md5 {
		if expected_md5.trim_matches('"') != base64::encode(data_md5sum) {
			return Err(Error::BadRequest(
				"Unable to validate content-md5".to_string(),
			));
		} else {
			trace!("Successfully validated content-md5");
		}
	}
	Ok(())
}

async fn read_and_put_blocks<S: Stream<Item = Result<Bytes, Error>> + Unpin>(
	garage: &Garage,
	version: &Version,
	part_number: u64,
	first_block: Vec<u8>,
	first_block_hash: Hash,
	chunker: &mut StreamChunker<S>,
) -> Result<(u64, GenericArray<u8, typenum::U16>, Hash), Error> {
	let mut md5hasher = Md5::new();
	let mut sha256hasher = Sha256::new();
	md5hasher.update(&first_block[..]);
	sha256hasher.update(&first_block[..]);

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
			md5hasher.update(&block[..]);
			sha256hasher.update(&block[..]);
			let block_hash = blake2sum(&block[..]);
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
	let data_md5sum = md5hasher.finalize();

	let data_sha256sum = sha256hasher.finalize();
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

struct StreamChunker<S: Stream<Item = Result<Bytes, Error>>> {
	stream: S,
	read_all: bool,
	block_size: usize,
	buf: VecDeque<u8>,
}

impl<S: Stream<Item = Result<Bytes, Error>> + Unpin> StreamChunker<S> {
	fn new(stream: S, block_size: usize) -> Self {
		Self {
			stream,
			read_all: false,
			block_size,
			buf: VecDeque::with_capacity(2 * block_size),
		}
	}

	async fn next(&mut self) -> Result<Option<Vec<u8>>, Error> {
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
		} else if self.buf.len() <= self.block_size {
			let block = self.buf.drain(..).collect::<Vec<u8>>();
			Ok(Some(block))
		} else {
			let block = self.buf.drain(..self.block_size).collect::<Vec<u8>>();
			Ok(Some(block))
		}
	}
}

pub fn put_response(version_uuid: Uuid, md5sum_hex: String) -> Response<Body> {
	Response::builder()
		.header("x-amz-version-id", hex::encode(version_uuid))
		.header("ETag", format!("\"{}\"", md5sum_hex))
		.body(Body::from(vec![]))
		.unwrap()
}

pub async fn handle_create_multipart_upload(
	garage: Arc<Garage>,
	req: &Request<Body>,
	bucket_name: &str,
	bucket_id: Uuid,
	key: &str,
) -> Result<Response<Body>, Error> {
	let version_uuid = gen_uuid();
	let headers = get_headers(req.headers())?;

	// Create object in object table
	let object_version = ObjectVersion {
		uuid: version_uuid,
		timestamp: now_msec(),
		state: ObjectVersionState::Uploading(headers),
	};
	let object = Object::new(bucket_id, key.to_string(), vec![object_version]);
	garage.object_table.insert(&object).await?;

	// Insert empty version so that block_ref entries refer to something
	// (they are inserted concurrently with blocks in the version table, so
	// there is the possibility that they are inserted before the version table
	// is created, in which case it is allowed to delete them, e.g. in repair_*)
	let version = Version::new(version_uuid, bucket_id, key.into(), false);
	garage.version_table.insert(&version).await?;

	// Send success response
	let result = s3_xml::InitiateMultipartUploadResult {
		xmlns: (),
		bucket: s3_xml::Value(bucket_name.to_string()),
		key: s3_xml::Value(key.to_string()),
		upload_id: s3_xml::Value(hex::encode(version_uuid)),
	};
	let xml = s3_xml::to_xml_with_header(&result)?;

	Ok(Response::new(Body::from(xml.into_bytes())))
}

pub async fn handle_put_part(
	garage: Arc<Garage>,
	req: Request<Body>,
	bucket_id: Uuid,
	key: &str,
	part_number: u64,
	upload_id: &str,
	content_sha256: Option<Hash>,
) -> Result<Response<Body>, Error> {
	let version_uuid = decode_upload_id(upload_id)?;

	let content_md5 = match req.headers().get("content-md5") {
		Some(x) => Some(x.to_str()?.to_string()),
		None => None,
	};

	// Read first chuck, and at the same time try to get object to see if it exists
	let key = key.to_string();

	let body = req.into_body().map_err(Error::from);
	let mut chunker = StreamChunker::new(body, garage.config.block_size);

	let (object, version, first_block) = futures::try_join!(
		garage
			.object_table
			.get(&bucket_id, &key)
			.map_err(Error::from),
		garage
			.version_table
			.get(&version_uuid, &EmptyKey)
			.map_err(Error::from),
		chunker.next(),
	)?;

	// Check object is valid and multipart block can be accepted
	let first_block = first_block.ok_or_bad_request("Empty body")?;
	let object = object.ok_or_bad_request("Object not found")?;

	if !object
		.versions()
		.iter()
		.any(|v| v.uuid == version_uuid && v.is_uploading())
	{
		return Err(Error::NoSuchUpload);
	}

	// Check part hasn't already been uploaded
	if let Some(v) = version {
		if v.has_part_number(part_number) {
			return Err(Error::BadRequest(format!(
				"Part number {} has already been uploaded",
				part_number
			)));
		}
	}

	// Copy block to store
	let version = Version::new(version_uuid, bucket_id, key, false);
	let first_block_hash = blake2sum(&first_block[..]);
	let (_, data_md5sum, data_sha256sum) = read_and_put_blocks(
		&garage,
		&version,
		part_number,
		first_block,
		first_block_hash,
		&mut chunker,
	)
	.await?;

	// Verify that checksums map
	ensure_checksum_matches(
		data_md5sum.as_slice(),
		data_sha256sum,
		content_md5.as_deref(),
		content_sha256,
	)?;

	// Store part etag in version
	let data_md5sum_hex = hex::encode(data_md5sum);
	let mut version = version;
	version
		.parts_etags
		.put(part_number, data_md5sum_hex.clone());
	garage.version_table.insert(&version).await?;

	let response = Response::builder()
		.header("ETag", format!("\"{}\"", data_md5sum_hex))
		.body(Body::empty())
		.unwrap();
	Ok(response)
}

pub async fn handle_complete_multipart_upload(
	garage: Arc<Garage>,
	req: Request<Body>,
	bucket_name: &str,
	bucket_id: Uuid,
	key: &str,
	upload_id: &str,
	content_sha256: Option<Hash>,
) -> Result<Response<Body>, Error> {
	let body = hyper::body::to_bytes(req.into_body()).await?;

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

	let version_uuid = decode_upload_id(upload_id)?;

	// Get object and version
	let key = key.to_string();
	let (object, version) = futures::try_join!(
		garage.object_table.get(&bucket_id, &key),
		garage.version_table.get(&version_uuid, &EmptyKey),
	)?;

	let object = object.ok_or(Error::NoSuchKey)?;
	let mut object_version = object
		.versions()
		.iter()
		.find(|v| v.uuid == version_uuid && v.is_uploading())
		.cloned()
		.ok_or(Error::NoSuchUpload)?;

	let version = version.ok_or(Error::NoSuchKey)?;
	if version.blocks.is_empty() {
		return Err(Error::BadRequest("No data was uploaded".to_string()));
	}

	let headers = match object_version.state {
		ObjectVersionState::Uploading(headers) => headers,
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

	// Garage-specific restriction, see #204: part numbers must be
	// consecutive starting at 1
	if body_list_of_parts[0].part_number != 1
		|| !body_list_of_parts
			.iter()
			.zip(body_list_of_parts.iter().skip(1))
			.all(|(p1, p2)| p1.part_number + 1 == p2.part_number)
	{
		return Err(Error::NotImplemented("Garage does not support completing a Multipart upload with non-consecutive part numbers. This is a restriction of Garage's data model, which might be fixed in a future release. See issue #204 for more information on this topic.".into()));
	}

	// Check that the list of parts they gave us corresponds to the parts we have here
	debug!("Expected parts from request: {:?}", body_list_of_parts);
	debug!("Parts stored in version: {:?}", version.parts_etags.items());
	let parts = version
		.parts_etags
		.items()
		.iter()
		.map(|pair| (&pair.0, &pair.1));
	let same_parts = body_list_of_parts
		.iter()
		.map(|x| (&x.part_number, &x.etag))
		.eq(parts);
	if !same_parts {
		return Err(Error::InvalidPart);
	}

	// Check that all blocks belong to one of the parts
	let block_parts = version
		.blocks
		.items()
		.iter()
		.map(|(bk, _)| bk.part_number)
		.collect::<BTreeSet<_>>();
	let same_parts = body_list_of_parts
		.iter()
		.map(|x| x.part_number)
		.eq(block_parts.into_iter());
	if !same_parts {
		return Err(Error::BadRequest(
			"Part numbers in block list and part list do not match. This can happen if a part was partially uploaded. Please abort the multipart upload and try again.".into(),
		));
	}

	// Calculate etag of final object
	// To understand how etags are calculated, read more here:
	// https://teppen.io/2018/06/23/aws_s3_etags/
	let num_parts = body_list_of_parts.len();
	let mut etag_md5_hasher = Md5::new();
	for (_, etag) in version.parts_etags.items().iter() {
		etag_md5_hasher.update(etag.as_bytes());
	}
	let etag = format!("{}-{}", hex::encode(etag_md5_hasher.finalize()), num_parts);

	// Calculate total size of final object
	let total_size = version.blocks.items().iter().map(|x| x.1.size).sum();

	// Write final object version
	object_version.state = ObjectVersionState::Complete(ObjectVersionData::FirstBlock(
		ObjectVersionMeta {
			headers,
			size: total_size,
			etag: etag.clone(),
		},
		version.blocks.items()[0].1.hash,
	));

	let final_object = Object::new(bucket_id, key.clone(), vec![object_version]);
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

	Ok(Response::new(Body::from(xml.into_bytes())))
}

pub async fn handle_abort_multipart_upload(
	garage: Arc<Garage>,
	bucket_id: Uuid,
	key: &str,
	upload_id: &str,
) -> Result<Response<Body>, Error> {
	let version_uuid = decode_upload_id(upload_id)?;

	let object = garage
		.object_table
		.get(&bucket_id, &key.to_string())
		.await?;
	let object = object.ok_or(Error::NoSuchKey)?;

	let object_version = object
		.versions()
		.iter()
		.find(|v| v.uuid == version_uuid && v.is_uploading());
	let mut object_version = match object_version {
		None => return Err(Error::NoSuchUpload),
		Some(x) => x.clone(),
	};

	object_version.state = ObjectVersionState::Aborted;
	let final_object = Object::new(bucket_id, key.to_string(), vec![object_version]);
	garage.object_table.insert(&final_object).await?;

	Ok(Response::new(Body::from(vec![])))
}

fn get_mime_type(headers: &HeaderMap<HeaderValue>) -> Result<String, Error> {
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
