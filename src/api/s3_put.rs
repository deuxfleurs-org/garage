use std::collections::{BTreeMap, VecDeque};
use std::fmt::Write;
use std::sync::Arc;

use futures::stream::*;
use hyper::{Body, Request, Response};
use md5::{digest::generic_array::*, Digest as Md5Digest, Md5};
use sha2::{Digest as Sha256Digest, Sha256};

use garage_table::*;
use garage_util::data::*;
use garage_util::error::Error as GarageError;

use crate::error::*;
use garage_model::block::INLINE_THRESHOLD;
use garage_model::block_ref_table::*;
use garage_model::garage::Garage;
use garage_model::object_table::*;
use garage_model::version_table::*;

use crate::encoding::*;

pub async fn handle_put(
	garage: Arc<Garage>,
	req: Request<Body>,
	bucket: &str,
	key: &str,
	content_sha256: Option<Hash>,
) -> Result<Response<Body>, Error> {
	// Generate identity of new version
	let version_uuid = gen_uuid();
	let version_timestamp = now_msec();

	// Retrieve interesting headers from request
	let headers = get_headers(&req)?;
	debug!("Object headers: {:?}", headers);

	let content_md5 = match req.headers().get("content-md5") {
		Some(x) => Some(x.to_str()?.to_string()),
		None => None,
	};

	// Parse body of uploaded file
	let body = req.into_body();

	let mut chunker = BodyChunker::new(body, garage.config.block_size);
	let first_block = chunker.next().await?.unwrap_or(vec![]);

	// If body is small enough, store it directly in the object table
	// as "inline data". We can then return immediately.
	if first_block.len() < INLINE_THRESHOLD {
		let mut md5sum = Md5::new();
		md5sum.update(&first_block[..]);
		let md5sum_arr = md5sum.finalize();
		let md5sum_hex = hex::encode(md5sum_arr);

		let sha256sum_hash = hash(&first_block[..]);

		ensure_checksum_matches(
			md5sum_arr.as_slice(),
			sha256sum_hash,
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
					etag: md5sum_hex.clone(),
				},
				first_block,
			)),
		};

		let object = Object::new(bucket.into(), key.into(), vec![object_version]);
		garage.object_table.insert(&object).await?;

		return Ok(put_response(version_uuid, md5sum_hex));
	}

	// Write version identifier in object table so that we have a trace
	// that we are uploading something
	let mut object_version = ObjectVersion {
		uuid: version_uuid,
		timestamp: now_msec(),
		state: ObjectVersionState::Uploading(headers.clone()),
	};
	let object = Object::new(bucket.into(), key.into(), vec![object_version.clone()]);
	garage.object_table.insert(&object).await?;

	// Initialize corresponding entry in version table
	let version = Version::new(version_uuid, bucket.into(), key.into(), false, vec![]);
	let first_block_hash = hash(&first_block[..]);

	// Transfer data and verify checksum
	let tx_result = read_and_put_blocks(
		&garage,
		version,
		1,
		first_block,
		first_block_hash,
		&mut chunker,
	)
	.await
	.and_then(|(total_size, md5sum_arr, sha256sum)| {
		ensure_checksum_matches(
			md5sum_arr.as_slice(),
			sha256sum,
			content_md5.as_deref(),
			content_sha256,
		)
		.map(|()| (total_size, md5sum_arr))
	});

	// If something went wrong, clean up
	let (total_size, md5sum_arr) = match tx_result {
		Ok(rv) => rv,
		Err(e) => {
			// Mark object as aborted, this will free the blocks further down
			object_version.state = ObjectVersionState::Aborted;
			let object = Object::new(bucket.into(), key.into(), vec![object_version.clone()]);
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
	let object = Object::new(bucket.into(), key.into(), vec![object_version]);
	garage.object_table.insert(&object).await?;

	Ok(put_response(version_uuid, md5sum_hex))
}

/// Validate MD5 sum against content-md5 header
/// and sha256sum against signed content-sha256
fn ensure_checksum_matches(
	md5sum: &[u8],
	sha256sum: garage_util::data::FixedBytes32,
	content_md5: Option<&str>,
	content_sha256: Option<garage_util::data::FixedBytes32>,
) -> Result<(), Error> {
	if let Some(expected_sha256) = content_sha256 {
		if expected_sha256 != sha256sum {
			return Err(Error::BadRequest(format!(
				"Unable to validate x-amz-content-sha256"
			)));
		} else {
			trace!("Successfully validated x-amz-content-sha256");
		}
	}
	if let Some(expected_md5) = content_md5 {
		if expected_md5.trim_matches('"') != base64::encode(md5sum) {
			return Err(Error::BadRequest(format!("Unable to validate content-md5")));
		} else {
			trace!("Successfully validated content-md5");
		}
	}
	Ok(())
}

async fn read_and_put_blocks(
	garage: &Arc<Garage>,
	version: Version,
	part_number: u64,
	first_block: Vec<u8>,
	first_block_hash: Hash,
	chunker: &mut BodyChunker,
) -> Result<(u64, GenericArray<u8, typenum::U16>, Hash), Error> {
	let mut md5sum = Md5::new();
	let mut sha256sum = Sha256::new();
	md5sum.update(&first_block[..]);
	sha256sum.input(&first_block[..]);

	let mut next_offset = first_block.len();
	let mut put_curr_version_block = put_block_meta(
		garage.clone(),
		&version,
		part_number,
		0,
		first_block_hash,
		first_block.len() as u64,
	);
	let mut put_curr_block = garage
		.block_manager
		.rpc_put_block(first_block_hash, first_block);

	loop {
		let (_, _, next_block) =
			futures::try_join!(put_curr_block, put_curr_version_block, chunker.next())?;
		if let Some(block) = next_block {
			md5sum.update(&block[..]);
			sha256sum.input(&block[..]);
			let block_hash = hash(&block[..]);
			let block_len = block.len();
			put_curr_version_block = put_block_meta(
				garage.clone(),
				&version,
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
	let md5sum_arr = md5sum.finalize();

	let sha256sum = sha256sum.result();
	let mut hash = [0u8; 32];
	hash.copy_from_slice(&sha256sum[..]);
	let sha256sum = Hash::from(hash);

	Ok((total_size, md5sum_arr, sha256sum))
}

async fn put_block_meta(
	garage: Arc<Garage>,
	version: &Version,
	part_number: u64,
	offset: u64,
	hash: Hash,
	size: u64,
) -> Result<(), GarageError> {
	// TODO: don't clone, restart from empty block list ??
	let mut version = version.clone();
	version
		.add_block(VersionBlock {
			part_number,
			offset,
			hash,
			size,
		})
		.unwrap();

	let block_ref = BlockRef {
		block: hash,
		version: version.uuid,
		deleted: false,
	};

	futures::try_join!(
		garage.version_table.insert(&version),
		garage.block_ref_table.insert(&block_ref),
	)?;
	Ok(())
}

struct BodyChunker {
	body: Body,
	read_all: bool,
	block_size: usize,
	buf: VecDeque<u8>,
}

impl BodyChunker {
	fn new(body: Body, block_size: usize) -> Self {
		Self {
			body,
			read_all: false,
			block_size,
			buf: VecDeque::with_capacity(2 * block_size),
		}
	}
	async fn next(&mut self) -> Result<Option<Vec<u8>>, GarageError> {
		while !self.read_all && self.buf.len() < self.block_size {
			if let Some(block) = self.body.next().await {
				let bytes = block?;
				trace!("Body next: {} bytes", bytes.len());
				self.buf.extend(&bytes[..]);
			} else {
				self.read_all = true;
			}
		}
		if self.buf.len() == 0 {
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

pub fn put_response(version_uuid: UUID, md5sum_hex: String) -> Response<Body> {
	Response::builder()
		.header("x-amz-version-id", hex::encode(version_uuid))
		.header("ETag", format!("\"{}\"", md5sum_hex))
		.body(Body::from(vec![]))
		.unwrap()
}

pub async fn handle_create_multipart_upload(
	garage: Arc<Garage>,
	req: &Request<Body>,
	bucket: &str,
	key: &str,
) -> Result<Response<Body>, Error> {
	let version_uuid = gen_uuid();
	let headers = get_headers(req)?;

	let object_version = ObjectVersion {
		uuid: version_uuid,
		timestamp: now_msec(),
		state: ObjectVersionState::Uploading(headers),
	};
	let object = Object::new(bucket.to_string(), key.to_string(), vec![object_version]);
	garage.object_table.insert(&object).await?;

	let mut xml = String::new();
	writeln!(&mut xml, r#"<?xml version="1.0" encoding="UTF-8"?>"#).unwrap();
	writeln!(
		&mut xml,
		r#"<InitiateMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#
	)
	.unwrap();
	writeln!(&mut xml, "\t<Bucket>{}</Bucket>", bucket).unwrap();
	writeln!(&mut xml, "\t<Key>{}</Key>", xml_escape(key)).unwrap();
	writeln!(
		&mut xml,
		"\t<UploadId>{}</UploadId>",
		hex::encode(version_uuid)
	)
	.unwrap();
	writeln!(&mut xml, "</InitiateMultipartUploadResult>").unwrap();

	Ok(Response::new(Body::from(xml.into_bytes())))
}

pub async fn handle_put_part(
	garage: Arc<Garage>,
	req: Request<Body>,
	bucket: &str,
	key: &str,
	part_number_str: &str,
	upload_id: &str,
	content_sha256: Option<Hash>,
) -> Result<Response<Body>, Error> {
	// Check parameters
	let part_number = part_number_str
		.parse::<u64>()
		.ok_or_bad_request("Invalid part number")?;

	let version_uuid = decode_upload_id(upload_id)?;

	let content_md5 = match req.headers().get("content-md5") {
		Some(x) => Some(x.to_str()?.to_string()),
		None => None,
	};

	// Read first chuck, and at the same time try to get object to see if it exists
	let bucket = bucket.to_string();
	let key = key.to_string();
	let mut chunker = BodyChunker::new(req.into_body(), garage.config.block_size);

	let (object, first_block) =
		futures::try_join!(garage.object_table.get(&bucket, &key), chunker.next(),)?;

	// Check object is valid and multipart block can be accepted
	let first_block = first_block.ok_or(Error::BadRequest(format!("Empty body")))?;
	let object = object.ok_or(Error::BadRequest(format!("Object not found")))?;

	if !object
		.versions()
		.iter()
		.any(|v| v.uuid == version_uuid && v.is_uploading())
	{
		return Err(Error::BadRequest(format!(
			"Multipart upload does not exist or is otherwise invalid"
		)));
	}

	// Copy block to store
	let version = Version::new(version_uuid, bucket, key, false, vec![]);
	let first_block_hash = hash(&first_block[..]);
	let (_, md5sum_arr, sha256sum) = read_and_put_blocks(
		&garage,
		version,
		part_number,
		first_block,
		first_block_hash,
		&mut chunker,
	)
	.await?;

	ensure_checksum_matches(
		md5sum_arr.as_slice(),
		sha256sum,
		content_md5.as_deref(),
		content_sha256,
	)?;

	let response = Response::builder()
		.header("ETag", format!("\"{}\"", hex::encode(md5sum_arr)))
		.body(Body::from(vec![]))
		.unwrap();
	Ok(response)
}

pub async fn handle_complete_multipart_upload(
	garage: Arc<Garage>,
	_req: Request<Body>,
	bucket: &str,
	key: &str,
	upload_id: &str,
) -> Result<Response<Body>, Error> {
	let version_uuid = decode_upload_id(upload_id)?;

	let bucket = bucket.to_string();
	let key = key.to_string();
	let (object, version) = futures::try_join!(
		garage.object_table.get(&bucket, &key),
		garage.version_table.get(&version_uuid, &EmptyKey),
	)?;

	let object = object.ok_or(Error::BadRequest(format!("Object not found")))?;
	let object_version = object
		.versions()
		.iter()
		.find(|v| v.uuid == version_uuid && v.is_uploading());
	let mut object_version = match object_version {
		None => {
			return Err(Error::BadRequest(format!(
				"Multipart upload does not exist or has already been completed"
			)))
		}
		Some(x) => x.clone(),
	};

	let version = version.ok_or(Error::BadRequest(format!("Version not found")))?;
	if version.blocks().len() == 0 {
		return Err(Error::BadRequest(format!("No data was uploaded")));
	}

	let headers = match object_version.state {
		ObjectVersionState::Uploading(headers) => headers.clone(),
		_ => unreachable!(),
	};

	// ETag calculation: we produce ETags that have the same form as
	// those of S3 multipart uploads, but we don't use their actual
	// calculation for the first part (we use random bytes). This
	// shouldn't impact compatibility as the S3 docs specify that
	// the ETag is an opaque value in case of a multipart upload.
	// See also: https://teppen.io/2018/06/23/aws_s3_etags/
	let num_parts = version.blocks().last().unwrap().part_number
		- version.blocks().first().unwrap().part_number
		+ 1;
	let etag = format!(
		"{}-{}",
		hex::encode(&rand::random::<[u8; 16]>()[..]),
		num_parts
	);

	// TODO: check that all the parts that they pretend they gave us are indeed there
	// TODO: when we read the XML from _req, remember to check the sha256 sum of the payload
	//       against the signed x-amz-content-sha256
	// TODO: check MD5 sum of all uploaded parts? but that would mean we have to store them somewhere...

	let total_size = version
		.blocks()
		.iter()
		.map(|x| x.size)
		.fold(0, |x, y| x + y);
	object_version.state = ObjectVersionState::Complete(ObjectVersionData::FirstBlock(
		ObjectVersionMeta {
			headers,
			size: total_size,
			etag: etag,
		},
		version.blocks()[0].hash,
	));

	let final_object = Object::new(bucket.clone(), key.clone(), vec![object_version]);
	garage.object_table.insert(&final_object).await?;

	let mut xml = String::new();
	writeln!(&mut xml, r#"<?xml version="1.0" encoding="UTF-8"?>"#).unwrap();
	writeln!(
		&mut xml,
		r#"<CompleteMultipartUploadResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">"#
	)
	.unwrap();
	writeln!(
		&mut xml,
		"\t<Location>{}</Location>",
		garage.config.s3_api.s3_region
	)
	.unwrap();
	writeln!(&mut xml, "\t<Bucket>{}</Bucket>", bucket).unwrap();
	writeln!(&mut xml, "\t<Key>{}</Key>", xml_escape(&key)).unwrap();
	writeln!(&mut xml, "</CompleteMultipartUploadResult>").unwrap();

	Ok(Response::new(Body::from(xml.into_bytes())))
}

pub async fn handle_abort_multipart_upload(
	garage: Arc<Garage>,
	bucket: &str,
	key: &str,
	upload_id: &str,
) -> Result<Response<Body>, Error> {
	let version_uuid = decode_upload_id(upload_id)?;

	let object = garage
		.object_table
		.get(&bucket.to_string(), &key.to_string())
		.await?;
	let object = object.ok_or(Error::BadRequest(format!("Object not found")))?;

	let object_version = object
		.versions()
		.iter()
		.find(|v| v.uuid == version_uuid && v.is_uploading());
	let mut object_version = match object_version {
		None => {
			return Err(Error::BadRequest(format!(
				"Multipart upload does not exist or has already been completed"
			)))
		}
		Some(x) => x.clone(),
	};

	object_version.state = ObjectVersionState::Aborted;
	let final_object = Object::new(bucket.to_string(), key.to_string(), vec![object_version]);
	garage.object_table.insert(&final_object).await?;

	Ok(Response::new(Body::from(vec![])))
}

fn get_mime_type(req: &Request<Body>) -> Result<String, Error> {
	Ok(req
		.headers()
		.get(hyper::header::CONTENT_TYPE)
		.map(|x| x.to_str())
		.unwrap_or(Ok("blob"))?
		.to_string())
}

fn get_headers(req: &Request<Body>) -> Result<ObjectVersionHeaders, Error> {
	let content_type = get_mime_type(req)?;
	let other_headers = vec![
		hyper::header::CACHE_CONTROL,
		hyper::header::CONTENT_DISPOSITION,
		hyper::header::CONTENT_ENCODING,
		hyper::header::CONTENT_LANGUAGE,
		hyper::header::EXPIRES,
	];
	let mut other = BTreeMap::new();
	for h in other_headers.iter() {
		if let Some(v) = req.headers().get(h) {
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
	Ok(ObjectVersionHeaders {
		content_type,
		other,
	})
}

fn decode_upload_id(id: &str) -> Result<UUID, Error> {
	let id_bin = hex::decode(id).ok_or_bad_request("Invalid upload ID")?;
	if id_bin.len() != 32 {
		return None.ok_or_bad_request("Invalid upload ID");
	}
	let mut uuid = [0u8; 32];
	uuid.copy_from_slice(&id_bin[..]);
	Ok(UUID::from(uuid))
}
