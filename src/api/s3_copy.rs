use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use futures::TryFutureExt;
use md5::{Digest as Md5Digest, Md5};

use hyper::{Body, Request, Response};
use serde::Serialize;

use garage_table::*;
use garage_util::data::*;
use garage_util::time::*;

use garage_model::block_ref_table::*;
use garage_model::garage::Garage;
use garage_model::key_table::Key;
use garage_model::object_table::*;
use garage_model::version_table::*;

use crate::api_server::{parse_bucket_key, resolve_bucket};
use crate::error::*;
use crate::s3_put::{decode_upload_id, get_headers};
use crate::s3_xml::{self, xmlns_tag};

pub async fn handle_copy(
	garage: Arc<Garage>,
	api_key: &Key,
	req: &Request<Body>,
	dest_bucket_id: Uuid,
	dest_key: &str,
) -> Result<Response<Body>, Error> {
	let copy_precondition = CopyPreconditionHeaders::parse(req)?;

	let source_object = get_copy_source(&garage, api_key, req).await?;

	let (source_version, source_version_data, source_version_meta) =
		extract_source_info(&source_object)?;

	// Check precondition, e.g. x-amz-copy-source-if-match
	copy_precondition.check(source_version, &source_version_meta.etag)?;

	// Generate parameters for copied object
	let new_uuid = gen_uuid();
	let new_timestamp = now_msec();

	// Implement x-amz-metadata-directive: REPLACE
	let new_meta = match req.headers().get("x-amz-metadata-directive") {
		Some(v) if v == hyper::header::HeaderValue::from_static("REPLACE") => ObjectVersionMeta {
			headers: get_headers(req)?,
			size: source_version_meta.size,
			etag: source_version_meta.etag.clone(),
		},
		_ => source_version_meta.clone(),
	};

	let etag = new_meta.etag.to_string();

	// Save object copy
	match source_version_data {
		ObjectVersionData::DeleteMarker => unreachable!(),
		ObjectVersionData::Inline(_meta, bytes) => {
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

			// Write an "uploading" marker in Object table
			// This holds a reference to the object in the Version table
			// so that it won't be deleted, e.g. by repair_versions.
			let tmp_dest_object_version = ObjectVersion {
				uuid: new_uuid,
				timestamp: new_timestamp,
				state: ObjectVersionState::Uploading(new_meta.headers.clone()),
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
			let mut dest_version =
				Version::new(new_uuid, dest_bucket_id, dest_key.to_string(), false);
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

	let last_modified = msec_to_rfc3339(new_timestamp);
	let result = CopyObjectResult {
		last_modified: s3_xml::Value(last_modified),
		etag: s3_xml::Value(format!("\"{}\"", etag)),
	};
	let xml = s3_xml::to_xml_with_header(&result)?;

	Ok(Response::builder()
		.header("Content-Type", "application/xml")
		.header("x-amz-version-id", hex::encode(new_uuid))
		.header(
			"x-amz-copy-source-version-id",
			hex::encode(source_version.uuid),
		)
		.body(Body::from(xml))?)
}

pub async fn handle_upload_part_copy(
	garage: Arc<Garage>,
	api_key: &Key,
	req: &Request<Body>,
	dest_bucket_id: Uuid,
	dest_key: &str,
	part_number: u64,
	upload_id: &str,
) -> Result<Response<Body>, Error> {
	let copy_precondition = CopyPreconditionHeaders::parse(req)?;

	let dest_version_uuid = decode_upload_id(upload_id)?;

	let dest_key = dest_key.to_string();
	let (source_object, dest_object) = futures::try_join!(
		get_copy_source(&garage, api_key, req),
		garage
			.object_table
			.get(&dest_bucket_id, &dest_key)
			.map_err(Error::from),
	)?;
	let dest_object = dest_object.ok_or(Error::NoSuchKey)?;

	let (source_object_version, source_version_data, source_version_meta) =
		extract_source_info(&source_object)?;

	// Check precondition on source, e.g. x-amz-copy-source-if-match
	copy_precondition.check(source_object_version, &source_version_meta.etag)?;

	// Check source range is valid
	let source_range = match req.headers().get("x-amz-copy-source-range") {
		Some(range) => {
			let range_str = range.to_str()?;
			let mut ranges = http_range::HttpRange::parse(range_str, source_version_meta.size)
				.map_err(|e| (e, source_version_meta.size))?;
			if ranges.len() != 1 {
				return Err(Error::BadRequest(
					"Invalid x-amz-copy-source-range header: exactly 1 range must be given".into(),
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

	// Check destination version is indeed in uploading state
	if !dest_object
		.versions()
		.iter()
		.any(|v| v.uuid == dest_version_uuid && v.is_uploading())
	{
		return Err(Error::NoSuchUpload);
	}

	// Check source version is not inlined
	match source_version_data {
		ObjectVersionData::DeleteMarker => unreachable!(),
		ObjectVersionData::Inline(_meta, _bytes) => {
			// This is only for small files, we don't bother handling this.
			// (in AWS UploadPartCopy works for parts at least 5MB which
			// is never the case of an inline object)
			return Err(Error::BadRequest(
				"Source object is too small (minimum part size is 5Mb)".into(),
			));
		}
		ObjectVersionData::FirstBlock(_meta, _first_block_hash) => (),
	};

	// Fetch source versin with its block list,
	// and destination version to check part hasn't yet been uploaded
	let (source_version, dest_version) = futures::try_join!(
		garage
			.version_table
			.get(&source_object_version.uuid, &EmptyKey),
		garage.version_table.get(&dest_version_uuid, &EmptyKey),
	)?;
	let source_version = source_version.ok_or(Error::NoSuchKey)?;

	// Check this part number hasn't yet been uploaded
	if let Some(dv) = dest_version {
		if dv.has_part_number(part_number) {
			return Err(Error::BadRequest(format!(
				"Part number {} has already been uploaded",
				part_number
			)));
		}
	}

	// We want to reuse blocks from the source version as much as possible.
	// However, we still need to get the data from these blocks
	// because we need to know it to calculate the MD5sum of the part
	// which is used as its ETag.

	// First, calculate what blocks we want to keep,
	// and the subrange of the block to take, if the bounds of the
	// requested range are in the middle.
	let (range_begin, range_end) = (source_range.start, source_range.start + source_range.length);

	let mut blocks_to_copy = vec![];
	let mut current_offset = 0;
	let mut size_to_copy = 0;
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
			size_to_copy += range_to_copy
				.as_ref()
				.map(|x| x.len() as u64)
				.unwrap_or(block.size);

			blocks_to_copy.push((block.hash, range_to_copy));
		}

		current_offset = block_end;
	}

	if size_to_copy < 1024 * 1024 {
		return Err(Error::BadRequest(format!(
			"Not enough data to copy: {} bytes (minimum: 1MB)",
			size_to_copy
		)));
	}

	// Now, actually copy the blocks
	let mut md5hasher = Md5::new();

	let mut block = Some(
		garage
			.block_manager
			.rpc_get_block(&blocks_to_copy[0].0)
			.await?,
	);

	let mut current_offset = 0;
	for (i, (block_hash, range_to_copy)) in blocks_to_copy.iter().enumerate() {
		let (current_block, subrange_hash) = match range_to_copy.clone() {
			Some(r) => {
				let subrange = block.take().unwrap()[r].to_vec();
				let hash = blake2sum(&subrange);
				(subrange, hash)
			}
			None => (block.take().unwrap(), *block_hash),
		};
		md5hasher.update(&current_block[..]);

		let mut version = Version::new(dest_version_uuid, dest_bucket_id, dest_key.clone(), false);
		version.blocks.put(
			VersionBlockKey {
				part_number,
				offset: current_offset,
			},
			VersionBlock {
				hash: subrange_hash,
				size: current_block.len() as u64,
			},
		);
		current_offset += current_block.len() as u64;

		let block_ref = BlockRef {
			block: subrange_hash,
			version: dest_version_uuid,
			deleted: false.into(),
		};

		let next_block_hash = blocks_to_copy.get(i + 1).map(|(h, _)| *h);

		let garage2 = garage.clone();
		let garage3 = garage.clone();
		let is_subrange = range_to_copy.is_some();

		let (_, _, _, next_block) = futures::try_join!(
			// Thing 1: if we are taking a subrange of the source block,
			// we need to insert that subrange as a new block.
			async move {
				if is_subrange {
					garage2
						.block_manager
						.rpc_put_block(subrange_hash, current_block)
						.await
				} else {
					Ok(())
				}
			},
			// Thing 2: we need to insert the block in the version
			garage.version_table.insert(&version),
			// Thing 3: we need to add a block reference
			garage.block_ref_table.insert(&block_ref),
			// Thing 4: we need to prefetch the next block
			async move {
				match next_block_hash {
					Some(h) => Ok(Some(garage3.block_manager.rpc_get_block(&h).await?)),
					None => Ok(None),
				}
			},
		)?;

		block = next_block;
	}

	let data_md5sum = md5hasher.finalize();
	let etag = hex::encode(data_md5sum);

	// Put the part's ETag in the Versiontable
	let mut version = Version::new(dest_version_uuid, dest_bucket_id, dest_key.clone(), false);
	version.parts_etags.put(part_number, etag.clone());
	garage.version_table.insert(&version).await?;

	// LGTM
	let resp_xml = s3_xml::to_xml_with_header(&CopyPartResult {
		xmlns: (),
		etag: s3_xml::Value(format!("\"{}\"", etag)),
		last_modified: s3_xml::Value(msec_to_rfc3339(source_object_version.timestamp)),
	})?;

	Ok(Response::builder()
		.header("Content-Type", "application/xml")
		.header(
			"x-amz-copy-source-version-id",
			hex::encode(source_object_version.uuid),
		)
		.body(Body::from(resp_xml))?)
}

async fn get_copy_source(
	garage: &Garage,
	api_key: &Key,
	req: &Request<Body>,
) -> Result<Object, Error> {
	let copy_source = req.headers().get("x-amz-copy-source").unwrap().to_str()?;
	let copy_source = percent_encoding::percent_decode_str(copy_source).decode_utf8()?;

	let (source_bucket, source_key) = parse_bucket_key(&copy_source, None)?;
	let source_bucket_id = resolve_bucket(garage, &source_bucket.to_string(), api_key).await?;

	if !api_key.allow_read(&source_bucket_id) {
		return Err(Error::Forbidden(format!(
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

struct CopyPreconditionHeaders {
	copy_source_if_match: Option<Vec<String>>,
	copy_source_if_modified_since: Option<SystemTime>,
	copy_source_if_none_match: Option<Vec<String>>,
	copy_source_if_unmodified_since: Option<SystemTime>,
}

impl CopyPreconditionHeaders {
	fn parse(req: &Request<Body>) -> Result<Self, Error> {
		Ok(Self {
			copy_source_if_match: req
				.headers()
				.get("x-amz-copy-source-if-match")
				.map(|x| x.to_str())
				.transpose()?
				.map(|x| {
					x.split(',')
						.map(|m| m.trim().trim_matches('"').to_string())
						.collect::<Vec<_>>()
				}),
			copy_source_if_modified_since: req
				.headers()
				.get("x-amz-copy-source-if-modified-since")
				.map(|x| x.to_str())
				.transpose()?
				.map(httpdate::parse_http_date)
				.transpose()
				.ok_or_bad_request("Invalid date in x-amz-copy-source-if-modified-since")?,
			copy_source_if_none_match: req
				.headers()
				.get("x-amz-copy-source-if-none-match")
				.map(|x| x.to_str())
				.transpose()?
				.map(|x| {
					x.split(',')
						.map(|m| m.trim().trim_matches('"').to_string())
						.collect::<Vec<_>>()
				}),
			copy_source_if_unmodified_since: req
				.headers()
				.get("x-amz-copy-source-if-unmodified-since")
				.map(|x| x.to_str())
				.transpose()?
				.map(httpdate::parse_http_date)
				.transpose()
				.ok_or_bad_request("Invalid date in x-amz-copy-source-if-unmodified-since")?,
		})
	}

	fn check(&self, v: &ObjectVersion, etag: &str) -> Result<(), Error> {
		let v_date = UNIX_EPOCH + Duration::from_millis(v.timestamp);

		let ok = match (
			&self.copy_source_if_match,
			&self.copy_source_if_unmodified_since,
			&self.copy_source_if_none_match,
			&self.copy_source_if_modified_since,
		) {
			// TODO I'm not sure all of the conditions are evaluated correctly here

			// If we have both if-match and if-unmodified-since,
			// basically we don't care about if-unmodified-since,
			// because in the spec it says that if if-match evaluates to
			// true but if-unmodified-since evaluates to false,
			// the copy is still done.
			(Some(im), _, None, None) => im.iter().any(|x| x == etag || x == "*"),
			(None, Some(ius), None, None) => v_date <= *ius,

			// If we have both if-none-match and if-modified-since,
			// then both of the two conditions must evaluate to true
			(None, None, Some(inm), Some(ims)) => {
				!inm.iter().any(|x| x == etag || x == "*") && v_date > *ims
			}
			(None, None, Some(inm), None) => !inm.iter().any(|x| x == etag || x == "*"),
			(None, None, None, Some(ims)) => v_date > *ims,
			(None, None, None, None) => true,
			_ => {
				return Err(Error::BadRequest(
					"Invalid combination of x-amz-copy-source-if-xxxxx headers".into(),
				))
			}
		};

		if ok {
			Ok(())
		} else {
			Err(Error::PreconditionFailed)
		}
	}
}

#[derive(Debug, Serialize, PartialEq)]
pub struct CopyObjectResult {
	#[serde(rename = "LastModified")]
	pub last_modified: s3_xml::Value,
	#[serde(rename = "ETag")]
	pub etag: s3_xml::Value,
}

#[derive(Debug, Serialize, PartialEq)]
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
	use crate::s3_xml::to_xml_with_header;

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
		println!("{}", to_xml_with_header(&v)?);

		assert_eq!(to_xml_with_header(&v)?, expected_retval);

		Ok(())
	}
}
