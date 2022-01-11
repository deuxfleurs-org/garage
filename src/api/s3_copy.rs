use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hyper::{Body, Request, Response};

use garage_table::*;
use garage_util::data::*;
use garage_util::time::*;

use garage_model::block_ref_table::*;
use garage_model::garage::Garage;
use garage_model::object_table::*;
use garage_model::version_table::*;

use crate::error::*;
use crate::s3_put::get_headers;
use crate::s3_xml;

pub async fn handle_copy(
	garage: Arc<Garage>,
	req: &Request<Body>,
	dest_bucket_id: Uuid,
	dest_key: &str,
	source_bucket_id: Uuid,
	source_key: &str,
) -> Result<Response<Body>, Error> {
	let copy_precondition = CopyPreconditionHeaders::parse(req)?;

	let source_object = garage
		.object_table
		.get(&source_bucket_id, &source_key.to_string())
		.await?
		.ok_or(Error::NoSuchKey)?;

	let source_last_v = source_object
		.versions()
		.iter()
		.rev()
		.find(|v| v.is_complete())
		.ok_or(Error::NoSuchKey)?;

	let source_last_state = match &source_last_v.state {
		ObjectVersionState::Complete(x) => x,
		_ => unreachable!(),
	};

	let new_uuid = gen_uuid();
	let new_timestamp = now_msec();

	// Implement x-amz-metadata-directive: REPLACE
	let old_meta = match source_last_state {
		ObjectVersionData::DeleteMarker => {
			return Err(Error::NoSuchKey);
		}
		ObjectVersionData::Inline(meta, _bytes) => meta,
		ObjectVersionData::FirstBlock(meta, _fbh) => meta,
	};
	let new_meta = match req.headers().get("x-amz-metadata-directive") {
		Some(v) if v == hyper::header::HeaderValue::from_static("REPLACE") => ObjectVersionMeta {
			headers: get_headers(req)?,
			size: old_meta.size,
			etag: old_meta.etag.clone(),
		},
		_ => old_meta.clone(),
	};

	let etag = new_meta.etag.to_string();

	// Check precondition, e.g. x-amz-copy-source-if-match
	copy_precondition.check(source_last_v, etag.as_str())?;

	// Save object copy
	match source_last_state {
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
				.get(&source_last_v.uuid, &EmptyKey)
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
	let result = s3_xml::CopyObjectResult {
		last_modified: s3_xml::Value(last_modified),
		etag: s3_xml::Value(etag),
	};
	let xml = s3_xml::to_xml_with_header(&result)?;

	Ok(Response::builder()
		.header("Content-Type", "application/xml")
		.header("x-amz-version-id", hex::encode(new_uuid))
		.header(
			"x-amz-copy-source-version-id",
			hex::encode(source_last_v.uuid),
		)
		.body(Body::from(xml))?)
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
				.map(|x| httpdate::parse_http_date(x))
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
				.map(|x| httpdate::parse_http_date(x))
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
