use std::fmt::Write;
use std::sync::Arc;

use chrono::{SecondsFormat, Utc};
use hyper::{Body, Response};

use garage_table::*;
use garage_util::data::*;

use garage_model::block_ref_table::*;
use garage_model::garage::Garage;
use garage_model::object_table::*;
use garage_model::version_table::*;

use crate::error::*;

pub async fn handle_copy(
	garage: Arc<Garage>,
	dest_bucket: &str,
	dest_key: &str,
	source_bucket: &str,
	source_key: &str,
) -> Result<Response<Body>, Error> {
	let source_object = garage
		.object_table
		.get(&source_bucket.to_string(), &source_key.to_string())
		.await?
		.ok_or(Error::NotFound)?;

	let source_last_v = source_object
		.versions()
		.iter()
		.rev()
		.filter(|v| v.is_complete())
		.next()
		.ok_or(Error::NotFound)?;

	let source_last_state = match &source_last_v.state {
		ObjectVersionState::Complete(x) => x,
		_ => unreachable!(),
	};

	let new_uuid = gen_uuid();
	let new_timestamp = now_msec();
	let dest_object_version = ObjectVersion {
		uuid: new_uuid,
		timestamp: new_timestamp,
		state: ObjectVersionState::Complete(source_last_state.clone()),
	};
	let dest_object = Object::new(
		dest_bucket.to_string(),
		dest_key.to_string(),
		vec![dest_object_version],
	);

	match source_last_state {
		ObjectVersionData::DeleteMarker => {
			return Err(Error::NotFound);
		}
		ObjectVersionData::Inline(_meta, _bytes) => {
			garage.object_table.insert(&dest_object).await?;
		}
		ObjectVersionData::FirstBlock(meta, _first_block_hash) => {
			// Get block list from source version
			let source_version = garage
				.version_table
				.get(&source_last_v.uuid, &EmptyKey)
				.await?;
			let source_version = source_version.ok_or(Error::NotFound)?;

			// Write an "uploading" marker in Object table
			// This holds a reference to the object in the Version table
			// so that it won't be deleted, e.g. by repair_versions.
			let tmp_dest_object_version = ObjectVersion {
				uuid: new_uuid,
				timestamp: new_timestamp,
				state: ObjectVersionState::Uploading(meta.headers.clone()),
			};
			let tmp_dest_object = Object::new(
				dest_bucket.to_string(),
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
				dest_bucket.to_string(),
				dest_key.to_string(),
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
			garage.object_table.insert(&dest_object).await?;
		}
	}

	let now = Utc::now(); // FIXME use the unix timestamp from above
	let last_modified = now.to_rfc3339_opts(SecondsFormat::Secs, true);
	let mut xml = String::new();
	writeln!(&mut xml, r#"<?xml version="1.0" encoding="UTF-8"?>"#).unwrap();
	writeln!(&mut xml, r#"<CopyObjectResult>"#).unwrap();
	writeln!(&mut xml, "\t<LastModified>{}</LastModified>", last_modified).unwrap();
	writeln!(&mut xml, "</CopyObjectResult>").unwrap();

	Ok(Response::builder()
		.header("Content-Type", "application/xml")
		.body(Body::from(xml.into_bytes()))?)
}
