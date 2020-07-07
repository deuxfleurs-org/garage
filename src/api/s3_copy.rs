use std::fmt::Write;
use std::sync::Arc;

use chrono::{SecondsFormat, Utc};
use hyper::Response;

use garage_table::*;
use garage_util::data::*;
use garage_util::error::Error;

use garage_model::block_ref_table::*;
use garage_model::garage::Garage;
use garage_model::object_table::*;
use garage_model::version_table::*;

use crate::http_util::*;

pub async fn handle_copy(
	garage: Arc<Garage>,
	dest_bucket: &str,
	dest_key: &str,
	source_bucket: &str,
	source_key: &str,
) -> Result<Response<BodyType>, Error> {
	let source_object = match garage
		.object_table
		.get(&source_bucket.to_string(), &source_key.to_string())
		.await?
	{
		None => return Err(Error::NotFound),
		Some(o) => o,
	};

	let source_last_v = match source_object
		.versions()
		.iter()
		.rev()
		.filter(|v| v.is_complete())
		.next()
	{
		Some(v) => v,
		None => return Err(Error::NotFound),
	};

	let new_uuid = gen_uuid();
	let dest_object_version = ObjectVersion {
		uuid: new_uuid,
		timestamp: now_msec(),
		mime_type: source_last_v.mime_type.clone(),
		size: source_last_v.size,
		state: ObjectVersionState::Complete,
		data: source_last_v.data.clone(),
	};

	match &source_last_v.data {
		ObjectVersionData::Uploading => {
			return Err(Error::Message(format!(
				"Version is_complete() but data is stil Uploading (internal error)"
			)));
		}
		ObjectVersionData::DeleteMarker => {
			return Err(Error::NotFound);
		}
		ObjectVersionData::Inline(_bytes) => {
			let dest_object = Object::new(
				dest_bucket.to_string(),
				dest_key.to_string(),
				vec![dest_object_version],
			);
			garage.object_table.insert(&dest_object).await?;
		}
		ObjectVersionData::FirstBlock(_first_block_hash) => {
			let source_version = garage
				.version_table
				.get(&source_last_v.uuid, &EmptyKey)
				.await?;
			let source_version = match source_version {
				Some(v) => v,
				None => return Err(Error::NotFound),
			};

			let dest_version = Version::new(
				new_uuid,
				dest_bucket.to_string(),
				dest_key.to_string(),
				false,
				source_version.blocks().to_vec(),
			);
			let dest_object = Object::new(
				dest_bucket.to_string(),
				dest_key.to_string(),
				vec![dest_object_version],
			);
			let dest_block_refs = dest_version
				.blocks()
				.iter()
				.map(|b| BlockRef {
					block: b.hash,
					version: new_uuid,
					deleted: false,
				})
				.collect::<Vec<_>>();
			futures::try_join!(
				garage.object_table.insert(&dest_object),
				garage.version_table.insert(&dest_version),
				garage.block_ref_table.insert_many(&dest_block_refs[..]),
			)?;
		}
	}

	let now = Utc::now();
	let last_modified = now.to_rfc3339_opts(SecondsFormat::Secs, true);
	let mut xml = String::new();
	writeln!(&mut xml, r#"<?xml version="1.0" encoding="UTF-8"?>"#).unwrap();
	writeln!(&mut xml, r#"<CopyObjectResult>"#).unwrap();
	writeln!(&mut xml, "\t<LastModified>{}</LastModified>", last_modified).unwrap();
	writeln!(&mut xml, "</CopyObjectResult>").unwrap();

	Ok(Response::new(Box::new(BytesBody::from(xml.into_bytes()))))
}
