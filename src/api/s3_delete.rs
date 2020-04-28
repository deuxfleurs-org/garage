use std::sync::Arc;

use garage_util::data::*;
use garage_util::error::Error;

use garage_core::garage::Garage;
use garage_core::object_table::*;

pub async fn handle_delete(garage: Arc<Garage>, bucket: &str, key: &str) -> Result<UUID, Error> {
	let object = match garage
		.object_table
		.get(&bucket.to_string(), &key.to_string())
		.await?
	{
		None => {
			// No need to delete
			return Ok([0u8; 32].into());
		}
		Some(o) => o,
	};

	let interesting_versions = object.versions().iter().filter(|v| {
		v.data != ObjectVersionData::DeleteMarker && v.state != ObjectVersionState::Aborted
	});

	let mut must_delete = false;
	let mut timestamp = now_msec();
	for v in interesting_versions {
		must_delete = true;
		timestamp = std::cmp::max(timestamp, v.timestamp + 1);
	}

	if !must_delete {
		return Ok([0u8; 32].into());
	}

	let version_uuid = gen_uuid();

	let object = Object::new(
		bucket.into(),
		key.into(),
		vec![ObjectVersion {
			uuid: version_uuid,
			timestamp: now_msec(),
			mime_type: "application/x-delete-marker".into(),
			size: 0,
			state: ObjectVersionState::Complete,
			data: ObjectVersionData::DeleteMarker,
		}],
	);

	garage.object_table.insert(&object).await?;
	return Ok(version_uuid);
}
