use std::sync::Arc;

use garage_util::data::*;
use garage_util::error::Error as GarageError;
use garage_util::time::now_msec;

use garage_table::EmptyKey;

use garage_model::garage::Garage;
use garage_model::s3::object_table::*;
use garage_model::s3::version_table::*;

use garage_api_common::common_error::CommonErrorDerivative;

use crate::api::*;
use crate::error::*;
use crate::{Admin, RequestHandler};

impl RequestHandler for LocalListBlockErrorsRequest {
	type Response = LocalListBlockErrorsResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<LocalListBlockErrorsResponse, Error> {
		let errors = garage.block_manager.list_resync_errors()?;
		let now = now_msec();
		let errors = errors
			.into_iter()
			.map(|e| BlockError {
				block_hash: hex::encode(&e.hash),
				refcount: e.refcount,
				error_count: e.error_count,
				last_try_secs_ago: now.saturating_sub(e.last_try) / 1000,
				next_try_in_secs: e.next_try.saturating_sub(now) / 1000,
			})
			.collect();
		Ok(LocalListBlockErrorsResponse(errors))
	}
}

impl RequestHandler for LocalGetBlockInfoRequest {
	type Response = LocalGetBlockInfoResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<LocalGetBlockInfoResponse, Error> {
		let hash = find_block_hash_by_prefix(garage, &self.block_hash)?;
		let refcount = garage.block_manager.get_block_rc(&hash)?;
		let block_refs = garage
			.block_ref_table
			.get_range(&hash, None, None, 10000, Default::default())
			.await?;
		let mut versions = vec![];
		for br in block_refs {
			if let Some(v) = garage.version_table.get(&br.version, &EmptyKey).await? {
				let bl = match &v.backlink {
					VersionBacklink::MultipartUpload { upload_id } => {
						if let Some(u) = garage.mpu_table.get(upload_id, &EmptyKey).await? {
							BlockVersionBacklink::Upload {
								upload_id: hex::encode(&upload_id),
								upload_deleted: u.deleted.get(),
								upload_garbage_collected: false,
								bucket_id: Some(hex::encode(&u.bucket_id)),
								key: Some(u.key.to_string()),
							}
						} else {
							BlockVersionBacklink::Upload {
								upload_id: hex::encode(&upload_id),
								upload_deleted: true,
								upload_garbage_collected: true,
								bucket_id: None,
								key: None,
							}
						}
					}
					VersionBacklink::Object { bucket_id, key } => BlockVersionBacklink::Object {
						bucket_id: hex::encode(&bucket_id),
						key: key.to_string(),
					},
				};
				versions.push(BlockVersion {
					version_id: hex::encode(&br.version),
					ref_deleted: br.deleted.get(),
					version_deleted: v.deleted.get(),
					garbage_collected: false,
					backlink: Some(bl),
				});
			} else {
				versions.push(BlockVersion {
					version_id: hex::encode(&br.version),
					ref_deleted: br.deleted.get(),
					version_deleted: true,
					garbage_collected: true,
					backlink: None,
				});
			}
		}
		Ok(LocalGetBlockInfoResponse {
			block_hash: hex::encode(&hash),
			refcount,
			versions,
		})
	}
}

impl RequestHandler for LocalRetryBlockResyncRequest {
	type Response = LocalRetryBlockResyncResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<LocalRetryBlockResyncResponse, Error> {
		match self {
			Self::All { all: true } => {
				let blocks = garage.block_manager.list_resync_errors()?;
				for b in blocks.iter() {
					garage.block_manager.resync.clear_backoff(&b.hash)?;
				}
				Ok(LocalRetryBlockResyncResponse {
					count: blocks.len() as u64,
				})
			}
			Self::All { all: false } => Err(Error::bad_request("nonsense")),
			Self::Blocks { block_hashes } => {
				for hash in block_hashes.iter() {
					let hash = hex::decode(hash).ok_or_bad_request("invalid hash")?;
					let hash = Hash::try_from(&hash).ok_or_bad_request("invalid hash")?;
					garage.block_manager.resync.clear_backoff(&hash)?;
				}
				Ok(LocalRetryBlockResyncResponse {
					count: block_hashes.len() as u64,
				})
			}
		}
	}
}

impl RequestHandler for LocalPurgeBlocksRequest {
	type Response = LocalPurgeBlocksResponse;

	async fn handle(
		self,
		garage: &Arc<Garage>,
		_admin: &Admin,
	) -> Result<LocalPurgeBlocksResponse, Error> {
		let mut obj_dels = 0;
		let mut mpu_dels = 0;
		let mut ver_dels = 0;
		let mut br_dels = 0;

		for hash in self.0.iter() {
			let hash = hex::decode(hash).ok_or_bad_request("invalid hash")?;
			let hash = Hash::try_from(&hash).ok_or_bad_request("invalid hash")?;
			let block_refs = garage
				.block_ref_table
				.get_range(&hash, None, None, 10000, Default::default())
				.await?;

			for br in block_refs {
				if let Some(version) = garage.version_table.get(&br.version, &EmptyKey).await? {
					handle_block_purge_version_backlink(
						garage,
						&version,
						&mut obj_dels,
						&mut mpu_dels,
					)
					.await?;

					if !version.deleted.get() {
						let deleted_version = Version::new(version.uuid, version.backlink, true);
						garage.version_table.insert(&deleted_version).await?;
						ver_dels += 1;
					}
				}
				if !br.deleted.get() {
					let mut br = br;
					br.deleted.set();
					garage.block_ref_table.insert(&br).await?;
					br_dels += 1;
				}
			}
		}

		Ok(LocalPurgeBlocksResponse {
			blocks_purged: self.0.len() as u64,
			block_refs_purged: br_dels,
			versions_deleted: ver_dels,
			objects_deleted: obj_dels,
			uploads_deleted: mpu_dels,
		})
	}
}

fn find_block_hash_by_prefix(garage: &Arc<Garage>, prefix: &str) -> Result<Hash, Error> {
	if prefix.len() < 4 {
		return Err(Error::bad_request(
			"Please specify at least 4 characters of the block hash",
		));
	}

	let prefix_bin = hex::decode(&prefix[..prefix.len() & !1]).ok_or_bad_request("invalid hash")?;

	let iter = garage
		.block_ref_table
		.data
		.store
		.range(&prefix_bin[..]..)
		.map_err(GarageError::from)?;
	let mut found = None;
	for item in iter {
		let (k, _v) = item.map_err(GarageError::from)?;
		let hash = Hash::try_from(&k[..32]).unwrap();
		if &hash.as_slice()[..prefix_bin.len()] != prefix_bin {
			break;
		}
		if hex::encode(hash.as_slice()).starts_with(prefix) {
			match &found {
				Some(x) if *x == hash => (),
				Some(_) => {
					return Err(Error::bad_request(format!(
						"Several blocks match prefix `{}`",
						prefix
					)));
				}
				None => {
					found = Some(hash);
				}
			}
		}
	}

	found.ok_or_else(|| Error::NoSuchBlock(prefix.to_string()))
}

async fn handle_block_purge_version_backlink(
	garage: &Arc<Garage>,
	version: &Version,
	obj_dels: &mut u64,
	mpu_dels: &mut u64,
) -> Result<(), Error> {
	let (bucket_id, key, ov_id) = match &version.backlink {
		VersionBacklink::Object { bucket_id, key } => (*bucket_id, key.clone(), version.uuid),
		VersionBacklink::MultipartUpload { upload_id } => {
			if let Some(mut mpu) = garage.mpu_table.get(upload_id, &EmptyKey).await? {
				if !mpu.deleted.get() {
					mpu.parts.clear();
					mpu.deleted.set();
					garage.mpu_table.insert(&mpu).await?;
					*mpu_dels += 1;
				}
				(mpu.bucket_id, mpu.key.clone(), *upload_id)
			} else {
				return Ok(());
			}
		}
	};

	if let Some(object) = garage.object_table.get(&bucket_id, &key).await? {
		let ov = object.versions().iter().rev().find(|v| v.is_complete());
		if let Some(ov) = ov {
			if ov.uuid == ov_id {
				let del_uuid = gen_uuid();
				let deleted_object = Object::new(
					bucket_id,
					key,
					vec![ObjectVersion {
						uuid: del_uuid,
						timestamp: ov.timestamp + 1,
						state: ObjectVersionState::Complete(ObjectVersionData::DeleteMarker),
					}],
				);
				garage.object_table.insert(&deleted_object).await?;
				*obj_dels += 1;
			}
		}
	}

	Ok(())
}
