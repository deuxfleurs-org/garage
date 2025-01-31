use std::sync::Arc;

use async_trait::async_trait;

use garage_util::data::*;
use garage_util::error::Error as GarageError;
use garage_util::time::now_msec;

use garage_table::EmptyKey;

use garage_model::garage::Garage;
use garage_model::s3::version_table::*;

use crate::admin::api::*;
use crate::admin::error::*;
use crate::admin::{Admin, RequestHandler};
use crate::common_error::CommonErrorDerivative;

#[async_trait]
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

#[async_trait]
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
					deleted: v.deleted.get(),
					garbage_collected: false,
					backlink: Some(bl),
				});
			} else {
				versions.push(BlockVersion {
					version_id: hex::encode(&br.version),
					deleted: true,
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
