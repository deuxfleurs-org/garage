use garage_util::data::*;

use garage_table::*;

use garage_model::helper::error::{Error, OkOrBadRequest};
use garage_model::s3::object_table::*;
use garage_model::s3::version_table::*;

use crate::cli::*;

use super::*;

impl AdminRpcHandler {
	pub(super) async fn handle_block_cmd(&self, cmd: &BlockOperation) -> Result<AdminRpc, Error> {
		match cmd {
			BlockOperation::ListErrors => Ok(AdminRpc::BlockErrorList(
				self.garage.block_manager.list_resync_errors()?,
			)),
			BlockOperation::Info { hash } => self.handle_block_info(hash).await,
			BlockOperation::RetryNow { all, blocks } => {
				self.handle_block_retry_now(*all, blocks).await
			}
			BlockOperation::Purge { yes, blocks } => self.handle_block_purge(*yes, blocks).await,
		}
	}

	async fn handle_block_info(&self, hash: &String) -> Result<AdminRpc, Error> {
		let hash = hex::decode(hash).ok_or_bad_request("invalid hash")?;
		let hash = Hash::try_from(&hash).ok_or_bad_request("invalid hash")?;
		let refcount = self.garage.block_manager.get_block_rc(&hash)?;
		let block_refs = self
			.garage
			.block_ref_table
			.get_range(&hash, None, None, 10000, Default::default())
			.await?;
		let mut versions = vec![];
		let mut uploads = vec![];
		for br in block_refs {
			if let Some(v) = self
				.garage
				.version_table
				.get(&br.version, &EmptyKey)
				.await?
			{
				if let VersionBacklink::MultipartUpload { upload_id } = &v.backlink {
					if let Some(u) = self.garage.mpu_table.get(upload_id, &EmptyKey).await? {
						uploads.push(u);
					}
				}
				versions.push(Ok(v));
			} else {
				versions.push(Err(br.version));
			}
		}
		Ok(AdminRpc::BlockInfo {
			hash,
			refcount,
			versions,
			uploads,
		})
	}

	async fn handle_block_retry_now(
		&self,
		all: bool,
		blocks: &[String],
	) -> Result<AdminRpc, Error> {
		if all {
			if !blocks.is_empty() {
				return Err(Error::BadRequest(
					"--all was specified, cannot also specify blocks".into(),
				));
			}
			let blocks = self.garage.block_manager.list_resync_errors()?;
			for b in blocks.iter() {
				self.garage.block_manager.resync.clear_backoff(&b.hash)?;
			}
			Ok(AdminRpc::Ok(format!(
				"{} blocks returned in queue for a retry now (check logs to see results)",
				blocks.len()
			)))
		} else {
			for hash in blocks {
				let hash = hex::decode(hash).ok_or_bad_request("invalid hash")?;
				let hash = Hash::try_from(&hash).ok_or_bad_request("invalid hash")?;
				self.garage.block_manager.resync.clear_backoff(&hash)?;
			}
			Ok(AdminRpc::Ok(format!(
				"{} blocks returned in queue for a retry now (check logs to see results)",
				blocks.len()
			)))
		}
	}

	async fn handle_block_purge(&self, yes: bool, blocks: &[String]) -> Result<AdminRpc, Error> {
		if !yes {
			return Err(Error::BadRequest(
				"Pass the --yes flag to confirm block purge operation.".into(),
			));
		}

		let mut obj_dels = 0;
		let mut mpu_dels = 0;
		let mut ver_dels = 0;

		for hash in blocks {
			let hash = hex::decode(hash).ok_or_bad_request("invalid hash")?;
			let hash = Hash::try_from(&hash).ok_or_bad_request("invalid hash")?;
			let block_refs = self
				.garage
				.block_ref_table
				.get_range(&hash, None, None, 10000, Default::default())
				.await?;

			for br in block_refs {
				if let Some(version) = self
					.garage
					.version_table
					.get(&br.version, &EmptyKey)
					.await?
				{
                    self.handle_block_purge_version_backlink(&version, &mut obj_dels, &mut mpu_dels).await?;

                    if !version.deleted.get() {
                        let deleted_version =
                            Version::new(version.uuid, version.backlink, true);
                        self.garage.version_table.insert(&deleted_version).await?;
                        ver_dels += 1;
                    }
                }
            }
        }

		Ok(AdminRpc::Ok(format!(
			"Purged {} blocks, {} versions, {} objects, {} multipart uploads",
			blocks.len(),
			ver_dels,
			obj_dels,
            mpu_dels,
		)))
    }

    async fn handle_block_purge_version_backlink(&self, version: &Version, obj_dels: &mut usize, mpu_dels: &mut usize) -> Result<(), Error> {
        let (bucket_id, key, ov_id) = match &version.backlink {
            VersionBacklink::Object{bucket_id, key} => {
                (*bucket_id, key.clone(), version.uuid)
            }
            VersionBacklink::MultipartUpload{upload_id} => {
                if let Some(mut mpu) = self.garage.mpu_table.get(&upload_id, &EmptyKey).await? {
                    if !mpu.deleted.get() {
                        mpu.parts.clear();
                        mpu.deleted.set();
                        self.garage.mpu_table.insert(&mpu).await?;
                        *mpu_dels += 1;
                    }
                    (mpu.bucket_id, mpu.key.clone(), *upload_id)
                } else {
                    return Ok(());
                }
            }
        };

        if let Some(object) = self
            .garage
            .object_table
            .get(&bucket_id, &key)
            .await?
        {
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
                            state: ObjectVersionState::Complete(
                                ObjectVersionData::DeleteMarker,
                            ),
                        }],
                    );
                    self.garage.object_table.insert(&deleted_object).await?;
                    *obj_dels += 1;
                }
            }
        }

        Ok(())
	}

}
