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
		for br in block_refs {
			if let Some(v) = self
				.garage
				.version_table
				.get(&br.version, &EmptyKey)
				.await?
			{
				versions.push(Ok(v));
			} else {
				versions.push(Err(br.version));
			}
		}
		Ok(AdminRpc::BlockInfo {
			hash,
			refcount,
			versions,
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
				let version = match self
					.garage
					.version_table
					.get(&br.version, &EmptyKey)
					.await?
				{
					Some(v) => v,
					None => continue,
				};

				if let Some(object) = self
					.garage
					.object_table
					.get(&version.bucket_id, &version.key)
					.await?
				{
					let ov = object.versions().iter().rev().find(|v| v.is_complete());
					if let Some(ov) = ov {
						if ov.uuid == br.version {
							let del_uuid = gen_uuid();
							let deleted_object = Object::new(
								version.bucket_id,
								version.key.clone(),
								vec![ObjectVersion {
									uuid: del_uuid,
									timestamp: ov.timestamp + 1,
									state: ObjectVersionState::Complete(
										ObjectVersionData::DeleteMarker,
									),
								}],
							);
							self.garage.object_table.insert(&deleted_object).await?;
							obj_dels += 1;
						}
					}
				}

				if !version.deleted.get() {
					let deleted_version =
						Version::new(version.uuid, version.bucket_id, version.key.clone(), true);
					self.garage.version_table.insert(&deleted_version).await?;
					ver_dels += 1;
				}
			}
		}
		Ok(AdminRpc::Ok(format!(
			"{} blocks were purged: {} object deletion markers added, {} versions marked deleted",
			blocks.len(),
			obj_dels,
			ver_dels
		)))
	}
}
