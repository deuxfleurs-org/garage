use std::sync::Arc;

use tokio::sync::watch;

use garage_model::block_ref_table::*;
use garage_model::garage::Garage;
use garage_model::object_table::*;
use garage_model::version_table::*;
use garage_table::*;
use garage_util::error::Error;

use crate::*;

pub struct Repair {
	pub garage: Arc<Garage>,
}

impl Repair {
	pub async fn repair_worker(&self, opt: RepairOpt, must_exit: watch::Receiver<bool>) {
		if let Err(e) = self.repair_worker_aux(opt, must_exit).await {
			warn!("Repair worker failed with error: {}", e);
		}
	}

	async fn repair_worker_aux(
		&self,
		opt: RepairOpt,
		must_exit: watch::Receiver<bool>,
	) -> Result<(), Error> {
		let todo = |x| opt.what.as_ref().map(|y| *y == x).unwrap_or(true);

		if todo(RepairWhat::Tables) {
			info!("Launching a full sync of tables");
			self.garage.bucket_table.syncer.add_full_sync();
			self.garage.object_table.syncer.add_full_sync();
			self.garage.version_table.syncer.add_full_sync();
			self.garage.block_ref_table.syncer.add_full_sync();
			self.garage.key_table.syncer.add_full_sync();
		}

		// TODO: wait for full sync to finish before proceeding to the rest?

		if todo(RepairWhat::Versions) {
			info!("Repairing the versions table");
			self.repair_versions(&must_exit).await?;
		}

		if todo(RepairWhat::BlockRefs) {
			info!("Repairing the block refs table");
			self.repair_block_ref(&must_exit).await?;
		}

		if opt.what.is_none() {
			info!("Repairing the RC");
			self.repair_rc(&must_exit).await?;
		}

		if todo(RepairWhat::Blocks) {
			info!("Repairing the stored blocks");
			self.garage
				.block_manager
				.repair_data_store(&must_exit)
				.await?;
		}

		if let Some(RepairWhat::BlockIntegrity { limit }) = opt.what {
			info!("Verifying integrity of stored blocks");
			self.garage
				.block_manager
				.verify_data_store_integrity(&must_exit, limit)
				.await?;
		}

		Ok(())
	}

	async fn repair_versions(&self, must_exit: &watch::Receiver<bool>) -> Result<(), Error> {
		let mut pos = vec![];

		while let Some((item_key, item_bytes)) =
			self.garage.version_table.data.store.get_gt(&pos)?
		{
			pos = item_key.to_vec();

			let version = rmp_serde::decode::from_read_ref::<_, Version>(item_bytes.as_ref())?;
			if version.deleted.get() {
				continue;
			}
			let object = self
				.garage
				.object_table
				.get(&version.bucket, &version.key)
				.await?;
			let version_exists = match object {
				Some(o) => o
					.versions()
					.iter()
					.any(|x| x.uuid == version.uuid && x.state != ObjectVersionState::Aborted),
				None => false,
			};
			if !version_exists {
				info!("Repair versions: marking version as deleted: {:?}", version);
				self.garage
					.version_table
					.insert(&Version::new(
						version.uuid,
						version.bucket,
						version.key,
						true,
					))
					.await?;
			}

			if *must_exit.borrow() {
				break;
			}
		}
		Ok(())
	}

	async fn repair_block_ref(&self, must_exit: &watch::Receiver<bool>) -> Result<(), Error> {
		let mut pos = vec![];

		while let Some((item_key, item_bytes)) =
			self.garage.block_ref_table.data.store.get_gt(&pos)?
		{
			pos = item_key.to_vec();

			let block_ref = rmp_serde::decode::from_read_ref::<_, BlockRef>(item_bytes.as_ref())?;
			if block_ref.deleted.get() {
				continue;
			}
			let version = self
				.garage
				.version_table
				.get(&block_ref.version, &EmptyKey)
				.await?;
			// The version might not exist if it has been GC'ed
			let ref_exists = version.map(|v| !v.deleted.get()).unwrap_or(false);
			if !ref_exists {
				info!(
					"Repair block ref: marking block_ref as deleted: {:?}",
					block_ref
				);
				self.garage
					.block_ref_table
					.insert(&BlockRef {
						block: block_ref.block,
						version: block_ref.version,
						deleted: true.into(),
					})
					.await?;
			}

			if *must_exit.borrow() {
				break;
			}
		}
		Ok(())
	}

	async fn repair_rc(&self, _must_exit: &watch::Receiver<bool>) -> Result<(), Error> {
		// TODO
		warn!("repair_rc: not implemented");
		Ok(())
	}
}
