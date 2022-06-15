use std::sync::Arc;

use tokio::sync::watch;

use garage_model::garage::Garage;
use garage_model::s3::block_ref_table::*;
use garage_model::s3::object_table::*;
use garage_model::s3::version_table::*;
use garage_table::*;
use garage_util::error::Error;

use crate::*;

pub struct OnlineRepair {
	pub garage: Arc<Garage>,
}

impl OnlineRepair {
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
		match opt.what {
			RepairWhat::Tables => {
				info!("Launching a full sync of tables");
				self.garage.bucket_table.syncer.add_full_sync();
				self.garage.object_table.syncer.add_full_sync();
				self.garage.version_table.syncer.add_full_sync();
				self.garage.block_ref_table.syncer.add_full_sync();
				self.garage.key_table.syncer.add_full_sync();
			}
			RepairWhat::Versions => {
				info!("Repairing the versions table");
				self.repair_versions(&must_exit).await?;
			}
			RepairWhat::BlockRefs => {
				info!("Repairing the block refs table");
				self.repair_block_ref(&must_exit).await?;
			}
			RepairWhat::Blocks => {
				info!("Repairing the stored blocks");
				self.garage
					.block_manager
					.repair_data_store(&must_exit)
					.await?;
			}
			RepairWhat::Scrub { tranquility } => {
				info!("Verifying integrity of stored blocks");
				self.garage
					.block_manager
					.scrub_data_store(&must_exit, tranquility)
					.await?;
			}
		}
		Ok(())
	}

	async fn repair_versions(&self, must_exit: &watch::Receiver<bool>) -> Result<(), Error> {
		let mut pos = vec![];
		let mut i = 0;

		while !*must_exit.borrow() {
			let item_bytes = match self.garage.version_table.data.store.get_gt(pos)? {
				Some((k, v)) => {
					pos = k;
					v
				}
				None => break,
			};

			i += 1;
			if i % 1000 == 0 {
				info!("repair_versions: {}", i);
			}

			let version = rmp_serde::decode::from_read_ref::<_, Version>(&item_bytes)?;
			if version.deleted.get() {
				continue;
			}
			let object = self
				.garage
				.object_table
				.get(&version.bucket_id, &version.key)
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
						version.bucket_id,
						version.key,
						true,
					))
					.await?;
			}
		}
		info!("repair_versions: finished, done {}", i);
		Ok(())
	}

	async fn repair_block_ref(&self, must_exit: &watch::Receiver<bool>) -> Result<(), Error> {
		let mut pos = vec![];
		let mut i = 0;

		while !*must_exit.borrow() {
			let item_bytes = match self.garage.block_ref_table.data.store.get_gt(pos)? {
				Some((k, v)) => {
					pos = k;
					v
				}
				None => break,
			};

			i += 1;
			if i % 1000 == 0 {
				info!("repair_block_ref: {}", i);
			}

			let block_ref = rmp_serde::decode::from_read_ref::<_, BlockRef>(&item_bytes)?;
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
		}
		info!("repair_block_ref: finished, done {}", i);
		Ok(())
	}
}
