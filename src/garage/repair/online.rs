use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::watch;

use garage_block::repair::ScrubWorkerCommand;

use garage_model::garage::Garage;
use garage_model::s3::block_ref_table::*;
use garage_model::s3::mpu_table::*;
use garage_model::s3::object_table::*;
use garage_model::s3::version_table::*;

use garage_table::replication::*;
use garage_table::*;

use garage_util::background::*;
use garage_util::error::Error;
use garage_util::migrate::Migrate;

use crate::*;

pub async fn launch_online_repair(
	garage: &Arc<Garage>,
	bg: &BackgroundRunner,
	opt: RepairOpt,
) -> Result<(), Error> {
	match opt.what {
		RepairWhat::Tables => {
			info!("Launching a full sync of tables");
			garage.bucket_table.syncer.add_full_sync()?;
			garage.object_table.syncer.add_full_sync()?;
			garage.version_table.syncer.add_full_sync()?;
			garage.block_ref_table.syncer.add_full_sync()?;
			garage.key_table.syncer.add_full_sync()?;
		}
		RepairWhat::Versions => {
			info!("Repairing the versions table");
			bg.spawn_worker(TableRepairWorker::new(garage.clone(), RepairVersions));
		}
		RepairWhat::MultipartUploads => {
			info!("Repairing the multipart uploads table");
			bg.spawn_worker(TableRepairWorker::new(garage.clone(), RepairMpu));
		}
		RepairWhat::BlockRefs => {
			info!("Repairing the block refs table");
			bg.spawn_worker(TableRepairWorker::new(garage.clone(), RepairBlockRefs));
		}
		RepairWhat::Blocks => {
			info!("Repairing the stored blocks");
			bg.spawn_worker(garage_block::repair::RepairWorker::new(
				garage.block_manager.clone(),
			));
		}
		RepairWhat::Scrub { cmd } => {
			let cmd = match cmd {
				ScrubCmd::Start => ScrubWorkerCommand::Start,
				ScrubCmd::Pause => ScrubWorkerCommand::Pause(Duration::from_secs(3600 * 24)),
				ScrubCmd::Resume => ScrubWorkerCommand::Resume,
				ScrubCmd::Cancel => ScrubWorkerCommand::Cancel,
				ScrubCmd::SetTranquility { tranquility } => {
					garage
						.block_manager
						.scrub_persister
						.set_with(|x| x.tranquility = tranquility)?;
					return Ok(());
				}
			};
			info!("Sending command to scrub worker: {:?}", cmd);
			garage.block_manager.send_scrub_command(cmd).await?;
		}
		RepairWhat::Rebalance => {
			info!("Rebalancing the stored blocks among storage locations");
			bg.spawn_worker(garage_block::repair::RebalanceWorker::new(
				garage.block_manager.clone(),
			));
		}
	}
	Ok(())
}

// ----

#[async_trait]
trait TableRepair: Send + Sync + 'static {
	type T: TableSchema;

	fn table(garage: &Garage) -> &Table<Self::T, TableShardedReplication>;

	async fn process(
		&mut self,
		garage: &Garage,
		entry: <<Self as TableRepair>::T as TableSchema>::E,
	) -> Result<bool, Error>;
}

struct TableRepairWorker<T: TableRepair> {
	garage: Arc<Garage>,
	pos: Vec<u8>,
	counter: usize,
	repairs: usize,
	inner: T,
}

impl<R: TableRepair> TableRepairWorker<R> {
	fn new(garage: Arc<Garage>, inner: R) -> Self {
		Self {
			garage,
			inner,
			pos: vec![],
			counter: 0,
			repairs: 0,
		}
	}
}

#[async_trait]
impl<R: TableRepair> Worker for TableRepairWorker<R> {
	fn name(&self) -> String {
		format!("{} repair worker", R::T::TABLE_NAME)
	}

	fn status(&self) -> WorkerStatus {
		WorkerStatus {
			progress: Some(format!("{} ({})", self.counter, self.repairs)),
			..Default::default()
		}
	}

	async fn work(&mut self, _must_exit: &mut watch::Receiver<bool>) -> Result<WorkerState, Error> {
		let (item_bytes, next_pos) = match R::table(&self.garage).data.store.get_gt(&self.pos)? {
			Some((k, v)) => (v, k),
			None => {
				info!(
					"{}: finished, done {}, fixed {}",
					self.name(),
					self.counter,
					self.repairs
				);
				return Ok(WorkerState::Done);
			}
		};

		let entry = <R::T as TableSchema>::E::decode(&item_bytes)
			.ok_or_message("Cannot decode table entry")?;
		if self.inner.process(&self.garage, entry).await? {
			self.repairs += 1;
		}

		self.counter += 1;
		self.pos = next_pos;

		Ok(WorkerState::Busy)
	}

	async fn wait_for_work(&mut self) -> WorkerState {
		unreachable!()
	}
}

// ----

struct RepairVersions;

#[async_trait]
impl TableRepair for RepairVersions {
	type T = VersionTable;

	fn table(garage: &Garage) -> &Table<Self::T, TableShardedReplication> {
		&garage.version_table
	}

	async fn process(&mut self, garage: &Garage, version: Version) -> Result<bool, Error> {
		if !version.deleted.get() {
			let ref_exists = match &version.backlink {
				VersionBacklink::Object { bucket_id, key } => garage
					.object_table
					.get(bucket_id, key)
					.await?
					.map(|o| {
						o.versions().iter().any(|x| {
							x.uuid == version.uuid && x.state != ObjectVersionState::Aborted
						})
					})
					.unwrap_or(false),
				VersionBacklink::MultipartUpload { upload_id } => garage
					.mpu_table
					.get(upload_id, &EmptyKey)
					.await?
					.map(|u| !u.deleted.get())
					.unwrap_or(false),
			};

			if !ref_exists {
				info!("Repair versions: marking version as deleted: {:?}", version);
				garage
					.version_table
					.insert(&Version::new(version.uuid, version.backlink, true))
					.await?;
				return Ok(true);
			}
		}

		Ok(false)
	}
}

// ----

struct RepairBlockRefs;

#[async_trait]
impl TableRepair for RepairBlockRefs {
	type T = BlockRefTable;

	fn table(garage: &Garage) -> &Table<Self::T, TableShardedReplication> {
		&garage.block_ref_table
	}

	async fn process(&mut self, garage: &Garage, mut block_ref: BlockRef) -> Result<bool, Error> {
		if !block_ref.deleted.get() {
			let ref_exists = garage
				.version_table
				.get(&block_ref.version, &EmptyKey)
				.await?
				.map(|v| !v.deleted.get())
				.unwrap_or(false);

			if !ref_exists {
				info!(
					"Repair block ref: marking block_ref as deleted: {:?}",
					block_ref
				);
				block_ref.deleted.set();
				garage.block_ref_table.insert(&block_ref).await?;
				return Ok(true);
			}
		}

		Ok(false)
	}
}

// ----

struct RepairMpu;

#[async_trait]
impl TableRepair for RepairMpu {
	type T = MultipartUploadTable;

	fn table(garage: &Garage) -> &Table<Self::T, TableShardedReplication> {
		&garage.mpu_table
	}

	async fn process(&mut self, garage: &Garage, mut mpu: MultipartUpload) -> Result<bool, Error> {
		if !mpu.deleted.get() {
			let ref_exists = garage
				.object_table
				.get(&mpu.bucket_id, &mpu.key)
				.await?
				.map(|o| {
					o.versions()
						.iter()
						.any(|x| x.uuid == mpu.upload_id && x.is_uploading(Some(true)))
				})
				.unwrap_or(false);

			if !ref_exists {
				info!(
					"Repair multipart uploads: marking mpu as deleted: {:?}",
					mpu
				);
				mpu.parts.clear();
				mpu.deleted.set();
				garage.mpu_table.insert(&mpu).await?;
				return Ok(true);
			}
		}

		Ok(false)
	}
}
