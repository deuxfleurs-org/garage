use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::watch;

use garage_block::manager::BlockManager;
use garage_block::repair::ScrubWorkerCommand;

use garage_model::garage::Garage;
use garage_model::s3::block_ref_table::*;
use garage_model::s3::mpu_table::*;
use garage_model::s3::object_table::*;
use garage_model::s3::version_table::*;

use garage_table::replication::*;
use garage_table::*;

use garage_util::background::*;
use garage_util::data::*;
use garage_util::error::Error;
use garage_util::migrate::Migrate;

use crate::*;

const RC_REPAIR_ITER_COUNT: usize = 64;

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
		RepairWhat::BlockRc => {
			info!("Repairing the block reference counters");
			bg.spawn_worker(BlockRcRepair::new(
				garage.block_manager.clone(),
				garage.block_ref_table.clone(),
			));
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
		RepairWhat::Aliases => {
			info!("Repairing bucket aliases (foreground)");
			garage.locked_helper().await.repair_aliases().await?;
		}
	}
	Ok(())
}

// ----

trait TableRepair: Send + Sync + 'static {
	type T: TableSchema;

	fn table(garage: &Garage) -> &Table<Self::T, TableShardedReplication>;

	fn process(
		&mut self,
		garage: &Garage,
		entry: <<Self as TableRepair>::T as TableSchema>::E,
	) -> impl Future<Output = Result<bool, Error>> + Send;
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

// ===== block reference counter repair =====

pub struct BlockRcRepair {
	block_manager: Arc<BlockManager>,
	block_ref_table: Arc<Table<BlockRefTable, TableShardedReplication>>,
	cursor: Hash,
	counter: u64,
	repairs: u64,
}

impl BlockRcRepair {
	fn new(
		block_manager: Arc<BlockManager>,
		block_ref_table: Arc<Table<BlockRefTable, TableShardedReplication>>,
	) -> Self {
		Self {
			block_manager,
			block_ref_table,
			cursor: [0u8; 32].into(),
			counter: 0,
			repairs: 0,
		}
	}
}

#[async_trait]
impl Worker for BlockRcRepair {
	fn name(&self) -> String {
		format!("Block refcount repair worker")
	}

	fn status(&self) -> WorkerStatus {
		WorkerStatus {
			progress: Some(format!("{} ({})", self.counter, self.repairs)),
			..Default::default()
		}
	}

	async fn work(&mut self, _must_exit: &mut watch::Receiver<bool>) -> Result<WorkerState, Error> {
		for _i in 0..RC_REPAIR_ITER_COUNT {
			let next1 = self
				.block_manager
				.rc
				.rc_table
				.range(self.cursor.as_slice()..)?
				.next()
				.transpose()?
				.map(|(k, _)| Hash::try_from(k.as_slice()).unwrap());
			let next2 = self
				.block_ref_table
				.data
				.store
				.range(self.cursor.as_slice()..)?
				.next()
				.transpose()?
				.map(|(k, _)| Hash::try_from(&k[..32]).unwrap());
			let next = match (next1, next2) {
				(Some(k1), Some(k2)) => std::cmp::min(k1, k2),
				(Some(k), None) | (None, Some(k)) => k,
				(None, None) => {
					info!(
						"{}: finished, done {}, fixed {}",
						self.name(),
						self.counter,
						self.repairs
					);
					return Ok(WorkerState::Done);
				}
			};

			if self.block_manager.rc.recalculate_rc(&next)?.1 {
				self.repairs += 1;
			}
			self.counter += 1;
			if let Some(next_incr) = next.increment() {
				self.cursor = next_incr;
			} else {
				info!(
					"{}: finished, done {}, fixed {}",
					self.name(),
					self.counter,
					self.repairs
				);
				return Ok(WorkerState::Done);
			}
		}

		Ok(WorkerState::Busy)
	}

	async fn wait_for_work(&mut self) -> WorkerState {
		unreachable!()
	}
}
