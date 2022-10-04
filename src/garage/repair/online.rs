use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::sync::watch;

use garage_block::repair::ScrubWorkerCommand;
use garage_model::garage::Garage;
use garage_model::s3::block_ref_table::*;
use garage_model::s3::object_table::*;
use garage_model::s3::version_table::*;
use garage_table::*;
use garage_util::background::*;
use garage_util::error::Error;

use crate::*;

pub async fn launch_online_repair(garage: Arc<Garage>, opt: RepairOpt) {
	match opt.what {
		RepairWhat::Tables => {
			info!("Launching a full sync of tables");
			garage.bucket_table.syncer.add_full_sync();
			garage.object_table.syncer.add_full_sync();
			garage.version_table.syncer.add_full_sync();
			garage.block_ref_table.syncer.add_full_sync();
			garage.key_table.syncer.add_full_sync();
		}
		RepairWhat::Versions => {
			info!("Repairing the versions table");
			garage
				.background
				.spawn_worker(RepairVersionsWorker::new(garage.clone()));
		}
		RepairWhat::BlockRefs => {
			info!("Repairing the block refs table");
			garage
				.background
				.spawn_worker(RepairBlockrefsWorker::new(garage.clone()));
		}
		RepairWhat::Blocks => {
			info!("Repairing the stored blocks");
			garage
				.background
				.spawn_worker(garage_block::repair::RepairWorker::new(
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
					ScrubWorkerCommand::SetTranquility(tranquility)
				}
			};
			info!("Sending command to scrub worker: {:?}", cmd);
			garage.block_manager.send_scrub_command(cmd).await;
		}
	}
}

// ----

struct RepairVersionsWorker {
	garage: Arc<Garage>,
	pos: Vec<u8>,
	counter: usize,
}

impl RepairVersionsWorker {
	fn new(garage: Arc<Garage>) -> Self {
		Self {
			garage,
			pos: vec![],
			counter: 0,
		}
	}
}

#[async_trait]
impl Worker for RepairVersionsWorker {
	fn name(&self) -> String {
		"Version repair worker".into()
	}

	fn info(&self) -> Option<String> {
		Some(format!("{} items done", self.counter))
	}

	async fn work(&mut self, _must_exit: &mut watch::Receiver<bool>) -> Result<WorkerState, Error> {
		let item_bytes = match self.garage.version_table.data.store.get_gt(&self.pos)? {
			Some((k, v)) => {
				self.pos = k;
				v
			}
			None => {
				info!("repair_versions: finished, done {}", self.counter);
				return Ok(WorkerState::Done);
			}
		};

		self.counter += 1;

		let version = rmp_serde::decode::from_read_ref::<_, Version>(&item_bytes)?;
		if !version.deleted.get() {
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

		Ok(WorkerState::Busy)
	}

	async fn wait_for_work(&mut self, _must_exit: &watch::Receiver<bool>) -> WorkerState {
		unreachable!()
	}
}

// ----

struct RepairBlockrefsWorker {
	garage: Arc<Garage>,
	pos: Vec<u8>,
	counter: usize,
}

impl RepairBlockrefsWorker {
	fn new(garage: Arc<Garage>) -> Self {
		Self {
			garage,
			pos: vec![],
			counter: 0,
		}
	}
}

#[async_trait]
impl Worker for RepairBlockrefsWorker {
	fn name(&self) -> String {
		"Block refs repair worker".into()
	}

	fn info(&self) -> Option<String> {
		Some(format!("{} items done", self.counter))
	}

	async fn work(&mut self, _must_exit: &mut watch::Receiver<bool>) -> Result<WorkerState, Error> {
		let item_bytes = match self.garage.block_ref_table.data.store.get_gt(&self.pos)? {
			Some((k, v)) => {
				self.pos = k;
				v
			}
			None => {
				info!("repair_block_ref: finished, done {}", self.counter);
				return Ok(WorkerState::Done);
			}
		};

		self.counter += 1;

		let block_ref = rmp_serde::decode::from_read_ref::<_, BlockRef>(&item_bytes)?;
		if !block_ref.deleted.get() {
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

		Ok(WorkerState::Busy)
	}

	async fn wait_for_work(&mut self, _must_exit: &watch::Receiver<bool>) -> WorkerState {
		unreachable!()
	}
}
