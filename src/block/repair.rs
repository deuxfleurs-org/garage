use core::ops::Bound;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::watch;

use garage_util::background::*;
use garage_util::data::*;
use garage_util::error::*;
use garage_util::persister::Persister;
use garage_util::time::*;
use garage_util::tranquilizer::Tranquilizer;

use crate::manager::*;

const SCRUB_INTERVAL: Duration = Duration::from_secs(3600 * 24 * 30); // full scrub every 30 days

pub struct RepairWorker {
	manager: Arc<BlockManager>,
	next_start: Option<Hash>,
	block_iter: Option<BlockStoreIterator>,
}

impl RepairWorker {
	pub fn new(manager: Arc<BlockManager>) -> Self {
		Self {
			manager,
			next_start: None,
			block_iter: None,
		}
	}
}

#[async_trait]
impl Worker for RepairWorker {
	fn name(&self) -> String {
		"Block repair worker".into()
	}

	fn info(&self) -> Option<String> {
		match self.block_iter.as_ref() {
			None => {
				let idx_bytes = self
					.next_start
					.as_ref()
					.map(|x| x.as_slice())
					.unwrap_or(&[]);
				let idx_bytes = if idx_bytes.len() > 4 {
					&idx_bytes[..4]
				} else {
					idx_bytes
				};
				Some(format!("Phase 1: {}", hex::encode(idx_bytes)))
			}
			Some(bi) => Some(format!("Phase 2: {:.2}% done", bi.progress() * 100.)),
		}
	}

	async fn work(&mut self, _must_exit: &mut watch::Receiver<bool>) -> Result<WorkerState, Error> {
		match self.block_iter.as_mut() {
			None => {
				// Phase 1: Repair blocks from RC table.

				// We have to do this complicated two-step process where we first read a bunch
				// of hashes from the RC table, and then insert them in the to-resync queue,
				// because of SQLite. Basically, as long as we have an iterator on a DB table,
				// we can't do anything else on the DB. The naive approach (which we had previously)
				// of just iterating on the RC table and inserting items one to one in the resync
				// queue can't work here, it would just provoke a deadlock in the SQLite adapter code.
				// This is mostly because the Rust bindings for SQLite assume a worst-case scenario
				// where SQLite is not compiled in thread-safe mode, so we have to wrap everything
				// in a mutex (see db/sqlite_adapter.rs and discussion in PR #322).
				// TODO: maybe do this with tokio::task::spawn_blocking ?
				let mut batch_of_hashes = vec![];
				let start_bound = match self.next_start.as_ref() {
					None => Bound::Unbounded,
					Some(x) => Bound::Excluded(x.as_slice()),
				};
				for entry in self
					.manager
					.rc
					.rc
					.range::<&[u8], _>((start_bound, Bound::Unbounded))?
				{
					let (hash, _) = entry?;
					let hash = Hash::try_from(&hash[..]).unwrap();
					batch_of_hashes.push(hash);
					if batch_of_hashes.len() >= 1000 {
						break;
					}
				}
				if batch_of_hashes.is_empty() {
					// move on to phase 2
					self.block_iter = Some(BlockStoreIterator::new(&self.manager));
					return Ok(WorkerState::Busy);
				}

				for hash in batch_of_hashes.into_iter() {
					self.manager.put_to_resync(&hash, Duration::from_secs(0))?;
					self.next_start = Some(hash)
				}

				Ok(WorkerState::Busy)
			}
			Some(bi) => {
				// Phase 2: Repair blocks actually on disk
				// Lists all blocks on disk and adds them to the resync queue.
				// This allows us to find blocks we are storing but don't actually need,
				// so that we can offload them if necessary and then delete them locally.
				if let Some(hash) = bi.next().await? {
					self.manager.put_to_resync(&hash, Duration::from_secs(0))?;
					Ok(WorkerState::Busy)
				} else {
					Ok(WorkerState::Done)
				}
			}
		}
	}

	async fn wait_for_work(&mut self, _must_exit: &watch::Receiver<bool>) -> WorkerState {
		unreachable!()
	}
}

// ----

pub struct ScrubWorker {
	manager: Arc<BlockManager>,
	rx_cmd: mpsc::Receiver<ScrubWorkerCommand>,

	work: ScrubWorkerState,
	tranquilizer: Tranquilizer,

	persister: Persister<ScrubWorkerPersisted>,
	persisted: ScrubWorkerPersisted,
}

#[derive(Serialize, Deserialize)]
struct ScrubWorkerPersisted {
	tranquility: u32,
	time_last_complete_scrub: u64,
	corruptions_detected: u64,
}

enum ScrubWorkerState {
	Running(BlockStoreIterator),
	Paused(BlockStoreIterator, u64), // u64 = time when to resume scrub
	Finished,
}

impl Default for ScrubWorkerState {
	fn default() -> Self {
		ScrubWorkerState::Finished
	}
}

#[derive(Debug)]
pub enum ScrubWorkerCommand {
	Start,
	Pause(Duration),
	Resume,
	Cancel,
	SetTranquility(u32),
}

impl ScrubWorker {
	pub fn new(manager: Arc<BlockManager>, rx_cmd: mpsc::Receiver<ScrubWorkerCommand>) -> Self {
		let persister = Persister::new(&manager.system.metadata_dir, "scrub_info");
		let persisted = match persister.load() {
			Ok(v) => v,
			Err(_) => ScrubWorkerPersisted {
				time_last_complete_scrub: 0,
				tranquility: 4,
				corruptions_detected: 0,
			},
		};
		Self {
			manager,
			rx_cmd,
			work: ScrubWorkerState::Finished,
			tranquilizer: Tranquilizer::new(30),
			persister,
			persisted,
		}
	}

	async fn handle_cmd(&mut self, cmd: ScrubWorkerCommand) {
		match cmd {
			ScrubWorkerCommand::Start => {
				self.work = match std::mem::take(&mut self.work) {
					ScrubWorkerState::Finished => {
						let iterator = BlockStoreIterator::new(&self.manager);
						ScrubWorkerState::Running(iterator)
					}
					work => {
						error!("Cannot start scrub worker: already running!");
						work
					}
				};
			}
			ScrubWorkerCommand::Pause(dur) => {
				self.work = match std::mem::take(&mut self.work) {
					ScrubWorkerState::Running(it) | ScrubWorkerState::Paused(it, _) => {
						ScrubWorkerState::Paused(it, now_msec() + dur.as_millis() as u64)
					}
					work => {
						error!("Cannot pause scrub worker: not running!");
						work
					}
				};
			}
			ScrubWorkerCommand::Resume => {
				self.work = match std::mem::take(&mut self.work) {
					ScrubWorkerState::Paused(it, _) => ScrubWorkerState::Running(it),
					work => {
						error!("Cannot resume scrub worker: not paused!");
						work
					}
				};
			}
			ScrubWorkerCommand::Cancel => {
				self.work = match std::mem::take(&mut self.work) {
					ScrubWorkerState::Running(_) | ScrubWorkerState::Paused(_, _) => {
						ScrubWorkerState::Finished
					}
					work => {
						error!("Cannot cancel scrub worker: not running!");
						work
					}
				}
			}
			ScrubWorkerCommand::SetTranquility(t) => {
				self.persisted.tranquility = t;
				if let Err(e) = self.persister.save_async(&self.persisted).await {
					error!("Could not save new tranquilitiy value: {}", e);
				}
			}
		}
	}
}

#[async_trait]
impl Worker for ScrubWorker {
	fn name(&self) -> String {
		"Block scrub worker".into()
	}

	fn info(&self) -> Option<String> {
		let s = match &self.work {
			ScrubWorkerState::Running(bsi) => format!(
				"{:.2}% done (tranquility = {})",
				bsi.progress() * 100.,
				self.persisted.tranquility
			),
			ScrubWorkerState::Paused(bsi, rt) => {
				format!(
					"Paused, {:.2}% done, resumes at {}",
					bsi.progress() * 100.,
					msec_to_rfc3339(*rt)
				)
			}
			ScrubWorkerState::Finished => format!(
				"Last completed scrub: {}",
				msec_to_rfc3339(self.persisted.time_last_complete_scrub)
			),
		};
		Some(format!(
			"{} ; corruptions detected: {}",
			s, self.persisted.corruptions_detected
		))
	}

	async fn work(&mut self, _must_exit: &mut watch::Receiver<bool>) -> Result<WorkerState, Error> {
		match self.rx_cmd.try_recv() {
			Ok(cmd) => self.handle_cmd(cmd).await,
			Err(mpsc::error::TryRecvError::Disconnected) => return Ok(WorkerState::Done),
			Err(mpsc::error::TryRecvError::Empty) => (),
		};

		match &mut self.work {
			ScrubWorkerState::Running(bsi) => {
				self.tranquilizer.reset();
				if let Some(hash) = bsi.next().await? {
					match self.manager.read_block(&hash).await {
						Err(Error::CorruptData(_)) => {
							error!("Found corrupt data block during scrub: {:?}", hash);
							self.persisted.corruptions_detected += 1;
							self.persister.save_async(&self.persisted).await?;
						}
						Err(e) => return Err(e),
						_ => (),
					};
					Ok(self
						.tranquilizer
						.tranquilize_worker(self.persisted.tranquility))
				} else {
					self.persisted.time_last_complete_scrub = now_msec();
					self.persister.save_async(&self.persisted).await?;
					self.work = ScrubWorkerState::Finished;
					self.tranquilizer.clear();
					Ok(WorkerState::Idle)
				}
			}
			_ => Ok(WorkerState::Idle),
		}
	}

	async fn wait_for_work(&mut self, _must_exit: &watch::Receiver<bool>) -> WorkerState {
		let (wait_until, command) = match &self.work {
			ScrubWorkerState::Running(_) => return WorkerState::Busy,
			ScrubWorkerState::Paused(_, resume_time) => (*resume_time, ScrubWorkerCommand::Resume),
			ScrubWorkerState::Finished => (
				self.persisted.time_last_complete_scrub + SCRUB_INTERVAL.as_millis() as u64,
				ScrubWorkerCommand::Start,
			),
		};

		let now = now_msec();
		if now >= wait_until {
			self.handle_cmd(command).await;
			return WorkerState::Busy;
		}
		let delay = Duration::from_millis(wait_until - now);
		select! {
			_ = tokio::time::sleep(delay) => self.handle_cmd(command).await,
			cmd = self.rx_cmd.recv() => if let Some(cmd) = cmd {
				self.handle_cmd(cmd).await;
			} else {
				return WorkerState::Done;
			}
		}

		match &self.work {
			ScrubWorkerState::Running(_) => WorkerState::Busy,
			_ => WorkerState::Idle,
		}
	}
}

// ----

struct BlockStoreIterator {
	path: Vec<ReadingDir>,
}

enum ReadingDir {
	Pending(PathBuf),
	Read {
		subpaths: Vec<fs::DirEntry>,
		pos: usize,
	},
}

impl BlockStoreIterator {
	fn new(manager: &BlockManager) -> Self {
		let root_dir = manager.data_dir.clone();
		Self {
			path: vec![ReadingDir::Pending(root_dir)],
		}
	}

	/// Returns progress done, between 0 and 1
	fn progress(&self) -> f32 {
		if self.path.is_empty() {
			1.0
		} else {
			let mut ret = 0.0;
			let mut next_div = 1;
			for p in self.path.iter() {
				match p {
					ReadingDir::Pending(_) => break,
					ReadingDir::Read { subpaths, pos } => {
						next_div *= subpaths.len();
						ret += ((*pos - 1) as f32) / (next_div as f32);
					}
				}
			}
			ret
		}
	}

	async fn next(&mut self) -> Result<Option<Hash>, Error> {
		loop {
			let last_path = match self.path.last_mut() {
				None => return Ok(None),
				Some(lp) => lp,
			};

			if let ReadingDir::Pending(path) = last_path {
				let mut reader = fs::read_dir(&path).await?;
				let mut subpaths = vec![];
				while let Some(ent) = reader.next_entry().await? {
					subpaths.push(ent);
				}
				*last_path = ReadingDir::Read { subpaths, pos: 0 };
			}

			let (subpaths, pos) = match *last_path {
				ReadingDir::Read {
					ref subpaths,
					ref mut pos,
				} => (subpaths, pos),
				ReadingDir::Pending(_) => unreachable!(),
			};

			let data_dir_ent = match subpaths.get(*pos) {
				None => {
					self.path.pop();
					continue;
				}
				Some(ent) => {
					*pos += 1;
					ent
				}
			};

			let name = data_dir_ent.file_name();
			let name = if let Ok(n) = name.into_string() {
				n
			} else {
				continue;
			};
			let ent_type = data_dir_ent.file_type().await?;

			let name = name.strip_suffix(".zst").unwrap_or(&name);
			if name.len() == 2 && hex::decode(&name).is_ok() && ent_type.is_dir() {
				let path = data_dir_ent.path();
				self.path.push(ReadingDir::Pending(path));
			} else if name.len() == 64 {
				if let Ok(h) = hex::decode(&name) {
					let mut hash = [0u8; 32];
					hash.copy_from_slice(&h);
					return Ok(Some(hash.into()));
				}
			}
		}
	}
}
