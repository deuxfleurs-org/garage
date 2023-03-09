use core::ops::Bound;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use rand::Rng;
use tokio::fs;
use tokio::select;
use tokio::sync::mpsc;
use tokio::sync::watch;

use garage_util::background::*;
use garage_util::data::*;
use garage_util::error::*;
use garage_util::persister::PersisterShared;
use garage_util::time::*;
use garage_util::tranquilizer::Tranquilizer;

use crate::manager::*;

// Full scrub every 25 days with a random element of 10 days mixed in below
const SCRUB_INTERVAL: Duration = Duration::from_secs(3600 * 24 * 25);
// Scrub tranquility is initially set to 4, but can be changed in the CLI
// and the updated version is persisted over Garage restarts
const INITIAL_SCRUB_TRANQUILITY: u32 = 4;

// ---- ---- ----
// FIRST KIND OF REPAIR: FINDING MISSING BLOCKS/USELESS BLOCKS
// This is a one-shot repair operation that can be launched,
// checks everything, and then exits.
// ---- ---- ----

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

	fn status(&self) -> WorkerStatus {
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
				WorkerStatus {
					progress: Some("0.00%".into()),
					freeform: vec![format!(
						"Currently in phase 1, iterator position: {}",
						hex::encode(idx_bytes)
					)],
					..Default::default()
				}
			}
			Some(bi) => WorkerStatus {
				progress: Some(format!("{:.2}%", bi.progress() * 100.)),
				freeform: vec!["Currently in phase 2".into()],
				..Default::default()
			},
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
					self.manager
						.resync
						.put_to_resync(&hash, Duration::from_secs(0))?;
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
					self.manager
						.resync
						.put_to_resync(&hash, Duration::from_secs(0))?;
					Ok(WorkerState::Busy)
				} else {
					Ok(WorkerState::Done)
				}
			}
		}
	}

	async fn wait_for_work(&mut self) -> WorkerState {
		unreachable!()
	}
}

// ---- ---- ----
// SECOND KIND OF REPAIR: SCRUBBING THE DATASTORE
// This is significantly more complex than the process above,
// as it is a continuously-running task that triggers automatically
// every SCRUB_INTERVAL, but can also be triggered manually
// and whose parameter (esp. speed) can be controlled at runtime.
// ---- ---- ----

mod v081 {
	use serde::{Deserialize, Serialize};

	#[derive(Serialize, Deserialize)]
	pub struct ScrubWorkerPersisted {
		pub tranquility: u32,
		pub(crate) time_last_complete_scrub: u64,
		pub(crate) corruptions_detected: u64,
	}

	impl garage_util::migrate::InitialFormat for ScrubWorkerPersisted {}
}

mod v082 {
	use serde::{Deserialize, Serialize};

	use super::v081;

	#[derive(Serialize, Deserialize)]
	pub struct ScrubWorkerPersisted {
		pub tranquility: u32,
		pub(crate) time_last_complete_scrub: u64,
		pub(crate) time_next_run_scrub: u64,
		pub(crate) corruptions_detected: u64,
	}

	impl garage_util::migrate::Migrate for ScrubWorkerPersisted {
		type Previous = v081::ScrubWorkerPersisted;

		fn migrate(old: v081::ScrubWorkerPersisted) -> ScrubWorkerPersisted {
			use crate::repair::randomize_next_scrub_run_time;

			ScrubWorkerPersisted {
				tranquility: old.tranquility,
				time_last_complete_scrub: old.time_last_complete_scrub,
				time_next_run_scrub: randomize_next_scrub_run_time(),
				corruptions_detected: old.corruptions_detected,
			}
		}
	}
}

pub use v082::*;

pub struct ScrubWorker {
	manager: Arc<BlockManager>,
	rx_cmd: mpsc::Receiver<ScrubWorkerCommand>,

	work: ScrubWorkerState,
	tranquilizer: Tranquilizer,

	persister: PersisterShared<ScrubWorkerPersisted>,
}

fn randomize_next_scrub_run_time() -> u64 {
	// Take SCRUB_INTERVAL and mix in a random interval of 10 days to attempt to
	// balance scrub load across different cluster nodes.

	let next_run_timestamp = now_msec()
		+ SCRUB_INTERVAL
			.saturating_add(Duration::from_secs(
				rand::thread_rng().gen_range(0..3600 * 24 * 10),
			))
			.as_millis() as u64;

	next_run_timestamp
}

impl Default for ScrubWorkerPersisted {
	fn default() -> Self {
		ScrubWorkerPersisted {
			time_last_complete_scrub: 0,
			time_next_run_scrub: randomize_next_scrub_run_time(),
			tranquility: INITIAL_SCRUB_TRANQUILITY,
			corruptions_detected: 0,
		}
	}
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
}

impl ScrubWorker {
	pub(crate) fn new(
		manager: Arc<BlockManager>,
		rx_cmd: mpsc::Receiver<ScrubWorkerCommand>,
		persister: PersisterShared<ScrubWorkerPersisted>,
	) -> Self {
		Self {
			manager,
			rx_cmd,
			work: ScrubWorkerState::Finished,
			tranquilizer: Tranquilizer::new(30),
			persister,
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
		}
	}
}

#[async_trait]
impl Worker for ScrubWorker {
	fn name(&self) -> String {
		"Block scrub worker".into()
	}

	fn status(&self) -> WorkerStatus {
		let (corruptions_detected, tranquility, time_last_complete_scrub, time_next_run_scrub) =
			self.persister.get_with(|p| {
				(
					p.corruptions_detected,
					p.tranquility,
					p.time_last_complete_scrub,
					p.time_next_run_scrub,
				)
			});

		let mut s = WorkerStatus {
			persistent_errors: Some(corruptions_detected),
			tranquility: Some(tranquility),
			..Default::default()
		};
		match &self.work {
			ScrubWorkerState::Running(bsi) => {
				s.progress = Some(format!("{:.2}%", bsi.progress() * 100.));
			}
			ScrubWorkerState::Paused(bsi, rt) => {
				s.progress = Some(format!("{:.2}%", bsi.progress() * 100.));
				s.freeform = vec![format!("Scrub paused, resumes at {}", msec_to_rfc3339(*rt))];
			}
			ScrubWorkerState::Finished => {
				s.freeform = vec![
					format!(
						"Last scrub completed at {}",
						msec_to_rfc3339(time_last_complete_scrub),
					),
					format!(
						"Next scrub scheduled for {}",
						msec_to_rfc3339(time_next_run_scrub)
					),
				];
			}
		}
		s
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
							self.persister.set_with(|p| p.corruptions_detected += 1)?;
						}
						Err(e) => return Err(e),
						_ => (),
					};
					Ok(self
						.tranquilizer
						.tranquilize_worker(self.persister.get_with(|p| p.tranquility)))
				} else {
					self.persister.set_with(|p| {
						p.time_last_complete_scrub = now_msec();
						p.time_next_run_scrub = randomize_next_scrub_run_time();
					})?;
					self.work = ScrubWorkerState::Finished;
					self.tranquilizer.clear();
					Ok(WorkerState::Idle)
				}
			}
			_ => Ok(WorkerState::Idle),
		}
	}

	async fn wait_for_work(&mut self) -> WorkerState {
		let (wait_until, command) = match &self.work {
			ScrubWorkerState::Running(_) => return WorkerState::Busy,
			ScrubWorkerState::Paused(_, resume_time) => (*resume_time, ScrubWorkerCommand::Resume),
			ScrubWorkerState::Finished => (
				self.persister.get_with(|p| p.time_next_run_scrub),
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

// ---- ---- ----
// UTILITY FOR ENUMERATING THE BLOCK STORE
// ---- ---- ----

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
			if name.len() == 2 && hex::decode(name).is_ok() && ent_type.is_dir() {
				let path = data_dir_ent.path();
				self.path.push(ReadingDir::Pending(path));
			} else if name.len() == 64 {
				if let Ok(h) = hex::decode(name) {
					let mut hash = [0u8; 32];
					hash.copy_from_slice(&h);
					return Ok(Some(hash.into()));
				}
			}
		}
	}
}
