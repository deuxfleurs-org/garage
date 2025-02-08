use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use rand::prelude::*;
use tokio::sync::watch;

use garage_util::background::*;
use garage_util::error::*;

use crate::garage::Garage;

// The two most recent snapshots are kept
const KEEP_SNAPSHOTS: usize = 2;

static SNAPSHOT_MUTEX: Mutex<()> = Mutex::new(());

// ================ snapshotting logic =====================

/// Run snapshot_metadata in a blocking thread and async await on it
pub async fn async_snapshot_metadata(garage: &Arc<Garage>) -> Result<(), Error> {
	let garage = garage.clone();
	let worker = tokio::task::spawn_blocking(move || snapshot_metadata(&garage));
	worker.await.unwrap()?;
	Ok(())
}

/// Take a snapshot of the metadata database, and erase older
/// snapshots if necessary.
/// This is not an async function, it should be spawned on a thread pool
pub fn snapshot_metadata(garage: &Garage) -> Result<(), Error> {
	let lock = match SNAPSHOT_MUTEX.try_lock() {
		Ok(lock) => lock,
		Err(_) => {
			return Err(Error::Message(
				"Cannot acquire lock, another snapshot might be in progress".into(),
			))
		}
	};

	let snapshots_dir = match &garage.config.metadata_snapshots_dir {
		Some(d) => d.clone(),
		None => {
			let mut default_snapshots_dir = garage.config.metadata_dir.clone();
			default_snapshots_dir.push("snapshots");
			default_snapshots_dir
		}
	};
	fs::create_dir_all(&snapshots_dir)?;

	let mut new_path = snapshots_dir.clone();
	new_path.push(chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true));

	info!("Snapshotting metadata db to {}", new_path.display());
	garage.db.snapshot(&new_path)?;
	info!("Metadata db snapshot finished");

	if let Err(e) = cleanup_snapshots(&snapshots_dir) {
		error!("Failed to do cleanup in snapshots directory: {}", e);
	}

	drop(lock);

	Ok(())
}

fn cleanup_snapshots(snapshots_dir: &PathBuf) -> Result<(), Error> {
	let mut snapshots =
		fs::read_dir(&snapshots_dir)?.collect::<Result<Vec<fs::DirEntry>, std::io::Error>>()?;

	snapshots.retain(|x| x.file_name().len() > 8);
	snapshots.sort_by_key(|x| x.file_name());

	for to_delete in snapshots.iter().rev().skip(KEEP_SNAPSHOTS) {
		let path = snapshots_dir.join(to_delete.path());
		if to_delete.metadata()?.file_type().is_dir() {
			for file in fs::read_dir(&path)? {
				let file = file?;
				if file.metadata()?.is_file() {
					fs::remove_file(path.join(file.path()))?;
				}
			}
			std::fs::remove_dir(&path)?;
		} else {
			std::fs::remove_file(&path)?;
		}
	}
	Ok(())
}

// ================ auto snapshot worker =====================

pub struct AutoSnapshotWorker {
	garage: Arc<Garage>,
	next_snapshot: Instant,
	snapshot_interval: Duration,
}

impl AutoSnapshotWorker {
	pub(crate) fn new(garage: Arc<Garage>, snapshot_interval: Duration) -> Self {
		Self {
			garage,
			snapshot_interval,
			next_snapshot: Instant::now() + (snapshot_interval / 2),
		}
	}
}

#[async_trait]
impl Worker for AutoSnapshotWorker {
	fn name(&self) -> String {
		"Metadata snapshot worker".into()
	}
	fn status(&self) -> WorkerStatus {
		WorkerStatus {
			freeform: vec![format!(
				"Next snapshot: {}",
				(chrono::Utc::now() + (self.next_snapshot - Instant::now())).to_rfc3339()
			)],
			..Default::default()
		}
	}
	async fn work(&mut self, _must_exit: &mut watch::Receiver<bool>) -> Result<WorkerState, Error> {
		if Instant::now() < self.next_snapshot {
			return Ok(WorkerState::Idle);
		}

		async_snapshot_metadata(&self.garage).await?;

		let rand_factor = 1f32 + thread_rng().gen::<f32>() / 5f32;
		self.next_snapshot = Instant::now() + self.snapshot_interval.mul_f32(rand_factor);

		Ok(WorkerState::Idle)
	}
	async fn wait_for_work(&mut self) -> WorkerState {
		tokio::time::sleep_until(self.next_snapshot.into()).await;
		WorkerState::Busy
	}
}
