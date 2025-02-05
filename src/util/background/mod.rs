//! Job runner for futures and async functions

pub mod vars;
pub mod worker;

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc, watch};

use worker::WorkerProcessor;
pub use worker::{Worker, WorkerState};

/// Job runner for futures and async functions
pub struct BackgroundRunner {
	send_worker: mpsc::UnboundedSender<Box<dyn Worker>>,
	worker_info: Arc<std::sync::Mutex<HashMap<usize, WorkerInfo>>>,
}

#[derive(Clone, Debug)]
pub struct WorkerInfo {
	pub name: String,
	pub status: WorkerStatus,
	pub state: WorkerState,
	pub errors: usize,
	pub consecutive_errors: usize,
	pub last_error: Option<(String, u64)>,
}

/// WorkerStatus is a struct returned by the worker with a bunch of canonical
/// fields to indicate their status to CLI users. All fields are optional.
#[derive(Clone, Debug, Default)]
pub struct WorkerStatus {
	pub tranquility: Option<u32>,
	pub progress: Option<String>,
	pub queue_length: Option<u64>,
	pub persistent_errors: Option<u64>,
	pub freeform: Vec<String>,
}

impl BackgroundRunner {
	/// Create a new BackgroundRunner
	pub fn new(stop_signal: watch::Receiver<bool>) -> (Arc<Self>, tokio::task::JoinHandle<()>) {
		let (send_worker, worker_out) = mpsc::unbounded_channel::<Box<dyn Worker>>();

		let worker_info = Arc::new(std::sync::Mutex::new(HashMap::new()));
		let mut worker_processor =
			WorkerProcessor::new(worker_out, stop_signal, worker_info.clone());

		let await_all_done = tokio::spawn(async move {
			worker_processor.run().await;
		});

		let bgrunner = Arc::new(Self {
			send_worker,
			worker_info,
		});
		(bgrunner, await_all_done)
	}

	pub fn get_worker_info(&self) -> HashMap<usize, WorkerInfo> {
		self.worker_info.lock().unwrap().clone()
	}

	pub fn spawn_worker<W>(&self, worker: W)
	where
		W: Worker + 'static,
	{
		self.send_worker
			.send(Box::new(worker))
			.ok()
			.expect("Could not put worker in queue");
	}
}
