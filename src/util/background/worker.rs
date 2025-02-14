use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::future::*;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use tokio::select;
use tokio::sync::{mpsc, watch};

use crate::background::{WorkerInfo, WorkerStatus};
use crate::error::Error;
use crate::time::now_msec;

// All workers that haven't exited for this time after an exit signal was received
// will be interrupted in the middle of whatever they are doing.
const EXIT_DEADLINE: Duration = Duration::from_secs(8);

#[derive(PartialEq, Copy, Clone, Debug)]
pub enum WorkerState {
	Busy,
	Throttled(f32),
	Idle,
	Done,
}

#[async_trait]
pub trait Worker: Send {
	fn name(&self) -> String;

	fn status(&self) -> WorkerStatus {
		Default::default()
	}

	/// Work: do a basic unit of work, if one is available (otherwise, should return
	/// WorkerState::Idle immediately).  We will do our best to not interrupt this future in the
	/// middle of processing, it will only be interrupted at the last minute when Garage is trying
	/// to exit and this hasn't returned yet. This function may return an error to indicate that
	/// its unit of work could not be processed due to an error: the error will be logged and
	/// .work() will be called again after a short delay.
	async fn work(&mut self, must_exit: &mut watch::Receiver<bool>) -> Result<WorkerState, Error>;

	/// Wait for work: await for some task to become available.  This future can be interrupted in
	/// the middle for any reason, for example if an interrupt signal was received.
	async fn wait_for_work(&mut self) -> WorkerState;
}

pub(crate) struct WorkerProcessor {
	stop_signal: watch::Receiver<bool>,
	worker_chan: mpsc::UnboundedReceiver<Box<dyn Worker>>,
	worker_info: Arc<std::sync::Mutex<HashMap<usize, WorkerInfo>>>,
}

impl WorkerProcessor {
	pub(crate) fn new(
		worker_chan: mpsc::UnboundedReceiver<Box<dyn Worker>>,
		stop_signal: watch::Receiver<bool>,
		worker_info: Arc<std::sync::Mutex<HashMap<usize, WorkerInfo>>>,
	) -> Self {
		Self {
			stop_signal,
			worker_chan,
			worker_info,
		}
	}

	pub(crate) async fn run(&mut self) {
		let mut workers = FuturesUnordered::new();
		let mut next_task_id = 1;

		while !*self.stop_signal.borrow() {
			let await_next_worker = async {
				if workers.is_empty() {
					futures::future::pending().await
				} else {
					workers.next().await
				}
			};
			select! {
				new_worker_opt = self.worker_chan.recv() => {
					if let Some(new_worker) = new_worker_opt {
						let task_id = next_task_id;
						next_task_id += 1;
						let stop_signal = self.stop_signal.clone();
						let mut worker = WorkerHandler {
								task_id,
								stop_signal,
								worker: new_worker,
								state: WorkerState::Busy,
								errors: 0,
								consecutive_errors: 0,
								last_error: None,
							};
						workers.push(async move {
							worker.step().await;
							worker
						}.boxed());
					}
				}
				worker = await_next_worker => {
					if let Some(mut worker) = worker {
						trace!("{} (TID {}): {:?}", worker.worker.name(), worker.task_id, worker.state);

						// Save worker info
						let mut wi = self.worker_info.lock().unwrap();
						match wi.get_mut(&worker.task_id) {
							Some(i) => {
								i.state = worker.state;
								i.status = worker.worker.status();
								i.errors = worker.errors;
								i.consecutive_errors = worker.consecutive_errors;
								if worker.last_error.is_some() {
									i.last_error = worker.last_error.take();
								}
							}
							None => {
								wi.insert(worker.task_id, WorkerInfo {
									name: worker.worker.name(),
									state: worker.state,
									status: worker.worker.status(),
									errors: worker.errors,
									consecutive_errors: worker.consecutive_errors,
									last_error: worker.last_error.take(),
								});
							}
						}

						if worker.state == WorkerState::Done {
							info!("Worker {} (TID {}) exited", worker.worker.name(), worker.task_id);
						} else {
							workers.push(async move {
								worker.step().await;
								worker
							}.boxed());
						}
					}
				}
				_ = self.stop_signal.changed() => (),
			}
		}

		// We are exiting, drain everything
		let drain_everything = async move {
			while let Some(worker) = workers.next().await {
				info!(
					"Worker {} (TID {}) exited (last state: {:?})",
					worker.worker.name(),
					worker.task_id,
					worker.state
				);
			}
		};

		select! {
			_ = drain_everything => {
				info!("All workers exited peacefully \\o/");
			}
			_ = tokio::time::sleep(EXIT_DEADLINE) => {
				error!("Some workers could not exit in time, we are cancelling some things in the middle");
			}
		}
	}
}

struct WorkerHandler {
	task_id: usize,
	stop_signal: watch::Receiver<bool>,
	worker: Box<dyn Worker>,
	state: WorkerState,
	errors: usize,
	consecutive_errors: usize,
	last_error: Option<(String, u64)>,
}

impl WorkerHandler {
	async fn step(&mut self) {
		match self.state {
			WorkerState::Busy => match self.worker.work(&mut self.stop_signal).await {
				Ok(s) => {
					self.state = s;
					self.consecutive_errors = 0;
				}
				Err(e) => {
					error!(
						"Error in worker {} (TID {}): {}",
						self.worker.name(),
						self.task_id,
						e
					);
					self.errors += 1;
					self.consecutive_errors += 1;
					self.last_error = Some((format!("{}", e), now_msec()));
					// Sleep a bit so that error won't repeat immediately, exponential backoff
					// strategy (min 1sec, max ~60sec)
					self.state = WorkerState::Throttled(
						(1.5f32).powf(std::cmp::min(10, self.consecutive_errors - 1) as f32),
					);
				}
			},
			WorkerState::Throttled(delay) => {
				// Sleep for given delay and go back to busy state
				select! {
					_ = tokio::time::sleep(Duration::from_secs_f32(delay)) => {
						self.state = WorkerState::Busy;
					}
					_ = self.stop_signal.changed() => (),
				}
			}
			WorkerState::Idle => {
				select! {
					new_st = self.worker.wait_for_work() => {
						self.state = new_st;
					}
					_ = self.stop_signal.changed() => (),
				}
			}
			WorkerState::Done => unreachable!(),
		}
	}
}
