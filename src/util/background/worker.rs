use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures::future::*;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::select;
use tokio::sync::{mpsc, watch};
use tracing::*;

use crate::background::WorkerInfo;
use crate::error::Error;
use crate::time::now_msec;

#[derive(PartialEq, Copy, Clone, Serialize, Deserialize, Debug)]
pub enum WorkerState {
	Busy,
	Throttled(f32),
	Idle,
	Done,
}

impl std::fmt::Display for WorkerState {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			WorkerState::Busy => write!(f, "Busy"),
			WorkerState::Throttled(t) => write!(f, "Thr:{:.3}", t),
			WorkerState::Idle => write!(f, "Idle"),
			WorkerState::Done => write!(f, "Done"),
		}
	}
}

#[async_trait]
pub trait Worker: Send {
	fn name(&self) -> String;

	fn info(&self) -> Option<String> {
		None
	}

	/// Work: do a basic unit of work, if one is available (otherwise, should return
	/// WorkerState::Idle immediately).  We will do our best to not interrupt this future in the
	/// middle of processing, it will only be interrupted at the last minute when Garage is trying
	/// to exit and this hasn't returned yet. This function may return an error to indicate that
	/// its unit of work could not be processed due to an error: the error will be logged and
	/// .work() will be called again after a short delay.
	async fn work(&mut self, must_exit: &mut watch::Receiver<bool>) -> Result<WorkerState, Error>;

	/// Wait for work: await for some task to become available.  This future can be interrupted in
	/// the middle for any reason.  This future doesn't have to await on must_exit.changed(), we
	/// are doing it for you.  Therefore it only receives a read refernce to must_exit which allows
	/// it to check if we are exiting.
	async fn wait_for_work(&mut self, must_exit: &watch::Receiver<bool>) -> WorkerState;
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
						let stop_signal_worker = self.stop_signal.clone();
						let mut worker = WorkerHandler {
								task_id,
								stop_signal,
								stop_signal_worker,
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
								i.info = worker.worker.info();
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
									info: worker.worker.info(),
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
		let drain_half_time = Instant::now() + Duration::from_secs(5);
		let drain_everything = async move {
			while let Some(mut worker) = workers.next().await {
				if worker.state == WorkerState::Done {
					info!(
						"Worker {} (TID {}) exited",
						worker.worker.name(),
						worker.task_id
					);
				} else if Instant::now() > drain_half_time {
					warn!("Worker {} (TID {}) interrupted between two iterations in state {:?} (this should be fine)", worker.worker.name(), worker.task_id, worker.state);
				} else {
					workers.push(
						async move {
							worker.step().await;
							worker
						}
						.boxed(),
					);
				}
			}
		};

		select! {
			_ = drain_everything => {
				info!("All workers exited peacefully \\o/");
			}
			_ = tokio::time::sleep(Duration::from_secs(9)) => {
				error!("Some workers could not exit in time, we are cancelling some things in the middle");
			}
		}
	}
}

struct WorkerHandler {
	task_id: usize,
	stop_signal: watch::Receiver<bool>,
	stop_signal_worker: watch::Receiver<bool>,
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
				if !*self.stop_signal.borrow() {
					select! {
						_ = tokio::time::sleep(Duration::from_secs_f32(delay)) => (),
						_ = self.stop_signal.changed() => (),
					}
				}
				self.state = WorkerState::Busy;
			}
			WorkerState::Idle => {
				if *self.stop_signal.borrow() {
					select! {
						new_st = self.worker.wait_for_work(&self.stop_signal_worker) => {
							self.state = new_st;
						}
						_ = tokio::time::sleep(Duration::from_secs(1)) => {
							// stay in Idle state
						}
					}
				} else {
					select! {
						new_st = self.worker.wait_for_work(&self.stop_signal_worker) => {
							self.state = new_st;
						}
						_ = self.stop_signal.changed() => {
							// stay in Idle state
						}
					}
				}
			}
			WorkerState::Done => unreachable!(),
		}
	}
}
