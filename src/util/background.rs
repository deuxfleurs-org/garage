use core::future::Future;
use std::pin::Pin;
use std::sync::Mutex;

use arc_swap::ArcSwapOption;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};

use crate::error::Error;

type JobOutput = Result<(), Error>;
type Job = Pin<Box<dyn Future<Output = JobOutput> + Send>>;

pub struct BackgroundRunner {
	pub stop_signal: watch::Receiver<bool>,

	queue_in: ArcSwapOption<mpsc::UnboundedSender<(Job, bool)>>,

	workers: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl BackgroundRunner {
	pub fn new(n_runners: usize, stop_signal: watch::Receiver<bool>) -> Arc<Self> {
		let (queue_in, queue_out) = mpsc::unbounded_channel();

		let mut workers = vec![];
		let queue_out = Arc::new(tokio::sync::Mutex::new(queue_out));

		for i in 0..n_runners {
			let queue_out = queue_out.clone();
			let stop_signal = stop_signal.clone();

			workers.push(tokio::spawn(async move {
				while let Some((job, cancellable)) = queue_out.lock().await.recv().await {
					if cancellable && *stop_signal.borrow() {
						continue;
					}
					if let Err(e) = job.await {
						error!("Job failed: {}", e)
					}
				}
				info!("Worker {} exiting", i);
			}));
		}

		Arc::new(Self {
			stop_signal,
			queue_in: ArcSwapOption::new(Some(Arc::new(queue_in))),
			workers: Mutex::new(workers),
		})
	}

	pub async fn run(self: Arc<Self>) {
		let mut stop_signal = self.stop_signal.clone();

		loop {
			let exit_now = match stop_signal.changed().await {
				Ok(()) => *stop_signal.borrow(),
				Err(e) => {
					error!("Watch .changed() error: {}", e);
					true
				}
			};
			if exit_now {
				break;
			}
		}

		info!("Closing background job queue_in...");
		drop(self.queue_in.swap(None));

		info!("Waiting for all workers to terminate...");
		while let Some(task) = self.workers.lock().unwrap().pop() {
			if let Err(e) = task.await {
				warn!("Error awaiting task: {}", e);
			}
		}
	}

	// Spawn a task to be run in background
	pub async fn spawn<T>(&self, job: T)
	where
		T: Future<Output = JobOutput> + Send + 'static,
	{
		match self.queue_in.load().as_ref() {
			Some(chan) => {
				let boxed: Job = Box::pin(job);
				chan.send((boxed, false)).map_err(|_| "send error").unwrap();
			}
			None => {
				warn!("Doing background job now because we are exiting...");
				if let Err(e) = job.await {
					warn!("Task failed: {}", e);
				}
			}
		}
	}

	pub fn spawn_cancellable<T>(&self, job: T)
	where
		T: Future<Output = JobOutput> + Send + 'static,
	{
		match self.queue_in.load().as_ref() {
			Some(chan) => {
				let boxed: Job = Box::pin(job);
				chan.send((boxed, false)).map_err(|_| "send error").unwrap();
			}
			None => (), // drop job if we are exiting
		}
	}

	pub fn spawn_worker<F, T>(&self, name: String, worker: F)
	where
		F: FnOnce(watch::Receiver<bool>) -> T + Send + 'static,
		T: Future<Output = ()> + Send + 'static,
	{
		let mut workers = self.workers.lock().unwrap();
		let stop_signal = self.stop_signal.clone();
		workers.push(tokio::spawn(async move {
			worker(stop_signal).await;
			info!("Worker exited: {}", name);
		}));
	}
}
