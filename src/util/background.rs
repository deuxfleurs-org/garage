//! Job runner for futures and async functions
use core::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use futures::future::*;
use futures::select;
use tokio::sync::{mpsc, watch, Mutex};

use crate::error::Error;

type JobOutput = Result<(), Error>;
type Job = Pin<Box<dyn Future<Output = JobOutput> + Send>>;

/// Job runner for futures and async functions
pub struct BackgroundRunner {
	stop_signal: watch::Receiver<bool>,
	queue_in: mpsc::UnboundedSender<(Job, bool)>,
	worker_in: mpsc::UnboundedSender<tokio::task::JoinHandle<()>>,
}

impl BackgroundRunner {
	/// Create a new BackgroundRunner
	pub fn new(
		n_runners: usize,
		stop_signal: watch::Receiver<bool>,
	) -> (Arc<Self>, tokio::task::JoinHandle<()>) {
		let (worker_in, mut worker_out) = mpsc::unbounded_channel();

		let stop_signal_2 = stop_signal.clone();
		let await_all_done = tokio::spawn(async move {
			loop {
				let wkr = {
					select! {
						item = worker_out.recv().fuse() => {
							match item {
								Some(x) => x,
								None => break,
							}
						}
						_ = tokio::time::sleep(Duration::from_secs(5)).fuse() => {
							if *stop_signal_2.borrow() {
								break;
							} else {
								continue;
							}
						}
					}
				};
				if let Err(e) = wkr.await {
					error!("Error while awaiting for worker: {}", e);
				}
			}
		});

		let (queue_in, queue_out) = mpsc::unbounded_channel();
		let queue_out = Arc::new(Mutex::new(queue_out));

		for i in 0..n_runners {
			let queue_out = queue_out.clone();
			let stop_signal = stop_signal.clone();

			worker_in
				.send(tokio::spawn(async move {
					loop {
						let (job, cancellable) = {
							select! {
								item = wait_job(&queue_out).fuse() => match item {
									// We received a task, process it
									Some(x) => x,
									// We received a signal that no more tasks will ever be sent
									// because the sending side was dropped. Exit now.
									None => break,
								},
								_ = tokio::time::sleep(Duration::from_secs(5)).fuse() => {
									if *stop_signal.borrow() {
										// Nothing has been going on for 5 secs, and we are shutting
										// down. Exit now.
										break;
									} else {
										// Nothing is going on but we don't want to exit.
										continue;
									}
								}
							}
						};
						if cancellable && *stop_signal.borrow() {
							continue;
						}
						if let Err(e) = job.await {
							error!("Job failed: {}", e)
						}
					}
					info!("Background worker {} exiting", i);
				}))
				.unwrap();
		}

		let bgrunner = Arc::new(Self {
			stop_signal,
			queue_in,
			worker_in,
		});
		(bgrunner, await_all_done)
	}

	/// Spawn a task to be run in background
	pub fn spawn<T>(&self, job: T)
	where
		T: Future<Output = JobOutput> + Send + 'static,
	{
		let boxed: Job = Box::pin(job);
		self.queue_in
			.send((boxed, false))
			.map_err(|_| "could not put job in queue")
			.unwrap();
	}

	/// Spawn a task to be run in background. It may get discarded before running if spawned while
	/// the runner is stopping
	pub fn spawn_cancellable<T>(&self, job: T)
	where
		T: Future<Output = JobOutput> + Send + 'static,
	{
		let boxed: Job = Box::pin(job);
		self.queue_in
			.send((boxed, true))
			.map_err(|_| "could not put job in queue")
			.unwrap();
	}

	pub fn spawn_worker<F, T>(&self, name: String, worker: F)
	where
		F: FnOnce(watch::Receiver<bool>) -> T + Send + 'static,
		T: Future<Output = ()> + Send + 'static,
	{
		let stop_signal = self.stop_signal.clone();
		let task = tokio::spawn(async move {
			info!("Worker started: {}", name);
			worker(stop_signal).await;
			info!("Worker exited: {}", name);
		});
		self.worker_in
			.send(task)
			.map_err(|_| "could not put job in queue")
			.unwrap();
	}
}

async fn wait_job(q: &Mutex<mpsc::UnboundedReceiver<(Job, bool)>>) -> Option<(Job, bool)> {
	q.lock().await.recv().await
}
