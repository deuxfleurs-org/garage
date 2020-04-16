use core::future::Future;
use std::pin::Pin;

use futures::future::join_all;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::sync::{mpsc, watch};

use crate::error::Error;

type JobOutput = Result<(), Error>;
type Job = Pin<Box<dyn Future<Output = JobOutput> + Send>>;

pub struct BackgroundRunner {
	n_runners: usize,
	pub stop_signal: watch::Receiver<bool>,

	queue_in: mpsc::UnboundedSender<(Job, bool)>,
	queue_out: Mutex<mpsc::UnboundedReceiver<(Job, bool)>>,

	workers: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl BackgroundRunner {
	pub fn new(n_runners: usize, stop_signal: watch::Receiver<bool>) -> Arc<Self> {
		let (queue_in, queue_out) = mpsc::unbounded_channel();
		Arc::new(Self {
			n_runners,
			stop_signal,
			queue_in,
			queue_out: Mutex::new(queue_out),
			workers: Mutex::new(Vec::new()),
		})
	}

	pub async fn run(self: Arc<Self>) {
		let mut workers = self.workers.lock().await;
		for _i in 0..self.n_runners {
			workers.push(tokio::spawn(self.clone().runner()));
		}
		drop(workers);

		let mut stop_signal = self.stop_signal.clone();
		while let Some(exit_now) = stop_signal.recv().await {
			if exit_now {
				let mut workers = self.workers.lock().await;
				let workers_vec = workers.drain(..).collect::<Vec<_>>();
				join_all(workers_vec).await;
				return;
			}
		}
	}

	pub fn spawn<T>(&self, job: T)
	where
		T: Future<Output = JobOutput> + Send + 'static,
	{
		let boxed: Job = Box::pin(job);
		let _: Result<_, _> = self.queue_in.clone().send((boxed, false));
	}

	pub fn spawn_cancellable<T>(&self, job: T)
	where
		T: Future<Output = JobOutput> + Send + 'static,
	{
		let boxed: Job = Box::pin(job);
		let _: Result<_, _> = self.queue_in.clone().send((boxed, true));
	}

	pub async fn spawn_worker<F, T>(&self, worker: F)
	where
		F: FnOnce(watch::Receiver<bool>) -> T + Send + 'static,
		T: Future<Output = JobOutput> + Send + 'static,
	{
		let mut workers = self.workers.lock().await;
		let stop_signal = self.stop_signal.clone();
		workers.push(tokio::spawn(async move {
			if let Err(e) = worker(stop_signal).await {
				eprintln!("Worker stopped with error: {}", e);
			}
		}));
	}

	async fn runner(self: Arc<Self>) {
		let stop_signal = self.stop_signal.clone();
		loop {
			let must_exit: bool = *stop_signal.borrow();
			if let Some(job) = self.dequeue_job(must_exit).await {
				if let Err(e) = job.await {
					eprintln!("Job failed: {}", e)
				}
			} else {
				if must_exit {
					return;
				}
				tokio::time::delay_for(Duration::from_secs(1)).await;
			}
		}
	}

	async fn dequeue_job(&self, must_exit: bool) -> Option<Job> {
		let mut queue = self.queue_out.lock().await;
		while let Ok((job, cancellable)) = queue.try_recv() {
			if cancellable && must_exit {
				continue;
			} else {
				return Some(job);
			}
		}
		None
	}
}
