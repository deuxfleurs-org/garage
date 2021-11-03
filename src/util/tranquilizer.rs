use std::collections::VecDeque;
use std::time::{Duration, Instant};

use tokio::time::sleep;

/// A tranquilizer is a helper object that is used to make
/// background operations not take up too much time.
///
/// Background operations are done in a loop that does the following:
/// - do one step of the background process
/// - tranquilize, i.e. wait some time to not overload the system
///
/// The tranquilizer observes how long the steps take, and keeps
/// in memory a number of observations. The tranquilize operation
/// simply sleeps k * avg(observed step times), where k is
/// the tranquility factor. For instance with a tranquility of 2,
/// the tranquilizer will sleep on average 2 units of time for every
/// 1 unit of time spent doing the background task.
pub struct Tranquilizer {
	n_observations: usize,
	observations: VecDeque<Duration>,
	sum_observations: Duration,
	last_step_begin: Instant,
}

impl Tranquilizer {
	pub fn new(n_observations: usize) -> Self {
		Self {
			n_observations,
			observations: VecDeque::with_capacity(n_observations + 1),
			sum_observations: Duration::ZERO,
			last_step_begin: Instant::now(),
		}
	}

	pub async fn tranquilize(&mut self, tranquility: u32) {
		let observation = Instant::now() - self.last_step_begin;

		self.observations.push_back(observation);
		self.sum_observations += observation;

		while self.observations.len() > self.n_observations {
			self.sum_observations -= self.observations.pop_front().unwrap();
		}

		if !self.observations.is_empty() {
			let delay = (tranquility * self.sum_observations) / (self.observations.len() as u32);
			sleep(delay).await;
		}

		self.reset();
	}

	pub fn reset(&mut self) {
		self.last_step_begin = Instant::now();
	}
}
