use std::time::{Duration, Instant};

use tokio::time::sleep;

pub struct TokenBucket {
	// Replenish rate: number of tokens per second
	replenish_rate: u64,
	// Current number of tokens
	tokens: u64,
	// Last replenish time
	last_replenish: Instant,
}

impl TokenBucket {
	pub fn new(replenish_rate: u64) -> Self {
		Self {
			replenish_rate,
			tokens: 0,
			last_replenish: Instant::now(),
		}
	}

	pub async fn take(&mut self, tokens: u64) {
		while self.tokens < tokens {
			let needed = tokens - self.tokens;
			let delay = (needed as f64) / (self.replenish_rate as f64);
			sleep(Duration::from_secs_f64(delay)).await;
			self.replenish();
		}
		self.tokens -= tokens;
	}

	pub fn replenish(&mut self) {
		let now = Instant::now();
		let new_tokens =
			((now - self.last_replenish).as_secs_f64() * (self.replenish_rate as f64)) as u64;
		self.tokens += new_tokens;
		self.last_replenish = now;
	}
}
