use bytes::Bytes;
use digest::Digest;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;

use crate::data::*;

/// Compute the sha256 of a slice,
/// spawning on a tokio thread for CPU-intensive processing
/// The argument has to be an owned Bytes, as it is moved out to a new thread.
pub async fn async_sha256sum(data: Bytes) -> Hash {
	tokio::task::spawn_blocking(move || sha256sum(&data))
		.await
		.unwrap()
}

/// Compute the blake2sum of a slice,
/// spawning on a tokio thread for CPU-intensive processing.
/// The argument has to be an owned Bytes, as it is moved out to a new thread.
pub async fn async_blake2sum(data: Bytes) -> Hash {
	tokio::task::spawn_blocking(move || blake2sum(&data))
		.await
		.unwrap()
}

// ----

pub struct AsyncHasher<D: Digest> {
	sendblk: mpsc::Sender<Bytes>,
	task: JoinHandle<digest::Output<D>>,
}

impl<D: Digest> AsyncHasher<D> {
	pub fn new() -> Self {
		let (sendblk, mut recvblk) = mpsc::channel::<Bytes>(1);
		let task = tokio::task::spawn_blocking(move || {
			let mut digest = D::new();
			while let Some(blk) = recvblk.blocking_recv() {
				digest.update(&blk[..]);
			}
			digest.finalize()
		});
		Self { sendblk, task }
	}

	pub async fn update(&self, b: Bytes) {
		self.sendblk.send(b).await.unwrap();
	}

	pub async fn finalize(self) -> digest::Output<D> {
		drop(self.sendblk);
		self.task.await.unwrap()
	}
}

impl<D: Digest> Default for AsyncHasher<D> {
	fn default() -> Self {
		Self::new()
	}
}
