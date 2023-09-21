use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use tokio::select;
use tokio::sync::watch;

use garage_util::background::*;
use garage_util::error::Error;

use crate::replication::*;
use crate::schema::*;
use crate::table::*;

const BATCH_SIZE: usize = 1024;

pub(crate) struct InsertQueueWorker<F, R>(pub(crate) Arc<Table<F, R>>)
where
	F: TableSchema,
	R: TableReplication;

#[async_trait]
impl<F: TableSchema, R: TableReplication> Worker for InsertQueueWorker<F, R> {
	fn name(&self) -> String {
		format!("{} queue", F::TABLE_NAME)
	}

	fn status(&self) -> WorkerStatus {
		WorkerStatus {
			queue_length: Some(self.0.data.insert_queue.len().unwrap_or(0) as u64),
			..Default::default()
		}
	}

	async fn work(&mut self, _must_exit: &mut watch::Receiver<bool>) -> Result<WorkerState, Error> {
		let mut kv_pairs = vec![];
		let mut values = vec![];

		for entry_kv in self.0.data.insert_queue.iter()? {
			let (k, v) = entry_kv?;

			values.push(self.0.data.decode_entry(&v)?);
			kv_pairs.push((k, v));

			if kv_pairs.len() > BATCH_SIZE {
				break;
			}
		}

		if kv_pairs.is_empty() {
			return Ok(WorkerState::Idle);
		}

		self.0.insert_many(values).await?;

		self.0.data.insert_queue.db().transaction(|tx| {
			for (k, v) in kv_pairs.iter() {
				if let Some(v2) = tx.get(&self.0.data.insert_queue, k)? {
					if &v2 == v {
						tx.remove(&self.0.data.insert_queue, k)?;
					}
				}
			}
			Ok(())
		})?;

		Ok(WorkerState::Busy)
	}

	async fn wait_for_work(&mut self) -> WorkerState {
		select! {
			_ = tokio::time::sleep(Duration::from_secs(600)) => (),
			_ = self.0.data.insert_queue_notify.notified() => (),
		}
		WorkerState::Busy
	}
}
