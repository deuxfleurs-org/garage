use std::collections::HashSet;
use std::convert::TryInto;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use tokio::select;
use tokio::sync::{watch, Notify};

use opentelemetry::{
	trace::{FutureExt as OtelFutureExt, TraceContextExt, Tracer},
	Context, KeyValue,
};

use garage_db as db;

use garage_util::background::*;
use garage_util::data::*;
use garage_util::error::*;
use garage_util::metrics::RecordDuration;
use garage_util::persister::PersisterShared;
use garage_util::time::*;
use garage_util::tranquilizer::Tranquilizer;

use garage_rpc::system::System;
use garage_rpc::*;

use garage_table::replication::TableReplication;

use crate::manager::*;

// The delay between the time where a resync operation fails
// and the time when it is retried, with exponential backoff
// (multiplied by 2, 4, 8, 16, etc. for every consecutive failure).
pub(crate) const RESYNC_RETRY_DELAY: Duration = Duration::from_secs(60);
// The minimum retry delay is 60 seconds = 1 minute
// The maximum retry delay is 60 seconds * 2^6 = 60 seconds << 6 = 64 minutes (~1 hour)
pub(crate) const RESYNC_RETRY_DELAY_MAX_BACKOFF_POWER: u64 = 6;

// No more than 4 resync workers can be running in the system
pub(crate) const MAX_RESYNC_WORKERS: usize = 8;
// Resync tranquility is initially set to 2, but can be changed in the CLI
// and the updated version is persisted over Garage restarts
const INITIAL_RESYNC_TRANQUILITY: u32 = 2;

pub struct BlockResyncManager {
	pub(crate) queue: db::Tree,
	pub(crate) notify: Arc<Notify>,
	pub(crate) errors: db::Tree,

	busy_set: BusySet,

	persister: PersisterShared<ResyncPersistedConfig>,
}

#[derive(Serialize, Deserialize, Clone, Copy)]
struct ResyncPersistedConfig {
	n_workers: usize,
	tranquility: u32,
}
impl garage_util::migrate::InitialFormat for ResyncPersistedConfig {}
impl Default for ResyncPersistedConfig {
	fn default() -> Self {
		ResyncPersistedConfig {
			n_workers: 1,
			tranquility: INITIAL_RESYNC_TRANQUILITY,
		}
	}
}

enum ResyncIterResult {
	BusyDidSomething,
	BusyDidNothing,
	IdleFor(Duration),
}

type BusySet = Arc<Mutex<HashSet<Vec<u8>>>>;

struct BusyBlock {
	time_bytes: Vec<u8>,
	hash_bytes: Vec<u8>,
	busy_set: BusySet,
}

impl BlockResyncManager {
	pub(crate) fn new(db: &db::Db, system: &System) -> Self {
		let queue = db
			.open_tree("block_local_resync_queue")
			.expect("Unable to open block_local_resync_queue tree");

		let errors = db
			.open_tree("block_local_resync_errors")
			.expect("Unable to open block_local_resync_errors tree");

		let persister = PersisterShared::new(&system.metadata_dir, "resync_cfg");

		Self {
			queue,
			notify: Arc::new(Notify::new()),
			errors,
			busy_set: Arc::new(Mutex::new(HashSet::new())),
			persister,
		}
	}

	/// Get length of resync queue
	pub fn queue_len(&self) -> Result<usize, Error> {
		Ok(self.queue.len()?)
	}

	/// Get number of blocks that have an error
	pub fn errors_len(&self) -> Result<usize, Error> {
		Ok(self.errors.len()?)
	}

	/// Clear the error counter for a block and put it in queue immediately
	pub fn clear_backoff(&self, hash: &Hash) -> Result<(), Error> {
		let now = now_msec();
		if let Some(ec) = self.errors.get(hash)? {
			let mut ec = ErrorCounter::decode(&ec);
			if ec.errors > 0 {
				ec.last_try = now - ec.delay_msec();
				self.errors.insert(hash, ec.encode())?;
				self.put_to_resync_at(hash, now)?;
				return Ok(());
			}
		}
		Err(Error::Message(format!(
			"Block {:?} was not in an errored state",
			hash
		)))
	}

	pub fn register_bg_vars(&self, vars: &mut vars::BgVars) {
		let notify = self.notify.clone();
		vars.register_rw(
			&self.persister,
			"resync-worker-count",
			|p| p.get_with(|x| x.n_workers),
			move |p, n_workers| {
				if !(1..=MAX_RESYNC_WORKERS).contains(&n_workers) {
					return Err(Error::Message(format!(
						"Invalid number of resync workers, must be between 1 and {}",
						MAX_RESYNC_WORKERS
					)));
				}
				p.set_with(|x| x.n_workers = n_workers)?;
				notify.notify_waiters();
				Ok(())
			},
		);

		let notify = self.notify.clone();
		vars.register_rw(
			&self.persister,
			"resync-tranquility",
			|p| p.get_with(|x| x.tranquility),
			move |p, tranquility| {
				p.set_with(|x| x.tranquility = tranquility)?;
				notify.notify_waiters();
				Ok(())
			},
		);
	}

	// ---- Resync loop ----

	// This part manages a queue of blocks that need to be
	// "resynchronized", i.e. that need to have a check that
	// they are at present if we need them, or that they are
	// deleted once the garbage collection delay has passed.
	//
	// Here are some explanations on how the resync queue works.
	// There are two db trees that are used to have information
	// about the status of blocks that need to be resynchronized:
	//
	// - resync.queue: a tree that is ordered first by a timestamp
	//   (in milliseconds since Unix epoch) that is the time at which
	//   the resync must be done, and second by block hash.
	//   The key in this tree is just:
	//       concat(timestamp (8 bytes), hash (32 bytes))
	//   The value is the same 32-byte hash.
	//
	// - resync.errors: a tree that indicates for each block
	//   if the last resync resulted in an error, and if so,
	//   the following two information (see the ErrorCounter struct):
	//   - how many consecutive resync errors for this block?
	//   - when was the last try?
	//   These two information are used to implement an
	//   exponential backoff retry strategy.
	//   The key in this tree is the 32-byte hash of the block,
	//   and the value is the encoded ErrorCounter value.
	//
	// We need to have these two trees, because the resync queue
	// is not just a queue of items to process, but a set of items
	// that are waiting a specific delay until we can process them
	// (the delay being necessary both internally for the exponential
	// backoff strategy, and exposed as a parameter when adding items
	// to the queue, e.g. to wait until the GC delay has passed).
	// This is why we need one tree ordered by time, and one
	// ordered by identifier of item to be processed (block hash).
	//
	// When the worker wants to process an item it takes from
	// resync.queue, it checks in resync.errors that if there is an
	// exponential back-off delay to await, it has passed before we
	// process the item. If not, the item in the queue is skipped
	// (but added back for later processing after the time of the
	// delay).
	//
	// An alternative that would have seemed natural is to
	// only add items to resync.queue with a processing time that is
	// after the delay, but there are several issues with this:
	// - This requires to synchronize updates to resync.queue and
	//   resync.errors (with the current model, there is only one thread,
	//   the worker thread, that accesses resync.errors,
	//   so no need to synchronize) by putting them both in a lock.
	//   This would mean that block_incref might need to take a lock
	//   before doing its thing, meaning it has much more chances of
	//   not completing successfully if something bad happens to Garage.
	//   Currently Garage is not able to recover from block_incref that
	//   doesn't complete successfully, because it is necessary to ensure
	//   the consistency between the state of the block manager and
	//   information in the BlockRef table.
	// - If a resync fails, we put that block in the resync.errors table,
	//   and also add it back to resync.queue to be processed after
	//   the exponential back-off delay,
	//   but maybe the block is already scheduled to be resynced again
	//   at another time that is before the exponential back-off delay,
	//   and we have no way to check that easily. This means that
	//   in all cases, we need to check the resync.errors table
	//   in the resync loop at the time when a block is popped from
	//   the resync.queue.
	// Overall, the current design is therefore simpler and more robust
	// because it tolerates inconsistencies between the resync.queue
	// and resync.errors table (items being scheduled in resync.queue
	// for times that are earlier than the exponential back-off delay
	// is a natural condition that is handled properly).

	pub(crate) fn put_to_resync(&self, hash: &Hash, delay: Duration) -> db::Result<()> {
		let when = now_msec() + delay.as_millis() as u64;
		self.put_to_resync_at(hash, when)
	}

	pub(crate) fn put_to_resync_at(&self, hash: &Hash, when: u64) -> db::Result<()> {
		trace!("Put resync_queue: {} {:?}", when, hash);
		let mut key = u64::to_be_bytes(when).to_vec();
		key.extend(hash.as_ref());
		self.queue.insert(key, hash.as_ref())?;
		self.notify.notify_waiters();
		Ok(())
	}

	async fn resync_iter(&self, manager: &BlockManager) -> Result<ResyncIterResult, db::Error> {
		if let Some(block) = self.get_block_to_resync()? {
			let time_msec = u64::from_be_bytes(block.time_bytes[0..8].try_into().unwrap());
			let now = now_msec();

			if now >= time_msec {
				let hash = Hash::try_from(&block.hash_bytes[..]).unwrap();

				if let Some(ec) = self.errors.get(hash.as_slice())? {
					let ec = ErrorCounter::decode(&ec);
					if now < ec.next_try() {
						// if next retry after an error is not yet,
						// don't do resync and return early, but still
						// make sure the item is still in queue at expected time
						self.put_to_resync_at(&hash, ec.next_try())?;
						// ec.next_try() > now >= time_msec, so this remove
						// is not removing the one we added just above
						// (we want to do the remove after the insert to ensure
						// that the item is not lost if we crash in-between)
						self.queue.remove(&block.time_bytes)?;
						return Ok(ResyncIterResult::BusyDidNothing);
					}
				}

				let tracer = opentelemetry::global::tracer("garage");
				let trace_id = gen_uuid();
				let span = tracer
					.span_builder("Resync block")
					.with_trace_id(
						opentelemetry::trace::TraceId::from_hex(&hex::encode(
							&trace_id.as_slice()[..16],
						))
						.unwrap(),
					)
					.with_attributes(vec![KeyValue::new("block", format!("{:?}", hash))])
					.start(&tracer);

				let res = self
					.resync_block(manager, &hash)
					.with_context(Context::current_with_span(span))
					.bound_record_duration(&manager.metrics.resync_duration)
					.await;

				manager.metrics.resync_counter.add(1);

				if let Err(e) = &res {
					manager.metrics.resync_error_counter.add(1);
					error!("Error when resyncing {:?}: {}", hash, e);

					let err_counter = match self.errors.get(hash.as_slice())? {
						Some(ec) => ErrorCounter::decode(&ec).add1(now + 1),
						None => ErrorCounter::new(now + 1),
					};

					self.errors.insert(hash.as_slice(), err_counter.encode())?;

					self.put_to_resync_at(&hash, err_counter.next_try())?;
					// err_counter.next_try() >= now + 1 > now,
					// the entry we remove from the queue is not
					// the entry we inserted with put_to_resync_at
					self.queue.remove(&block.time_bytes)?;
				} else {
					self.errors.remove(hash.as_slice())?;
					self.queue.remove(&block.time_bytes)?;
				}

				Ok(ResyncIterResult::BusyDidSomething)
			} else {
				Ok(ResyncIterResult::IdleFor(Duration::from_millis(
					time_msec - now,
				)))
			}
		} else {
			// Here we wait either for a notification that an item has been
			// added to the queue, or for a constant delay of 10 secs to expire.
			// The delay avoids a race condition where the notification happens
			// between the time we checked the queue and the first poll
			// to resync_notify.notified(): if that happens, we'll just loop
			// back 10 seconds later, which is fine.
			Ok(ResyncIterResult::IdleFor(Duration::from_secs(10)))
		}
	}

	fn get_block_to_resync(&self) -> Result<Option<BusyBlock>, db::Error> {
		let mut busy = self.busy_set.lock().unwrap();
		for it in self.queue.iter()? {
			let (time_bytes, hash_bytes) = it?;
			if !busy.contains(&time_bytes) {
				busy.insert(time_bytes.clone());
				return Ok(Some(BusyBlock {
					time_bytes,
					hash_bytes,
					busy_set: self.busy_set.clone(),
				}));
			}
		}
		Ok(None)
	}

	async fn resync_block(&self, manager: &BlockManager, hash: &Hash) -> Result<(), Error> {
		let existing_path = manager.find_block(hash).await;
		let exists = existing_path.is_some();
		let rc = manager.rc.get_block_rc(hash)?;

		if exists != rc.is_needed() || exists != rc.is_nonzero() {
			debug!(
				"Resync block {:?}: exists {}, nonzero rc {}, deletable {}",
				hash,
				exists,
				rc.is_nonzero(),
				rc.is_deletable(),
			);
		}

		if exists && rc.is_deletable() {
			if manager.rc.recalculate_rc(hash)?.0 > 0 {
				return Err(Error::Message(format!(
					"Refcount for block {:?} was inconsistent, retrying later",
					hash
				)));
			}

			info!("Resync block {:?}: offloading and deleting", hash);
			let existing_path = existing_path.unwrap();

			let mut who = manager
				.system
				.cluster_layout()
				.current_storage_nodes_of(hash);
			if who.len() < manager.replication.write_quorum() {
				return Err(Error::Message("Not trying to offload block because we don't have a quorum of nodes to write to".to_string()));
			}
			who.retain(|id| *id != manager.system.id);

			let who_needs_resps = manager
				.system
				.rpc_helper()
				.call_many(
					&manager.endpoint,
					&who,
					BlockRpc::NeedBlockQuery(*hash),
					RequestStrategy::with_priority(PRIO_BACKGROUND),
				)
				.await?;

			let mut need_nodes = vec![];
			for (node, needed) in who_needs_resps {
				match needed.err_context("NeedBlockQuery RPC")? {
					BlockRpc::NeedBlockReply(needed) => {
						if needed {
							need_nodes.push(node);
						}
					}
					m => {
						return Err(Error::unexpected_rpc_message(m));
					}
				}
			}

			if !need_nodes.is_empty() {
				trace!(
					"Block {:?} needed by {} nodes, sending",
					hash,
					need_nodes.len()
				);

				for node in need_nodes.iter() {
					manager
						.metrics
						.resync_send_counter
						.add(1, &[KeyValue::new("to", format!("{:?}", node))]);
				}

				let block = manager.read_block_from(hash, &existing_path).await?;
				let (header, bytes) = block.into_parts();
				let put_block_message = Req::new(BlockRpc::PutBlock {
					hash: *hash,
					header,
				})?
				.with_stream_from_buffer(bytes);
				manager
					.system
					.rpc_helper()
					.try_call_many(
						&manager.endpoint,
						&need_nodes,
						put_block_message,
						RequestStrategy::with_priority(PRIO_BACKGROUND | PRIO_SECONDARY)
							.with_quorum(need_nodes.len()),
					)
					.await
					.err_context("PutBlock RPC")?;
			}
			info!(
				"Deleting unneeded block {:?}, offload finished ({} / {})",
				hash,
				need_nodes.len(),
				who.len()
			);

			manager.delete_if_unneeded(hash).await?;

			manager.rc.clear_deleted_block_rc(hash)?;
		}

		if rc.is_nonzero() && !exists {
			// The refcount is > 0, and the block is not present locally.
			// We might need to fetch it from another node.

			// First, check whether we are still supposed to store that
			// block in the latest cluster layout version.
			let storage_nodes = manager
				.system
				.cluster_layout()
				.current_storage_nodes_of(&hash);

			if !storage_nodes.contains(&manager.system.id) {
				info!(
					"Resync block {:?}: block is absent with refcount > 0, but it will drop to zero after all metadata is synced. Not fetching the block.",
					hash
				);
				return Ok(());
			}

			// We know we need the block. Fetch it.
			info!(
				"Resync block {:?}: fetching absent but needed block (refcount > 0)",
				hash
			);

			let block_data = manager
				.rpc_get_raw_block(hash, PRIO_BACKGROUND | PRIO_SECONDARY, None)
				.await;
			if matches!(block_data, Err(Error::MissingBlock(_))) {
				warn!(
					"Could not fetch needed block {:?}, no node returned valid data. Checking that refcount is correct.",
					hash
				);
				manager.rc.recalculate_rc(hash)?;
			}
			let block_data = block_data?;

			manager.metrics.resync_recv_counter.add(1);

			manager.write_block(hash, &block_data).await?;
		}

		Ok(())
	}
}

impl Drop for BusyBlock {
	fn drop(&mut self) {
		let mut busy = self.busy_set.lock().unwrap();
		busy.remove(&self.time_bytes);
	}
}

pub(crate) struct ResyncWorker {
	index: usize,
	manager: Arc<BlockManager>,
	tranquilizer: Tranquilizer,
	next_delay: Duration,
	persister: PersisterShared<ResyncPersistedConfig>,
}

impl ResyncWorker {
	pub(crate) fn new(index: usize, manager: Arc<BlockManager>) -> Self {
		let persister = manager.resync.persister.clone();
		Self {
			index,
			manager,
			tranquilizer: Tranquilizer::new(30),
			next_delay: Duration::from_secs(10),
			persister,
		}
	}
}

#[async_trait]
impl Worker for ResyncWorker {
	fn name(&self) -> String {
		format!("Block resync worker #{}", self.index + 1)
	}

	fn status(&self) -> WorkerStatus {
		let (n_workers, tranquility) = self.persister.get_with(|x| (x.n_workers, x.tranquility));

		if self.index >= n_workers {
			return WorkerStatus {
				freeform: vec!["This worker is currently disabled".into()],
				..Default::default()
			};
		}

		WorkerStatus {
			queue_length: Some(self.manager.resync.queue_len().unwrap_or(0) as u64),
			tranquility: Some(tranquility),
			persistent_errors: Some(self.manager.resync.errors_len().unwrap_or(0) as u64),
			..Default::default()
		}
	}

	async fn work(&mut self, _must_exit: &mut watch::Receiver<bool>) -> Result<WorkerState, Error> {
		let (n_workers, tranquility) = self.persister.get_with(|x| (x.n_workers, x.tranquility));

		if self.index >= n_workers {
			return Ok(WorkerState::Idle);
		}

		self.tranquilizer.reset();
		match self.manager.resync.resync_iter(&self.manager).await {
			Ok(ResyncIterResult::BusyDidSomething) => {
				Ok(self.tranquilizer.tranquilize_worker(tranquility))
			}
			Ok(ResyncIterResult::BusyDidNothing) => Ok(WorkerState::Busy),
			Ok(ResyncIterResult::IdleFor(delay)) => {
				self.next_delay = delay;
				Ok(WorkerState::Idle)
			}
			Err(e) => {
				// The errors that we have here are only db errors
				// We don't really know how to handle them so just ¯\_(ツ)_/¯
				// (there is kind of an assumption that the db won't error on us,
				// if it does there is not much we can do -- TODO should we just panic?)
				// Here we just give the error to the worker manager,
				// it will print it to the logs and increment a counter
				Err(e.into())
			}
		}
	}

	async fn wait_for_work(&mut self) -> WorkerState {
		while self.index >= self.persister.get_with(|x| x.n_workers) {
			self.manager.resync.notify.notified().await
		}

		select! {
			_ = tokio::time::sleep(self.next_delay) => (),
			_ = self.manager.resync.notify.notified() => (),
		};

		WorkerState::Busy
	}
}

/// Counts the number of errors when resyncing a block,
/// and the time of the last try.
/// Used to implement exponential backoff.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ErrorCounter {
	pub(crate) errors: u64,
	pub(crate) last_try: u64,
}

impl ErrorCounter {
	fn new(now: u64) -> Self {
		Self {
			errors: 1,
			last_try: now,
		}
	}

	pub(crate) fn decode(data: &[u8]) -> Self {
		Self {
			errors: u64::from_be_bytes(data[0..8].try_into().unwrap()),
			last_try: u64::from_be_bytes(data[8..16].try_into().unwrap()),
		}
	}

	fn encode(&self) -> Vec<u8> {
		[
			u64::to_be_bytes(self.errors),
			u64::to_be_bytes(self.last_try),
		]
		.concat()
	}

	fn add1(self, now: u64) -> Self {
		Self {
			errors: self.errors + 1,
			last_try: now,
		}
	}

	fn delay_msec(&self) -> u64 {
		(RESYNC_RETRY_DELAY.as_millis() as u64)
			<< std::cmp::min(self.errors - 1, RESYNC_RETRY_DELAY_MAX_BACKOFF_POWER)
	}

	pub(crate) fn next_try(&self) -> u64 {
		self.last_try + self.delay_msec()
	}
}
