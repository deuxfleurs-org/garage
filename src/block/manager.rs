use std::convert::TryInto;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use futures::future::*;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::select;
use tokio::sync::{mpsc, watch, Mutex, Notify};

use opentelemetry::{
	trace::{FutureExt as OtelFutureExt, TraceContextExt, Tracer},
	Context, KeyValue,
};

use garage_db as db;
use garage_db::counted_tree_hack::CountedTree;

use garage_util::background::*;
use garage_util::data::*;
use garage_util::error::*;
use garage_util::metrics::RecordDuration;
use garage_util::time::*;
use garage_util::tranquilizer::Tranquilizer;

use garage_rpc::system::System;
use garage_rpc::*;

use garage_table::replication::{TableReplication, TableShardedReplication};

use crate::block::*;
use crate::metrics::*;
use crate::rc::*;
use crate::repair::*;

/// Size under which data will be stored inlined in database instead of as files
pub const INLINE_THRESHOLD: usize = 3072;

// Timeout for RPCs that read and write blocks to remote nodes
const BLOCK_RW_TIMEOUT: Duration = Duration::from_secs(30);
// Timeout for RPCs that ask other nodes whether they need a copy
// of a given block before we delete it locally
const NEED_BLOCK_QUERY_TIMEOUT: Duration = Duration::from_secs(5);

// The delay between the time where a resync operation fails
// and the time when it is retried, with exponential backoff
// (multiplied by 2, 4, 8, 16, etc. for every consecutive failure).
const RESYNC_RETRY_DELAY: Duration = Duration::from_secs(60);
// The minimum retry delay is 60 seconds = 1 minute
// The maximum retry delay is 60 seconds * 2^6 = 60 seconds << 6 = 64 minutes (~1 hour)
const RESYNC_RETRY_DELAY_MAX_BACKOFF_POWER: u64 = 6;

// The delay between the moment when the reference counter
// drops to zero, and the moment where we allow ourselves
// to delete the block locally.
pub(crate) const BLOCK_GC_DELAY: Duration = Duration::from_secs(600);

/// RPC messages used to share blocks of data between nodes
#[derive(Debug, Serialize, Deserialize)]
pub enum BlockRpc {
	Ok,
	/// Message to ask for a block of data, by hash
	GetBlock(Hash),
	/// Message to send a block of data, either because requested, of for first delivery of new
	/// block
	PutBlock {
		hash: Hash,
		data: DataBlock,
	},
	/// Ask other node if they should have this block, but don't actually have it
	NeedBlockQuery(Hash),
	/// Response : whether the node do require that block
	NeedBlockReply(bool),
}

impl Rpc for BlockRpc {
	type Response = Result<BlockRpc, Error>;
}

/// The block manager, handling block exchange between nodes, and block storage on local node
pub struct BlockManager {
	/// Replication strategy, allowing to find on which node blocks should be located
	pub replication: TableShardedReplication,
	/// Directory in which block are stored
	pub data_dir: PathBuf,

	compression_level: Option<i32>,
	background_tranquility: u32,

	mutation_lock: Mutex<BlockManagerLocked>,

	pub(crate) rc: BlockRc,

	resync_queue: CountedTree,
	resync_notify: Notify,
	resync_errors: CountedTree,

	pub(crate) system: Arc<System>,
	endpoint: Arc<Endpoint<BlockRpc, Self>>,

	metrics: BlockManagerMetrics,

	tx_scrub_command: ArcSwapOption<mpsc::Sender<ScrubWorkerCommand>>,
}

// This custom struct contains functions that must only be ran
// when the lock is held. We ensure that it is the case by storing
// it INSIDE a Mutex.
struct BlockManagerLocked();

enum ResyncIterResult {
	BusyDidSomething,
	BusyDidNothing,
	IdleFor(Duration),
}

impl BlockManager {
	pub fn new(
		db: &db::Db,
		data_dir: PathBuf,
		compression_level: Option<i32>,
		background_tranquility: u32,
		replication: TableShardedReplication,
		system: Arc<System>,
	) -> Arc<Self> {
		let rc = db
			.open_tree("block_local_rc")
			.expect("Unable to open block_local_rc tree");
		let rc = BlockRc::new(rc);

		let resync_queue = db
			.open_tree("block_local_resync_queue")
			.expect("Unable to open block_local_resync_queue tree");
		let resync_queue =
			CountedTree::new(resync_queue).expect("Could not count block_local_resync_queue");

		let resync_errors = db
			.open_tree("block_local_resync_errors")
			.expect("Unable to open block_local_resync_errors tree");
		let resync_errors =
			CountedTree::new(resync_errors).expect("Could not count block_local_resync_errors");

		let endpoint = system
			.netapp
			.endpoint("garage_block/manager.rs/Rpc".to_string());

		let manager_locked = BlockManagerLocked();

		let metrics = BlockManagerMetrics::new(resync_queue.clone(), resync_errors.clone());

		let block_manager = Arc::new(Self {
			replication,
			data_dir,
			compression_level,
			background_tranquility,
			mutation_lock: Mutex::new(manager_locked),
			rc,
			resync_queue,
			resync_notify: Notify::new(),
			resync_errors,
			system,
			endpoint,
			metrics,
			tx_scrub_command: ArcSwapOption::new(None),
		});
		block_manager.endpoint.set_handler(block_manager.clone());

		block_manager.clone().spawn_background_workers();

		block_manager
	}

	/// Ask nodes that might have a (possibly compressed) block for it
	async fn rpc_get_raw_block(&self, hash: &Hash) -> Result<DataBlock, Error> {
		let who = self.replication.read_nodes(hash);
		let resps = self
			.system
			.rpc
			.try_call_many(
				&self.endpoint,
				&who[..],
				BlockRpc::GetBlock(*hash),
				RequestStrategy::with_priority(PRIO_NORMAL)
					.with_quorum(1)
					.with_timeout(BLOCK_RW_TIMEOUT)
					.interrupt_after_quorum(true),
			)
			.await?;

		for resp in resps {
			if let BlockRpc::PutBlock { data, .. } = resp {
				return Ok(data);
			}
		}
		Err(Error::Message(format!(
			"Unable to read block {:?}: no valid blocks returned",
			hash
		)))
	}

	// ---- Public interface ----

	/// Ask nodes that might have a block for it
	pub async fn rpc_get_block(&self, hash: &Hash) -> Result<Vec<u8>, Error> {
		self.rpc_get_raw_block(hash).await?.verify_get(*hash)
	}

	/// Send block to nodes that should have it
	pub async fn rpc_put_block(&self, hash: Hash, data: Vec<u8>) -> Result<(), Error> {
		let who = self.replication.write_nodes(&hash);
		let data = DataBlock::from_buffer(data, self.compression_level);
		self.system
			.rpc
			.try_call_many(
				&self.endpoint,
				&who[..],
				BlockRpc::PutBlock { hash, data },
				RequestStrategy::with_priority(PRIO_NORMAL)
					.with_quorum(self.replication.write_quorum())
					.with_timeout(BLOCK_RW_TIMEOUT),
			)
			.await?;
		Ok(())
	}

	/// Get lenght of resync queue
	pub fn resync_queue_len(&self) -> Result<usize, Error> {
		// This currently can't return an error because the CountedTree hack
		// doesn't error on .len(), but this will change when we remove the hack
		// (hopefully someday!)
		Ok(self.resync_queue.len())
	}

	/// Get number of blocks that have an error
	pub fn resync_errors_len(&self) -> Result<usize, Error> {
		// (see resync_queue_len comment)
		Ok(self.resync_errors.len())
	}

	/// Get number of items in the refcount table
	pub fn rc_len(&self) -> Result<usize, Error> {
		Ok(self.rc.rc.len()?)
	}

	/// Send command to start/stop/manager scrub worker
	pub async fn send_scrub_command(&self, cmd: ScrubWorkerCommand) {
		let _ = self
			.tx_scrub_command
			.load()
			.as_ref()
			.unwrap()
			.send(cmd)
			.await;
	}

	//// ----- Managing the reference counter ----

	/// Increment the number of time a block is used, putting it to resynchronization if it is
	/// required, but not known
	pub fn block_incref(
		self: &Arc<Self>,
		tx: &mut db::Transaction,
		hash: Hash,
	) -> db::TxOpResult<()> {
		if self.rc.block_incref(tx, &hash)? {
			// When the reference counter is incremented, there is
			// normally a node that is responsible for sending us the
			// data of the block. However that operation may fail,
			// so in all cases we add the block here to the todo list
			// to check later that it arrived correctly, and if not
			// we will fecth it from someone.
			let this = self.clone();
			tokio::spawn(async move {
				if let Err(e) = this.put_to_resync(&hash, 2 * BLOCK_RW_TIMEOUT) {
					error!("Block {:?} could not be put in resync queue: {}.", hash, e);
				}
			});
		}
		Ok(())
	}

	/// Decrement the number of time a block is used
	pub fn block_decref(
		self: &Arc<Self>,
		tx: &mut db::Transaction,
		hash: Hash,
	) -> db::TxOpResult<()> {
		if self.rc.block_decref(tx, &hash)? {
			// When the RC is decremented, it might drop to zero,
			// indicating that we don't need the block.
			// There is a delay before we garbage collect it;
			// make sure that it is handled in the resync loop
			// after that delay has passed.
			let this = self.clone();
			tokio::spawn(async move {
				if let Err(e) = this.put_to_resync(&hash, BLOCK_GC_DELAY + Duration::from_secs(10))
				{
					error!("Block {:?} could not be put in resync queue: {}.", hash, e);
				}
			});
		}
		Ok(())
	}

	// ---- Reading and writing blocks locally ----

	/// Write a block to disk
	async fn write_block(&self, hash: &Hash, data: &DataBlock) -> Result<BlockRpc, Error> {
		let write_size = data.inner_buffer().len() as u64;

		let res = self
			.mutation_lock
			.lock()
			.await
			.write_block(hash, data, self)
			.bound_record_duration(&self.metrics.block_write_duration)
			.await?;

		self.metrics.bytes_written.add(write_size);

		Ok(res)
	}

	/// Read block from disk, verifying it's integrity
	pub(crate) async fn read_block(&self, hash: &Hash) -> Result<BlockRpc, Error> {
		let data = self
			.read_block_internal(hash)
			.bound_record_duration(&self.metrics.block_read_duration)
			.await?;

		self.metrics
			.bytes_read
			.add(data.inner_buffer().len() as u64);

		Ok(BlockRpc::PutBlock { hash: *hash, data })
	}

	async fn read_block_internal(&self, hash: &Hash) -> Result<DataBlock, Error> {
		let mut path = self.block_path(hash);
		let compressed = match self.is_block_compressed(hash).await {
			Ok(c) => c,
			Err(e) => {
				// Not found but maybe we should have had it ??
				self.put_to_resync(hash, 2 * BLOCK_RW_TIMEOUT)?;
				return Err(Into::into(e));
			}
		};
		if compressed {
			path.set_extension("zst");
		}
		let mut f = fs::File::open(&path).await?;

		let mut data = vec![];
		f.read_to_end(&mut data).await?;
		drop(f);

		let data = if compressed {
			DataBlock::Compressed(data)
		} else {
			DataBlock::Plain(data)
		};

		if data.verify(*hash).is_err() {
			self.metrics.corruption_counter.add(1);

			self.mutation_lock
				.lock()
				.await
				.move_block_to_corrupted(hash, self)
				.await?;
			self.put_to_resync(hash, Duration::from_millis(0))?;
			return Err(Error::CorruptData(*hash));
		}

		Ok(data)
	}

	/// Check if this node should have a block, but don't actually have it
	async fn need_block(&self, hash: &Hash) -> Result<bool, Error> {
		let BlockStatus { exists, needed } = self
			.mutation_lock
			.lock()
			.await
			.check_block_status(hash, self)
			.await?;
		Ok(needed.is_nonzero() && !exists)
	}

	/// Utility: gives the path of the directory in which a block should be found
	fn block_dir(&self, hash: &Hash) -> PathBuf {
		let mut path = self.data_dir.clone();
		path.push(hex::encode(&hash.as_slice()[0..1]));
		path.push(hex::encode(&hash.as_slice()[1..2]));
		path
	}

	/// Utility: give the full path where a block should be found, minus extension if block is
	/// compressed
	fn block_path(&self, hash: &Hash) -> PathBuf {
		let mut path = self.block_dir(hash);
		path.push(hex::encode(hash.as_ref()));
		path
	}

	/// Utility: check if block is stored compressed. Error if block is not stored
	async fn is_block_compressed(&self, hash: &Hash) -> Result<bool, Error> {
		let mut path = self.block_path(hash);
		path.set_extension("zst");
		if fs::metadata(&path).await.is_ok() {
			return Ok(true);
		}
		path.set_extension("");
		fs::metadata(&path).await.map(|_| false).map_err(Into::into)
	}

	// ---- Resync loop ----

	// This part manages a queue of blocks that need to be
	// "resynchronized", i.e. that need to have a check that
	// they are at present if we need them, or that they are
	// deleted once the garbage collection delay has passed.
	//
	// Here are some explanations on how the resync queue works.
	// There are two Sled trees that are used to have information
	// about the status of blocks that need to be resynchronized:
	//
	// - resync_queue: a tree that is ordered first by a timestamp
	//   (in milliseconds since Unix epoch) that is the time at which
	//   the resync must be done, and second by block hash.
	//   The key in this tree is just:
	//       concat(timestamp (8 bytes), hash (32 bytes))
	//   The value is the same 32-byte hash.
	//
	// - resync_errors: a tree that indicates for each block
	//   if the last resync resulted in an error, and if so,
	//   the following two informations (see the ErrorCounter struct):
	//   - how many consecutive resync errors for this block?
	//   - when was the last try?
	//   These two informations are used to implement an
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
	// resync_queue, it checks in resync_errors that if there is an
	// exponential back-off delay to await, it has passed before we
	// process the item. If not, the item in the queue is skipped
	// (but added back for later processing after the time of the
	// delay).
	//
	// An alternative that would have seemed natural is to
	// only add items to resync_queue with a processing time that is
	// after the delay, but there are several issues with this:
	// - This requires to synchronize updates to resync_queue and
	//   resync_errors (with the current model, there is only one thread,
	//   the worker thread, that accesses resync_errors,
	//   so no need to synchronize) by putting them both in a lock.
	//   This would mean that block_incref might need to take a lock
	//   before doing its thing, meaning it has much more chances of
	//   not completing successfully if something bad happens to Garage.
	//   Currently Garage is not able to recover from block_incref that
	//   doesn't complete successfully, because it is necessary to ensure
	//   the consistency between the state of the block manager and
	//   information in the BlockRef table.
	// - If a resync fails, we put that block in the resync_errors table,
	//   and also add it back to resync_queue to be processed after
	//   the exponential back-off delay,
	//   but maybe the block is already scheduled to be resynced again
	//   at another time that is before the exponential back-off delay,
	//   and we have no way to check that easily. This means that
	//   in all cases, we need to check the resync_errors table
	//   in the resync loop at the time when a block is popped from
	//   the resync_queue.
	// Overall, the current design is therefore simpler and more robust
	// because it tolerates inconsistencies between the resync_queue
	// and resync_errors table (items being scheduled in resync_queue
	// for times that are earlier than the exponential back-off delay
	// is a natural condition that is handled properly).

	fn spawn_background_workers(self: Arc<Self>) {
		// Launch a background workers for background resync loop processing
		let background = self.system.background.clone();
		let worker = ResyncWorker::new(self.clone());
		tokio::spawn(async move {
			tokio::time::sleep(Duration::from_secs(10)).await;
			background.spawn_worker(worker);
		});

		// Launch a background worker for data store scrubs
		let (scrub_tx, scrub_rx) = mpsc::channel(1);
		self.tx_scrub_command.store(Some(Arc::new(scrub_tx)));
		let scrub_worker = ScrubWorker::new(self.clone(), scrub_rx);
		self.system.background.spawn_worker(scrub_worker);
	}

	pub(crate) fn put_to_resync(&self, hash: &Hash, delay: Duration) -> db::Result<()> {
		let when = now_msec() + delay.as_millis() as u64;
		self.put_to_resync_at(hash, when)
	}

	fn put_to_resync_at(&self, hash: &Hash, when: u64) -> db::Result<()> {
		trace!("Put resync_queue: {} {:?}", when, hash);
		let mut key = u64::to_be_bytes(when).to_vec();
		key.extend(hash.as_ref());
		self.resync_queue.insert(key, hash.as_ref())?;
		self.resync_notify.notify_waiters();
		Ok(())
	}

	async fn resync_iter(&self) -> Result<ResyncIterResult, db::Error> {
		if let Some((time_bytes, hash_bytes)) = self.resync_queue.first()? {
			let time_msec = u64::from_be_bytes(time_bytes[0..8].try_into().unwrap());
			let now = now_msec();

			if now >= time_msec {
				let hash = Hash::try_from(&hash_bytes[..]).unwrap();

				if let Some(ec) = self.resync_errors.get(hash.as_slice())? {
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
						self.resync_queue.remove(time_bytes)?;
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
					.resync_block(&hash)
					.with_context(Context::current_with_span(span))
					.bound_record_duration(&self.metrics.resync_duration)
					.await;

				self.metrics.resync_counter.add(1);

				if let Err(e) = &res {
					self.metrics.resync_error_counter.add(1);
					warn!("Error when resyncing {:?}: {}", hash, e);

					let err_counter = match self.resync_errors.get(hash.as_slice())? {
						Some(ec) => ErrorCounter::decode(&ec).add1(now + 1),
						None => ErrorCounter::new(now + 1),
					};

					self.resync_errors
						.insert(hash.as_slice(), err_counter.encode())?;

					self.put_to_resync_at(&hash, err_counter.next_try())?;
					// err_counter.next_try() >= now + 1 > now,
					// the entry we remove from the queue is not
					// the entry we inserted with put_to_resync_at
					self.resync_queue.remove(time_bytes)?;
				} else {
					self.resync_errors.remove(hash.as_slice())?;
					self.resync_queue.remove(time_bytes)?;
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

	async fn resync_block(&self, hash: &Hash) -> Result<(), Error> {
		let BlockStatus { exists, needed } = self
			.mutation_lock
			.lock()
			.await
			.check_block_status(hash, self)
			.await?;

		if exists != needed.is_needed() || exists != needed.is_nonzero() {
			debug!(
				"Resync block {:?}: exists {}, nonzero rc {}, deletable {}",
				hash,
				exists,
				needed.is_nonzero(),
				needed.is_deletable(),
			);
		}

		if exists && needed.is_deletable() {
			info!("Resync block {:?}: offloading and deleting", hash);

			let mut who = self.replication.write_nodes(hash);
			if who.len() < self.replication.write_quorum() {
				return Err(Error::Message("Not trying to offload block because we don't have a quorum of nodes to write to".to_string()));
			}
			who.retain(|id| *id != self.system.id);

			let msg = Arc::new(BlockRpc::NeedBlockQuery(*hash));
			let who_needs_fut = who.iter().map(|to| {
				self.system.rpc.call_arc(
					&self.endpoint,
					*to,
					msg.clone(),
					RequestStrategy::with_priority(PRIO_BACKGROUND)
						.with_timeout(NEED_BLOCK_QUERY_TIMEOUT),
				)
			});
			let who_needs_resps = join_all(who_needs_fut).await;

			let mut need_nodes = vec![];
			for (node, needed) in who.iter().zip(who_needs_resps.into_iter()) {
				match needed.err_context("NeedBlockQuery RPC")? {
					BlockRpc::NeedBlockReply(needed) => {
						if needed {
							need_nodes.push(*node);
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
					self.metrics
						.resync_send_counter
						.add(1, &[KeyValue::new("to", format!("{:?}", node))]);
				}

				let put_block_message = self.read_block(hash).await?;
				self.system
					.rpc
					.try_call_many(
						&self.endpoint,
						&need_nodes[..],
						put_block_message,
						RequestStrategy::with_priority(PRIO_BACKGROUND)
							.with_quorum(need_nodes.len())
							.with_timeout(BLOCK_RW_TIMEOUT),
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

			self.mutation_lock
				.lock()
				.await
				.delete_if_unneeded(hash, self)
				.await?;

			self.rc.clear_deleted_block_rc(hash)?;
		}

		if needed.is_nonzero() && !exists {
			info!(
				"Resync block {:?}: fetching absent but needed block (refcount > 0)",
				hash
			);

			let block_data = self.rpc_get_raw_block(hash).await?;

			self.metrics.resync_recv_counter.add(1);

			self.write_block(hash, &block_data).await?;
		}

		Ok(())
	}
}

#[async_trait]
impl EndpointHandler<BlockRpc> for BlockManager {
	async fn handle(
		self: &Arc<Self>,
		message: &BlockRpc,
		_from: NodeID,
	) -> Result<BlockRpc, Error> {
		match message {
			BlockRpc::PutBlock { hash, data } => self.write_block(hash, data).await,
			BlockRpc::GetBlock(h) => self.read_block(h).await,
			BlockRpc::NeedBlockQuery(h) => self.need_block(h).await.map(BlockRpc::NeedBlockReply),
			m => Err(Error::unexpected_rpc_message(m)),
		}
	}
}

struct ResyncWorker {
	manager: Arc<BlockManager>,
	tranquilizer: Tranquilizer,
	next_delay: Duration,
}

impl ResyncWorker {
	fn new(manager: Arc<BlockManager>) -> Self {
		Self {
			manager,
			tranquilizer: Tranquilizer::new(30),
			next_delay: Duration::from_secs(10),
		}
	}
}

#[async_trait]
impl Worker for ResyncWorker {
	fn name(&self) -> String {
		"Block resync worker".into()
	}

	fn info(&self) -> Option<String> {
		let mut ret = vec![];
		let qlen = self.manager.resync_queue_len().unwrap_or(0);
		let elen = self.manager.resync_errors_len().unwrap_or(0);
		if qlen > 0 {
			ret.push(format!("{} blocks in queue", qlen));
		}
		if elen > 0 {
			ret.push(format!("{} blocks in error state", elen));
		}
		if !ret.is_empty() {
			Some(ret.join(", "))
		} else {
			None
		}
	}

	async fn work(&mut self, _must_exit: &mut watch::Receiver<bool>) -> Result<WorkerState, Error> {
		self.tranquilizer.reset();
		match self.manager.resync_iter().await {
			Ok(ResyncIterResult::BusyDidSomething) => Ok(self
				.tranquilizer
				.tranquilize_worker(self.manager.background_tranquility)),
			Ok(ResyncIterResult::BusyDidNothing) => Ok(WorkerState::Busy),
			Ok(ResyncIterResult::IdleFor(delay)) => {
				self.next_delay = delay;
				Ok(WorkerState::Idle)
			}
			Err(e) => {
				// The errors that we have here are only Sled errors
				// We don't really know how to handle them so just ¯\_(ツ)_/¯
				// (there is kind of an assumption that Sled won't error on us,
				// if it does there is not much we can do -- TODO should we just panic?)
				// Here we just give the error to the worker manager,
				// it will print it to the logs and increment a counter
				Err(e.into())
			}
		}
	}

	async fn wait_for_work(&mut self, _must_exit: &watch::Receiver<bool>) -> WorkerState {
		select! {
			_ = tokio::time::sleep(self.next_delay) => (),
			_ = self.manager.resync_notify.notified() => (),
		};
		WorkerState::Busy
	}
}

struct BlockStatus {
	exists: bool,
	needed: RcEntry,
}

impl BlockManagerLocked {
	async fn check_block_status(
		&self,
		hash: &Hash,
		mgr: &BlockManager,
	) -> Result<BlockStatus, Error> {
		let exists = mgr.is_block_compressed(hash).await.is_ok();
		let needed = mgr.rc.get_block_rc(hash)?;

		Ok(BlockStatus { exists, needed })
	}

	async fn write_block(
		&self,
		hash: &Hash,
		data: &DataBlock,
		mgr: &BlockManager,
	) -> Result<BlockRpc, Error> {
		let compressed = data.is_compressed();
		let data = data.inner_buffer();

		let mut path = mgr.block_dir(hash);
		let directory = path.clone();
		path.push(hex::encode(hash));

		fs::create_dir_all(&directory).await?;

		let to_delete = match (mgr.is_block_compressed(hash).await, compressed) {
			(Ok(true), _) => return Ok(BlockRpc::Ok),
			(Ok(false), false) => return Ok(BlockRpc::Ok),
			(Ok(false), true) => {
				let path_to_delete = path.clone();
				path.set_extension("zst");
				Some(path_to_delete)
			}
			(Err(_), compressed) => {
				if compressed {
					path.set_extension("zst");
				}
				None
			}
		};

		let mut path2 = path.clone();
		path2.set_extension("tmp");
		let mut f = fs::File::create(&path2).await?;
		f.write_all(data).await?;
		f.sync_all().await?;
		drop(f);

		fs::rename(path2, path).await?;
		if let Some(to_delete) = to_delete {
			fs::remove_file(to_delete).await?;
		}

		// We want to ensure that when this function returns, data is properly persisted
		// to disk. The first step is the sync_all above that does an fsync on the data file.
		// Now, we do an fsync on the containing directory, to ensure that the rename
		// is persisted properly. See:
		// http://thedjbway.b0llix.net/qmail/syncdir.html
		let dir = fs::OpenOptions::new()
			.read(true)
			.mode(0)
			.open(directory)
			.await?;
		dir.sync_all().await?;
		drop(dir);

		Ok(BlockRpc::Ok)
	}

	async fn move_block_to_corrupted(&self, hash: &Hash, mgr: &BlockManager) -> Result<(), Error> {
		warn!(
			"Block {:?} is corrupted. Renaming to .corrupted and resyncing.",
			hash
		);
		let mut path = mgr.block_path(hash);
		let mut path2 = path.clone();
		if mgr.is_block_compressed(hash).await? {
			path.set_extension("zst");
			path2.set_extension("zst.corrupted");
		} else {
			path2.set_extension("corrupted");
		}
		fs::rename(path, path2).await?;
		Ok(())
	}

	async fn delete_if_unneeded(&self, hash: &Hash, mgr: &BlockManager) -> Result<(), Error> {
		let BlockStatus { exists, needed } = self.check_block_status(hash, mgr).await?;

		if exists && needed.is_deletable() {
			let mut path = mgr.block_path(hash);
			if mgr.is_block_compressed(hash).await? {
				path.set_extension("zst");
			}
			fs::remove_file(path).await?;
			mgr.metrics.delete_counter.add(1);
		}
		Ok(())
	}
}

/// Counts the number of errors when resyncing a block,
/// and the time of the last try.
/// Used to implement exponential backoff.
#[derive(Clone, Copy, Debug)]
struct ErrorCounter {
	errors: u64,
	last_try: u64,
}

impl ErrorCounter {
	fn new(now: u64) -> Self {
		Self {
			errors: 1,
			last_try: now,
		}
	}

	fn decode(data: &[u8]) -> Self {
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
	fn next_try(&self) -> u64 {
		self.last_try + self.delay_msec()
	}
}
