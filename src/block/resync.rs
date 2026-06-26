use std::collections::{BTreeSet, HashSet};
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

use crate::manager::*;

// The delay between the time where a resync operation fails
// and the time when it is retried, with exponential backoff
// (multiplied by 2, 4, 8, 16, etc. for every consecutive failure).
pub(crate) const RESYNC_RETRY_DELAY: Duration = Duration::from_secs(60);
// The minimum retry delay is 60 seconds = 1 minute
// The maximum retry delay is 60 seconds * 2^6 = 60 seconds << 6 = 64 minutes (~1 hour)
pub(crate) const RESYNC_RETRY_DELAY_MAX_BACKOFF_POWER: u64 = 6;

pub(crate) fn retry_delay_ms(errors: u64) -> u64 {
	(RESYNC_RETRY_DELAY.as_millis() as u64)
		<< u64::min(errors, RESYNC_RETRY_DELAY_MAX_BACKOFF_POWER)
}

// No more than 4 resync workers can be running in the system
pub(crate) const MAX_RESYNC_WORKERS: usize = 8;
// Resync tranquility is initially set to 2, but can be changed in the CLI
// and the updated version is persisted over Garage restarts
const INITIAL_RESYNC_TRANQUILITY: u32 = 2;

/// Counts the number of errors when resyncing a block,
/// and the time of the last try.
///
/// Used to implement exponential backoff.
#[derive(Clone, Copy, Debug)]
pub(crate) struct ResyncEntry {
	pub(crate) when: u64,
	pub(crate) errors: u64,
}

impl ResyncEntry {
	const ENCODE_LEN: usize =
		{
			let res = 2 * size_of::<u64>();
			if res != size_of::<Self>() {
				panic!("Size of ResyncEntry has changed, you likely want to change the encoding as well")
			}
			res
		};
}

impl db::DbBytes for ResyncEntry {
	fn encode(&self) -> Vec<u8> {
		let res = [u64::to_be_bytes(self.when), u64::to_be_bytes(self.errors)].concat();
		assert_eq!(res.len(), Self::ENCODE_LEN);
		res
	}

	fn decode(bytes: &[u8]) -> std::result::Result<Self, db::DecodeError> {
		if bytes.len() != Self::ENCODE_LEN {
			return Err(db::DecodeError(
				format!(
					"invalid error counter: expected {}  bytes, got {}",
					Self::ENCODE_LEN,
					bytes.len()
				)
				.into(),
			));
		}
		// Split the encoding as [when || errors || last_try]
		let parts: [[u8; 8]; 2] = bytes.as_chunks().0.try_into().unwrap();
		// Decode each word
		let deser = parts.map(|serialized| u64::from_be_bytes(serialized));
		Ok(Self {
			when: deser[0],
			errors: deser[1],
		})
	}
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
// /!\ Order of fields matter for the derivation of Ord!
struct WhenIndexEntry {
	when: u64,
	hash: Hash,
}

pub struct IndexedQueue {
	queue: db::TypedTree<Hash, ResyncEntry>,
	when_index: BTreeSet<WhenIndexEntry>,
	errored: u64,
	busy_set: HashSet<Hash>,
}

impl IndexedQueue {
	fn entry(&mut self, hash: Hash) -> Result<IndexedQueueEntry<'_>, db::Error> {
		Ok(match self.queue.get(&hash)? {
			Some(value) => IndexedQueueEntry::Occupied(OccupiedEntry {
				origin: self,
				hash,
				value,
			}),
			None => IndexedQueueEntry::Vacant(VacantEntry { origin: self, hash }),
		})
	}

	fn clear(&mut self) -> Result<(), db::Error> {
		self.queue.clear()?;
		self.when_index.clear();
		Ok(())
	}

	/// Hash order
	pub(crate) fn iter_with_errors(
		&self,
	) -> Result<
		impl Iterator<Item = Result<(FixedBytes32, ResyncEntry), db::Error>> + use<'_>,
		db::Error,
	> {
		self.queue.iter()
	}

	pub fn errored(&self) -> u64 {
		self.errored
	}

	pub fn approximate_len(&self) -> Result<usize, garage_db::DbError> {
		self.queue.approximate_len()
	}
}

struct OccupiedEntry<'idxqueue> {
	origin: &'idxqueue mut IndexedQueue,
	hash: Hash,
	value: ResyncEntry,
}

impl<'idxqueue> OccupiedEntry<'idxqueue> {
	fn set_when_and_errors(&mut self, new_when: u64, new_errors: u64) -> Result<(), db::Error> {
		self.origin.queue.insert(
			&self.hash,
			&ResyncEntry {
				when: new_when,
				errors: new_errors,
			},
		)?;
		let was_there = self.origin.when_index.remove(&WhenIndexEntry {
			when: self.value.when,
			hash: self.hash,
		});
		debug_assert!(
			was_there,
			"The entry was not in the when index anymore (set_when_and_errors)"
		);
		self.origin.when_index.insert(WhenIndexEntry {
			when: new_when,
			hash: self.hash,
		});
		match (self.value.errors, new_errors) {
			(0, 0) => (),
			(0, _) => self.origin.errored = self.origin.errored.checked_add(1).unwrap(),
			(_, 0) => self.origin.errored = self.origin.errored.checked_sub(1).unwrap(),
			(_, _) => (),
		}
		self.value.when = new_when;
		self.value.errors = new_errors;
		Ok(())
	}

	fn set_when(&mut self, new_when: u64) -> Result<(), db::Error> {
		self.set_when_and_errors(new_when, self.errors())
	}

	fn remove(self) -> Result<(), db::Error> {
		self.origin.queue.remove(&self.hash)?;
		let was_there = self.origin.when_index.remove(&WhenIndexEntry {
			when: self.when(),
			hash: self.hash,
		});
		debug_assert!(
			was_there,
			"The entry was not in the when index anymore (remove)"
		);
		if self.value.errors > 0 {
			self.origin.errored = self.origin.errored.checked_sub(1).unwrap()
		}
		Ok(())
	}

	fn errors(&self) -> u64 {
		self.value.errors
	}

	fn when(&self) -> u64 {
		self.value.when
	}
}

struct VacantEntry<'idxqueue> {
	origin: &'idxqueue mut IndexedQueue,
	hash: Hash,
}
impl<'idxqueue> VacantEntry<'idxqueue> {
	fn insert(&mut self, when: u64) -> Result<(), db::Error> {
		self.origin
			.queue
			.insert(&self.hash, &ResyncEntry { when, errors: 0 })?;
		self.origin.when_index.insert(WhenIndexEntry {
			when,
			hash: self.hash,
		});
		Ok(())
	}
}

enum IndexedQueueEntry<'idxqueue> {
	Occupied(OccupiedEntry<'idxqueue>),
	Vacant(VacantEntry<'idxqueue>),
}

// There is a possibility of deadlock between inner and busy set.
// Avoid it by always locking busy_set first if you'll need it.
// (Locking only inner is fine)
pub struct BlockResyncManager {
	pub(crate) idxqueue: Arc<Mutex<IndexedQueue>>,
	pub(crate) notify: Arc<Notify>,
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
	IdleFor(Duration),
}

struct BusyBlock {
	when: u64,
	hash: Hash,
}

impl BlockResyncManager {
	pub(crate) fn new(db: &db::Db, system: &System) -> Result<Self, Error> {
		let queue = db
			.open_typed_tree::<Hash, ResyncEntry, _>("block_local_resync_queue_v2")
			.expect("Unable to open block_local_resync_queue tree");

		let persister = PersisterShared::new(&system.metadata_dir, "resync_cfg");

		let when_index = queue
			.iter()?
			.try_fold(BTreeSet::new(), |mut index, tree_row| {
				let (hash, ResyncEntry { when, .. }) = tree_row?;
				index.insert(WhenIndexEntry { when, hash });
				Ok::<_, Error>(index)
			})?;

		Ok(Self {
			persister,
			notify: Arc::new(Notify::new()),
			idxqueue: Arc::new(Mutex::new(IndexedQueue {
				queue,
				when_index,
				errored: 0,
				busy_set: HashSet::new(),
			})),
		})
	}

	/// Get length of resync queue
	pub fn queue_approximate_len(&self) -> Result<usize, Error> {
		let idxqueue = self.idxqueue.lock().unwrap();
		Ok(idxqueue.approximate_len()?)
	}

	/// Get length of resync queue
	pub fn errored(&self) -> usize {
		let idxqueue = self.idxqueue.lock().unwrap();
		idxqueue.errored().try_into().unwrap()
	}

	/// Clear the error counter for a block and put it in queue immediately
	pub fn clear_backoff(&self, hash: &Hash) -> Result<(), Error> {
		let now = now_msec();
		let mut idxqueue = self.idxqueue.lock().unwrap();
		if let IndexedQueueEntry::Occupied(mut resync_entry) = idxqueue.entry(*hash)? {
			if resync_entry.errors() > 0 {
				resync_entry.set_when(now)?;
				return Ok(());
			}
		}
		Err(Error::Message(format!(
			"Block {:?} was not in an errored state",
			hash
		)))
	}

	/// Clear the entire resync queue and list of errored blocks
	/// Corresponds to `garage repair clear-resync-queue`
	pub fn clear_resync_queue(&self) -> Result<(), Error> {
		let mut idxqueue = self.idxqueue.lock().unwrap();
		idxqueue.clear()?;
		Ok(())
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

	pub(crate) fn put_to_resync(&self, hash: &Hash, delay: Duration) -> Result<(), Error> {
		let when = now_msec() + delay.as_millis() as u64;
		self.put_to_resync_at(hash, when)
	}

	pub(crate) fn put_to_resync_at(&self, hash: &Hash, when: u64) -> Result<(), Error> {
		trace!("Put resync_queue: {} {:?}", when, hash);
		let mut idxqueue = self.idxqueue.lock().unwrap();
		match idxqueue.entry(*hash)? {
			IndexedQueueEntry::Occupied(mut occupied_entry) => {
				let old_when = occupied_entry.when();
				// TODO(armael) : decide on a merge policy
				occupied_entry.set_when(u64::min(old_when, when))?;
			}
			IndexedQueueEntry::Vacant(mut vacant_entry) => {
				vacant_entry.insert(when)?;
			}
		}
		self.notify.notify_waiters();
		Ok(())
	}

	async fn resync_iter(&self, manager: &BlockManager) -> Result<ResyncIterResult, db::Error> {
		let mut block = None;
		{
			let idxqueue = &mut *self.idxqueue.lock().unwrap();
			for &WhenIndexEntry { when, hash } in idxqueue.when_index.iter() {
				if !idxqueue.busy_set.contains(&hash) {
					idxqueue.busy_set.insert(hash);
					block = Some(BusyBlock { when, hash });
				}
			}
		}
		if let Some(BusyBlock { when, hash }) = block {
			let res = self
				.resync_iter_process_one_block(when, hash, manager)
				.await;
			let mut idxqueue = self.idxqueue.lock().unwrap();
			// This "lock" (ie removing from the busy set) will not be
			// released in case there is a panic in resync_iter_process_one_block
			// that is later caught, but this is currently not an issue
			let was_there = idxqueue.busy_set.remove(&hash);
			debug_assert!(was_there);
			res
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

	async fn resync_iter_process_one_block(
		&self,
		when: u64,
		hash: FixedBytes32,
		manager: &BlockManager,
	) -> Result<ResyncIterResult, garage_db::Error> {
		let time_msec = when;
		let now = now_msec();

		if now >= time_msec {
			let hash = hash;

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

			let mut idxqueue = self.idxqueue.lock().unwrap();
			let mut entry = match idxqueue.entry(hash)? {
				IndexedQueueEntry::Vacant(_) => {
					unreachable!("We should be the sole processor of this block and we have not removed it yet")
				}
				IndexedQueueEntry::Occupied(occupied_entry) => occupied_entry,
			};
			if let Err(e) = &res {
				manager.metrics.resync_error_counter.add(1);
				error!("Error when resyncing {:?}: {}", hash, e);

				entry.set_when_and_errors(
					now + retry_delay_ms(entry.errors()),
					entry.errors() + 1,
				)?;
			} else {
				entry.remove()?;
			}

			Ok(ResyncIterResult::BusyDidSomething)
		} else {
			Ok(ResyncIterResult::IdleFor(Duration::from_millis(
				time_msec - now,
			)))
		}
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

			let mut who = manager.storage_nodes_of(hash)?;
			if who.len() < manager.write_quorum {
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
			let storage_nodes = manager.storage_nodes_of(hash)?;

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

pub(crate) struct ResyncWorker {
	index: usize,
	manager: Arc<BlockManager>,
	tranquilizer: Tranquilizer,
	next_delay: Duration,
	persister: PersisterShared<ResyncPersistedConfig>,
	had_decode_error: bool,
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
			had_decode_error: false,
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
			queue_length: Some(self.manager.resync.queue_approximate_len().unwrap_or(0) as u64),
			tranquility: Some(tranquility),
			persistent_errors: Some(self.manager.resync.errored().try_into().unwrap()),
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
			Ok(ResyncIterResult::IdleFor(delay)) => {
				self.next_delay = delay;
				Ok(WorkerState::Idle)
			}
			Err(db::Error::Decode(e)) => {
				// We give it one second chance in the very unlikely case that the bytes would somehow
				// have been corrupted during read and that a new read would lead to a correct decoding.
				if self.had_decode_error {
					panic!("An error has happened when decoding something stored in the local k/v store: {}.", e);
				}
				self.had_decode_error = true;
				Ok(WorkerState::Busy)
			}
			Err(e) => {
				// The errors that we have here are only db errors
				// We don't really know how to handle them so just ¯\_(ツ)_/¯
				// (there is kind of an assumption that the db won't error on us,
				// if it does there is not much we can do -- TODO should we just panic?)
				// Here we just give the error to the worker manager,
				// it will print it to the logs and increment a counter
				self.had_decode_error = false;
				Err(e.into())
			}
		}
	}

	async fn wait_for_work(&mut self) -> WorkerState {
		while self.index >= self.persister.get_with(|x| x.n_workers) {
			self.manager.resync.notify.notified().await;
		}

		select! {
			_ = tokio::time::sleep(self.next_delay) => (),
			_ = self.manager.resync.notify.notified() => (),
		};

		WorkerState::Busy
	}
}
