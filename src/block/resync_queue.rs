use std::collections::{BTreeSet, HashMap, HashSet};
use std::convert::TryInto;
use std::sync::Mutex;

use tokio::sync::Notify;

use garage_db as db;

use garage_util::data::*;
use garage_util::error::*;

// Maximum number of blocks added to the resync queue in a single db transaction
// when flushing deferred puts
const DEFERRED_PUTS_BATCH_SIZE: usize = 1024;

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
		// Split the encoding as [when || errors]
		let parts: [[u8; 8]; 2] = bytes.as_chunks().0.try_into().unwrap();
		// Decode each word
		let deser: [u64; 2] = parts.map(u64::from_be_bytes);
		Ok(Self {
			when: deser[0],
			errors: deser[1],
		})
	}
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Debug)]
// /!\ Order of fields matter for the derivation of Ord!
pub(crate) struct WhenIndexEntry {
	pub(crate) when: u64,
	pub(crate) hash: Hash,
}

pub struct IndexedQueue {
	queue: db::TypedTree<Hash, ResyncEntry>,
	// Index by when in the queue (the same set of hashes is contained in both)
	pub(crate) when_index: BTreeSet<WhenIndexEntry>,
	// Numbers of elements of queue that have errors > 0
	errored: u64,
	// This is the set of blocks being resynced by one of the workers.
	// All the hashes here must appear in queue as well.
	pub(crate) busy_set: HashSet<Hash>,
}

impl IndexedQueue {
	pub(crate) fn load(queue: db::TypedTree<Hash, ResyncEntry>) -> Result<Self, Error> {
		let (when_index, errored) =
			queue
				.iter()?
				.try_fold((BTreeSet::new(), 0), |(mut index, errored), tree_row| {
					let (hash, ResyncEntry { when, errors }) = tree_row?;
					index.insert(WhenIndexEntry { when, hash });
					let errored = errored + if errors > 0 { 1 } else { 0 };
					Ok::<_, Error>((index, errored))
				})?;
		Ok(Self {
			queue,
			when_index,
			errored,
			busy_set: HashSet::new(),
		})
	}

	/// Put many blocks in the queue using a single db transaction, with the
	/// same semantics as `put_to_resync_at_or_later` for each of them.
	/// Each hash must appear at most once in `items`.
	fn put_many(&mut self, items: &[(Hash, u64)]) -> Result<(), db::Error> {
		let queue = &self.queue;
		// (hash, when before this batch if it was queued, when after this batch)
		let changes = queue
			.db()
			.transaction(|tx| {
				let mut changes = Vec::with_capacity(items.len());
				for &(hash, when) in items {
					let current = queue.tx_get(tx, &hash)?;
					let errors = match current {
						Some(cur) if cur.when >= when => continue,
						Some(cur) => cur.errors,
						None => 0,
					};
					queue.tx_insert(tx, &hash, &ResyncEntry { when, errors })?;
					changes.push((hash, current.map(|cur| cur.when), when));
				}
				Ok(changes)
			})
			.map_err(|e| e.cannot_have_aborted())?;

		for (hash, old_when, new_when) in changes {
			self.reindex(hash, old_when, new_when);
		}
		Ok(())
	}

	/// Move `hash` in the when index from `old_when` (if it was queued) to `new_when`
	fn reindex(&mut self, hash: Hash, old_when: Option<u64>, new_when: u64) {
		if let Some(old_when) = old_when {
			let was_there = self.when_index.remove(&WhenIndexEntry {
				when: old_when,
				hash,
			});
			debug_assert!(was_there, "The entry was not in the when index anymore");
		}
		self.when_index.insert(WhenIndexEntry {
			when: new_when,
			hash,
		});
	}

	pub(crate) fn entry(&mut self, hash: Hash) -> Result<IndexedQueueEntry<'_>, db::Error> {
		Ok(match self.queue.get(&hash)? {
			Some(value) => IndexedQueueEntry::Occupied(OccupiedEntry {
				origin: self,
				hash,
				value,
			}),
			None => IndexedQueueEntry::Vacant(VacantEntry { origin: self, hash }),
		})
	}

	pub(crate) fn clear(&mut self) -> Result<(), db::Error> {
		self.queue.clear()?;
		self.when_index.clear();
		Ok(())
	}

	/// Hash order
	pub(crate) fn iter(
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

	// May actually be exact in our case because of the mutex
	pub fn approximate_len(&self) -> Result<usize, garage_db::DbError> {
		self.queue.approximate_len()
	}

	pub fn put_to_resync_at_or_later(&mut self, hash: &Hash, when: u64) -> Result<(), Error> {
		trace!("Put resync_queue: {} {:?}", when, hash);
		match self.entry(*hash)? {
			IndexedQueueEntry::Occupied(mut occupied_entry) => {
				let old_when = occupied_entry.when();
				// We consider that `when` is a constraint indicating that a resync should be done
				// but would lead to worth performance or maybe incorrect behavior if done before the given date,
				// so we give precedence to the latest of the two.
				occupied_entry.set_when(u64::max(old_when, when))?;
			}
			IndexedQueueEntry::Vacant(mut vacant_entry) => {
				vacant_entry.insert(when)?;
			}
		}
		Ok(())
	}
}

pub(crate) struct OccupiedEntry<'idxqueue> {
	pub(crate) origin: &'idxqueue mut IndexedQueue,
	pub(crate) hash: Hash,
	pub(crate) value: ResyncEntry,
}

impl<'idxqueue> OccupiedEntry<'idxqueue> {
	pub(crate) fn set_when_and_errors(
		&mut self,
		new_when: u64,
		new_errors: u64,
	) -> Result<(), db::Error> {
		self.origin.queue.insert(
			&self.hash,
			&ResyncEntry {
				when: new_when,
				errors: new_errors,
			},
		)?;
		self.origin
			.reindex(self.hash, Some(self.value.when), new_when);
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

	pub(crate) fn set_when(&mut self, new_when: u64) -> Result<(), db::Error> {
		self.set_when_and_errors(new_when, self.errors())
	}

	pub(crate) fn remove(self) -> Result<(), db::Error> {
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
			self.origin.errored = self.origin.errored.checked_sub(1).unwrap();
		}
		Ok(())
	}

	pub(crate) fn errors(&self) -> u64 {
		self.value.errors
	}

	pub(crate) fn when(&self) -> u64 {
		self.value.when
	}
}

pub(crate) struct VacantEntry<'idxqueue> {
	pub(crate) origin: &'idxqueue mut IndexedQueue,
	pub(crate) hash: Hash,
}

impl<'idxqueue> VacantEntry<'idxqueue> {
	pub(crate) fn insert(&mut self, when: u64) -> Result<(), db::Error> {
		self.origin
			.queue
			.insert(&self.hash, &ResyncEntry { when, errors: 0 })?;
		self.origin.reindex(self.hash, None, when);
		Ok(())
	}
}

pub(crate) enum IndexedQueueEntry<'idxqueue> {
	Occupied(OccupiedEntry<'idxqueue>),
	Vacant(VacantEntry<'idxqueue>),
}

// -------

/// Blocks waiting to be added to the resync queue, see `put_to_resync_after_commit`
#[derive(Default)]
pub(crate) struct DeferredPuts {
	items: HashMap<Hash, u64>,
	// Whether a flush task is running; it keeps going until `items` is empty
	flush_running: bool,
}

impl DeferredPuts {
	/// Add a block, keeping the latest `when` if it is already pending.
	/// Returns true if the caller has to start a flush task.
	pub(crate) fn push(&mut self, hash: Hash, when: u64) -> bool {
		self.items
			.entry(hash)
			.and_modify(|w| *w = u64::max(*w, when))
			.or_insert(when);
		!std::mem::replace(&mut self.flush_running, true)
	}
}

/// Clears `flush_running` if the flush task exits without finishing (a panic),
/// so that the next deferred put starts a new flush instead of leaving the
/// pending list stuck forever.
struct ClearFlushRunningOnDrop<'a>(Option<&'a Mutex<DeferredPuts>>);

impl ClearFlushRunningOnDrop<'_> {
	fn cancel(&mut self) {
		self.0 = None;
	}
}

impl Drop for ClearFlushRunningOnDrop<'_> {
	fn drop(&mut self) {
		if let Some(deferred) = self.0.take() {
			let mut deferred = deferred.lock().unwrap_or_else(|e| e.into_inner());
			deferred.flush_running = false;
		}
	}
}

pub(crate) fn flush_deferred_puts(
	idxqueue: &Mutex<IndexedQueue>,
	deferred: &Mutex<DeferredPuts>,
	notify: &Notify,
) {
	let mut cleanup = ClearFlushRunningOnDrop(Some(deferred));
	loop {
		let items = {
			let mut deferred = deferred.lock().unwrap();
			if deferred.items.is_empty() {
				// Must be cleared under the same lock as the emptiness check,
				// otherwise a concurrent push could see a flush still running
				deferred.flush_running = false;
				cleanup.cancel();
				return;
			}
			std::mem::take(&mut deferred.items)
		};
		let items = items.into_iter().collect::<Vec<_>>();
		for batch in items.chunks(DEFERRED_PUTS_BATCH_SIZE) {
			match idxqueue.lock().unwrap().put_many(batch) {
				Ok(()) => notify.notify_waiters(),
				Err(e) => error!(
					"{} blocks could not be put in resync queue: {}.",
					batch.len(),
					e
				),
			}
		}
	}
}

// -----------

pub(crate) mod v1 {
	use std::convert::TryInto as _;

	use garage_db::{self as db, Error};
	use garage_util::data::Hash;

	use super::ResyncEntry;

	/// Key of the resync queue tree: blocks are resynced in order of increasing
	/// `when` (msec timestamp of the next try), with the block hash as tie-breaker.
	///
	// CAREFUL: this type implements `DbOrdKey`, so its byte encoding must be
	// order-preserving.
	// The derived `Ord` compares fields in declaration order, which must match
	// the order in which `encode()` writes them; and `when` must remain an
	// *unsigned* integer, as the big-endian encoding is only order-preserving
	// for unsigned types.
	#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
	struct ResyncQueueKey {
		when: u64,
		hash: Hash,
	}

	impl db::DbBytes for ResyncQueueKey {
		fn encode(&self) -> Vec<u8> {
			let mut v = Vec::with_capacity(40);
			v.extend_from_slice(&u64::to_be_bytes(self.when));
			v.extend_from_slice(self.hash.as_slice());
			v
		}

		fn decode(bytes: &[u8]) -> std::result::Result<Self, db::DecodeError> {
			if bytes.len() != 40 {
				return Err(db::DecodeError(
					format!(
						"invalid resync queue key: expected 40 bytes, got {}",
						bytes.len()
					)
					.into(),
				));
			}
			Ok(ResyncQueueKey {
				when: u64::from_be_bytes(bytes[..8].try_into().unwrap()),
				hash: Hash::try_from(&bytes[8..])
					.ok_or_else(|| db::DecodeError("invalid resync queue key: bad hash".into()))?,
			})
		}
	}

	impl db::DbOrdKey for ResyncQueueKey {}

	/// Counts the number of errors when resyncing a block,
	/// and the time of the last try.
	///
	/// Used to implement exponential backoff.
	#[derive(Clone, Copy, Debug)]
	struct ErrorCounter {
		errors: u64,
		last_try: u64,
	}

	impl db::DbBytes for ErrorCounter {
		fn encode(&self) -> Vec<u8> {
			let mut v = Vec::with_capacity(16);
			v.extend_from_slice(&u64::to_be_bytes(self.errors));
			v.extend_from_slice(&u64::to_be_bytes(self.last_try));
			v
		}

		fn decode(bytes: &[u8]) -> std::result::Result<Self, db::DecodeError> {
			if bytes.len() != 16 {
				return Err(db::DecodeError(
					format!(
						"invalid error counter: expected 16 bytes, got {}",
						bytes.len()
					)
					.into(),
				));
			}
			Ok(Self {
				errors: u64::from_be_bytes(bytes[..8].try_into().unwrap()),
				last_try: u64::from_be_bytes(bytes[8..].try_into().unwrap()),
			})
		}
	}

	/// Migrate resync queue and error table from v1 (two trees) to v2 (one merged tree).
	///
	/// v1 format:
	///   `block_local_resync_queue`  — `TypedTree<ResyncQueueKey, Hash>`
	///   `block_local_resync_errors` — `TypedTree<Hash, ErrorCounter>`
	///
	/// v2 format:
	///   `block_local_resync_queue_v2` — `TypedTree<Hash, ResyncEntry>`
	///
	/// Safe to call again after an interrupted run. If the old queue is empty (either
	/// never populated or already cleared by a prior completed migration) the function is a no-op.
	/// The old trees are emptied rather than dropped because the `Db` API has no drop-tree primitive.
	pub(crate) fn migrate_resync_queue_v1_to_v2(db: &db::Db) -> Result<(), Error> {
		let old_queue =
			db.open_typed_tree::<ResyncQueueKey, Hash, _>("block_local_resync_queue")?;
		if old_queue.is_empty()? {
			return Ok(());
		}

		let new_queue =
			db.open_typed_tree::<Hash, ResyncEntry, _>("block_local_resync_queue_v2")?;
		let old_errors =
			db.open_typed_tree::<Hash, ErrorCounter, _>("block_local_resync_errors")?;

		// All reads and writes below are done in a single db-level transaction: mixing a
		// standalone iterator (which holds its own read transaction open) with further
		// standalone reads/writes on the same thread is invalid for the LMDB backend
		// (`MDB_BAD_RSLOT: Invalid reuse of reader locktable slot`).
		db.transaction(|tx| {
			// The old queue is ordered by (when, hash). For a given hash, its occurrences are
			// therefore visited in ascending `when` order, so overwriting on every match
			// means the last (and thus highest) `when` we see for a hash is the one that survives
			// in the v2 tree — consistent with the max-wins semantics used when updating an
			// already-queued entry elsewhere (see `put_to_resync_at_or_later`).
			let rows = old_queue.tx_iter(tx)?.collect::<db::TxOpResult<Vec<_>>>()?;

			for (ResyncQueueKey { when, hash }, _) in rows {
				let errors = old_errors
					.tx_get(tx, &hash)?
					.map(|ec| ec.errors)
					.unwrap_or(0);
				new_queue.tx_insert(tx, &hash, &ResyncEntry { when, errors })?;
			}

			old_queue.tx_clear(tx)?;
			old_errors.tx_clear(tx)?;
			Ok(())
		})
		.map_err(|e| e.cannot_have_aborted())
	}

	#[cfg(test)]
	mod test {
		use super::*;

		fn h(byte: u8) -> Hash {
			Hash::from([byte; 32])
		}

		/// Regression test for a bug where `migrate_resync_queue_v1_to_v2` mixed a
		/// standalone iterator (which keeps its own LMDB read transaction open for the
		/// duration of the loop) with further standalone reads and writes on the same
		/// thread while migrating a non-empty v1 queue. On the LMDB backend, opening
		/// those extra transactions while the iterator's read transaction was still
		/// alive produced:
		/// "LMDB: MDB_BAD_RSLOT: Invalid reuse of reader locktable slot"
		///
		/// This only reproduces against a real LMDB-backed `Db`, since the bug is
		/// specific to how the `heed`/LMDB backend hands out reader-lock-table slots
		/// per thread.
		#[test]
		fn migrate_resync_queue_v1_to_v2_with_lmdb_backend() {
			let tmp_dir = mktemp::Temp::new_dir().unwrap();
			let db = db::open_db(
				&tmp_dir.to_path_buf(),
				db::Engine::Lmdb,
				&db::OpenOpt::default(),
			)
			.expect("failed to open temporary lmdb database");

			let old_queue = db
				.open_typed_tree::<ResyncQueueKey, Hash, _>("block_local_resync_queue")
				.unwrap();
			let old_errors = db
				.open_typed_tree::<Hash, ErrorCounter, _>("block_local_resync_errors")
				.unwrap();

			let h1 = h(1);
			let h2 = h(2);

			// h2 is queued twice: the migration must keep the entry with the highest
			// `when`, following the max-wins semantics used elsewhere in this module.
			old_queue
				.insert(
					&ResyncQueueKey {
						when: 100,
						hash: h1,
					},
					&h1,
				)
				.unwrap();
			old_queue
				.insert(
					&ResyncQueueKey {
						when: 200,
						hash: h2,
					},
					&h2,
				)
				.unwrap();
			old_queue
				.insert(
					&ResyncQueueKey {
						when: 300,
						hash: h2,
					},
					&h2,
				)
				.unwrap();
			old_errors
				.insert(
					&h2,
					&ErrorCounter {
						errors: 3,
						last_try: 250,
					},
				)
				.unwrap();

			// Before the fix, this call errored out with MDB_BAD_RSLOT as soon as the
			// old queue was non-empty.
			migrate_resync_queue_v1_to_v2(&db).expect("migration failed");

			assert!(old_queue.is_empty().unwrap());
			assert!(old_errors.is_empty().unwrap());

			let new_queue = db
				.open_typed_tree::<Hash, ResyncEntry, _>("block_local_resync_queue_v2")
				.unwrap();

			let e1 = new_queue
				.get(&h1)
				.unwrap()
				.expect("h1 missing from v2 queue");
			assert_eq!((e1.when, e1.errors), (100, 0));

			let e2 = new_queue
				.get(&h2)
				.unwrap()
				.expect("h2 missing from v2 queue");
			assert_eq!((e2.when, e2.errors), (300, 3));
		}
	}
}

#[cfg(test)]
mod test {
	use super::*;

	fn h(byte: u8) -> Hash {
		Hash::from([byte; 32])
	}

	fn open_queue(tmp_dir: &mktemp::Temp) -> db::TypedTree<Hash, ResyncEntry> {
		let db = db::open_db(
			&tmp_dir.to_path_buf(),
			db::Engine::Lmdb,
			&db::OpenOpt::default(),
		)
		.expect("failed to open temporary lmdb database");
		db.open_typed_tree::<Hash, ResyncEntry, _>("block_local_resync_queue_v2")
			.unwrap()
	}

	fn when_index_of(idxqueue: &IndexedQueue) -> Vec<(u64, Hash)> {
		idxqueue
			.when_index
			.iter()
			.map(|e| (e.when, e.hash))
			.collect()
	}

	#[test]
	fn put_many_keeps_latest_when_and_errors() {
		let tmp_dir = mktemp::Temp::new_dir().unwrap();
		let queue = open_queue(&tmp_dir);

		// h1: queued later than what we will put, must not move
		// h2: queued earlier than what we will put, with errors that must be kept
		queue
			.insert(
				&h(1),
				&ResyncEntry {
					when: 500,
					errors: 0,
				},
			)
			.unwrap();
		queue
			.insert(
				&h(2),
				&ResyncEntry {
					when: 100,
					errors: 2,
				},
			)
			.unwrap();

		let mut idxqueue = IndexedQueue::load(queue.clone()).unwrap();
		assert_eq!(idxqueue.errored(), 1);

		// h3: not queued yet
		idxqueue
			.put_many(&[(h(1), 200), (h(2), 300), (h(3), 400)])
			.unwrap();

		let get = |hash| {
			let e = queue.get(&hash).unwrap().expect("missing from queue");
			(e.when, e.errors)
		};
		assert_eq!(get(h(1)), (500, 0));
		assert_eq!(get(h(2)), (300, 2));
		assert_eq!(get(h(3)), (400, 0));

		assert_eq!(
			when_index_of(&idxqueue),
			vec![(300, h(2)), (400, h(3)), (500, h(1))]
		);
		assert_eq!(idxqueue.errored(), 1);

		// The in-memory index must match what a fresh load from the db gives
		let reloaded = IndexedQueue::load(queue).unwrap();
		assert_eq!(when_index_of(&reloaded), when_index_of(&idxqueue));
	}

	#[test]
	fn deferred_puts_push_keeps_latest_when() {
		let mut deferred = DeferredPuts::default();

		// Only the first push needs to start a flush
		assert!(deferred.push(h(1), 400));
		assert!(!deferred.push(h(1), 350));
		assert!(!deferred.push(h(2), 100));
		assert!(!deferred.push(h(2), 200));

		assert_eq!(deferred.items, HashMap::from([(h(1), 400), (h(2), 200)]));
	}

	#[test]
	fn flush_deferred_puts_empties_pending_list() {
		let tmp_dir = mktemp::Temp::new_dir().unwrap();
		let queue = open_queue(&tmp_dir);
		let idxqueue = Mutex::new(IndexedQueue::load(queue.clone()).unwrap());

		// More items than fit in one batch, so the flush has to loop
		let n = DEFERRED_PUTS_BATCH_SIZE as u64 + 10;
		let items = (0..n)
			.map(|i| {
				let mut hash = [0u8; 32];
				hash[..8].copy_from_slice(&i.to_be_bytes());
				(Hash::from(hash), 1000 + i)
			})
			.collect::<HashMap<_, _>>();
		let deferred = Mutex::new(DeferredPuts {
			items: items.clone(),
			flush_running: true,
		});

		flush_deferred_puts(&idxqueue, &deferred, &Notify::new());

		let deferred = deferred.lock().unwrap();
		assert!(deferred.items.is_empty());
		assert!(!deferred.flush_running);

		for (hash, when) in items {
			let e = queue.get(&hash).unwrap().expect("missing from queue");
			assert_eq!((e.when, e.errors), (when, 0));
		}
		assert_eq!(idxqueue.lock().unwrap().when_index.len(), n as usize);
	}

	#[test]
	fn flush_deferred_puts_panic_clears_flush_running() {
		let tmp_dir = mktemp::Temp::new_dir().unwrap();
		let queue = open_queue(&tmp_dir);
		let idxqueue = Mutex::new(IndexedQueue::load(queue).unwrap());

		// Poison the queue mutex so that the flush panics when it locks it
		std::thread::scope(|s| {
			s.spawn(|| {
				let _lock = idxqueue.lock().unwrap();
				panic!("poisoning the queue mutex");
			})
			.join()
			.unwrap_err();
		});
		assert!(idxqueue.is_poisoned());

		let deferred = Mutex::new(DeferredPuts {
			items: HashMap::from([(h(1), 1000)]),
			flush_running: true,
		});

		let res = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
			flush_deferred_puts(&idxqueue, &deferred, &Notify::new());
		}));
		assert!(res.is_err());

		let deferred = deferred.lock().unwrap();
		assert!(!deferred.flush_running);
	}
}
