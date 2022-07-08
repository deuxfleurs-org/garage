use std::collections::HashMap;
use std::convert::TryInto;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

use futures::future::join_all;
use tokio::sync::watch;

use garage_db::counted_tree_hack::CountedTree;

use garage_util::background::*;
use garage_util::data::*;
use garage_util::error::*;
use garage_util::time::*;

use garage_rpc::system::System;
use garage_rpc::*;

use crate::data::*;
use crate::replication::*;
use crate::schema::*;

const TABLE_GC_BATCH_SIZE: usize = 1024;
const TABLE_GC_RPC_TIMEOUT: Duration = Duration::from_secs(30);

// GC delay for table entries: 1 day (24 hours)
// (the delay before the entry is added in the GC todo list
// and the moment the garbage collection actually happens)
const TABLE_GC_DELAY: Duration = Duration::from_secs(24 * 3600);

pub(crate) struct TableGc<F: TableSchema + 'static, R: TableReplication + 'static> {
	system: Arc<System>,
	data: Arc<TableData<F, R>>,

	endpoint: Arc<Endpoint<GcRpc, Self>>,
}

#[derive(Serialize, Deserialize)]
enum GcRpc {
	Update(Vec<ByteBuf>),
	DeleteIfEqualHash(Vec<(ByteBuf, Hash)>),
	Ok,
}

impl Rpc for GcRpc {
	type Response = Result<GcRpc, Error>;
}

impl<F, R> TableGc<F, R>
where
	F: TableSchema + 'static,
	R: TableReplication + 'static,
{
	pub(crate) fn launch(system: Arc<System>, data: Arc<TableData<F, R>>) -> Arc<Self> {
		let endpoint = system
			.netapp
			.endpoint(format!("garage_table/gc.rs/Rpc:{}", F::TABLE_NAME));

		let gc = Arc::new(Self {
			system: system.clone(),
			data,
			endpoint,
		});

		gc.endpoint.set_handler(gc.clone());

		system.background.spawn_worker(GcWorker::new(gc.clone()));

		gc
	}

	async fn gc_loop_iter(&self) -> Result<Option<Duration>, Error> {
		let now = now_msec();

		// List entries in the GC todo list
		// These entries are put there when a tombstone is inserted in the table
		// (see update_entry in data.rs)
		let mut candidates = vec![];
		for entry_kv in self.data.gc_todo.iter()? {
			let (k, vhash) = entry_kv?;
			let todo_entry = GcTodoEntry::parse(&k, &vhash);

			if todo_entry.deletion_time() > now {
				if candidates.is_empty() {
					// If the earliest entry in the todo list shouldn't yet be processed,
					// return a duration to wait in the loop
					return Ok(Some(Duration::from_millis(
						todo_entry.deletion_time() - now,
					)));
				} else {
					// Otherwise we have some entries to process, do a normal iteration.
					break;
				}
			}

			candidates.push(todo_entry);
			if candidates.len() >= 2 * TABLE_GC_BATCH_SIZE {
				break;
			}
		}

		let mut entries = vec![];
		let mut excluded = vec![];
		for mut todo_entry in candidates {
			// Check if the tombstone is still the current value of the entry.
			// If not, we don't actually want to GC it, and we will remove it
			// from the gc_todo table later (below).
			let vhash = todo_entry.value_hash;
			todo_entry.value = self
				.data
				.store
				.get(&todo_entry.key[..])?
				.filter(|v| blake2sum(&v[..]) == vhash)
				.map(|v| v.to_vec());

			if todo_entry.value.is_some() {
				entries.push(todo_entry);
				if entries.len() >= TABLE_GC_BATCH_SIZE {
					break;
				}
			} else {
				excluded.push(todo_entry);
			}
		}

		// Remove from gc_todo entries for tombstones where we have
		// detected that the current value has changed and
		// is no longer a tombstone.
		for entry in excluded {
			entry.remove_if_equal(&self.data.gc_todo)?;
		}

		// Remaining in `entries` is the list of entries we want to GC,
		// and for which they are still currently tombstones in the table.

		if entries.is_empty() {
			// Nothing to do in this iteration (no entries present)
			// Wait for a default delay of 60 seconds
			return Ok(Some(Duration::from_secs(60)));
		}

		debug!("({}) GC: doing {} items", F::TABLE_NAME, entries.len());

		// Split entries to GC by the set of nodes on which they are stored.
		// Here we call them partitions but they are not exactly
		// the same as partitions as defined in the ring: those partitions
		// are defined by the first 8 bits of the hash, but two of these
		// partitions can be stored on the same set of nodes.
		// Here we detect when entries are stored on the same set of nodes:
		// even if they are not in the same 8-bit partition, we can still
		// handle them together.
		let mut partitions = HashMap::new();
		for entry in entries {
			let pkh = Hash::try_from(&entry.key[..32]).unwrap();
			let mut nodes = self.data.replication.write_nodes(&pkh);
			nodes.retain(|x| *x != self.system.id);
			nodes.sort();

			if !partitions.contains_key(&nodes) {
				partitions.insert(nodes.clone(), vec![]);
			}
			partitions.get_mut(&nodes).unwrap().push(entry);
		}

		// For each set of nodes that contains some items,
		// ensure they are aware of the tombstone status, and once they
		// are, instruct them to delete the entries.
		let resps = join_all(
			partitions
				.into_iter()
				.map(|(nodes, items)| self.try_send_and_delete(nodes, items)),
		)
		.await;

		// Collect errors and return a single error value even if several
		// errors occurred.
		let mut errs = vec![];
		for resp in resps {
			if let Err(e) = resp {
				errs.push(e);
			}
		}

		if errs.is_empty() {
			Ok(None)
		} else {
			Err(Error::Message(
				errs.into_iter()
					.map(|x| format!("{}", x))
					.collect::<Vec<_>>()
					.join(", "),
			))
			.err_context("in try_send_and_delete in table GC:")
		}
	}

	async fn try_send_and_delete(
		&self,
		nodes: Vec<Uuid>,
		mut items: Vec<GcTodoEntry>,
	) -> Result<(), Error> {
		let n_items = items.len();

		// Strategy: we first send all of the values to the remote nodes,
		// to ensure that they are aware of the tombstone state,
		// and that the previous state was correctly overwritten
		// (if they have a newer state that overrides the tombstone, that's fine).
		// Second, once everyone is at least at the tombstone state,
		// we instruct everyone to delete the tombstone IF that is still their current state.
		// If they are now at a different state, it means that that state overrides the
		// tombstone in the CRDT lattice, and it will be propagated back to us at some point
		// (either just a regular update that hasn't reached us yet, or later when the
		// table is synced).

		// Here, we store in updates all of the tombstones to send for step 1,
		// and in deletes the list of keys and hashes of value for step 2.
		let mut updates = vec![];
		let mut deletes = vec![];
		for item in items.iter_mut() {
			updates.push(ByteBuf::from(item.value.take().unwrap()));
			deletes.push((ByteBuf::from(item.key.clone()), item.value_hash));
		}

		// Step 1: ensure everyone is at least at tombstone in CRDT lattice
		// Here the quorum is nodes.len(): we cannot tolerate even a single failure,
		// otherwise old values before the tombstone might come back in the data.
		// GC'ing is not a critical function of the system, so it's not a big
		// deal if we can't do it right now.
		self.system
			.rpc
			.try_call_many(
				&self.endpoint,
				&nodes[..],
				GcRpc::Update(updates),
				RequestStrategy::with_priority(PRIO_BACKGROUND)
					.with_quorum(nodes.len())
					.with_timeout(TABLE_GC_RPC_TIMEOUT),
			)
			.await
			.err_context("GC: send tombstones")?;

		info!(
			"({}) GC: {} items successfully pushed, will try to delete.",
			F::TABLE_NAME,
			n_items
		);

		// Step 2: delete tombstones everywhere.
		// Here we also fail if even a single node returns a failure:
		// it means that the garbage collection wasn't completed and has
		// to be retried later.
		self.system
			.rpc
			.try_call_many(
				&self.endpoint,
				&nodes[..],
				GcRpc::DeleteIfEqualHash(deletes),
				RequestStrategy::with_priority(PRIO_BACKGROUND)
					.with_quorum(nodes.len())
					.with_timeout(TABLE_GC_RPC_TIMEOUT),
			)
			.await
			.err_context("GC: remote delete tombstones")?;

		// GC has been successfull for all of these entries.
		// We now remove them all from our local table and from the GC todo list.
		for item in items {
			self.data
				.delete_if_equal_hash(&item.key[..], item.value_hash)
				.err_context("GC: local delete tombstones")?;
			item.remove_if_equal(&self.data.gc_todo)
				.err_context("GC: remove from todo list after successfull GC")?;
		}

		Ok(())
	}
}

#[async_trait]
impl<F, R> EndpointHandler<GcRpc> for TableGc<F, R>
where
	F: TableSchema + 'static,
	R: TableReplication + 'static,
{
	async fn handle(self: &Arc<Self>, message: &GcRpc, _from: NodeID) -> Result<GcRpc, Error> {
		match message {
			GcRpc::Update(items) => {
				self.data.update_many(items)?;
				Ok(GcRpc::Ok)
			}
			GcRpc::DeleteIfEqualHash(items) => {
				for (key, vhash) in items.iter() {
					self.data.delete_if_equal_hash(&key[..], *vhash)?;
				}
				Ok(GcRpc::Ok)
			}
			m => Err(Error::unexpected_rpc_message(m)),
		}
	}
}

struct GcWorker<F, R>
where
	F: TableSchema + 'static,
	R: TableReplication + 'static,
{
	gc: Arc<TableGc<F, R>>,
	wait_delay: Duration,
}

impl<F, R> GcWorker<F, R>
where
	F: TableSchema + 'static,
	R: TableReplication + 'static,
{
	fn new(gc: Arc<TableGc<F, R>>) -> Self {
		Self {
			gc,
			wait_delay: Duration::from_secs(0),
		}
	}
}

#[async_trait]
impl<F, R> Worker for GcWorker<F, R>
where
	F: TableSchema + 'static,
	R: TableReplication + 'static,
{
	fn name(&self) -> String {
		format!("{} GC", F::TABLE_NAME)
	}

	fn info(&self) -> Option<String> {
		let l = self.gc.data.gc_todo_len().unwrap_or(0);
		if l > 0 {
			Some(format!("{} items in queue", l))
		} else {
			None
		}
	}

	async fn work(&mut self, _must_exit: &mut watch::Receiver<bool>) -> Result<WorkerState, Error> {
		match self.gc.gc_loop_iter().await? {
			None => Ok(WorkerState::Busy),
			Some(delay) => {
				self.wait_delay = delay;
				Ok(WorkerState::Idle)
			}
		}
	}

	async fn wait_for_work(&mut self, must_exit: &watch::Receiver<bool>) -> WorkerState {
		if *must_exit.borrow() {
			return WorkerState::Done;
		}
		tokio::time::sleep(self.wait_delay).await;
		WorkerState::Busy
	}
}

/// An entry stored in the gc_todo Sled tree associated with the table
/// Contains helper function for parsing, saving, and removing
/// such entry in Sled
///
/// Format of an entry:
/// - key =    8 bytes: timestamp of tombstone
///                     (used to implement GC delay)
///            n bytes: key in the main data table
/// - value =  hash of the table entry to delete (the tombstone)
///            for verification purpose, because we don't want to delete
///            things that aren't tombstones
pub(crate) struct GcTodoEntry {
	tombstone_timestamp: u64,
	key: Vec<u8>,
	value_hash: Hash,
	value: Option<Vec<u8>>,
}

impl GcTodoEntry {
	/// Creates a new GcTodoEntry (not saved in Sled) from its components:
	/// the key of an entry in the table, and the hash of the associated
	/// serialized value
	pub(crate) fn new(key: Vec<u8>, value_hash: Hash) -> Self {
		Self {
			tombstone_timestamp: now_msec(),
			key,
			value_hash,
			value: None,
		}
	}

	/// Parses a GcTodoEntry from a (k, v) pair stored in the gc_todo tree
	pub(crate) fn parse(db_k: &[u8], db_v: &[u8]) -> Self {
		Self {
			tombstone_timestamp: u64::from_be_bytes(db_k[0..8].try_into().unwrap()),
			key: db_k[8..].to_vec(),
			value_hash: Hash::try_from(db_v).unwrap(),
			value: None,
		}
	}

	/// Saves the GcTodoEntry in the gc_todo tree
	pub(crate) fn save(&self, gc_todo_tree: &CountedTree) -> Result<(), Error> {
		gc_todo_tree.insert(self.todo_table_key(), self.value_hash.as_slice())?;
		Ok(())
	}

	/// Removes the GcTodoEntry from the gc_todo tree if the
	/// hash of the serialized value is the same here as in the tree.
	/// This is usefull to remove a todo entry only under the condition
	/// that it has not changed since the time it was read, i.e.
	/// what we have to do is still the same
	pub(crate) fn remove_if_equal(&self, gc_todo_tree: &CountedTree) -> Result<(), Error> {
		gc_todo_tree.compare_and_swap::<_, _, &[u8]>(
			&self.todo_table_key(),
			Some(self.value_hash),
			None,
		)?;
		Ok(())
	}

	fn todo_table_key(&self) -> Vec<u8> {
		[
			&u64::to_be_bytes(self.tombstone_timestamp)[..],
			&self.key[..],
		]
		.concat()
	}

	fn deletion_time(&self) -> u64 {
		self.tombstone_timestamp + TABLE_GC_DELAY.as_millis() as u64
	}
}
