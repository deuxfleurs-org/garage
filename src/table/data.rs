use core::borrow::Borrow;
use std::convert::TryInto;
use std::sync::Arc;

use serde_bytes::ByteBuf;
use tokio::sync::Notify;

use garage_db as db;
use garage_db::counted_tree_hack::CountedTree;

use garage_util::data::*;
use garage_util::error::*;

use garage_rpc::system::System;

use crate::crdt::Crdt;
use crate::gc::GcTodoEntry;
use crate::metrics::*;
use crate::replication::*;
use crate::schema::*;
use crate::util::*;

pub struct TableData<F: TableSchema, R: TableReplication> {
	system: Arc<System>,

	pub instance: F,
	pub replication: R,

	pub store: db::Tree,

	pub(crate) merkle_tree: db::Tree,
	pub(crate) merkle_todo: db::Tree,
	pub(crate) merkle_todo_notify: Notify,
	pub(crate) gc_todo: CountedTree,

	pub(crate) metrics: TableMetrics,
}

impl<F, R> TableData<F, R>
where
	F: TableSchema,
	R: TableReplication,
{
	pub fn new(system: Arc<System>, instance: F, replication: R, db: &db::Db) -> Arc<Self> {
		let store = db
			.open_tree(&format!("{}:table", F::TABLE_NAME))
			.expect("Unable to open DB tree");

		let merkle_tree = db
			.open_tree(&format!("{}:merkle_tree", F::TABLE_NAME))
			.expect("Unable to open DB Merkle tree tree");
		let merkle_todo = db
			.open_tree(&format!("{}:merkle_todo", F::TABLE_NAME))
			.expect("Unable to open DB Merkle TODO tree");

		let gc_todo = db
			.open_tree(&format!("{}:gc_todo_v2", F::TABLE_NAME))
			.expect("Unable to open DB tree");
		let gc_todo = CountedTree::new(gc_todo).expect("Cannot count gc_todo_v2");

		let metrics = TableMetrics::new(F::TABLE_NAME, merkle_todo.clone(), gc_todo.clone());

		Arc::new(Self {
			system,
			instance,
			replication,
			store,
			merkle_tree,
			merkle_todo,
			merkle_todo_notify: Notify::new(),
			gc_todo,
			metrics,
		})
	}

	// Read functions

	pub fn read_entry(&self, p: &F::P, s: &F::S) -> Result<Option<ByteBuf>, Error> {
		let tree_key = self.tree_key(p, s);
		if let Some(bytes) = self.store.get(&tree_key)? {
			Ok(Some(ByteBuf::from(bytes.to_vec())))
		} else {
			Ok(None)
		}
	}

	pub fn read_range(
		&self,
		partition_key: &F::P,
		start: &Option<F::S>,
		filter: &Option<F::Filter>,
		limit: usize,
		enumeration_order: EnumerationOrder,
	) -> Result<Vec<Arc<ByteBuf>>, Error> {
		let partition_hash = partition_key.hash();
		match enumeration_order {
			EnumerationOrder::Forward => {
				let first_key = match start {
					None => partition_hash.to_vec(),
					Some(sk) => self.tree_key(partition_key, sk),
				};
				let range = self.store.range(first_key..)?;
				self.read_range_aux(partition_hash, range, filter, limit)
			}
			EnumerationOrder::Reverse => match start {
				Some(sk) => {
					let last_key = self.tree_key(partition_key, sk);
					let range = self.store.range_rev(..=last_key)?;
					self.read_range_aux(partition_hash, range, filter, limit)
				}
				None => {
					let mut last_key = partition_hash.to_vec();
					let lower = u128::from_be_bytes(last_key[16..32].try_into().unwrap());
					last_key[16..32].copy_from_slice(&u128::to_be_bytes(lower + 1));
					let range = self.store.range_rev(..last_key)?;
					self.read_range_aux(partition_hash, range, filter, limit)
				}
			},
		}
	}

	fn read_range_aux<'a>(
		&self,
		partition_hash: Hash,
		range: db::ValueIter<'a>,
		filter: &Option<F::Filter>,
		limit: usize,
	) -> Result<Vec<Arc<ByteBuf>>, Error> {
		let mut ret = vec![];
		for item in range {
			let (key, value) = item?;
			if &key[..32] != partition_hash.as_slice() {
				break;
			}
			let keep = match filter {
				None => true,
				Some(f) => {
					let entry = self.decode_entry(value.as_ref())?;
					F::matches_filter(&entry, f)
				}
			};
			if keep {
				ret.push(Arc::new(ByteBuf::from(value)));
			}
			if ret.len() >= limit {
				break;
			}
		}
		Ok(ret)
	}

	// Mutation functions
	// When changing this code, take care of propagating modifications correctly:
	// - When an entry is modified or deleted, call the updated() function
	//   on the table instance
	// - When an entry is modified or deleted, add it to the merkle updater's todo list.
	//   This has to be done atomically with the modification for the merkle updater
	//   to maintain consistency. The merkle updater must then be notified with todo_notify.
	// - When an entry is updated to be a tombstone, add it to the gc_todo tree

	pub(crate) fn update_many<T: Borrow<ByteBuf>>(&self, entries: &[T]) -> Result<(), Error> {
		for update_bytes in entries.iter() {
			self.update_entry(update_bytes.borrow().as_slice())?;
		}
		Ok(())
	}

	pub(crate) fn update_entry(&self, update_bytes: &[u8]) -> Result<(), Error> {
		let update = self.decode_entry(update_bytes)?;
		let tree_key = self.tree_key(update.partition_key(), update.sort_key());

		self.update_entry_with(&tree_key[..], |ent| match ent {
			Some(mut ent) => {
				ent.merge(&update);
				ent
			}
			None => update.clone(),
		})?;
		Ok(())
	}

	pub fn update_entry_with(
		&self,
		tree_key: &[u8],
		f: impl Fn(Option<F::E>) -> F::E,
	) -> Result<Option<F::E>, Error> {
		let changed = self.store.db().transaction(|mut tx| {
			let (old_entry, old_bytes, new_entry) = match tx.get(&self.store, tree_key)? {
				Some(old_bytes) => {
					let old_entry = self.decode_entry(&old_bytes).map_err(db::TxError::Abort)?;
					let new_entry = f(Some(old_entry.clone()));
					(Some(old_entry), Some(old_bytes), new_entry)
				}
				None => (None, None, f(None)),
			};

			// Scenario 1: the value changed, so of course there is a change
			let value_changed = Some(&new_entry) != old_entry.as_ref();

			// Scenario 2: the value didn't change but due to a migration in the
			// data format, the messagepack encoding changed. In this case
			// we have to write the migrated value in the table and update
			// the associated Merkle tree entry.
			let new_bytes = rmp_to_vec_all_named(&new_entry)
				.map_err(Error::RmpEncode)
				.map_err(db::TxError::Abort)?;
			let encoding_changed = Some(&new_bytes[..]) != old_bytes.as_ref().map(|x| &x[..]);
			drop(old_bytes);

			if value_changed || encoding_changed {
				let new_bytes_hash = blake2sum(&new_bytes[..]);
				tx.insert(&self.merkle_todo, tree_key, new_bytes_hash.as_slice())?;
				tx.insert(&self.store, tree_key, new_bytes)?;

				self.instance
					.updated(&mut tx, old_entry.as_ref(), Some(&new_entry))?;

				Ok(Some((new_entry, new_bytes_hash)))
			} else {
				Ok(None)
			}
		})?;

		if let Some((new_entry, new_bytes_hash)) = changed {
			self.metrics.internal_update_counter.add(1);

			let is_tombstone = new_entry.is_tombstone();
			self.merkle_todo_notify.notify_one();
			if is_tombstone {
				// We are only responsible for GC'ing this item if we are the
				// "leader" of the partition, i.e. the first node in the
				// set of nodes that replicates this partition.
				// This avoids GC loops and does not change the termination properties
				// of the GC algorithm, as in all cases GC is suspended if
				// any node of the partition is unavailable.
				let pk_hash = Hash::try_from(&tree_key[..32]).unwrap();
				let nodes = self.replication.write_nodes(&pk_hash);
				if nodes.first() == Some(&self.system.id) {
					GcTodoEntry::new(tree_key.to_vec(), new_bytes_hash).save(&self.gc_todo)?;
				}
			}

			Ok(Some(new_entry))
		} else {
			Ok(None)
		}
	}

	pub(crate) fn delete_if_equal(self: &Arc<Self>, k: &[u8], v: &[u8]) -> Result<bool, Error> {
		let removed = self
			.store
			.db()
			.transaction(|mut tx| match tx.get(&self.store, k)? {
				Some(cur_v) if cur_v == v => {
					tx.remove(&self.store, k)?;
					tx.insert(&self.merkle_todo, k, vec![])?;

					let old_entry = self.decode_entry(v).map_err(db::TxError::Abort)?;
					self.instance.updated(&mut tx, Some(&old_entry), None)?;
					Ok(true)
				}
				_ => Ok(false),
			})?;

		if removed {
			self.metrics.internal_delete_counter.add(1);
			self.merkle_todo_notify.notify_one();
		}
		Ok(removed)
	}

	pub(crate) fn delete_if_equal_hash(
		self: &Arc<Self>,
		k: &[u8],
		vhash: Hash,
	) -> Result<bool, Error> {
		let removed = self
			.store
			.db()
			.transaction(|mut tx| match tx.get(&self.store, k)? {
				Some(cur_v) if blake2sum(&cur_v[..]) == vhash => {
					tx.remove(&self.store, k)?;
					tx.insert(&self.merkle_todo, k, vec![])?;

					let old_entry = self.decode_entry(&cur_v[..]).map_err(db::TxError::Abort)?;
					self.instance.updated(&mut tx, Some(&old_entry), None)?;
					Ok(true)
				}
				_ => Ok(false),
			})?;

		if removed {
			self.metrics.internal_delete_counter.add(1);
			self.merkle_todo_notify.notify_one();
		}
		Ok(removed)
	}

	// ---- Utility functions ----

	pub fn tree_key(&self, p: &F::P, s: &F::S) -> Vec<u8> {
		let mut ret = p.hash().to_vec();
		ret.extend(s.sort_key());
		ret
	}

	pub fn decode_entry(&self, bytes: &[u8]) -> Result<F::E, Error> {
		match rmp_serde::decode::from_read_ref::<_, F::E>(bytes) {
			Ok(x) => Ok(x),
			Err(e) => match F::try_migrate(bytes) {
				Some(x) => Ok(x),
				None => {
					warn!("Unable to decode entry of {}: {}", F::TABLE_NAME, e);
					for line in hexdump::hexdump_iter(bytes) {
						debug!("{}", line);
					}
					Err(e.into())
				}
			},
		}
	}

	pub fn gc_todo_len(&self) -> Result<usize, Error> {
		Ok(self.gc_todo.len())
	}
}
