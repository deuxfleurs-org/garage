use core::borrow::Borrow;
use std::sync::Arc;

use log::warn;
use serde_bytes::ByteBuf;
use sled::Transactional;
use tokio::sync::Notify;

use garage_util::data::*;
use garage_util::error::*;

use garage_rpc::membership::System;

use crate::crdt::CRDT;
use crate::replication::*;
use crate::schema::*;

pub struct TableData<F: TableSchema, R: TableReplication> {
	system: Arc<System>,

	pub name: String,
	pub(crate) instance: F,
	pub(crate) replication: R,

	pub store: sled::Tree,

	pub(crate) merkle_tree: sled::Tree,
	pub(crate) merkle_todo: sled::Tree,
	pub(crate) merkle_todo_notify: Notify,
	pub(crate) gc_todo: sled::Tree,
}

impl<F, R> TableData<F, R>
where
	F: TableSchema,
	R: TableReplication,
{
	pub fn new(
		system: Arc<System>,
		name: String,
		instance: F,
		replication: R,
		db: &sled::Db,
	) -> Arc<Self> {
		let store = db
			.open_tree(&format!("{}:table", name))
			.expect("Unable to open DB tree");

		let merkle_tree = db
			.open_tree(&format!("{}:merkle_tree", name))
			.expect("Unable to open DB Merkle tree tree");
		let merkle_todo = db
			.open_tree(&format!("{}:merkle_todo", name))
			.expect("Unable to open DB Merkle TODO tree");

		let gc_todo = db
			.open_tree(&format!("{}:gc_todo", name))
			.expect("Unable to open DB tree");

		Arc::new(Self {
			system,
			name,
			instance,
			replication,
			store,
			merkle_tree,
			merkle_todo,
			merkle_todo_notify: Notify::new(),
			gc_todo,
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
		p: &F::P,
		s: &Option<F::S>,
		filter: &Option<F::Filter>,
		limit: usize,
	) -> Result<Vec<Arc<ByteBuf>>, Error> {
		let partition_hash = p.hash();
		let first_key = match s {
			None => partition_hash.to_vec(),
			Some(sk) => self.tree_key(p, sk),
		};
		let mut ret = vec![];
		for item in self.store.range(first_key..) {
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
				ret.push(Arc::new(ByteBuf::from(value.as_ref())));
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

		let changed = (&self.store, &self.merkle_todo).transaction(|(store, mkl_todo)| {
			let (old_entry, new_entry) = match store.get(&tree_key)? {
				Some(prev_bytes) => {
					let old_entry = self
						.decode_entry(&prev_bytes)
						.map_err(sled::transaction::ConflictableTransactionError::Abort)?;
					let mut new_entry = old_entry.clone();
					new_entry.merge(&update);
					(Some(old_entry), new_entry)
				}
				None => (None, update.clone()),
			};

			if Some(&new_entry) != old_entry.as_ref() {
				let new_bytes = rmp_to_vec_all_named(&new_entry)
					.map_err(Error::RMPEncode)
					.map_err(sled::transaction::ConflictableTransactionError::Abort)?;
				let new_bytes_hash = blake2sum(&new_bytes[..]);
				mkl_todo.insert(tree_key.clone(), new_bytes_hash.as_slice())?;
				store.insert(tree_key.clone(), new_bytes)?;
				Ok(Some((old_entry, new_entry, new_bytes_hash)))
			} else {
				Ok(None)
			}
		})?;

		if let Some((old_entry, new_entry, new_bytes_hash)) = changed {
			let is_tombstone = new_entry.is_tombstone();
			self.instance.updated(old_entry, Some(new_entry));
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
					self.gc_todo.insert(&tree_key, new_bytes_hash.as_slice())?;
				}
			}
		}

		Ok(())
	}

	pub(crate) fn delete_if_equal(self: &Arc<Self>, k: &[u8], v: &[u8]) -> Result<bool, Error> {
		let removed = (&self.store, &self.merkle_todo).transaction(|(store, mkl_todo)| {
			if let Some(cur_v) = store.get(k)? {
				if cur_v == v {
					store.remove(k)?;
					mkl_todo.insert(k, vec![])?;
					return Ok(true);
				}
			}
			Ok(false)
		})?;

		if removed {
			let old_entry = self.decode_entry(v)?;
			self.instance.updated(Some(old_entry), None);
			self.merkle_todo_notify.notify_one();
		}
		Ok(removed)
	}

	pub(crate) fn delete_if_equal_hash(
		self: &Arc<Self>,
		k: &[u8],
		vhash: Hash,
	) -> Result<bool, Error> {
		let removed = (&self.store, &self.merkle_todo).transaction(|(store, mkl_todo)| {
			if let Some(cur_v) = store.get(k)? {
				if blake2sum(&cur_v[..]) == vhash {
					store.remove(k)?;
					mkl_todo.insert(k, vec![])?;
					return Ok(Some(cur_v));
				}
			}
			Ok(None)
		})?;

		if let Some(old_v) = removed {
			let old_entry = self.decode_entry(&old_v[..])?;
			self.instance.updated(Some(old_entry), None);
			self.merkle_todo_notify.notify_one();
			Ok(true)
		} else {
			Ok(false)
		}
	}

	// ---- Utility functions ----

	pub(crate) fn tree_key(&self, p: &F::P, s: &F::S) -> Vec<u8> {
		let mut ret = p.hash().to_vec();
		ret.extend(s.sort_key());
		ret
	}

	pub(crate) fn decode_entry(&self, bytes: &[u8]) -> Result<F::E, Error> {
		match rmp_serde::decode::from_read_ref::<_, F::E>(bytes) {
			Ok(x) => Ok(x),
			Err(e) => match F::try_migrate(bytes) {
				Some(x) => Ok(x),
				None => {
					warn!("Unable to decode entry of {}: {}", self.name, e);
					for line in hexdump::hexdump_iter(bytes) {
						debug!("{}", line);
					}
					Err(e.into())
				}
			},
		}
	}

	pub fn gc_todo_len(&self) -> usize {
		self.gc_todo.len()
	}
}
