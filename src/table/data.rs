use std::sync::Arc;

use log::warn;
use serde_bytes::ByteBuf;
use sled::Transactional;

use garage_util::background::BackgroundRunner;
use garage_util::data::*;
use garage_util::error::*;

use crate::crdt::CRDT;
use crate::merkle::*;
use crate::schema::*;

pub struct TableData<F: TableSchema> {
	pub name: String,
	pub instance: F,

	pub store: sled::Tree,
	pub(crate) merkle_updater: Arc<MerkleUpdater>,
}

impl<F> TableData<F>
where
	F: TableSchema,
{
	pub fn new(
		name: String,
		instance: F,
		db: &sled::Db,
		background: Arc<BackgroundRunner>,
	) -> Arc<Self> {
		let store = db
			.open_tree(&format!("{}:table", name))
			.expect("Unable to open DB tree");

		let merkle_todo_store = db
			.open_tree(&format!("{}:merkle_todo", name))
			.expect("Unable to open DB Merkle TODO tree");
		let merkle_tree_store = db
			.open_tree(&format!("{}:merkle_tree", name))
			.expect("Unable to open DB Merkle tree tree");

		let merkle_updater = MerkleUpdater::launch(
			name.clone(),
			background,
			merkle_todo_store,
			merkle_tree_store,
		);

		Arc::new(Self {
			name,
			instance,
			store,
			merkle_updater,
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

	pub(crate) fn update_many(&self, entries: &[Arc<ByteBuf>]) -> Result<(), Error> {
		for update_bytes in entries.iter() {
			self.update_entry(update_bytes.as_slice())?;
		}
		Ok(())
	}

	pub(crate) fn update_entry(&self, update_bytes: &[u8]) -> Result<(), Error> {
		let update = self.decode_entry(update_bytes)?;
		let tree_key = self.tree_key(update.partition_key(), update.sort_key());

		let changed = (&self.store, &self.merkle_updater.todo).transaction(|(db, mkl_todo)| {
			let (old_entry, new_entry) = match db.get(&tree_key)? {
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
				mkl_todo.insert(tree_key.clone(), blake2sum(&new_bytes[..]).to_vec())?;
				db.insert(tree_key.clone(), new_bytes)?;
				Ok(Some((old_entry, new_entry)))
			} else {
				Ok(None)
			}
		})?;

		if let Some((old_entry, new_entry)) = changed {
			self.instance.updated(old_entry, Some(new_entry));
			self.merkle_updater.todo_notify.notify();
		}

		Ok(())
	}

	pub(crate) fn delete_if_equal(self: &Arc<Self>, k: &[u8], v: &[u8]) -> Result<bool, Error> {
		let removed = (&self.store, &self.merkle_updater.todo).transaction(|(txn, mkl_todo)| {
			if let Some(cur_v) = txn.get(k)? {
				if cur_v == v {
					txn.remove(k)?;
					mkl_todo.insert(k, vec![])?;
					return Ok(true);
				}
			}
			Ok(false)
		})?;

		if removed {
			let old_entry = self.decode_entry(v)?;
			self.instance.updated(Some(old_entry), None);
			self.merkle_updater.todo_notify.notify();
		}
		Ok(removed)
	}

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
}
