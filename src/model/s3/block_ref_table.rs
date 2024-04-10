use std::sync::Arc;

use garage_db as db;

use garage_util::data::*;
use garage_util::error::*;
use garage_util::migrate::Migrate;

use garage_block::CalculateRefcount;
use garage_table::crdt::Crdt;
use garage_table::replication::TableShardedReplication;
use garage_table::*;

use garage_block::manager::*;

mod v08 {
	use garage_util::crdt;
	use garage_util::data::{Hash, Uuid};
	use serde::{Deserialize, Serialize};

	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct BlockRef {
		/// Hash (blake2 sum) of the block, used as partition key
		pub block: Hash,

		/// Id of the Version for the object containing this block, used as sorting key
		pub version: Uuid,

		// Keep track of deleted status
		/// Is the Version that contains this block deleted
		pub deleted: crdt::Bool,
	}

	impl garage_util::migrate::InitialFormat for BlockRef {}
}

pub use v08::*;

impl Entry<Hash, Uuid> for BlockRef {
	fn partition_key(&self) -> &Hash {
		&self.block
	}
	fn sort_key(&self) -> &Uuid {
		&self.version
	}
	fn is_tombstone(&self) -> bool {
		self.deleted.get()
	}
}

impl Crdt for BlockRef {
	fn merge(&mut self, other: &Self) {
		self.deleted.merge(&other.deleted);
	}
}

pub struct BlockRefTable {
	pub block_manager: Arc<BlockManager>,
}

impl TableSchema for BlockRefTable {
	const TABLE_NAME: &'static str = "block_ref";

	type P = Hash;
	type S = Uuid;
	type E = BlockRef;
	type Filter = DeletedFilter;

	fn updated(
		&self,
		tx: &mut db::Transaction,
		old: Option<&Self::E>,
		new: Option<&Self::E>,
	) -> db::TxOpResult<()> {
		let block = old.or(new).unwrap().block;
		let was_before = old.map(|x| !x.deleted.get()).unwrap_or(false);
		let is_after = new.map(|x| !x.deleted.get()).unwrap_or(false);
		if is_after && !was_before {
			self.block_manager.block_incref(tx, block)?;
		}
		if was_before && !is_after {
			self.block_manager.block_decref(tx, block)?;
		}
		Ok(())
	}

	fn matches_filter(entry: &Self::E, filter: &Self::Filter) -> bool {
		filter.apply(entry.deleted.get())
	}
}

pub fn block_ref_recount_fn(
	block_ref_table: &Arc<Table<BlockRefTable, TableShardedReplication>>,
) -> CalculateRefcount {
	let table = Arc::downgrade(block_ref_table);
	Box::new(move |tx: &db::Transaction, block: &Hash| {
		let table = table
			.upgrade()
			.ok_or_message("cannot upgrade weak ptr to block_ref_table")
			.map_err(db::TxError::Abort)?;
		Ok(calculate_refcount(&table, tx, block)?)
	})
}

fn calculate_refcount(
	block_ref_table: &Table<BlockRefTable, TableShardedReplication>,
	tx: &db::Transaction,
	block: &Hash,
) -> db::TxResult<usize, Error> {
	let mut result = 0;
	for entry in tx.range(&block_ref_table.data.store, block.as_slice()..)? {
		let (key, value) = entry?;
		if &key[..32] != block.as_slice() {
			break;
		}
		let value = BlockRef::decode(&value)
			.ok_or_message("could not decode block_ref")
			.map_err(db::TxError::Abort)?;
		assert_eq!(value.block, *block);
		if !value.deleted.get() {
			result += 1;
		}
	}
	Ok(result)
}
