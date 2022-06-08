use serde::{Deserialize, Serialize};
use std::sync::Arc;

use garage_db as db;

use garage_util::data::*;

use garage_table::crdt::Crdt;
use garage_table::*;

use garage_block::manager::*;

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct BlockRef {
	/// Hash (blake2 sum) of the block, used as partition key
	pub block: Hash,

	/// Id of the Version for the object containing this block, used as sorting key
	pub version: Uuid,

	// Keep track of deleted status
	/// Is the Version that contains this block deleted
	pub deleted: crdt::Bool,
}

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
