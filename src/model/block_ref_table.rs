use serde::{Deserialize, Serialize};
use std::sync::Arc;

use garage_util::data::*;

use garage_table::crdt::CRDT;
use garage_table::*;

use crate::block::*;

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct BlockRef {
	/// Hash (blake2 sum) of the block, used as partition key
	pub block: Hash,

	/// Id of the Version for the object containing this block, used as sorting key
	pub version: UUID,

	// Keep track of deleted status
	/// Is the Version that contains this block deleted
	pub deleted: crdt::Bool,
}

impl Entry<Hash, UUID> for BlockRef {
	fn partition_key(&self) -> &Hash {
		&self.block
	}
	fn sort_key(&self) -> &UUID {
		&self.version
	}
	fn is_tombstone(&self) -> bool {
		self.deleted.get()
	}
}

impl CRDT for BlockRef {
	fn merge(&mut self, other: &Self) {
		self.deleted.merge(&other.deleted);
	}
}

pub struct BlockRefTable {
	pub block_manager: Arc<BlockManager>,
}

impl TableSchema for BlockRefTable {
	type P = Hash;
	type S = UUID;
	type E = BlockRef;
	type Filter = DeletedFilter;

	fn updated(&self, old: Option<Self::E>, new: Option<Self::E>) {
		let block = &old.as_ref().or(new.as_ref()).unwrap().block;
		let was_before = old.as_ref().map(|x| !x.deleted.get()).unwrap_or(false);
		let is_after = new.as_ref().map(|x| !x.deleted.get()).unwrap_or(false);
		if is_after && !was_before {
			if let Err(e) = self.block_manager.block_incref(block) {
				warn!("block_incref failed for block {:?}: {}", block, e);
			}
		}
		if was_before && !is_after {
			if let Err(e) = self.block_manager.block_decref(block) {
				warn!("block_decref failed for block {:?}: {}", block, e);
			}
		}
	}

	fn matches_filter(entry: &Self::E, filter: &Self::Filter) -> bool {
		filter.apply(entry.deleted.get())
	}
}
