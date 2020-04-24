use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use garage_util::background::*;
use garage_util::data::*;
use garage_util::error::Error;

use garage_table::*;

use crate::block::*;

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct BlockRef {
	// Primary key
	pub block: Hash,

	// Sort key
	pub version: UUID,

	// Keep track of deleted status
	pub deleted: bool,
}

impl Entry<Hash, UUID> for BlockRef {
	fn partition_key(&self) -> &Hash {
		&self.block
	}
	fn sort_key(&self) -> &UUID {
		&self.version
	}

	fn merge(&mut self, other: &Self) {
		if other.deleted {
			self.deleted = true;
		}
	}
}

pub struct BlockRefTable {
	pub background: Arc<BackgroundRunner>,
	pub block_manager: Arc<BlockManager>,
}

#[async_trait]
impl TableSchema for BlockRefTable {
	type P = Hash;
	type S = UUID;
	type E = BlockRef;
	type Filter = ();

	async fn updated(&self, old: Option<Self::E>, new: Option<Self::E>) -> Result<(), Error> {
		let block = &old.as_ref().or(new.as_ref()).unwrap().block;
		let was_before = old.as_ref().map(|x| !x.deleted).unwrap_or(false);
		let is_after = new.as_ref().map(|x| !x.deleted).unwrap_or(false);
		if is_after && !was_before {
			self.block_manager.block_incref(block)?;
		}
		if was_before && !is_after {
			self.block_manager.block_decref(block)?;
		}
		Ok(())
	}

	fn matches_filter(entry: &Self::E, _filter: &Self::Filter) -> bool {
		!entry.deleted
	}
}
