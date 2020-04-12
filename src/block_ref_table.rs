use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::data::*;
use crate::table::*;
use crate::background::*;
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
impl TableFormat for BlockRefTable {
	type P = Hash;
	type S = UUID;
	type E = BlockRef;

	async fn updated(&self, old: Option<Self::E>, new: Self::E) {
		let was_before = old.map(|x| !x.deleted).unwrap_or(false);
		let is_after = !new.deleted;
		if is_after && !was_before {
			if let Err(e) = self.block_manager.block_incref(&new.block) {
				eprintln!("Failed to incref block {:?}: {}", &new.block, e);
			}
		}
		if was_before && !is_after {
			if let Err(e) = self
				.block_manager
				.block_decref(&new.block, &self.background)
			{
				eprintln!("Failed to decref block {:?}: {}", &new.block, e);
			}
		}
	}
}
