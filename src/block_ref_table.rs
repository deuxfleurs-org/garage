use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::data::*;
use crate::server::Garage;
use crate::table::*;

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
	pub garage: RwLock<Option<Arc<Garage>>>,
}

#[async_trait]
impl TableFormat for BlockRefTable {
	type P = Hash;
	type S = UUID;
	type E = BlockRef;

	async fn updated(&self, old: Option<Self::E>, new: Self::E) {
		let garage = self.garage.read().await.as_ref().cloned().unwrap();

		let was_before = old.map(|x| !x.deleted).unwrap_or(false);
		let is_after = !new.deleted;
		if is_after && !was_before {
			if let Err(e) = garage.block_manager.block_incref(&new.block) {
				eprintln!("Failed to incref block {:?}: {}", &new.block, e);
			}
		}
		if was_before && !is_after {
			if let Err(e) = garage
				.block_manager
				.block_decref(&new.block, &garage.background)
			{
				eprintln!("Failed to decref block {:?}: {}", &new.block, e);
			}
		}
	}
}
