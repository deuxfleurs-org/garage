use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::data::*;
use crate::table::*;
use crate::background::BackgroundRunner;

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct Version {
	// Primary key
	pub uuid: UUID,

	// Actual data: the blocks for this version
	pub deleted: bool,
	pub blocks: Vec<VersionBlock>,

	// Back link to bucket+key so that we can figure if
	// this was deleted later on
	pub bucket: String,
	pub key: String,
}

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct VersionBlock {
	pub offset: u64,
	pub hash: Hash,
}

impl Entry<Hash, EmptySortKey> for Version {
	fn partition_key(&self) -> &Hash {
		&self.uuid
	}
	fn sort_key(&self) -> &EmptySortKey {
		&EmptySortKey
	}

	fn merge(&mut self, other: &Self) {
		if other.deleted {
			self.deleted = true;
			self.blocks.clear();
		} else if !self.deleted {
			for bi in other.blocks.iter() {
				match self.blocks.binary_search_by(|x| x.offset.cmp(&bi.offset)) {
					Ok(_) => (),
					Err(pos) => {
						self.blocks.insert(pos, bi.clone());
					}
				}
			}
		}
	}
}

pub struct VersionTable {
	pub background: Arc<BackgroundRunner>,
	pub block_ref_table: Arc<Table<BlockRefTable>>,
}

#[async_trait]
impl TableFormat for VersionTable {
	type P = Hash;
	type S = EmptySortKey;
	type E = Version;

	async fn updated(&self, old: Option<Self::E>, new: Self::E) {
		let block_ref_table = self.block_ref_table.clone();
		self.background.spawn(async move {
			// Propagate deletion of version blocks
			if let Some(old_v) = old {
				if new.deleted && !old_v.deleted {
					let deleted_block_refs = old_v
						.blocks
						.iter()
						.map(|vb| BlockRef {
							block: vb.hash.clone(),
							version: old_v.uuid.clone(),
							deleted: true,
						})
						.collect::<Vec<_>>();
					block_ref_table
						.insert_many(&deleted_block_refs[..])
						.await?;
				}
			}
			Ok(())
		});
	}
}
