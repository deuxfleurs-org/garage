use serde::{Deserialize, Serialize};
use std::sync::Arc;

use garage_util::background::BackgroundRunner;
use garage_util::data::*;

use garage_table::crdt::*;
use garage_table::replication::sharded::*;
use garage_table::*;

use crate::block_ref_table::*;

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct Version {
	// Primary key
	pub uuid: UUID,

	// Actual data: the blocks for this version
	// In the case of a multipart upload, also store the etags
	// of individual parts and check them when doing CompleteMultipartUpload
	pub deleted: crdt::Bool,
	pub blocks: crdt::Map<VersionBlockKey, VersionBlock>,
	pub parts_etags: crdt::Map<u64, String>,

	// Back link to bucket+key so that we can figure if
	// this was deleted later on
	pub bucket: String,
	pub key: String,
}

impl Version {
	pub fn new(uuid: UUID, bucket: String, key: String, deleted: bool) -> Self {
		Self {
			uuid,
			deleted: deleted.into(),
			blocks: crdt::Map::new(),
			parts_etags: crdt::Map::new(),
			bucket,
			key,
		}
	}
}

#[derive(PartialEq, Eq, Clone, Copy, Debug, Serialize, Deserialize)]
pub struct VersionBlockKey {
	pub part_number: u64,
	pub offset: u64,
}

impl Ord for VersionBlockKey {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		self.part_number
			.cmp(&other.part_number)
			.then(self.offset.cmp(&other.offset))
	}
}

impl PartialOrd for VersionBlockKey {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}

#[derive(PartialEq, Eq, Ord, PartialOrd, Clone, Copy, Debug, Serialize, Deserialize)]
pub struct VersionBlock {
	pub hash: Hash,
	pub size: u64,
}

impl AutoCRDT for VersionBlock {
	const WARN_IF_DIFFERENT: bool = true;
}

impl Entry<Hash, EmptyKey> for Version {
	fn partition_key(&self) -> &Hash {
		&self.uuid
	}
	fn sort_key(&self) -> &EmptyKey {
		&EmptyKey
	}
}

impl CRDT for Version {
	fn merge(&mut self, other: &Self) {
		self.deleted.merge(&other.deleted);

		if self.deleted.get() {
			self.blocks.clear();
			self.parts_etags.clear();
		} else {
			self.blocks.merge(&other.blocks);
			self.parts_etags.merge(&other.parts_etags);
		}
	}
}

pub struct VersionTable {
	pub background: Arc<BackgroundRunner>,
	pub block_ref_table: Arc<Table<BlockRefTable, TableShardedReplication>>,
}

impl TableSchema for VersionTable {
	type P = Hash;
	type S = EmptyKey;
	type E = Version;
	type Filter = DeletedFilter;

	fn updated(&self, old: Option<Self::E>, new: Option<Self::E>) {
		let block_ref_table = self.block_ref_table.clone();
		self.background.spawn(async move {
			if let (Some(old_v), Some(new_v)) = (old, new) {
				// Propagate deletion of version blocks
				if new_v.deleted.get() && !old_v.deleted.get() {
					let deleted_block_refs = old_v
						.blocks
						.items()
						.iter()
						.map(|(_k, vb)| BlockRef {
							block: vb.hash,
							version: old_v.uuid,
							deleted: true.into(),
						})
						.collect::<Vec<_>>();
					block_ref_table.insert_many(&deleted_block_refs[..]).await?;
				}
			}
			Ok(())
		})
	}

	fn matches_filter(entry: &Self::E, filter: &Self::Filter) -> bool {
		filter.apply(entry.deleted.get())
	}
}
