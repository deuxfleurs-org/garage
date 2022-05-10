use serde::{Deserialize, Serialize};
use std::sync::Arc;

use garage_util::background::BackgroundRunner;
use garage_util::data::*;

use garage_table::crdt::*;
use garage_table::replication::TableShardedReplication;
use garage_table::*;

use crate::s3::block_ref_table::*;

use garage_model_050::version_table as old;

/// A version of an object
#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct Version {
	/// UUID of the version, used as partition key
	pub uuid: Uuid,

	// Actual data: the blocks for this version
	// In the case of a multipart upload, also store the etags
	// of individual parts and check them when doing CompleteMultipartUpload
	/// Is this version deleted
	pub deleted: crdt::Bool,
	/// list of blocks of data composing the version
	pub blocks: crdt::Map<VersionBlockKey, VersionBlock>,
	/// Etag of each part in case of a multipart upload, empty otherwise
	pub parts_etags: crdt::Map<u64, String>,

	// Back link to bucket+key so that we can figure if
	// this was deleted later on
	/// Bucket in which the related object is stored
	pub bucket_id: Uuid,
	/// Key in which the related object is stored
	pub key: String,
}

impl Version {
	pub fn new(uuid: Uuid, bucket_id: Uuid, key: String, deleted: bool) -> Self {
		Self {
			uuid,
			deleted: deleted.into(),
			blocks: crdt::Map::new(),
			parts_etags: crdt::Map::new(),
			bucket_id,
			key,
		}
	}

	pub fn has_part_number(&self, part_number: u64) -> bool {
		let case1 = self
			.parts_etags
			.items()
			.binary_search_by(|(k, _)| k.cmp(&part_number))
			.is_ok();
		let case2 = self
			.blocks
			.items()
			.binary_search_by(|(k, _)| k.part_number.cmp(&part_number))
			.is_ok();
		case1 || case2
	}
}

#[derive(PartialEq, Eq, Clone, Copy, Debug, Serialize, Deserialize)]
pub struct VersionBlockKey {
	/// Number of the part
	pub part_number: u64,
	/// Offset of this sub-segment in its part
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

/// Informations about a single block
#[derive(PartialEq, Eq, Ord, PartialOrd, Clone, Copy, Debug, Serialize, Deserialize)]
pub struct VersionBlock {
	/// Blake2 sum of the block
	pub hash: Hash,
	/// Size of the block
	pub size: u64,
}

impl AutoCrdt for VersionBlock {
	const WARN_IF_DIFFERENT: bool = true;
}

impl Entry<Uuid, EmptyKey> for Version {
	fn partition_key(&self) -> &Uuid {
		&self.uuid
	}
	fn sort_key(&self) -> &EmptyKey {
		&EmptyKey
	}
	fn is_tombstone(&self) -> bool {
		self.deleted.get()
	}
}

impl Crdt for Version {
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
	const TABLE_NAME: &'static str = "version";

	type P = Uuid;
	type S = EmptyKey;
	type E = Version;
	type Filter = DeletedFilter;

	fn updated(&self, old: Option<&Self::E>, new: Option<&Self::E>) {
		let block_ref_table = self.block_ref_table.clone();
		let old = old.cloned();
		let new = new.cloned();

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

	fn try_migrate(bytes: &[u8]) -> Option<Self::E> {
		let old = rmp_serde::decode::from_read_ref::<_, old::Version>(bytes).ok()?;

		let blocks = old
			.blocks
			.items()
			.iter()
			.map(|(k, v)| {
				(
					VersionBlockKey {
						part_number: k.part_number,
						offset: k.offset,
					},
					VersionBlock {
						hash: Hash::try_from(v.hash.as_slice()).unwrap(),
						size: v.size,
					},
				)
			})
			.collect::<crdt::Map<_, _>>();

		let parts_etags = old
			.parts_etags
			.items()
			.iter()
			.map(|(k, v)| (*k, v.clone()))
			.collect::<crdt::Map<_, _>>();

		Some(Version {
			uuid: Hash::try_from(old.uuid.as_slice()).unwrap(),
			deleted: crdt::Bool::new(old.deleted.get()),
			blocks,
			parts_etags,
			bucket_id: blake2sum(old.bucket.as_bytes()),
			key: old.key,
		})
	}
}
