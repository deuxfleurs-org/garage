use std::sync::Arc;

use garage_db as db;

use garage_util::data::*;
use garage_util::error::*;

use garage_table::crdt::*;
use garage_table::replication::TableShardedReplication;
use garage_table::*;

use crate::s3::block_ref_table::*;

mod v08 {
	use garage_util::crdt;
	use garage_util::data::{Hash, Uuid};
	use serde::{Deserialize, Serialize};

	/// A version of an object
	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
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

	#[derive(PartialEq, Eq, Clone, Copy, Debug, Serialize, Deserialize)]
	pub struct VersionBlockKey {
		/// Number of the part
		pub part_number: u64,
		/// Offset of this sub-segment in its part as sent by the client
		/// (before any kind of compression or encryption)
		pub offset: u64,
	}

	/// Information about a single block
	#[derive(PartialEq, Eq, Ord, PartialOrd, Clone, Copy, Debug, Serialize, Deserialize)]
	pub struct VersionBlock {
		/// Blake2 sum of the block
		pub hash: Hash,
		/// Size of the block, before any kind of compression or encryption
		pub size: u64,
	}

	impl garage_util::migrate::InitialFormat for Version {}
}

pub(crate) mod v09 {
	use garage_util::crdt;
	use garage_util::data::Uuid;
	use serde::{Deserialize, Serialize};

	use super::v08;

	pub use v08::{VersionBlock, VersionBlockKey};

	/// A version of an object
	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
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

		// Back link to owner of this version (either an object or a multipart
		// upload), used to find whether it has been deleted and this version
		// should in turn be deleted (see versions repair procedure)
		pub backlink: VersionBacklink,
	}

	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub enum VersionBacklink {
		Object {
			/// Bucket in which the related object is stored
			bucket_id: Uuid,
			/// Key in which the related object is stored
			key: String,
		},
		MultipartUpload {
			upload_id: Uuid,
		},
	}

	impl garage_util::migrate::Migrate for Version {
		const VERSION_MARKER: &'static [u8] = b"G09s3v";

		type Previous = v08::Version;

		fn migrate(old: v08::Version) -> Version {
			Version {
				uuid: old.uuid,
				deleted: old.deleted,
				blocks: old.blocks,
				backlink: VersionBacklink::Object {
					bucket_id: old.bucket_id,
					key: old.key,
				},
			}
		}
	}
}

pub use v09::*;

impl Version {
	pub fn new(uuid: Uuid, backlink: VersionBacklink, deleted: bool) -> Self {
		Self {
			uuid,
			deleted: deleted.into(),
			blocks: crdt::Map::new(),
			backlink,
		}
	}

	pub fn has_part_number(&self, part_number: u64) -> bool {
		self.blocks
			.items()
			.binary_search_by(|(k, _)| k.part_number.cmp(&part_number))
			.is_ok()
	}

	pub fn n_parts(&self) -> Result<u64, Error> {
		Ok(self
			.blocks
			.items()
			.last()
			.ok_or_message("version has no parts")?
			.0
			.part_number)
	}
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
		} else {
			self.blocks.merge(&other.blocks);
		}
	}
}

pub struct VersionTable {
	pub block_ref_table: Arc<Table<BlockRefTable, TableShardedReplication>>,
}

impl TableSchema for VersionTable {
	const TABLE_NAME: &'static str = "version";

	type P = Uuid;
	type S = EmptyKey;
	type E = Version;
	type Filter = DeletedFilter;

	fn updated(
		&self,
		tx: &mut db::Transaction,
		old: Option<&Self::E>,
		new: Option<&Self::E>,
	) -> db::TxOpResult<()> {
		if let (Some(old_v), Some(new_v)) = (old, new) {
			// Propagate deletion of version blocks
			if new_v.deleted.get() && !old_v.deleted.get() {
				let deleted_block_refs = old_v.blocks.items().iter().map(|(_k, vb)| BlockRef {
					block: vb.hash,
					version: old_v.uuid,
					deleted: true.into(),
				});
				for block_ref in deleted_block_refs {
					let res = self.block_ref_table.queue_insert(tx, &block_ref);
					if let Err(e) = db::unabort(res)? {
						error!("Unable to enqueue block ref deletion propagation: {}. A repair will be needed.", e);
					}
				}
			}
		}

		Ok(())
	}

	fn matches_filter(entry: &Self::E, filter: &Self::Filter) -> bool {
		filter.apply(entry.deleted.get())
	}
}
