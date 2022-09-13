use serde::{Deserialize, Serialize};

use garage_util::data::*;

use garage_table::crdt::*;
use garage_table::*;

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
	pub bucket: String,
	/// Key in which the related object is stored
	pub key: String,
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
