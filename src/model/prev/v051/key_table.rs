use serde::{Deserialize, Serialize};

use garage_table::crdt::*;
use garage_table::*;

/// An api key
#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct Key {
	/// The id of the key (immutable), used as partition key
	pub key_id: String,

	/// The secret_key associated
	pub secret_key: String,

	/// Name for the key
	pub name: crdt::Lww<String>,

	/// Is the key deleted
	pub deleted: crdt::Bool,

	/// Buckets in which the key is authorized. Empty if `Key` is deleted
	// CRDT interaction: deleted implies authorized_buckets is empty
	pub authorized_buckets: crdt::LwwMap<String, PermissionSet>,
}

/// Permission given to a key in a bucket
#[derive(PartialOrd, Ord, PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub struct PermissionSet {
	/// The key can be used to read the bucket
	pub allow_read: bool,
	/// The key can be used to write in the bucket
	pub allow_write: bool,
}

impl AutoCrdt for PermissionSet {
	const WARN_IF_DIFFERENT: bool = true;
}

impl Crdt for Key {
	fn merge(&mut self, other: &Self) {
		self.name.merge(&other.name);
		self.deleted.merge(&other.deleted);

		if self.deleted.get() {
			self.authorized_buckets.clear();
		} else {
			self.authorized_buckets.merge(&other.authorized_buckets);
		}
	}
}
