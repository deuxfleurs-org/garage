use serde::{Deserialize, Serialize};

use garage_table::crdt::*;
use garage_table::*;

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct Key {
	// Primary key
	pub key_id: String,

	// Associated secret key (immutable)
	pub secret_key: String,

	// Name
	pub name: crdt::LWW<String>,

	// Deletion
	pub deleted: crdt::Bool,

	// Authorized keys
	pub authorized_buckets: crdt::LWWMap<String, PermissionSet>,
	// CRDT interaction: deleted implies authorized_buckets is empty
}

impl Key {
	pub fn new(name: String) -> Self {
		let key_id = format!("GK{}", hex::encode(&rand::random::<[u8; 12]>()[..]));
		let secret_key = hex::encode(&rand::random::<[u8; 32]>()[..]);
		Self {
			key_id,
			secret_key,
			name: crdt::LWW::new(name),
			deleted: crdt::Bool::new(false),
			authorized_buckets: crdt::LWWMap::new(),
		}
	}
	pub fn delete(key_id: String) -> Self {
		Self {
			key_id,
			secret_key: "".into(),
			name: crdt::LWW::new("".to_string()),
			deleted: crdt::Bool::new(true),
			authorized_buckets: crdt::LWWMap::new(),
		}
	}
	/// Add an authorized bucket, only if it wasn't there before
	pub fn allow_read(&self, bucket: &str) -> bool {
		self.authorized_buckets
			.get(&bucket.to_string())
			.map(|x| x.allow_read)
			.unwrap_or(false)
	}
	pub fn allow_write(&self, bucket: &str) -> bool {
		self.authorized_buckets
			.get(&bucket.to_string())
			.map(|x| x.allow_write)
			.unwrap_or(false)
	}
}

#[derive(PartialOrd, Ord, PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub struct PermissionSet {
	pub allow_read: bool,
	pub allow_write: bool,
}

impl AutoCRDT for PermissionSet {
	const WARN_IF_DIFFERENT: bool = true;
}

impl Entry<EmptyKey, String> for Key {
	fn partition_key(&self) -> &EmptyKey {
		&EmptyKey
	}
	fn sort_key(&self) -> &String {
		&self.key_id
	}
}

impl CRDT for Key {
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

pub struct KeyTable;

impl TableSchema for KeyTable {
	type P = EmptyKey;
	type S = String;
	type E = Key;
	type Filter = DeletedFilter;

	fn matches_filter(entry: &Self::E, filter: &Self::Filter) -> bool {
		filter.apply(entry.deleted.get())
	}
}
