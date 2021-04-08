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
	pub name: crdt::LWW<String>,

	/// Is the key deleted
	pub deleted: crdt::Bool,

	/// Buckets in which the key is authorized. Empty if `Key` is deleted
	// CRDT interaction: deleted implies authorized_buckets is empty
	pub authorized_buckets: crdt::LWWMap<String, PermissionSet>,
}

impl Key {
	/// Initialize a new Key, generating a random identifier and associated secret key
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

	/// Import a key from it's parts
	pub fn import(key_id: &str, secret_key: &str, name: &str) -> Self {
		Self {
			key_id: key_id.to_string(),
			secret_key: secret_key.to_string(),
			name: crdt::LWW::new(name.to_string()),
			deleted: crdt::Bool::new(false),
			authorized_buckets: crdt::LWWMap::new(),
		}
	}

	/// Create a new Key which can me merged to mark an existing key deleted
	pub fn delete(key_id: String) -> Self {
		Self {
			key_id,
			secret_key: "".into(),
			name: crdt::LWW::new("".to_string()),
			deleted: crdt::Bool::new(true),
			authorized_buckets: crdt::LWWMap::new(),
		}
	}

	/// Check if `Key` is allowed to read in bucket
	pub fn allow_read(&self, bucket: &str) -> bool {
		self.authorized_buckets
			.get(&bucket.to_string())
			.map(|x| x.allow_read)
			.unwrap_or(false)
	}

	/// Check if `Key` is allowed to write in bucket
	pub fn allow_write(&self, bucket: &str) -> bool {
		self.authorized_buckets
			.get(&bucket.to_string())
			.map(|x| x.allow_write)
			.unwrap_or(false)
	}
}

/// Permission given to a key in a bucket
#[derive(PartialOrd, Ord, PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub struct PermissionSet {
	/// The key can be used to read the bucket
	pub allow_read: bool,
	/// The key can be used to write in the bucket
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum KeyFilter {
	Deleted(DeletedFilter),
	Matches(String),
}

impl TableSchema for KeyTable {
	type P = EmptyKey;
	type S = String;
	type E = Key;
	type Filter = KeyFilter;

	fn matches_filter(entry: &Self::E, filter: &Self::Filter) -> bool {
		match filter {
			KeyFilter::Deleted(df) => df.apply(entry.deleted.get()),
			KeyFilter::Matches(pat) => {
				let pat = pat.to_lowercase();
				entry.key_id.to_lowercase().starts_with(&pat)
					|| entry.name.get().to_lowercase() == pat
			}
		}
	}
}
