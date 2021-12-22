use serde::{Deserialize, Serialize};

use garage_table::crdt::*;
use garage_table::*;
use garage_util::data::*;

use crate::permission::BucketKeyPerm;

use garage_model_050::key_table as old;

/// An api key
#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct Key {
	/// The id of the key (immutable), used as partition key
	pub key_id: String,

	/// The secret_key associated
	pub secret_key: String,

	/// Name for the key
	pub name: crdt::Lww<String>,

	/// If the key is present: it gives some permissions,
	/// a map of bucket IDs (uuids) to permissions.
	/// Otherwise no permissions are granted to key
	pub state: crdt::Deletable<KeyParams>,
}

/// Configuration for a key
#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct KeyParams {
	pub allow_create_bucket: crdt::Lww<bool>,
	pub authorized_buckets: crdt::Map<Uuid, BucketKeyPerm>,
	pub local_aliases: crdt::LwwMap<String, crdt::Deletable<Uuid>>,
}

impl KeyParams {
	pub fn new() -> Self {
		KeyParams {
			allow_create_bucket: crdt::Lww::new(false),
			authorized_buckets: crdt::Map::new(),
			local_aliases: crdt::LwwMap::new(),
		}
	}
}

impl Default for KeyParams {
	fn default() -> Self {
		Self::new()
	}
}

impl Crdt for KeyParams {
	fn merge(&mut self, o: &Self) {
		self.allow_create_bucket.merge(&o.allow_create_bucket);
		self.authorized_buckets.merge(&o.authorized_buckets);
		self.local_aliases.merge(&o.local_aliases);
	}
}

impl Key {
	/// Initialize a new Key, generating a random identifier and associated secret key
	pub fn new(name: String) -> Self {
		let key_id = format!("GK{}", hex::encode(&rand::random::<[u8; 12]>()[..]));
		let secret_key = hex::encode(&rand::random::<[u8; 32]>()[..]);
		Self {
			key_id,
			secret_key,
			name: crdt::Lww::new(name),
			state: crdt::Deletable::present(KeyParams::new()),
		}
	}

	/// Import a key from it's parts
	pub fn import(key_id: &str, secret_key: &str, name: &str) -> Self {
		Self {
			key_id: key_id.to_string(),
			secret_key: secret_key.to_string(),
			name: crdt::Lww::new(name.to_string()),
			state: crdt::Deletable::present(KeyParams::new()),
		}
	}

	/// Create a new Key which can me merged to mark an existing key deleted
	pub fn delete(key_id: String) -> Self {
		Self {
			key_id,
			secret_key: "".into(),
			name: crdt::Lww::new("".to_string()),
			state: crdt::Deletable::Deleted,
		}
	}

	/// Check if `Key` is allowed to read in bucket
	pub fn allow_read(&self, bucket: &Uuid) -> bool {
		if let crdt::Deletable::Present(params) = &self.state {
			params
				.authorized_buckets
				.get(bucket)
				.map(|x| x.allow_read)
				.unwrap_or(false)
		} else {
			false
		}
	}

	/// Check if `Key` is allowed to write in bucket
	pub fn allow_write(&self, bucket: &Uuid) -> bool {
		if let crdt::Deletable::Present(params) = &self.state {
			params
				.authorized_buckets
				.get(bucket)
				.map(|x| x.allow_write)
				.unwrap_or(false)
		} else {
			false
		}
	}

	/// Check if `Key` is owner of bucket
	pub fn allow_owner(&self, bucket: &Uuid) -> bool {
		if let crdt::Deletable::Present(params) = &self.state {
			params
				.authorized_buckets
				.get(bucket)
				.map(|x| x.allow_owner)
				.unwrap_or(false)
		} else {
			false
		}
	}
}

impl Entry<EmptyKey, String> for Key {
	fn partition_key(&self) -> &EmptyKey {
		&EmptyKey
	}
	fn sort_key(&self) -> &String {
		&self.key_id
	}
}

impl Crdt for Key {
	fn merge(&mut self, other: &Self) {
		self.name.merge(&other.name);
		self.state.merge(&other.state);
	}
}

pub struct KeyTable;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum KeyFilter {
	Deleted(DeletedFilter),
	Matches(String),
}

impl TableSchema for KeyTable {
	const TABLE_NAME: &'static str = "key";

	type P = EmptyKey;
	type S = String;
	type E = Key;
	type Filter = KeyFilter;

	fn matches_filter(entry: &Self::E, filter: &Self::Filter) -> bool {
		match filter {
			KeyFilter::Deleted(df) => df.apply(entry.state.is_deleted()),
			KeyFilter::Matches(pat) => {
				let pat = pat.to_lowercase();
				entry.key_id.to_lowercase().starts_with(&pat)
					|| entry.name.get().to_lowercase() == pat
			}
		}
	}

	fn try_migrate(bytes: &[u8]) -> Option<Self::E> {
		let old_k = rmp_serde::decode::from_read_ref::<_, old::Key>(bytes).ok()?;

		let state = if old_k.deleted.get() {
			crdt::Deletable::Deleted
		} else {
			// Authorized buckets is ignored here,
			// migration is performed in specific migration code in
			// garage/migrate.rs
			crdt::Deletable::Present(KeyParams {
				allow_create_bucket: crdt::Lww::new(false),
				authorized_buckets: crdt::Map::new(),
				local_aliases: crdt::LwwMap::new(),
			})
		};
		let name = crdt::Lww::raw(old_k.name.timestamp(), old_k.name.get().clone());
		Some(Key {
			key_id: old_k.key_id,
			secret_key: old_k.secret_key,
			name,
			state,
		})
	}
}
