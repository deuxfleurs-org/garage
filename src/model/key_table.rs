use serde::{Deserialize, Serialize};

use garage_util::crdt::{self, Crdt};
use garage_util::data::*;
use garage_util::time::now_msec;

use garage_table::{DeletedFilter, EmptyKey, Entry, TableSchema};

use crate::permission::BucketKeyPerm;

mod v08 {
	use crate::permission::BucketKeyPerm;
	use garage_util::crdt;
	use garage_util::data::Uuid;
	use serde::{Deserialize, Serialize};

	/// An api key
	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct Key {
		/// The id of the key (immutable), used as partition key
		pub key_id: String,

		/// Internal state of the key
		pub state: crdt::Deletable<KeyParams>,
	}

	/// Configuration for a key
	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct KeyParams {
		/// The secret_key associated (immutable)
		pub secret_key: String,

		/// Name for the key
		pub name: crdt::Lww<String>,

		/// Flag to allow users having this key to create buckets
		pub allow_create_bucket: crdt::Lww<bool>,

		/// If the key is present: it gives some permissions,
		/// a map of bucket IDs (uuids) to permissions.
		/// Otherwise no permissions are granted to key
		pub authorized_buckets: crdt::Map<Uuid, BucketKeyPerm>,

		/// A key can have a local view of buckets names it is
		/// the only one to see, this is the namespace for these aliases
		pub local_aliases: crdt::LwwMap<String, Option<Uuid>>,
	}

	impl garage_util::migrate::InitialFormat for Key {}
}

mod v2 {
	use crate::permission::BucketKeyPerm;
	use garage_util::crdt;
	use garage_util::data::Uuid;
	use serde::{Deserialize, Serialize};

	use super::v08;

	/// An api key
	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct Key {
		/// The id of the key (immutable), used as partition key
		pub key_id: String,

		/// Internal state of the key
		pub state: crdt::Deletable<KeyParams>,
	}

	/// Configuration for a key
	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct KeyParams {
		/// Key's creation date, if known (older versions of Garage didn't keep track
		/// of this information)
		pub created: Option<u64>,
		/// The secret_key associated (immutable)
		pub secret_key: String,

		/// Name for the key
		pub name: crdt::Lww<String>,
		/// The optional time of expiration of the key
		pub expiration: crdt::Lww<Option<u64>>,

		/// Flag to allow users having this key to create buckets
		pub allow_create_bucket: crdt::Lww<bool>,

		/// If the key is present: it gives some permissions,
		/// a map of bucket IDs (uuids) to permissions.
		/// Otherwise no permissions are granted to key
		pub authorized_buckets: crdt::Map<Uuid, BucketKeyPerm>,

		/// A key can have a local view of buckets names it is
		/// the only one to see, this is the namespace for these aliases
		pub local_aliases: crdt::LwwMap<String, Option<Uuid>>,
	}

	impl garage_util::migrate::Migrate for Key {
		const VERSION_MARKER: &'static [u8] = b"G2key";

		type Previous = v08::Key;

		fn migrate(old: v08::Key) -> Key {
			Key {
				key_id: old.key_id,
				state: old.state.map(|x| KeyParams {
					created: None,
					secret_key: x.secret_key,
					name: x.name,
					expiration: crdt::Lww::raw(0, None),
					allow_create_bucket: x.allow_create_bucket,
					authorized_buckets: x.authorized_buckets,
					local_aliases: x.local_aliases,
				}),
			}
		}
	}
}

pub use v2::*;

impl KeyParams {
	fn new(secret_key: &str, name: &str) -> Self {
		KeyParams {
			created: Some(now_msec()),
			secret_key: secret_key.to_string(),
			name: crdt::Lww::new(name.to_string()),
			expiration: crdt::Lww::new(None),
			allow_create_bucket: crdt::Lww::new(false),
			authorized_buckets: crdt::Map::new(),
			local_aliases: crdt::LwwMap::new(),
		}
	}
}

impl Crdt for KeyParams {
	fn merge(&mut self, o: &Self) {
		self.name.merge(&o.name);
		self.expiration.merge(&o.expiration);
		self.allow_create_bucket.merge(&o.allow_create_bucket);
		self.authorized_buckets.merge(&o.authorized_buckets);
		self.local_aliases.merge(&o.local_aliases);
	}
}

impl Key {
	/// Initialize a new Key, generating a random identifier and associated secret key
	pub fn new(name: &str) -> Self {
		let key_id = format!("GK{}", hex::encode(&rand::random::<[u8; 12]>()[..]));
		let secret_key = hex::encode(&rand::random::<[u8; 32]>()[..]);
		Self {
			key_id,
			state: crdt::Deletable::present(KeyParams::new(&secret_key, name)),
		}
	}

	/// Import a key from it's parts
	pub fn import(key_id: &str, secret_key: &str, name: &str) -> Result<Self, &'static str> {
		if key_id.len() != 26 || &key_id[..2] != "GK" || hex::decode(&key_id[2..]).is_err() {
			return Err("The specified key ID is not a valid Garage key ID (starts with `GK`, followed by 12 hex-encoded bytes)");
		}

		if secret_key.len() != 64 || hex::decode(&secret_key).is_err() {
			return Err("The specified secret key is not a valid Garage secret key (composed of 32 hex-encoded bytes)");
		}

		Ok(Self {
			key_id: key_id.to_string(),
			state: crdt::Deletable::present(KeyParams::new(secret_key, name)),
		})
	}

	/// Create a new Key which can me merged to mark an existing key deleted
	pub fn delete(key_id: String) -> Self {
		Self {
			key_id,
			state: crdt::Deletable::Deleted,
		}
	}

	/// Returns true if this represents a deleted bucket
	pub fn is_deleted(&self) -> bool {
		self.state.is_deleted()
	}

	/// Returns an option representing the params (None if in deleted state)
	pub fn params(&self) -> Option<&KeyParams> {
		self.state.as_option()
	}

	/// Mutable version of `.state()`
	pub fn params_mut(&mut self) -> Option<&mut KeyParams> {
		self.state.as_option_mut()
	}

	/// Get permissions for a bucket
	pub fn bucket_permissions(&self, bucket: &Uuid) -> BucketKeyPerm {
		self.params()
			.and_then(|params| params.authorized_buckets.get(bucket))
			.cloned()
			.unwrap_or(BucketKeyPerm::NO_PERMISSIONS)
	}

	/// Check if `Key` is allowed to read in bucket
	pub fn allow_read(&self, bucket: &Uuid) -> bool {
		self.bucket_permissions(bucket).allow_read
	}

	/// Check if `Key` is allowed to write in bucket
	pub fn allow_write(&self, bucket: &Uuid) -> bool {
		self.bucket_permissions(bucket).allow_write
	}

	/// Check if `Key` is owner of bucket
	pub fn allow_owner(&self, bucket: &Uuid) -> bool {
		self.bucket_permissions(bucket).allow_owner
	}
}

impl KeyParams {
	pub fn is_expired(&self, ts_now: u64) -> bool {
		match *self.expiration.get() {
			None => false,
			Some(exp) => ts_now >= exp,
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
		self.state.merge(&other.state);
	}
}

pub struct KeyTable;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum KeyFilter {
	Deleted(DeletedFilter),
	MatchesAndNotDeleted(String),
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
			KeyFilter::MatchesAndNotDeleted(pat) => {
				let pat = pat.to_lowercase();
				entry
					.params()
					.map(|p| {
						entry.key_id.to_lowercase().starts_with(&pat)
							|| p.name.get().to_lowercase() == pat
					})
					.unwrap_or(false)
			}
		}
	}
}
