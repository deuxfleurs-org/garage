use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::error::Error;
use crate::table::*;

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct Bucket {
	// Primary key
	pub name: String,

	// Timestamp and deletion
	// Upon version increment, all info is replaced
	pub timestamp: u64,
	pub deleted: bool,

	// Authorized keys
	authorized_keys: Vec<AllowedKey>,
}

impl Bucket {
	pub fn new(
		name: String,
		timestamp: u64,
		deleted: bool,
		authorized_keys: Vec<AllowedKey>,
	) -> Self {
		let mut ret = Bucket {
			name,
			timestamp,
			deleted,
			authorized_keys: vec![],
		};
		for key in authorized_keys {
			ret.add_key(key)
				.expect("Duplicate AllowedKey in Bucket constructor");
		}
		ret
	}
	/// Add a key only if it is not already present
	pub fn add_key(&mut self, key: AllowedKey) -> Result<(), ()> {
		match self
			.authorized_keys
			.binary_search_by(|k| k.key_id.cmp(&key.key_id))
		{
			Err(i) => {
				self.authorized_keys.insert(i, key);
				Ok(())
			}
			Ok(_) => Err(()),
		}
	}
	pub fn authorized_keys(&self) -> &[AllowedKey] {
		&self.authorized_keys[..]
	}
	pub fn clear_keys(&mut self) {
		self.authorized_keys.clear();
	}
}

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct AllowedKey {
	pub key_id: String,
	pub timestamp: u64,
	pub allow_read: bool,
	pub allow_write: bool,
}

impl Entry<EmptyKey, String> for Bucket {
	fn partition_key(&self) -> &EmptyKey {
		&EmptyKey
	}
	fn sort_key(&self) -> &String {
		&self.name
	}

	fn merge(&mut self, other: &Self) {
		if other.timestamp < self.timestamp {
			*self = other.clone();
			return;
		}
		if self.timestamp > other.timestamp || self.deleted {
			return;
		}

		for ak in other.authorized_keys.iter() {
			match self
				.authorized_keys
				.binary_search_by(|our_ak| our_ak.key_id.cmp(&ak.key_id))
			{
				Ok(i) => {
					let our_ak = &mut self.authorized_keys[i];
					if ak.timestamp > our_ak.timestamp {
						*our_ak = ak.clone();
					}
				}
				Err(i) => {
					self.authorized_keys.insert(i, ak.clone());
				}
			}
		}
	}
}

pub struct BucketTable;

#[async_trait]
impl TableSchema for BucketTable {
	type P = EmptyKey;
	type S = String;
	type E = Bucket;
	type Filter = ();

	async fn updated(&self, _old: Option<Self::E>, _new: Option<Self::E>) -> Result<(), Error> {
		Ok(())
	}

	fn matches_filter(entry: &Self::E, _filter: &Self::Filter) -> bool {
		!entry.deleted
	}
}
