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
	pub authorized_keys: Vec<AllowedKey>,
}

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct AllowedKey {
	pub access_key_id: String,
	pub timestamp: u64,
	pub allowed_read: bool,
	pub allowed_write: bool,
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
		if self.timestamp > other.timestamp {
			return;
		}
		for ak in other.authorized_keys.iter() {
			match self
				.authorized_keys
				.binary_search_by(|our_ak| our_ak.access_key_id.cmp(&ak.access_key_id))
			{
				Ok(i) => {
					let our_ak = &mut self.authorized_keys[i];
					if ak.timestamp > our_ak.timestamp {
						our_ak.timestamp = ak.timestamp;
						our_ak.allowed_read = ak.allowed_read;
						our_ak.allowed_write = ak.allowed_write;
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
