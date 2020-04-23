use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::data::*;
use crate::error::Error;
use crate::table::*;

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct Key {
	// Primary key
	pub key_id: String,

	// Associated secret key (immutable)
	pub secret_key: String,

	// Name
	pub name: String,
	pub name_timestamp: u64,

	// Deletion
	pub deleted: bool,

	// Authorized keys
	authorized_buckets: Vec<AllowedBucket>,
}

impl Key {
	pub fn new(name: String, buckets: Vec<AllowedBucket>) -> Self {
		let key_id = format!("GK{}", hex::encode(&rand::random::<[u8; 12]>()[..]));
		let secret_key = hex::encode(&rand::random::<[u8; 32]>()[..]);
		let mut ret = Self {
			key_id,
			secret_key,
			name,
			name_timestamp: now_msec(),
			deleted: false,
			authorized_buckets: vec![],
		};
		for b in buckets {
			ret.add_bucket(b)
				.expect("Duplicate AllowedBucket in Key constructor");
		}
		ret
	}
	pub fn delete(key_id: String) -> Self {
		Self {
			key_id,
			secret_key: "".into(),
			name: "".into(),
			name_timestamp: now_msec(),
			deleted: true,
			authorized_buckets: vec![],
		}
	}
	/// Add an authorized bucket, only if it wasn't there before
	pub fn add_bucket(&mut self, new: AllowedBucket) -> Result<(), ()> {
		match self
			.authorized_buckets
			.binary_search_by(|b| b.bucket.cmp(&new.bucket))
		{
			Err(i) => {
				self.authorized_buckets.insert(i, new);
				Ok(())
			}
			Ok(_) => Err(()),
		}
	}
	pub fn authorized_buckets(&self) -> &[AllowedBucket] {
		&self.authorized_buckets[..]
	}
	pub fn clear_buckets(&mut self) {
		self.authorized_buckets.clear();
	}
	pub fn allow_read(&self, bucket: &str) -> bool {
		self.authorized_buckets
			.iter()
			.find(|x| x.bucket.as_str() == bucket)
			.map(|x| x.allow_read)
			.unwrap_or(false)
	}
	pub fn allow_write(&self, bucket: &str) -> bool {
		self.authorized_buckets
			.iter()
			.find(|x| x.bucket.as_str() == bucket)
			.map(|x| x.allow_write)
			.unwrap_or(false)
	}
}

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct AllowedBucket {
	pub bucket: String,
	pub timestamp: u64,
	pub allow_read: bool,
	pub allow_write: bool,
}

impl Entry<EmptyKey, String> for Key {
	fn partition_key(&self) -> &EmptyKey {
		&EmptyKey
	}
	fn sort_key(&self) -> &String {
		&self.key_id
	}

	fn merge(&mut self, other: &Self) {
		if other.deleted {
			self.deleted = true;
		}
		if self.deleted {
			self.authorized_buckets.clear();
			return;
		}
		if other.name_timestamp > self.name_timestamp {
			self.name_timestamp = other.name_timestamp;
			self.name = other.name.clone();
		}

		for ab in other.authorized_buckets.iter() {
			match self
				.authorized_buckets
				.binary_search_by(|our_ab| our_ab.bucket.cmp(&ab.bucket))
			{
				Ok(i) => {
					let our_ab = &mut self.authorized_buckets[i];
					if ab.timestamp > our_ab.timestamp {
						*our_ab = ab.clone();
					}
				}
				Err(i) => {
					self.authorized_buckets.insert(i, ab.clone());
				}
			}
		}
	}
}

pub struct KeyTable;

#[async_trait]
impl TableSchema for KeyTable {
	type P = EmptyKey;
	type S = String;
	type E = Key;
	type Filter = ();

	async fn updated(&self, _old: Option<Self::E>, _new: Option<Self::E>) -> Result<(), Error> {
		Ok(())
	}

	fn matches_filter(entry: &Self::E, _filter: &Self::Filter) -> bool {
		!entry.deleted
	}
}
