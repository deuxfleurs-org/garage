use serde::{Deserialize, Serialize};

use garage_table::crdt::CRDT;
use garage_table::*;

use crate::key_table::PermissionSet;

/// A bucket is a collection of objects
///
/// Its parameters are not directly accessible as:
///  - It must be possible to merge paramaters, hence the use of a LWW CRDT.
///  - A bucket has 2 states, Present or Deleted and parameters make sense only if present.
#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct Bucket {
	/// Name of the bucket
	pub name: String,
	/// State, and configuration if not deleted, of the bucket
	pub state: crdt::LWW<BucketState>,
}

/// State of a bucket
#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub enum BucketState {
	/// The bucket is deleted
	Deleted,
	/// The bucket exists
	Present(BucketParams),
}

impl CRDT for BucketState {
	fn merge(&mut self, o: &Self) {
		match o {
			BucketState::Deleted => *self = BucketState::Deleted,
			BucketState::Present(other_params) => {
				if let BucketState::Present(params) = self {
					params.merge(other_params);
				}
			}
		}
	}
}

/// Configuration for a bucket
#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct BucketParams {
	/// Map of key with access to the bucket, and what kind of access they give
	pub authorized_keys: crdt::LWWMap<String, PermissionSet>,
	/// Is the bucket served as http
	pub website: crdt::LWW<bool>,
}

impl CRDT for BucketParams {
	fn merge(&mut self, o: &Self) {
		self.authorized_keys.merge(&o.authorized_keys);
		self.website.merge(&o.website);
	}
}

impl BucketParams {
	/// Create an empty BucketParams with no authorized keys and no website accesss
	pub fn new() -> Self {
		BucketParams {
			authorized_keys: crdt::LWWMap::new(),
			website: crdt::LWW::new(false),
		}
	}
}

impl Bucket {
	/// Initializes a new instance of the Bucket struct
	pub fn new(name: String) -> Self {
		Bucket {
			name,
			state: crdt::LWW::new(BucketState::Present(BucketParams::new())),
		}
	}

	/// Returns true if this represents a deleted bucket
	pub fn is_deleted(&self) -> bool {
		*self.state.get() == BucketState::Deleted
	}

	/// Return the list of authorized keys, when each was updated, and the permission associated to
	/// the key
	pub fn authorized_keys(&self) -> &[(String, u64, PermissionSet)] {
		match self.state.get() {
			BucketState::Deleted => &[],
			BucketState::Present(state) => state.authorized_keys.items(),
		}
	}
}

impl Entry<EmptyKey, String> for Bucket {
	fn partition_key(&self) -> &EmptyKey {
		&EmptyKey
	}
	fn sort_key(&self) -> &String {
		&self.name
	}
}

impl CRDT for Bucket {
	fn merge(&mut self, other: &Self) {
		self.state.merge(&other.state);
	}
}

pub struct BucketTable;

impl TableSchema for BucketTable {
	type P = EmptyKey;
	type S = String;
	type E = Bucket;
	type Filter = DeletedFilter;

	fn matches_filter(entry: &Self::E, filter: &Self::Filter) -> bool {
		filter.apply(entry.is_deleted())
	}
}
