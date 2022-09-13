use serde::{Deserialize, Serialize};

use garage_table::crdt::Crdt;
use garage_table::*;

use super::key_table::PermissionSet;

/// A bucket is a collection of objects
///
/// Its parameters are not directly accessible as:
///  - It must be possible to merge paramaters, hence the use of a LWW CRDT.
///  - A bucket has 2 states, Present or Deleted and parameters make sense only if present.
#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub struct Bucket {
	/// Name of the bucket
	pub name: String,
	/// State, and configuration if not deleted, of the bucket
	pub state: crdt::Lww<BucketState>,
}

/// State of a bucket
#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub enum BucketState {
	/// The bucket is deleted
	Deleted,
	/// The bucket exists
	Present(BucketParams),
}

impl Crdt for BucketState {
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
#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
pub struct BucketParams {
	/// Map of key with access to the bucket, and what kind of access they give
	pub authorized_keys: crdt::LwwMap<String, PermissionSet>,
	/// Is the bucket served as http
	pub website: crdt::Lww<bool>,
}

impl Crdt for BucketParams {
	fn merge(&mut self, o: &Self) {
		self.authorized_keys.merge(&o.authorized_keys);
		self.website.merge(&o.website);
	}
}

impl Crdt for Bucket {
	fn merge(&mut self, other: &Self) {
		self.state.merge(&other.state);
	}
}
