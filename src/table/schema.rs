use serde::{Deserialize, Serialize};

use garage_util::data::*;

use crate::crdt::CRDT;

/// Trait for field used to partition data
pub trait PartitionKey {
	/// Get the key used to partition
	fn hash(&self) -> Hash;
}

impl PartitionKey for String {
	fn hash(&self) -> Hash {
		blake2sum(self.as_bytes())
	}
}

impl PartitionKey for Hash {
	fn hash(&self) -> Hash {
		*self
	}
}

/// Trait for field used to sort data
pub trait SortKey {
	/// Get the key used to sort
	fn sort_key(&self) -> &[u8];
}

impl SortKey for String {
	fn sort_key(&self) -> &[u8] {
		self.as_bytes()
	}
}

impl SortKey for Hash {
	fn sort_key(&self) -> &[u8] {
		self.as_slice()
	}
}

/// Trait for an entry in a table. It must be sortable and partitionnable.
pub trait Entry<P: PartitionKey, S: SortKey>:
	CRDT + PartialEq + Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync
{
	/// Get the key used to partition
	fn partition_key(&self) -> &P;
	/// Get the key used to sort
	fn sort_key(&self) -> &S;

	/// Is the entry a tombstone? Default implementation always return false
	fn is_tombstone(&self) -> bool {
		false
	}
}

/// Trait for the schema used in a table
pub trait TableSchema: Send + Sync {
	/// The partition key used in that table
	type P: PartitionKey + Clone + PartialEq + Serialize + for<'de> Deserialize<'de> + Send + Sync;
	/// The sort key used int that table
	type S: SortKey + Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync;
	/// They type for an entry in that table
	type E: Entry<Self::P, Self::S>;
	type Filter: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync;

	// Action to take if not able to decode current version:
	// try loading from an older version
	/// Try migrating an entry from an older version
	fn try_migrate(_bytes: &[u8]) -> Option<Self::E> {
		None
	}

	// Updated triggers some stuff downstream, but it is not supposed to block or fail,
	// as the update itself is an unchangeable fact that will never go back
	// due to CRDT logic. Typically errors in propagation of info should be logged
	// to stderr.
	fn updated(&self, _old: Option<Self::E>, _new: Option<Self::E>) {}

	fn matches_filter(entry: &Self::E, filter: &Self::Filter) -> bool;
}
