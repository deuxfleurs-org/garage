use serde::{Deserialize, Serialize};

use garage_db as db;
use garage_util::data::*;

use crate::crdt::Crdt;

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

/// Values of type FixedBytes32 are assumed to be random,
/// either a hash or a random UUID. This means we can use
/// them directly as an index into the hash table.
impl PartitionKey for FixedBytes32 {
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

impl SortKey for FixedBytes32 {
	fn sort_key(&self) -> &[u8] {
		self.as_slice()
	}
}

/// Trait for an entry in a table. It must be sortable and partitionnable.
pub trait Entry<P: PartitionKey, S: SortKey>:
	Crdt + PartialEq + Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync
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
	/// The name of the table in the database
	const TABLE_NAME: &'static str;

	/// The partition key used in that table
	type P: PartitionKey + Clone + PartialEq + Serialize + for<'de> Deserialize<'de> + Send + Sync;
	/// The sort key used int that table
	type S: SortKey + Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync;

	/// They type for an entry in that table
	type E: Entry<Self::P, Self::S>;

	/// The type for a filter that can be applied to select entries
	/// (e.g. filter out deleted entries)
	type Filter: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync;

	// Action to take if not able to decode current version:
	// try loading from an older version
	/// Try migrating an entry from an older version
	fn try_migrate(_bytes: &[u8]) -> Option<Self::E> {
		None
	}

	/// Actions triggered by data changing in a table. If such actions
	/// include updates to the local database that should be applied
	/// atomically with the item update itself, a db transaction is
	/// provided on which these changes should be done.
	/// This function can return a DB error but that's all.
	fn updated(
		&self,
		_tx: &mut db::Transaction,
		_old: Option<&Self::E>,
		_new: Option<&Self::E>,
	) -> db::TxOpResult<()> {
		Ok(())
	}

	fn matches_filter(entry: &Self::E, filter: &Self::Filter) -> bool;
}
