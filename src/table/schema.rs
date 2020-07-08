use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use garage_util::data::*;
use garage_util::error::Error;

pub trait PartitionKey {
	fn hash(&self) -> Hash;
}

pub trait SortKey {
	fn sort_key(&self) -> &[u8];
}

pub trait Entry<P: PartitionKey, S: SortKey>:
	PartialEq + Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync
{
	fn partition_key(&self) -> &P;
	fn sort_key(&self) -> &S;

	fn merge(&mut self, other: &Self);
}

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EmptyKey;
impl SortKey for EmptyKey {
	fn sort_key(&self) -> &[u8] {
		&[]
	}
}
impl PartitionKey for EmptyKey {
	fn hash(&self) -> Hash {
		[0u8; 32].into()
	}
}

impl PartitionKey for String {
	fn hash(&self) -> Hash {
		hash(self.as_bytes())
	}
}
impl SortKey for String {
	fn sort_key(&self) -> &[u8] {
		self.as_bytes()
	}
}

impl PartitionKey for Hash {
	fn hash(&self) -> Hash {
		self.clone()
	}
}
impl SortKey for Hash {
	fn sort_key(&self) -> &[u8] {
		self.as_slice()
	}
}

#[async_trait]
pub trait TableSchema: Send + Sync {
	type P: PartitionKey + Clone + PartialEq + Serialize + for<'de> Deserialize<'de> + Send + Sync;
	type S: SortKey + Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync;
	type E: Entry<Self::P, Self::S>;
	type Filter: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync;

	// Action to take if not able to decode current version:
	// try loading from an older version
	fn try_migrate(_bytes: &[u8]) -> Option<Self::E> {
		None
	}

	async fn updated(&self, old: Option<Self::E>, new: Option<Self::E>) -> Result<(), Error>;
	fn matches_filter(_entry: &Self::E, _filter: &Self::Filter) -> bool {
		true
	}
}
