use serde::{Deserialize, Serialize};

use garage_table::crdt::*;
use garage_table::*;
use garage_util::data::*;

/// The bucket alias table holds the names given to buckets
/// in the global namespace.
#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct BucketAlias {
	pub name: String,
	pub state: crdt::Lww<crdt::Deletable<AliasParams>>,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct AliasParams {
	pub bucket_id: Uuid,
}

impl AutoCrdt for AliasParams {
	const WARN_IF_DIFFERENT: bool = true;
}

impl BucketAlias {
	pub fn new(name: String, bucket_id: Uuid) -> Self {
		BucketAlias {
			name,
			state: crdt::Lww::new(crdt::Deletable::present(AliasParams { bucket_id })),
		}
	}
	pub fn is_deleted(&self) -> bool {
		self.state.get().is_deleted()
	}
}

impl Crdt for BucketAlias {
	fn merge(&mut self, o: &Self) {
		self.state.merge(&o.state);
	}
}

impl Entry<EmptyKey, String> for BucketAlias {
	fn partition_key(&self) -> &EmptyKey {
		&EmptyKey
	}
	fn sort_key(&self) -> &String {
		&self.name
	}
}

pub struct BucketAliasTable;

impl TableSchema for BucketAliasTable {
	const TABLE_NAME: &'static str = "bucket_alias";

	type P = EmptyKey;
	type S = String;
	type E = BucketAlias;
	type Filter = DeletedFilter;

	fn matches_filter(entry: &Self::E, filter: &Self::Filter) -> bool {
		filter.apply(entry.is_deleted())
	}
}
