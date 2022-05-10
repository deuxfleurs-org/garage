use serde::{Deserialize, Serialize};

use garage_util::data::*;

use crate::schema::*;

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

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum DeletedFilter {
	Any,
	Deleted,
	NotDeleted,
}

impl DeletedFilter {
	pub fn apply(&self, deleted: bool) -> bool {
		match self {
			DeletedFilter::Any => true,
			DeletedFilter::Deleted => deleted,
			DeletedFilter::NotDeleted => !deleted,
		}
	}
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum EnumerationOrder {
	Forward,
	Reverse,
}

impl EnumerationOrder {
	pub fn from_reverse(reverse: bool) -> Self {
		if reverse {
			Self::Reverse
		} else {
			Self::Forward
		}
	}
}
