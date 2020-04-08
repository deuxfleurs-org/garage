use std::sync::Arc;
use serde::{Serialize, Deserialize};
use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::data::*;
use crate::table::*;
use crate::server::Garage;


#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct VersionMetaKey {
	pub bucket: String,
	pub key: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VersionMetaValue {
	pub timestamp: u64,
	pub uuid: UUID,

	pub mime_type: String,
	pub size: u64,
	pub is_complete: bool,

	pub data: VersionData,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum VersionData {
	DeleteMarker,
	Inline(#[serde(with="serde_bytes")] Vec<u8>),
	FirstBlock(Hash),
}

pub struct VersionTable {
	pub garage: RwLock<Option<Arc<Garage>>>,
}

impl KeyHash for VersionMetaKey {
	fn hash(&self) -> Hash {
		hash(self.bucket.as_bytes())
	}
}

impl ValueMerge for VersionMetaValue {
	fn merge(&mut self, other: &Self) {
		unimplemented!()
	}
}

#[async_trait]
impl TableFormat for VersionTable {
	type K = VersionMetaKey;
	type V = VersionMetaValue;

	async fn updated(&self, key: &Self::K, old: Option<&Self::V>, new: &Self::V) {
		unimplemented!()
	}
}
