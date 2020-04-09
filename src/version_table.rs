use std::sync::Arc;
use serde::{Serialize, Deserialize};
use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::data::*;
use crate::table::*;
use crate::server::Garage;


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VersionMeta {
	pub bucket: StringKey,
	pub key: StringKey,

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

impl Entry<StringKey, StringKey> for VersionMeta {
	fn partition_key(&self) -> &StringKey {
		&self.bucket
	}
	fn sort_key(&self) -> &StringKey {
		&self.key
	}

	fn merge(&mut self, other: &Self) {
		unimplemented!()
	}
}

#[async_trait]
impl TableFormat for VersionTable {
	type P = StringKey;
	type S = StringKey;
	type E = VersionMeta;

	async fn updated(&self, old: Option<&Self::E>, new: &Self::E) {
		unimplemented!()
	}
}
