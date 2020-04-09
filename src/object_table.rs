use std::sync::Arc;
use serde::{Serialize, Deserialize};
use async_trait::async_trait;
use tokio::sync::RwLock;

use crate::data::*;
use crate::table::*;
use crate::server::Garage;


#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Object {
	pub bucket: String,
	pub key: String,

	pub versions: Vec<Box<Version>>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Version {
	pub uuid: UUID,
	pub timestamp: u64,

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

pub struct ObjectTable {
	pub garage: RwLock<Option<Arc<Garage>>>,
}

impl Entry<String, String> for Object {
	fn partition_key(&self) -> &String {
		&self.bucket
	}
	fn sort_key(&self) -> &String {
		&self.key
	}

	fn merge(&mut self, other: &Self) {
		for other_v in other.versions.iter() {
			match self.versions.binary_search_by(|v| (v.timestamp, &v.uuid).cmp(&(other_v.timestamp, &other_v.uuid))) {
				Ok(i) => {
					let mut v = &mut self.versions[i];
					if other_v.size > v.size {
						v.size = other_v.size;
					}
					if other_v.is_complete {
						v.is_complete = true;
					}
				}
				Err(i) => {
					self.versions.insert(i, other_v.clone());
				}
			}
		}
		let last_complete = self.versions
			.iter().enumerate().rev()
			.filter(|(_, v)| v.is_complete)
			.next()
			.map(|(vi, _)| vi);

		if let Some(last_vi) = last_complete {
			self.versions = self.versions.drain(last_vi..).collect::<Vec<_>>();
		}
	}
}

#[async_trait]
impl TableFormat for ObjectTable {
	type P = String;
	type S = String;
	type E = Object;

	async fn updated(&self, old: Option<&Self::E>, new: &Self::E) {
		unimplemented!()
	}
}
