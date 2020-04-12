use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::data::*;
use crate::table::*;
use crate::background::BackgroundRunner;

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct Object {
	// Primary key
	pub bucket: String,

	// Sort key
	pub key: String,

	// Data
	pub versions: Vec<Box<ObjectVersion>>,
}

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct ObjectVersion {
	pub uuid: UUID,
	pub timestamp: u64,

	pub mime_type: String,
	pub size: u64,
	pub is_complete: bool,

	pub data: ObjectVersionData,
}

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub enum ObjectVersionData {
	DeleteMarker,
	Inline(#[serde(with = "serde_bytes")] Vec<u8>),
	FirstBlock(Hash),
}

impl ObjectVersion {
	fn cmp_key(&self) -> (u64, &UUID) {
		(self.timestamp, &self.uuid)
	}
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
			match self
				.versions
				.binary_search_by(|v| v.cmp_key().cmp(&other_v.cmp_key()))
			{
				Ok(i) => {
					let mut v = &mut self.versions[i];
					if other_v.size > v.size {
						v.size = other_v.size;
					}
					if other_v.is_complete && !v.is_complete {
						v.is_complete = true;
					}
				}
				Err(i) => {
					self.versions.insert(i, other_v.clone());
				}
			}
		}
		let last_complete = self
			.versions
			.iter()
			.enumerate()
			.rev()
			.filter(|(_, v)| v.is_complete)
			.next()
			.map(|(vi, _)| vi);

		if let Some(last_vi) = last_complete {
			self.versions = self.versions.drain(last_vi..).collect::<Vec<_>>();
		}
	}
}

pub struct ObjectTable {
	pub background: Arc<BackgroundRunner>,
	pub version_table: Arc<Table<VersionTable>>,
}

#[async_trait]
impl TableFormat for ObjectTable {
	type P = String;
	type S = String;
	type E = Object;

	async fn updated(&self, old: Option<Self::E>, new: Self::E) {
		let version_table = self.version_table.clone();
		self.background.spawn(async move {
			// Propagate deletion of old versions
			if let Some(old_v) = old {
				for v in old_v.versions.iter() {
					if new
						.versions
						.binary_search_by(|nv| nv.cmp_key().cmp(&v.cmp_key()))
						.is_err()
					{
						let deleted_version = Version {
							uuid: v.uuid.clone(),
							deleted: true,
							blocks: vec![],
							bucket: old_v.bucket.clone(),
							key: old_v.key.clone(),
						};
						version_table.insert(&deleted_version).await?;
					}
				}
			}
			Ok(())
		});
	}
}
