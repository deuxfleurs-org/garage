use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use garage_util::background::BackgroundRunner;
use garage_util::data::*;
use garage_util::error::Error;

use garage_table::table_sharded::*;
use garage_table::*;

use crate::version_table::*;

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct Object {
	// Primary key
	pub bucket: String,

	// Sort key
	pub key: String,

	// Data
	versions: Vec<ObjectVersion>,
}

impl Object {
	pub fn new(bucket: String, key: String, versions: Vec<ObjectVersion>) -> Self {
		let mut ret = Self {
			bucket,
			key,
			versions: vec![],
		};
		for v in versions {
			ret.add_version(v)
				.expect("Twice the same ObjectVersion in Object constructor");
		}
		ret
	}
	/// Adds a version if it wasn't already present
	pub fn add_version(&mut self, new: ObjectVersion) -> Result<(), ()> {
		match self
			.versions
			.binary_search_by(|v| v.cmp_key().cmp(&new.cmp_key()))
		{
			Err(i) => {
				self.versions.insert(i, new);
				Ok(())
			}
			Ok(_) => Err(()),
		}
	}
	pub fn versions(&self) -> &[ObjectVersion] {
		&self.versions[..]
	}
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
	pub version_table: Arc<Table<VersionTable, TableShardedReplication>>,
}

#[async_trait]
impl TableSchema for ObjectTable {
	type P = String;
	type S = String;
	type E = Object;
	type Filter = ();

	async fn updated(&self, old: Option<Self::E>, new: Option<Self::E>) -> Result<(), Error> {
		let version_table = self.version_table.clone();
		if let (Some(old_v), Some(new_v)) = (old, new) {
			// Propagate deletion of old versions
			for v in old_v.versions.iter() {
				if new_v
					.versions
					.binary_search_by(|nv| nv.cmp_key().cmp(&v.cmp_key()))
					.is_err()
				{
					let deleted_version = Version::new(
						v.uuid,
						old_v.bucket.clone(),
						old_v.key.clone(),
						true,
						vec![],
					);
					version_table.insert(&deleted_version).await?;
				}
			}
		}
		Ok(())
	}

	fn matches_filter(entry: &Self::E, _filter: &Self::Filter) -> bool {
		entry
			.versions
			.iter()
			.any(|x| x.is_complete && x.data != ObjectVersionData::DeleteMarker)
	}
}
