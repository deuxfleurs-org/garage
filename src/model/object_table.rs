use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

use garage_util::background::BackgroundRunner;
use garage_util::data::*;

use garage_table::crdt::*;
use garage_table::replication::TableShardedReplication;
use garage_table::*;

use crate::version_table::*;

/// An object
#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct Object {
	/// The bucket in which the object is stored, used as partition key
	pub bucket: String,

	/// The key at which the object is stored in its bucket, used as sorting key
	pub key: String,

	/// The list of currenty stored versions of the object
	versions: Vec<ObjectVersion>,
}

impl Object {
	/// Initialize an Object struct from parts
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

	/// Get a list of currently stored versions of `Object`
	pub fn versions(&self) -> &[ObjectVersion] {
		&self.versions[..]
	}
}

/// Informations about a version of an object
#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct ObjectVersion {
	/// Id of the version
	pub uuid: UUID,
	/// Timestamp of when the object was created
	pub timestamp: u64,
	/// State of the version
	pub state: ObjectVersionState,
}

/// State of an object version
#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub enum ObjectVersionState {
	/// The version is being received
	Uploading(ObjectVersionHeaders),
	/// The version is fully received
	Complete(ObjectVersionData),
	/// The version uploaded containded errors or the upload was explicitly aborted
	Aborted,
}

impl CRDT for ObjectVersionState {
	fn merge(&mut self, other: &Self) {
		use ObjectVersionState::*;
		match other {
			Aborted => {
				*self = Aborted;
			}
			Complete(b) => match self {
				Aborted => {}
				Complete(a) => {
					a.merge(b);
				}
				Uploading(_) => {
					*self = Complete(b.clone());
				}
			},
			Uploading(_) => {}
		}
	}
}

/// Data stored in object version
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub enum ObjectVersionData {
	/// The object was deleted, this Version is a tombstone to mark it as such
	DeleteMarker,
	/// The object is short, it's stored inlined
	Inline(ObjectVersionMeta, #[serde(with = "serde_bytes")] Vec<u8>),
	/// The object is not short, Hash of first block is stored here, next segments hashes are
	/// stored in the version table
	FirstBlock(ObjectVersionMeta, Hash),
}

impl AutoCRDT for ObjectVersionData {
	const WARN_IF_DIFFERENT: bool = true;
}

/// Metadata about the object version
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct ObjectVersionMeta {
	/// Headers to send to the client
	pub headers: ObjectVersionHeaders,
	/// Size of the object
	pub size: u64,
	/// etag of the object
	pub etag: String,
}

/// Additional headers for an object
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct ObjectVersionHeaders {
	/// Content type of the object
	pub content_type: String,
	/// Any other http headers to send
	pub other: BTreeMap<String, String>,
}

impl ObjectVersion {
	fn cmp_key(&self) -> (u64, UUID) {
		(self.timestamp, self.uuid)
	}

	/// Is the object version currently being uploaded
	pub fn is_uploading(&self) -> bool {
		match self.state {
			ObjectVersionState::Uploading(_) => true,
			_ => false,
		}
	}

	/// Is the object version completely received
	pub fn is_complete(&self) -> bool {
		match self.state {
			ObjectVersionState::Complete(_) => true,
			_ => false,
		}
	}

	/// Is the object version available (received and not a tombstone)
	pub fn is_data(&self) -> bool {
		match self.state {
			ObjectVersionState::Complete(ObjectVersionData::DeleteMarker) => false,
			ObjectVersionState::Complete(_) => true,
			_ => false,
		}
	}
}

impl Entry<String, String> for Object {
	fn partition_key(&self) -> &String {
		&self.bucket
	}
	fn sort_key(&self) -> &String {
		&self.key
	}
	fn is_tombstone(&self) -> bool {
		self.versions.len() == 1
			&& self.versions[0].state
				== ObjectVersionState::Complete(ObjectVersionData::DeleteMarker)
	}
}

impl CRDT for Object {
	fn merge(&mut self, other: &Self) {
		// Merge versions from other into here
		for other_v in other.versions.iter() {
			match self
				.versions
				.binary_search_by(|v| v.cmp_key().cmp(&other_v.cmp_key()))
			{
				Ok(i) => {
					self.versions[i].state.merge(&other_v.state);
				}
				Err(i) => {
					self.versions.insert(i, other_v.clone());
				}
			}
		}

		// Remove versions which are obsolete, i.e. those that come
		// before the last version which .is_complete().
		let last_complete = self
			.versions
			.iter()
			.enumerate()
			.rev()
			.filter(|(_, v)| v.is_complete())
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

impl TableSchema for ObjectTable {
	type P = String;
	type S = String;
	type E = Object;
	type Filter = DeletedFilter;

	fn updated(&self, old: Option<Self::E>, new: Option<Self::E>) {
		let version_table = self.version_table.clone();
		self.background.spawn(async move {
			if let (Some(old_v), Some(new_v)) = (old, new) {
				// Propagate deletion of old versions
				for v in old_v.versions.iter() {
					let newly_deleted = match new_v
						.versions
						.binary_search_by(|nv| nv.cmp_key().cmp(&v.cmp_key()))
					{
						Err(_) => true,
						Ok(i) => {
							new_v.versions[i].state == ObjectVersionState::Aborted
								&& v.state != ObjectVersionState::Aborted
						}
					};
					if newly_deleted {
						let deleted_version =
							Version::new(v.uuid, old_v.bucket.clone(), old_v.key.clone(), true);
						version_table.insert(&deleted_version).await?;
					}
				}
			}
			Ok(())
		})
	}

	fn matches_filter(entry: &Self::E, filter: &Self::Filter) -> bool {
		let deleted = !entry.versions.iter().any(|v| v.is_data());
		filter.apply(deleted)
	}
}
