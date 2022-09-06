use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

use garage_util::data::*;

use garage_table::crdt::*;

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
	/// Get a list of currently stored versions of `Object`
	pub fn versions(&self) -> &[ObjectVersion] {
		&self.versions[..]
	}
}

/// Informations about a version of an object
#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct ObjectVersion {
	/// Id of the version
	pub uuid: Uuid,
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

impl Crdt for ObjectVersionState {
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

impl AutoCrdt for ObjectVersionData {
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
	fn cmp_key(&self) -> (u64, Uuid) {
		(self.timestamp, self.uuid)
	}

	/// Is the object version completely received
	pub fn is_complete(&self) -> bool {
		matches!(self.state, ObjectVersionState::Complete(_))
	}
}

impl Crdt for Object {
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
			.find(|(_, v)| v.is_complete())
			.map(|(vi, _)| vi);

		if let Some(last_vi) = last_complete {
			self.versions = self.versions.drain(last_vi..).collect::<Vec<_>>();
		}
	}
}

