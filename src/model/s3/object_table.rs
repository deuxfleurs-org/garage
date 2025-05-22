use serde::{Deserialize, Serialize};
use std::sync::Arc;

use garage_db as db;

use garage_util::data::*;

use garage_table::crdt::*;
use garage_table::replication::TableShardedReplication;
use garage_table::*;

use crate::index_counter::*;
use crate::s3::mpu_table::*;
use crate::s3::version_table::*;

pub const OBJECTS: &str = "objects";
pub const UNFINISHED_UPLOADS: &str = "unfinished_uploads";
pub const BYTES: &str = "bytes";

mod v08 {
	use garage_util::data::{Hash, Uuid};
	use serde::{Deserialize, Serialize};
	use std::collections::BTreeMap;

	/// An object
	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct Object {
		/// The bucket in which the object is stored, used as partition key
		pub bucket_id: Uuid,

		/// The key at which the object is stored in its bucket, used as sorting key
		pub key: String,

		/// The list of currently stored versions of the object
		pub(super) versions: Vec<ObjectVersion>,
	}

	/// Information about a version of an object
	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct ObjectVersion {
		/// Id of the version
		pub uuid: Uuid,
		/// Timestamp of when the object was created
		pub timestamp: u64,
		/// State of the version
		pub state: ObjectVersionState,
	}

	/// State of an object version
	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub enum ObjectVersionState {
		/// The version is being received
		Uploading(ObjectVersionHeaders),
		/// The version is fully received
		Complete(ObjectVersionData),
		/// The version uploaded containded errors or the upload was explicitly aborted
		Aborted,
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

	impl garage_util::migrate::InitialFormat for Object {}
}

mod v09 {
	use garage_util::data::Uuid;
	use serde::{Deserialize, Serialize};

	use super::v08;

	pub use v08::{ObjectVersionData, ObjectVersionHeaders, ObjectVersionMeta};

	/// An object
	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct Object {
		/// The bucket in which the object is stored, used as partition key
		pub bucket_id: Uuid,

		/// The key at which the object is stored in its bucket, used as sorting key
		pub key: String,

		/// The list of currently stored versions of the object
		pub(super) versions: Vec<ObjectVersion>,
	}

	/// Information about a version of an object
	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct ObjectVersion {
		/// Id of the version
		pub uuid: Uuid,
		/// Timestamp of when the object was created
		pub timestamp: u64,
		/// State of the version
		pub state: ObjectVersionState,
	}

	/// State of an object version
	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub enum ObjectVersionState {
		/// The version is being received
		Uploading {
			/// Indicates whether this is a multipart upload
			multipart: bool,
			/// Headers to be included in the final object
			headers: ObjectVersionHeaders,
		},
		/// The version is fully received
		Complete(ObjectVersionData),
		/// The version uploaded containded errors or the upload was explicitly aborted
		Aborted,
	}

	impl garage_util::migrate::Migrate for Object {
		const VERSION_MARKER: &'static [u8] = b"G09s3o";

		type Previous = v08::Object;

		fn migrate(old: v08::Object) -> Object {
			let versions = old
				.versions
				.into_iter()
				.map(|x| ObjectVersion {
					uuid: x.uuid,
					timestamp: x.timestamp,
					state: match x.state {
						v08::ObjectVersionState::Uploading(h) => ObjectVersionState::Uploading {
							multipart: false,
							headers: h,
						},
						v08::ObjectVersionState::Complete(d) => ObjectVersionState::Complete(d),
						v08::ObjectVersionState::Aborted => ObjectVersionState::Aborted,
					},
				})
				.collect();
			Object {
				bucket_id: old.bucket_id,
				key: old.key,
				versions,
			}
		}
	}
}

mod v010 {
	use garage_util::data::{Hash, Uuid};
	use serde::{Deserialize, Serialize};

	use super::v09;

	/// An object
	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct Object {
		/// The bucket in which the object is stored, used as partition key
		pub bucket_id: Uuid,

		/// The key at which the object is stored in its bucket, used as sorting key
		pub key: String,

		/// The list of currently stored versions of the object
		pub(super) versions: Vec<ObjectVersion>,
	}

	/// Information about a version of an object
	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct ObjectVersion {
		/// Id of the version
		pub uuid: Uuid,
		/// Timestamp of when the object was created
		pub timestamp: u64,
		/// State of the version
		pub state: ObjectVersionState,
	}

	/// State of an object version
	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub enum ObjectVersionState {
		/// The version is being received
		Uploading {
			/// Indicates whether this is a multipart upload
			multipart: bool,
			/// Checksum algorithm to use
			checksum_algorithm: Option<ChecksumAlgorithm>,
			/// Encryption params + headers to be included in the final object
			encryption: ObjectVersionEncryption,
		},
		/// The version is fully received
		Complete(ObjectVersionData),
		/// The version uploaded containded errors or the upload was explicitly aborted
		Aborted,
	}

	/// Data stored in object version
	#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
	pub enum ObjectVersionData {
		/// The object was deleted, this Version is a tombstone to mark it as such
		DeleteMarker,
		/// The object is short, it's stored inlined.
		/// It is never compressed. For encrypted objects, it is encrypted using
		/// AES256-GCM, like the encrypted headers.
		Inline(ObjectVersionMeta, #[serde(with = "serde_bytes")] Vec<u8>),
		/// The object is not short, Hash of first block is stored here, next segments hashes are
		/// stored in the version table
		FirstBlock(ObjectVersionMeta, Hash),
	}

	/// Metadata about the object version
	#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
	pub struct ObjectVersionMeta {
		/// Size of the object. If object is encrypted/compressed,
		/// this is always the size of the unencrypted/uncompressed data
		pub size: u64,
		/// etag of the object
		pub etag: String,
		/// Encryption params + headers (encrypted or plaintext)
		pub encryption: ObjectVersionEncryption,
	}

	/// Encryption information + metadata
	#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
	pub enum ObjectVersionEncryption {
		SseC {
			/// Encrypted serialized ObjectVersionInner struct.
			/// This is never compressed, just encrypted using AES256-GCM.
			#[serde(with = "serde_bytes")]
			inner: Vec<u8>,
			/// Whether data blocks are compressed in addition to being encrypted
			/// (compression happens before encryption, whereas for non-encrypted
			/// objects, compression is handled at the level of the block manager)
			compressed: bool,
			/// Whether the encryption uses an Object Encryption Key derived
			/// from the master SSE-C key, instead of the master SSE-C key itself.
			/// This is the case of objects created in Garage v2+.
			/// This field is kept for compatibility with Garage v2.0.0-beta1,
			/// which did not yet implement the v2 module below.
			#[serde(default)]
			use_oek: bool,
		},
		Plaintext {
			/// Plain-text headers
			inner: ObjectVersionMetaInner,
		},
	}

	/// Vector of headers, as tuples of the format (header name, header value)
	#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
	pub struct ObjectVersionMetaInner {
		pub headers: HeaderList,
		pub checksum: Option<ChecksumValue>,
	}

	pub type HeaderList = Vec<(String, String)>;

	#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug, Serialize, Deserialize)]
	pub enum ChecksumAlgorithm {
		Crc32,
		Crc32c,
		Crc64Nvme,
		Sha1,
		Sha256,
	}

	/// Checksum value for x-amz-checksum-algorithm
	#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug, Serialize, Deserialize)]
	pub enum ChecksumValue {
		Crc32(#[serde(with = "serde_bytes")] [u8; 4]),
		Crc32c(#[serde(with = "serde_bytes")] [u8; 4]),
		Crc64Nvme(#[serde(with = "serde_bytes")] [u8; 8]),
		Sha1(#[serde(with = "serde_bytes")] [u8; 20]),
		Sha256(#[serde(with = "serde_bytes")] [u8; 32]),
	}

	impl garage_util::migrate::Migrate for Object {
		const VERSION_MARKER: &'static [u8] = b"G010s3ob";

		type Previous = v09::Object;

		fn migrate(old: v09::Object) -> Object {
			Object {
				bucket_id: old.bucket_id,
				key: old.key,
				versions: old.versions.into_iter().map(migrate_version).collect(),
			}
		}
	}

	fn migrate_version(old: v09::ObjectVersion) -> ObjectVersion {
		ObjectVersion {
			uuid: old.uuid,
			timestamp: old.timestamp,
			state: match old.state {
				v09::ObjectVersionState::Uploading { multipart, headers } => {
					ObjectVersionState::Uploading {
						multipart,
						checksum_algorithm: None,
						encryption: migrate_headers(headers),
					}
				}
				v09::ObjectVersionState::Complete(d) => {
					ObjectVersionState::Complete(migrate_data(d))
				}
				v09::ObjectVersionState::Aborted => ObjectVersionState::Aborted,
			},
		}
	}

	fn migrate_data(old: v09::ObjectVersionData) -> ObjectVersionData {
		match old {
			v09::ObjectVersionData::DeleteMarker => ObjectVersionData::DeleteMarker,
			v09::ObjectVersionData::Inline(meta, data) => {
				ObjectVersionData::Inline(migrate_meta(meta), data)
			}
			v09::ObjectVersionData::FirstBlock(meta, fb) => {
				ObjectVersionData::FirstBlock(migrate_meta(meta), fb)
			}
		}
	}

	fn migrate_meta(old: v09::ObjectVersionMeta) -> ObjectVersionMeta {
		ObjectVersionMeta {
			size: old.size,
			etag: old.etag,
			encryption: migrate_headers(old.headers),
		}
	}

	fn migrate_headers(old: v09::ObjectVersionHeaders) -> ObjectVersionEncryption {
		use http::header::CONTENT_TYPE;

		let mut new_headers = Vec::with_capacity(old.other.len() + 1);
		if old.content_type != "blob" {
			new_headers.push((CONTENT_TYPE.as_str().to_string(), old.content_type));
		}
		for (name, value) in old.other.into_iter() {
			new_headers.push((name, value));
		}

		ObjectVersionEncryption::Plaintext {
			inner: ObjectVersionMetaInner {
				headers: new_headers,
				checksum: None,
			},
		}
	}

	// Since ObjectVersionMetaInner can now be serialized independently, for the
	// purpose of being encrypted, we need it to support migrations on its own
	// as well.
	impl garage_util::migrate::InitialFormat for ObjectVersionMetaInner {
		const VERSION_MARKER: &'static [u8] = b"G010s3om";
	}
}

mod v2 {
	use garage_util::data::{Hash, Uuid};
	use garage_util::migrate::Migrate;
	use serde::{Deserialize, Serialize};

	use super::v010;
	pub use v010::{ChecksumAlgorithm, ChecksumValue};

	/// An object
	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct Object {
		/// The bucket in which the object is stored, used as partition key
		pub bucket_id: Uuid,

		/// The key at which the object is stored in its bucket, used as sorting key
		pub key: String,

		/// The list of currently stored versions of the object
		pub(super) versions: Vec<ObjectVersion>,
	}

	/// Information about a version of an object
	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct ObjectVersion {
		/// Id of the version
		pub uuid: Uuid,
		/// Timestamp of when the object was created
		pub timestamp: u64,
		/// State of the version
		pub state: ObjectVersionState,
	}

	/// State of an object version
	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub enum ObjectVersionState {
		/// The version is being received
		Uploading {
			/// Indicates whether this is a multipart upload
			multipart: bool,
			/// Checksum algorithm and algorithm type to use
			checksum_algorithm: Option<(ChecksumAlgorithm, ChecksumType)>,
			/// Encryption params + headers to be included in the final object
			encryption: ObjectVersionEncryption,
		},
		/// The version is fully received
		Complete(ObjectVersionData),
		/// The version uploaded containded errors or the upload was explicitly aborted
		Aborted,
	}

	/// Data stored in object version
	#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
	pub enum ObjectVersionData {
		/// The object was deleted, this Version is a tombstone to mark it as such
		DeleteMarker,
		/// The object is short, it's stored inlined.
		/// It is never compressed. For encrypted objects, it is encrypted using
		/// AES256-GCM, like the encrypted headers.
		Inline(ObjectVersionMeta, #[serde(with = "serde_bytes")] Vec<u8>),
		/// The object is not short, Hash of first block is stored here, next segments hashes are
		/// stored in the version table
		FirstBlock(ObjectVersionMeta, Hash),
	}

	/// Metadata about the object version
	#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
	pub struct ObjectVersionMeta {
		/// Size of the object. If object is encrypted/compressed,
		/// this is always the size of the unencrypted/uncompressed data
		pub size: u64,
		/// etag of the object
		pub etag: String,
		/// Encryption params + headers (encrypted or plaintext)
		pub encryption: ObjectVersionEncryption,
	}

	/// Encryption information + metadata
	#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
	pub enum ObjectVersionEncryption {
		SseC {
			/// Encrypted serialized ObjectVersionInner struct.
			/// This is never compressed, just encrypted using AES256-GCM.
			#[serde(with = "serde_bytes")]
			inner: Vec<u8>,
			/// Whether data blocks are compressed in addition to being encrypted
			/// (compression happens before encryption, whereas for non-encrypted
			/// objects, compression is handled at the level of the block manager)
			compressed: bool,
			/// Whether the encryption uses an Object Encryption Key derived
			/// from the master SSE-C key, instead of the master SSE-C key itself.
			/// This is the case of objects created in Garage v2+
			use_oek: bool,
		},
		Plaintext {
			/// Plain-text headers
			inner: ObjectVersionMetaInner,
		},
	}

	/// Vector of headers, as tuples of the format (header name, header value)
	/// Note: checksum can be Some(_) with checksum_type = None for objects that
	/// have been migrated from Garage version before v2.0, as the distinction between
	/// full-object and composite checksums was not implemented yet.
	#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
	pub struct ObjectVersionMetaInner {
		pub headers: HeaderList,
		pub checksum: Option<ChecksumValue>,
		// checksum_type has to be stored separately, because when migrating
		// from older versions of Garage, we can't know the correct value in
		// ObjectVersionMetaInner::migrate (because it cannot take an argument
		// that says whether the object was multipart or not)
		pub checksum_type: Option<ChecksumType>,
	}

	pub type HeaderList = Vec<(String, String)>;

	#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug, Serialize, Deserialize)]
	pub enum ChecksumType {
		FullObject,
		Composite,
	}

	impl garage_util::migrate::Migrate for Object {
		const VERSION_MARKER: &'static [u8] = b"G2s3ob";

		type Previous = v010::Object;

		fn migrate(old: v010::Object) -> Object {
			Object {
				bucket_id: old.bucket_id,
				key: old.key,
				versions: old.versions.into_iter().map(migrate_version).collect(),
			}
		}
	}

	fn migrate_version(old: v010::ObjectVersion) -> ObjectVersion {
		ObjectVersion {
			uuid: old.uuid,
			timestamp: old.timestamp,
			state: match old.state {
				v010::ObjectVersionState::Uploading {
					multipart,
					checksum_algorithm,
					encryption,
				} => ObjectVersionState::Uploading {
					multipart,
					checksum_algorithm: checksum_algorithm.map(|algo| match multipart {
						false => (algo, ChecksumType::FullObject),
						true => (algo, ChecksumType::Composite),
					}),
					encryption: migrate_encryption(encryption),
				},
				v010::ObjectVersionState::Complete(d) => {
					ObjectVersionState::Complete(migrate_data(d))
				}
				v010::ObjectVersionState::Aborted => ObjectVersionState::Aborted,
			},
		}
	}

	fn migrate_data(old: v010::ObjectVersionData) -> ObjectVersionData {
		match old {
			v010::ObjectVersionData::DeleteMarker => ObjectVersionData::DeleteMarker,
			v010::ObjectVersionData::Inline(meta, data) => {
				ObjectVersionData::Inline(migrate_meta(meta), data)
			}
			v010::ObjectVersionData::FirstBlock(meta, fb) => {
				ObjectVersionData::FirstBlock(migrate_meta(meta), fb)
			}
		}
	}

	fn migrate_meta(old: v010::ObjectVersionMeta) -> ObjectVersionMeta {
		ObjectVersionMeta {
			size: old.size,
			etag: old.etag,
			encryption: migrate_encryption(old.encryption),
		}
	}

	fn migrate_encryption(old: v010::ObjectVersionEncryption) -> ObjectVersionEncryption {
		match old {
			v010::ObjectVersionEncryption::SseC {
				inner,
				compressed,
				use_oek,
			} => ObjectVersionEncryption::SseC {
				inner,
				compressed,
				use_oek,
			},
			v010::ObjectVersionEncryption::Plaintext { inner } => {
				ObjectVersionEncryption::Plaintext {
					inner: ObjectVersionMetaInner::migrate(inner),
				}
			}
		}
	}

	impl Migrate for ObjectVersionMetaInner {
		const VERSION_MARKER: &'static [u8] = b"G2s3om";

		type Previous = v010::ObjectVersionMetaInner;

		fn migrate(old: v010::ObjectVersionMetaInner) -> ObjectVersionMetaInner {
			ObjectVersionMetaInner {
				headers: old.headers,
				checksum: old.checksum,
				checksum_type: None,
			}
		}
	}
}

pub use v2::*;

impl Object {
	/// Initialize an Object struct from parts
	pub fn new(bucket_id: Uuid, key: String, versions: Vec<ObjectVersion>) -> Self {
		let mut ret = Self {
			bucket_id,
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
	#[allow(clippy::result_unit_err)]
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
				Uploading { .. } => {
					*self = Complete(b.clone());
				}
			},
			Uploading { .. } => {}
		}
	}
}

impl AutoCrdt for ObjectVersionData {
	const WARN_IF_DIFFERENT: bool = true;
}

impl ObjectVersion {
	fn cmp_key(&self) -> (u64, Uuid) {
		(self.timestamp, self.uuid)
	}

	/// Is the object version currently being uploaded
	///
	/// matches only multipart uploads if check_multipart is Some(true)
	/// matches only non-multipart uploads if check_multipart is Some(false)
	/// matches both if check_multipart is None
	pub fn is_uploading(&self, check_multipart: Option<bool>) -> bool {
		match &self.state {
			ObjectVersionState::Uploading { multipart, .. } => {
				check_multipart.map(|x| x == *multipart).unwrap_or(true)
			}
			_ => false,
		}
	}

	/// Is the object version completely received
	pub fn is_complete(&self) -> bool {
		matches!(self.state, ObjectVersionState::Complete(_))
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

impl Entry<Uuid, String> for Object {
	fn partition_key(&self) -> &Uuid {
		&self.bucket_id
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

impl ChecksumValue {
	pub fn algorithm(&self) -> ChecksumAlgorithm {
		match self {
			ChecksumValue::Crc32(_) => ChecksumAlgorithm::Crc32,
			ChecksumValue::Crc32c(_) => ChecksumAlgorithm::Crc32c,
			ChecksumValue::Crc64Nvme(_) => ChecksumAlgorithm::Crc64Nvme,
			ChecksumValue::Sha1(_) => ChecksumAlgorithm::Sha1,
			ChecksumValue::Sha256(_) => ChecksumAlgorithm::Sha256,
		}
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

pub struct ObjectTable {
	pub version_table: Arc<Table<VersionTable, TableShardedReplication>>,
	pub mpu_table: Arc<Table<MultipartUploadTable, TableShardedReplication>>,
	pub object_counter_table: Arc<IndexCounter<Object>>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum ObjectFilter {
	/// Is the object version available (received and not a tombstone)
	IsData,
	/// Is the object version currently being uploaded
	///
	/// matches only multipart uploads if check_multipart is Some(true)
	/// matches only non-multipart uploads if check_multipart is Some(false)
	/// matches both if check_multipart is None
	IsUploading { check_multipart: Option<bool> },
}

impl TableSchema for ObjectTable {
	const TABLE_NAME: &'static str = "object";

	type P = Uuid;
	type S = String;
	type E = Object;
	type Filter = ObjectFilter;

	fn updated(
		&self,
		tx: &mut db::Transaction,
		old: Option<&Self::E>,
		new: Option<&Self::E>,
	) -> db::TxOpResult<()> {
		// 1. Count
		let counter_res = self.object_counter_table.count(tx, old, new);
		if let Err(e) = db::unabort(counter_res)? {
			error!(
				"Unable to update object counter: {}. Index values will be wrong!",
				e
			);
		}

		// 2. Enqueue propagation deletions to version table
		if let (Some(old_v), Some(new_v)) = (old, new) {
			for v in old_v.versions.iter() {
				let new_v_id = new_v
					.versions
					.binary_search_by(|nv| nv.cmp_key().cmp(&v.cmp_key()));

				// Propagate deletion of old versions to the Version table
				let delete_version = match new_v_id {
					Err(_) => true,
					Ok(i) => {
						new_v.versions[i].state == ObjectVersionState::Aborted
							&& v.state != ObjectVersionState::Aborted
					}
				};
				if delete_version {
					let deleted_version = Version::new(
						v.uuid,
						VersionBacklink::Object {
							bucket_id: old_v.bucket_id,
							key: old_v.key.clone(),
						},
						true,
					);
					let res = self.version_table.queue_insert(tx, &deleted_version);
					if let Err(e) = db::unabort(res)? {
						error!(
							"Unable to enqueue version deletion propagation: {}. A repair will be needed.",
							e
						);
					}
				}

				// After abortion or completion of multipart uploads, delete MPU table entry
				if matches!(
					v.state,
					ObjectVersionState::Uploading {
						multipart: true,
						..
					}
				) {
					let delete_mpu = match new_v_id {
						Err(_) => true,
						Ok(i) => !matches!(
							new_v.versions[i].state,
							ObjectVersionState::Uploading { .. }
						),
					};
					if delete_mpu {
						let deleted_mpu = MultipartUpload::new(
							v.uuid,
							v.timestamp,
							old_v.bucket_id,
							old_v.key.clone(),
							true,
						);
						let res = self.mpu_table.queue_insert(tx, &deleted_mpu);
						if let Err(e) = db::unabort(res)? {
							error!(
								"Unable to enqueue multipart upload deletion propagation: {}. A repair will be needed.",
								e
							);
						}
					}
				}
			}
		}

		Ok(())
	}

	fn matches_filter(entry: &Self::E, filter: &Self::Filter) -> bool {
		match filter {
			ObjectFilter::IsData => entry.versions.iter().any(|v| v.is_data()),
			ObjectFilter::IsUploading { check_multipart } => entry
				.versions
				.iter()
				.any(|v| v.is_uploading(*check_multipart)),
		}
	}
}

impl CountedItem for Object {
	const COUNTER_TABLE_NAME: &'static str = "bucket_object_counter";

	// Partition key = bucket id
	type CP = Uuid;
	// Sort key = nothing
	type CS = EmptyKey;

	fn counter_partition_key(&self) -> &Uuid {
		&self.bucket_id
	}
	fn counter_sort_key(&self) -> &EmptyKey {
		&EmptyKey
	}

	fn counts(&self) -> Vec<(&'static str, i64)> {
		let versions = self.versions();
		let n_objects = if versions.iter().any(|v| v.is_data()) {
			1
		} else {
			0
		};
		let n_unfinished_uploads = versions.iter().filter(|v| v.is_uploading(None)).count();
		let n_bytes = versions
			.iter()
			.map(|v| match &v.state {
				ObjectVersionState::Complete(ObjectVersionData::Inline(meta, _))
				| ObjectVersionState::Complete(ObjectVersionData::FirstBlock(meta, _)) => meta.size,
				_ => 0,
			})
			.sum::<u64>();

		vec![
			(OBJECTS, n_objects),
			(UNFINISHED_UPLOADS, n_unfinished_uploads as i64),
			(BYTES, n_bytes as i64),
		]
	}
}
