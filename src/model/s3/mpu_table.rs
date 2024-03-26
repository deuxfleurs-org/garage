use std::sync::Arc;

use garage_db as db;

use garage_util::crdt::Crdt;
use garage_util::data::*;
use garage_util::time::*;

use garage_table::replication::TableShardedReplication;
use garage_table::*;

use crate::index_counter::*;
use crate::s3::version_table::*;

pub const UPLOADS: &str = "uploads";
pub const PARTS: &str = "parts";
pub const BYTES: &str = "bytes";

mod v09 {
	use crate::s3::object_table::ChecksumValue;
	use garage_util::crdt;
	use garage_util::data::Uuid;
	use serde::{Deserialize, Serialize};

	/// A part of a multipart upload
	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct MultipartUpload {
		/// Partition key = Upload id = UUID of the object version
		pub upload_id: Uuid,

		/// The timestamp at which the multipart upload was created
		pub timestamp: u64,
		/// Is this multipart upload deleted
		/// The MultipartUpload is marked as deleted as soon as the
		/// multipart upload is either completed or aborted
		pub deleted: crdt::Bool,
		/// List of uploaded parts, key = (part number, timestamp)
		/// In case of retries, all versions for each part are kept
		/// Everything is cleaned up only once the MultipartUpload is marked deleted
		pub parts: crdt::Map<MpuPartKey, MpuPart>,

		// Back link to bucket+key so that we can find the object this mpu
		// belongs to and check whether it is still valid
		/// Bucket in which the related object is stored
		pub bucket_id: Uuid,
		/// Key in which the related object is stored
		pub key: String,
	}

	#[derive(PartialEq, Eq, Clone, Copy, Debug, Serialize, Deserialize)]
	pub struct MpuPartKey {
		/// Number of the part
		pub part_number: u64,
		/// Timestamp of part upload
		pub timestamp: u64,
	}

	/// The version of an uploaded part
	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct MpuPart {
		/// Links to a Version in VersionTable
		pub version: Uuid,
		/// ETag of the content of this part (known only once done uploading)
		pub etag: Option<String>,
		/// Checksum requested by x-amz-checksum-algorithm
		#[serde(default)]
		pub checksum: Option<ChecksumValue>,
		/// Size of this part (known only once done uploading)
		pub size: Option<u64>,
	}

	impl garage_util::migrate::InitialFormat for MultipartUpload {
		const VERSION_MARKER: &'static [u8] = b"G09s3mpu";
	}
}

pub use v09::*;

impl Ord for MpuPartKey {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		self.part_number
			.cmp(&other.part_number)
			.then(self.timestamp.cmp(&other.timestamp))
	}
}

impl PartialOrd for MpuPartKey {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}

impl MultipartUpload {
	pub fn new(
		upload_id: Uuid,
		timestamp: u64,
		bucket_id: Uuid,
		key: String,
		deleted: bool,
	) -> Self {
		Self {
			upload_id,
			timestamp,
			deleted: crdt::Bool::new(deleted),
			parts: crdt::Map::new(),
			bucket_id,
			key,
		}
	}

	pub fn next_timestamp(&self, part_number: u64) -> u64 {
		std::cmp::max(
			now_msec(),
			1 + self
				.parts
				.items()
				.iter()
				.filter(|(x, _)| x.part_number == part_number)
				.map(|(x, _)| x.timestamp)
				.max()
				.unwrap_or(0),
		)
	}
}

impl Entry<Uuid, EmptyKey> for MultipartUpload {
	fn partition_key(&self) -> &Uuid {
		&self.upload_id
	}
	fn sort_key(&self) -> &EmptyKey {
		&EmptyKey
	}
	fn is_tombstone(&self) -> bool {
		self.deleted.get()
	}
}

impl Crdt for MultipartUpload {
	fn merge(&mut self, other: &Self) {
		self.deleted.merge(&other.deleted);

		if self.deleted.get() {
			self.parts.clear();
		} else {
			self.parts.merge(&other.parts);
		}
	}
}

impl Crdt for MpuPart {
	fn merge(&mut self, other: &Self) {
		self.etag = match (self.etag.take(), &other.etag) {
			(None, Some(_)) => other.etag.clone(),
			(Some(x), Some(y)) if x < *y => other.etag.clone(),
			(x, _) => x,
		};
		self.size = match (self.size, other.size) {
			(None, Some(_)) => other.size,
			(Some(x), Some(y)) if x < y => other.size,
			(x, _) => x,
		};
		self.checksum = match (self.checksum.take(), &other.checksum) {
			(None, Some(_)) => other.checksum.clone(),
			(Some(x), Some(y)) if x < *y => other.checksum.clone(),
			(x, _) => x,
		};
	}
}

pub struct MultipartUploadTable {
	pub version_table: Arc<Table<VersionTable, TableShardedReplication>>,
	pub mpu_counter_table: Arc<IndexCounter<MultipartUpload>>,
}

impl TableSchema for MultipartUploadTable {
	const TABLE_NAME: &'static str = "multipart_upload";

	type P = Uuid;
	type S = EmptyKey;
	type E = MultipartUpload;
	type Filter = DeletedFilter;

	fn updated(
		&self,
		tx: &mut db::Transaction,
		old: Option<&Self::E>,
		new: Option<&Self::E>,
	) -> db::TxOpResult<()> {
		// 1. Count
		let counter_res = self.mpu_counter_table.count(tx, old, new);
		if let Err(e) = db::unabort(counter_res)? {
			error!(
				"Unable to update multipart object part counter: {}. Index values will be wrong!",
				e
			);
		}

		// 2. Propagate deletions to version table
		if let (Some(old_mpu), Some(new_mpu)) = (old, new) {
			if new_mpu.deleted.get() && !old_mpu.deleted.get() {
				let deleted_versions = old_mpu.parts.items().iter().map(|(_k, p)| {
					Version::new(
						p.version,
						VersionBacklink::MultipartUpload {
							upload_id: old_mpu.upload_id,
						},
						true,
					)
				});
				for version in deleted_versions {
					let res = self.version_table.queue_insert(tx, &version);
					if let Err(e) = db::unabort(res)? {
						error!("Unable to enqueue version deletion propagation: {}. A repair will be needed.", e);
					}
				}
			}
		}

		Ok(())
	}

	fn matches_filter(entry: &Self::E, filter: &Self::Filter) -> bool {
		filter.apply(entry.is_tombstone())
	}
}

impl CountedItem for MultipartUpload {
	const COUNTER_TABLE_NAME: &'static str = "bucket_mpu_counter";

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
		let uploads = if self.deleted.get() { 0 } else { 1 };
		let mut parts = self
			.parts
			.items()
			.iter()
			.map(|(k, _)| k.part_number)
			.collect::<Vec<_>>();
		parts.dedup();
		let bytes = self
			.parts
			.items()
			.iter()
			.map(|(_, p)| p.size.unwrap_or(0))
			.sum::<u64>();
		vec![
			(UPLOADS, uploads),
			(PARTS, parts.len() as i64),
			(BYTES, bytes as i64),
		]
	}
}
