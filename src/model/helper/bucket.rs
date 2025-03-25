use std::time::Duration;

use garage_util::data::*;
use garage_util::error::{Error as GarageError, OkOrMessage};
use garage_util::time::*;

use garage_table::util::*;

use crate::bucket_table::*;
use crate::garage::Garage;
use crate::helper::error::*;
use crate::key_table::*;
use crate::s3::object_table::*;

pub struct BucketHelper<'a>(pub(crate) &'a Garage);

#[allow(clippy::ptr_arg)]
impl<'a> BucketHelper<'a> {
	// ================
	//      Local functions to find buckets FAST.
	//      This is only for the fast path in API requests.
	//      They do not conserve the read-after-write guarantee.
	// ================

	/// Return bucket ID corresponding to global bucket name.
	///
	/// The name can be of two forms:
	/// 1. A global bucket alias
	/// 2. The full ID of a bucket encoded in hex
	///
	/// This will not do any network interaction to check the alias table,
	/// it will only check the local copy of the table.
	/// As a consequence, it does not conserve read-after-write guarantees.
	pub fn resolve_global_bucket_fast(
		&self,
		bucket_name: &String,
	) -> Result<Option<Bucket>, GarageError> {
		// Bucket names in Garage are aliases, true bucket identifiers
		// are 32-byte UUIDs. This function resolves bucket names into
		// their full identifier by looking up in the bucket_alias_table.
		// This function also allows buckets to be identified by their
		// full UUID (hex-encoded). Here, if the name to be resolved is a
		// hex string of the correct length, it is directly parsed as a bucket
		// identifier which is returned. There is no risk of this conflicting
		// with an actual bucket name: bucket names are max 63 chars long by
		// the AWS spec, and hex-encoded UUIDs are 64 chars long.
		let hexbucket = hex::decode(bucket_name.as_str())
			.ok()
			.and_then(|by| Uuid::try_from(&by));
		let bucket_id = match hexbucket {
			Some(id) => id,
			None => {
				let alias = self
					.0
					.bucket_alias_table
					.get_local(&EmptyKey, bucket_name)?
					.and_then(|x| *x.state.get());
				match alias {
					Some(id) => id,
					None => return Ok(None),
				}
			}
		};
		Ok(self
			.0
			.bucket_table
			.get_local(&EmptyKey, &bucket_id)?
			.filter(|x| !x.state.is_deleted()))
	}

	/// Return bucket ID corresponding to a bucket name from the perspective of
	/// a given access key.
	///
	/// The name can be of three forms:
	/// 1. A global bucket alias
	/// 2. A local bucket alias
	/// 3. The full ID of a bucket encoded in hex
	///
	/// This will not do any network interaction to check the alias table,
	/// it will only check the local copy of the table.
	/// As a consequence, it does not conserve read-after-write guarantees.
	///
	/// This function transforms non-existing buckets in a NoSuchBucket error.
	#[allow(clippy::ptr_arg)]
	pub fn resolve_bucket_fast(
		&self,
		bucket_name: &String,
		api_key: &Key,
	) -> Result<Bucket, Error> {
		let api_key_params = api_key
			.state
			.as_option()
			.ok_or_message("Key should not be deleted at this point")?;

		let bucket_opt =
			if let Some(Some(bucket_id)) = api_key_params.local_aliases.get(bucket_name) {
				self.0
					.bucket_table
					.get_local(&EmptyKey, &bucket_id)?
					.filter(|x| !x.state.is_deleted())
			} else {
				self.resolve_global_bucket_fast(bucket_name)?
			};
		bucket_opt.ok_or_else(|| Error::NoSuchBucket(bucket_name.to_string()))
	}

	// ================
	//      Global functions that do quorum reads/writes,
	//      for admin operations.
	// ================

	/// See resolve_global_bucket_fast,
	/// but this one does a quorum read to ensure consistency
	pub async fn resolve_global_bucket(
		&self,
		bucket_name: &String,
	) -> Result<Option<Bucket>, GarageError> {
		let hexbucket = hex::decode(bucket_name.as_str())
			.ok()
			.and_then(|by| Uuid::try_from(&by));
		let bucket_id = match hexbucket {
			Some(id) => id,
			None => {
				let alias = self
					.0
					.bucket_alias_table
					.get(&EmptyKey, bucket_name)
					.await?
					.and_then(|x| *x.state.get());
				match alias {
					Some(id) => id,
					None => return Ok(None),
				}
			}
		};
		Ok(self
			.0
			.bucket_table
			.get(&EmptyKey, &bucket_id)
			.await?
			.filter(|x| !x.state.is_deleted()))
	}

	/// See resolve_bucket_fast, but this one does a quorum read to ensure consistency.
	/// Also, this function does not return a HelperError::NoSuchBucket if bucket is absent.
	#[allow(clippy::ptr_arg)]
	pub async fn resolve_bucket(
		&self,
		bucket_name: &String,
		key_id: &String,
	) -> Result<Option<Bucket>, GarageError> {
		let local_alias = self
			.0
			.key_table
			.get(&EmptyKey, &key_id)
			.await?
			.and_then(|k| k.state.into_option())
			.ok_or_else(|| GarageError::Message(format!("access key {} has been deleted", key_id)))?
			.local_aliases
			.get(bucket_name)
			.copied()
			.flatten();

		if let Some(bucket_id) = local_alias {
			Ok(self
				.0
				.bucket_table
				.get(&EmptyKey, &bucket_id)
				.await?
				.filter(|x| !x.state.is_deleted()))
		} else {
			Ok(self.resolve_global_bucket(bucket_name).await?)
		}
	}

	/// Returns a Bucket if it is present in bucket table,
	/// even if it is in deleted state. Querying a non-existing
	/// bucket ID returns an internal error.
	pub async fn get_internal_bucket(&self, bucket_id: Uuid) -> Result<Bucket, Error> {
		Ok(self
			.0
			.bucket_table
			.get(&EmptyKey, &bucket_id)
			.await?
			.ok_or_message(format!("Bucket {:?} does not exist", bucket_id))?)
	}

	/// Returns a Bucket if it is present in bucket table,
	/// only if it is in non-deleted state.
	/// Querying a non-existing bucket ID or a deleted bucket
	/// returns a bad request error.
	pub async fn get_existing_bucket(&self, bucket_id: Uuid) -> Result<Bucket, Error> {
		self.0
			.bucket_table
			.get(&EmptyKey, &bucket_id)
			.await?
			.filter(|b| !b.is_deleted())
			.ok_or_else(|| Error::NoSuchBucket(hex::encode(bucket_id)))
	}

	// ----

	pub async fn is_bucket_empty(&self, bucket_id: Uuid) -> Result<bool, Error> {
		let objects = self
			.0
			.object_table
			.get_range(
				&bucket_id,
				None,
				Some(ObjectFilter::IsData),
				10,
				EnumerationOrder::Forward,
			)
			.await?;
		if !objects.is_empty() {
			return Ok(false);
		}

		#[cfg(feature = "k2v")]
		{
			let node_id_vec = self
				.0
				.system
				.cluster_layout()
				.all_nongateway_nodes()
				.to_vec();
			let k2vindexes = self
				.0
				.k2v
				.counter_table
				.table
				.get_range(
					&bucket_id,
					None,
					Some((DeletedFilter::NotDeleted, node_id_vec)),
					10,
					EnumerationOrder::Forward,
				)
				.await?;
			if !k2vindexes.is_empty() {
				return Ok(false);
			}
		}

		Ok(true)
	}

	// ----

	/// Deletes all incomplete multipart uploads that are older than a certain time.
	/// Returns the number of uploads aborted.
	/// This will also include non-multipart uploads, which may be lingering
	/// after a node crash
	pub async fn cleanup_incomplete_uploads(
		&self,
		bucket_id: &Uuid,
		older_than: Duration,
	) -> Result<usize, Error> {
		let older_than = now_msec() - older_than.as_millis() as u64;

		let mut ret = 0usize;
		let mut start = None;

		loop {
			let objects = self
				.0
				.object_table
				.get_range(
					bucket_id,
					start,
					Some(ObjectFilter::IsUploading {
						check_multipart: None,
					}),
					1000,
					EnumerationOrder::Forward,
				)
				.await?;

			let abortions = objects
				.iter()
				.filter_map(|object| {
					let aborted_versions = object
						.versions()
						.iter()
						.filter(|v| v.is_uploading(None) && v.timestamp < older_than)
						.map(|v| ObjectVersion {
							state: ObjectVersionState::Aborted,
							uuid: v.uuid,
							timestamp: v.timestamp,
						})
						.collect::<Vec<_>>();
					if !aborted_versions.is_empty() {
						Some(Object::new(
							object.bucket_id,
							object.key.clone(),
							aborted_versions,
						))
					} else {
						None
					}
				})
				.collect::<Vec<_>>();

			ret += abortions.len();
			self.0.object_table.insert_many(abortions).await?;

			if objects.len() < 1000 {
				break;
			} else {
				start = Some(objects.last().unwrap().key.clone());
			}
		}

		Ok(ret)
	}
}
