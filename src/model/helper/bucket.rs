use std::time::Duration;

use garage_util::data::*;
use garage_util::error::OkOrMessage;
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
	pub async fn resolve_global_bucket_name(
		&self,
		bucket_name: &String,
	) -> Result<Option<Uuid>, Error> {
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
		if let Some(bucket_id) = hexbucket {
			Ok(self
				.0
				.bucket_table
				.get(&EmptyKey, &bucket_id)
				.await?
				.filter(|x| !x.state.is_deleted())
				.map(|_| bucket_id))
		} else {
			Ok(self
				.0
				.bucket_alias_table
				.get(&EmptyKey, bucket_name)
				.await?
				.and_then(|x| *x.state.get()))
		}
	}

	#[allow(clippy::ptr_arg)]
	pub async fn resolve_bucket(&self, bucket_name: &String, api_key: &Key) -> Result<Uuid, Error> {
		let api_key_params = api_key
			.state
			.as_option()
			.ok_or_message("Key should not be deleted at this point")?;

		if let Some(Some(bucket_id)) = api_key_params.local_aliases.get(bucket_name) {
			Ok(*bucket_id)
		} else {
			Ok(self
				.resolve_global_bucket_name(bucket_name)
				.await?
				.ok_or_else(|| Error::NoSuchBucket(bucket_name.to_string()))?)
		}
	}

	/// Find a bucket by its global alias or a prefix of its uuid
	pub async fn admin_get_existing_matching_bucket(
		&self,
		pattern: &String,
	) -> Result<Uuid, Error> {
		if let Some(uuid) = self.resolve_global_bucket_name(pattern).await? {
			return Ok(uuid);
		} else if pattern.len() >= 2 {
			let hexdec = pattern
				.get(..pattern.len() & !1)
				.and_then(|x| hex::decode(x).ok());
			if let Some(hex) = hexdec {
				let mut start = [0u8; 32];
				start
					.as_mut_slice()
					.get_mut(..hex.len())
					.ok_or_bad_request("invalid length")?
					.copy_from_slice(&hex);
				let mut candidates = self
					.0
					.bucket_table
					.get_range(
						&EmptyKey,
						Some(start.into()),
						Some(DeletedFilter::NotDeleted),
						10,
						EnumerationOrder::Forward,
					)
					.await?
					.into_iter()
					.collect::<Vec<_>>();
				candidates.retain(|x| hex::encode(x.id).starts_with(pattern));
				if candidates.len() == 1 {
					return Ok(candidates.into_iter().next().unwrap().id);
				}
			}
		}
		Err(Error::BadRequest(format!(
			"Bucket not found / several matching buckets: {}",
			pattern
		)))
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
