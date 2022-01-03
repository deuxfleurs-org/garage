use garage_table::util::EmptyKey;
use garage_util::data::*;

use crate::bucket_table::Bucket;
use crate::garage::Garage;
use crate::helper::error::*;

pub struct BucketHelper<'a>(pub(crate) &'a Garage);

impl<'a> BucketHelper<'a> {
	#[allow(clippy::ptr_arg)]
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
			.map(|by| Uuid::try_from(&by))
			.flatten();
		if let Some(bucket_id) = hexbucket {
			Ok(self
				.0
				.bucket_table
				.get(&bucket_id, &EmptyKey)
				.await?
				.filter(|x| !x.state.is_deleted())
				.map(|_| bucket_id))
		} else {
			Ok(self
				.0
				.bucket_alias_table
				.get(&EmptyKey, bucket_name)
				.await?
				.map(|x| x.state.get().as_option().map(|x| x.bucket_id))
				.flatten())
		}
	}

	pub async fn get_existing_bucket(&self, bucket_id: Uuid) -> Result<Bucket, Error> {
		self.0
			.bucket_table
			.get(&bucket_id, &EmptyKey)
			.await?
			.filter(|b| !b.is_deleted())
			.ok_or_bad_request(format!("Bucket {:?} does not exist", bucket_id))
	}
}
