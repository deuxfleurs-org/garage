use garage_util::data::*;
use garage_util::error::*;

use garage_table::util::EmptyKey;

use crate::bucket_table::Bucket;
use crate::garage::Garage;

pub struct BucketHelper<'a>(pub(crate) &'a Garage);

#[allow(clippy::ptr_arg)]
impl<'a> BucketHelper<'a> {
	pub async fn resolve_global_bucket_name(
		&self,
		bucket_name: &String,
	) -> Result<Option<Uuid>, Error> {
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

	#[allow(clippy::ptr_arg)]
	pub async fn get_existing_bucket(&self, bucket_id: Uuid) -> Result<Bucket, Error> {
		self.0
			.bucket_table
			.get(&bucket_id, &EmptyKey)
			.await?
			.filter(|b| !b.is_deleted())
			.map(Ok)
			.unwrap_or_else(|| {
				Err(Error::BadRpc(format!(
					"Bucket {:?} does not exist",
					bucket_id
				)))
			})
	}
}
