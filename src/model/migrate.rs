use std::sync::Arc;

use serde_bytes::ByteBuf;

use garage_table::util::EmptyKey;
use garage_util::crdt::*;
use garage_util::data::*;
use garage_util::error::*;
use garage_util::time::*;

use garage_model_050::bucket_table as old_bucket;

use crate::bucket_alias_table::*;
use crate::bucket_table::*;
use crate::garage::Garage;
use crate::permission::*;

pub struct Migrate {
	pub garage: Arc<Garage>,
}

impl Migrate {
	pub async fn migrate_buckets050(&self) -> Result<(), Error> {
		let tree = self.garage.db.open_tree("bucket:table")?;

		for res in tree.iter() {
			let (_k, v) = res?;
			let bucket = rmp_serde::decode::from_read_ref::<_, old_bucket::Bucket>(&v[..])?;

			if let old_bucket::BucketState::Present(p) = bucket.state.get() {
				self.migrate_buckets050_do_bucket(&bucket, p).await?;
			}
		}

		Ok(())
	}

	pub async fn migrate_buckets050_do_bucket(
		&self,
		old_bucket: &old_bucket::Bucket,
		old_bucket_p: &old_bucket::BucketParams,
	) -> Result<(), Error> {
		let bucket_id = blake2sum(old_bucket.name.as_bytes());

		let new_name = if is_valid_bucket_name(&old_bucket.name) {
			old_bucket.name.clone()
		} else {
			// if old bucket name was not valid, replace it by
			// a hex-encoded name derived from its identifier
			hex::encode(&bucket_id.as_slice()[..16])
		};

		let mut new_ak = Map::new();
		for (k, ts, perm) in old_bucket_p.authorized_keys.items().iter() {
			new_ak.put(
				k.to_string(),
				BucketKeyPerm {
					timestamp: *ts,
					allow_read: perm.allow_read,
					allow_write: perm.allow_write,
					allow_owner: false,
				},
			);
		}

		let mut aliases = LwwMap::new();
		aliases.update_in_place(new_name.clone(), true);

		let website = if *old_bucket_p.website.get() {
			Some(ByteBuf::from(DEFAULT_WEBSITE_CONFIGURATION.to_vec()))
		} else {
			None
		};

		let new_bucket = Bucket {
			id: bucket_id,
			state: Deletable::Present(BucketParams {
				creation_date: now_msec(),
				authorized_keys: new_ak.clone(),
				website_config: Lww::new(website),
				aliases,
				local_aliases: LwwMap::new(),
			}),
		};
		self.garage.bucket_table.insert(&new_bucket).await?;

		let new_alias = BucketAlias::new(new_name.clone(), new_bucket.id).unwrap();
		self.garage.bucket_alias_table.insert(&new_alias).await?;

		for (k, perm) in new_ak.items().iter() {
			let mut key = self
				.garage
				.key_table
				.get(&EmptyKey, k)
				.await?
				.ok_or_message(format!("Missing key: {}", k))?;
			if let Some(p) = key.state.as_option_mut() {
				p.authorized_buckets.put(new_bucket.id, *perm);
			}
			self.garage.key_table.insert(&key).await?;
		}

		Ok(())
	}
}
