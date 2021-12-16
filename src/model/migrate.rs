use std::sync::Arc;

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
		aliases.update_in_place(old_bucket.name.clone(), true);

		let new_bucket = Bucket {
			id: blake2sum(old_bucket.name.as_bytes()),
			state: Deletable::Present(BucketParams {
				creation_date: now_msec(),
				authorized_keys: new_ak.clone(),
				website_access: Lww::new(*old_bucket_p.website.get()),
				website_config: Lww::new(None),
				aliases,
				local_aliases: LwwMap::new(),
			}),
		};
		self.garage.bucket_table.insert(&new_bucket).await?;

		let new_alias = BucketAlias {
			name: old_bucket.name.clone(),
			state: Lww::new(Deletable::Present(AliasParams {
				bucket_id: new_bucket.id,
			})),
		};
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
