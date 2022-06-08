use std::sync::Arc;

use garage_util::crdt::*;
use garage_util::data::*;
use garage_util::error::Error as GarageError;
use garage_util::time::*;

use garage_model_050::bucket_table as old_bucket;

use crate::bucket_alias_table::*;
use crate::bucket_table::*;
use crate::garage::Garage;
use crate::helper::error::*;
use crate::permission::*;

pub struct Migrate {
	pub garage: Arc<Garage>,
}

impl Migrate {
	pub async fn migrate_buckets050(&self) -> Result<(), Error> {
		let tree = self
			.garage
			.db
			.open_tree("bucket:table")
			.map_err(GarageError::from)?;

		let mut old_buckets = vec![];
		for res in tree.iter().map_err(GarageError::from)? {
			let (_k, v) = res.map_err(GarageError::from)?;
			let bucket = rmp_serde::decode::from_read_ref::<_, old_bucket::Bucket>(&v[..])
				.map_err(GarageError::from)?;
			old_buckets.push(bucket);
		}

		for bucket in old_buckets {
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

		let website = if *old_bucket_p.website.get() {
			Some(WebsiteConfig {
				index_document: "index.html".into(),
				error_document: None,
			})
		} else {
			None
		};

		self.garage
			.bucket_table
			.insert(&Bucket {
				id: bucket_id,
				state: Deletable::Present(BucketParams {
					creation_date: now_msec(),
					authorized_keys: Map::new(),
					aliases: LwwMap::new(),
					local_aliases: LwwMap::new(),
					website_config: Lww::new(website),
					cors_config: Lww::new(None),
				}),
			})
			.await?;

		self.garage
			.bucket_helper()
			.set_global_bucket_alias(bucket_id, &new_name)
			.await?;

		for (k, ts, perm) in old_bucket_p.authorized_keys.items().iter() {
			self.garage
				.bucket_helper()
				.set_bucket_key_permissions(
					bucket_id,
					k,
					BucketKeyPerm {
						timestamp: *ts,
						allow_read: perm.allow_read,
						allow_write: perm.allow_write,
						allow_owner: false,
					},
				)
				.await?;
		}

		Ok(())
	}
}
