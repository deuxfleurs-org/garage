use std::collections::{HashMap, HashSet};

use garage_db as db;

use garage_util::crdt::*;
use garage_util::data::*;
use garage_util::error::{Error as GarageError, OkOrMessage};
use garage_util::time::*;

use garage_table::util::*;

use crate::bucket_alias_table::*;
use crate::garage::Garage;
use crate::helper::bucket::BucketHelper;
use crate::helper::error::*;
use crate::helper::key::KeyHelper;
use crate::key_table::*;
use crate::permission::BucketKeyPerm;

/// A LockedHelper is the mandatory struct to hold when doing operations
/// that modify access keys or bucket aliases. This structure takes
/// a lock to a unit value that is in the globally-shared Garage struct.
///
/// This avoid several concurrent requests to modify the list of buckets
/// and aliases at the same time, ending up in inconsistent states.
/// This DOES NOT FIX THE FUNDAMENTAL ISSUE as CreateBucket requests handled
/// by different API nodes can still break the cluster, but it is a first
/// fix that allows consistency to be maintained if all such requests are
/// directed to a single node, which is doable for many deployments.
///
/// See issues: #649, #723
pub struct LockedHelper<'a>(
	pub(crate) &'a Garage,
	pub(crate) Option<tokio::sync::MutexGuard<'a, ()>>,
);

impl<'a> Drop for LockedHelper<'a> {
	fn drop(&mut self) {
		// make it explicit that the mutexguard lives until here
		drop(self.1.take())
	}
}

#[allow(clippy::ptr_arg)]
impl<'a> LockedHelper<'a> {
	pub fn bucket(&self) -> BucketHelper<'a> {
		BucketHelper(self.0)
	}

	pub fn key(&self) -> KeyHelper<'a> {
		KeyHelper(self.0)
	}

	// ================================================
	//      global bucket aliases
	// ================================================

	/// Sets a new alias for a bucket in global namespace.
	/// This function fails if:
	/// - alias name is not valid according to S3 spec
	/// - bucket does not exist or is deleted
	/// - alias already exists and points to another bucket
	pub async fn set_global_bucket_alias(
		&self,
		bucket_id: Uuid,
		alias_name: &String,
	) -> Result<(), Error> {
		if !is_valid_bucket_name(alias_name, self.0.config.allow_punycode) {
			return Err(Error::InvalidBucketName(alias_name.to_string()));
		}

		let mut bucket = self.bucket().get_existing_bucket(bucket_id).await?;

		let alias = self.0.bucket_alias_table.get(&EmptyKey, alias_name).await?;

		if let Some(existing_alias) = alias.as_ref() {
			if let Some(p_bucket) = existing_alias.state.get() {
				if *p_bucket != bucket_id {
					return Err(Error::BadRequest(format!(
						"Alias {} already exists and points to different bucket: {:?}",
						alias_name, p_bucket
					)));
				}
			}
		}

		// Checks ok, add alias
		let bucket_p = bucket.state.as_option_mut().unwrap();

		let alias_ts = increment_logical_clock_2(
			bucket_p.aliases.get_timestamp(alias_name),
			alias.as_ref().map(|a| a.state.timestamp()).unwrap_or(0),
		);

		// ---- timestamp-ensured causality barrier ----
		// writes are now done and all writes use timestamp alias_ts

		let alias = match alias {
			None => BucketAlias::new(alias_name.clone(), alias_ts, Some(bucket_id)),
			Some(mut a) => {
				a.state = Lww::raw(alias_ts, Some(bucket_id));
				a
			}
		};
		self.0.bucket_alias_table.insert(&alias).await?;

		bucket_p.aliases = LwwMap::raw_item(alias_name.clone(), alias_ts, true);
		self.0.bucket_table.insert(&bucket).await?;

		Ok(())
	}

	/// Unsets an alias for a bucket in global namespace.
	/// This function fails if:
	/// - bucket does not exist or is deleted
	/// - alias does not exist or maps to another bucket (-> internal error)
	/// - bucket has no other aliases (global or local)
	pub async fn unset_global_bucket_alias(
		&self,
		bucket_id: Uuid,
		alias_name: &String,
	) -> Result<(), Error> {
		let mut bucket = self.bucket().get_existing_bucket(bucket_id).await?;
		let bucket_state = bucket.state.as_option_mut().unwrap();

		let mut alias = self
			.0
			.bucket_alias_table
			.get(&EmptyKey, alias_name)
			.await?
			.filter(|a| a.state.get().map(|x| x == bucket_id).unwrap_or(false))
			.ok_or_message(format!(
				"Internal error: alias not found or does not point to bucket {:?}",
				bucket_id
			))?;

		let has_other_global_aliases = bucket_state
			.aliases
			.items()
			.iter()
			.any(|(name, _, active)| name != alias_name && *active);
		let has_other_local_aliases = bucket_state
			.local_aliases
			.items()
			.iter()
			.any(|(_, _, active)| *active);
		if !has_other_global_aliases && !has_other_local_aliases {
			return Err(Error::BadRequest(format!("Bucket {} doesn't have other aliases, please delete it instead of just unaliasing.", alias_name)));
		}

		// Checks ok, remove alias
		let alias_ts = increment_logical_clock_2(
			alias.state.timestamp(),
			bucket_state.aliases.get_timestamp(alias_name),
		);

		// ---- timestamp-ensured causality barrier ----
		// writes are now done and all writes use timestamp alias_ts

		alias.state = Lww::raw(alias_ts, None);
		self.0.bucket_alias_table.insert(&alias).await?;

		bucket_state.aliases = LwwMap::raw_item(alias_name.clone(), alias_ts, false);
		self.0.bucket_table.insert(&bucket).await?;

		Ok(())
	}

	/// Ensures a bucket does not have a certain global alias.
	/// Contrarily to unset_global_bucket_alias, this does not
	/// fail on any condition other than:
	/// - bucket cannot be found (its fine if it is in deleted state)
	/// - alias cannot be found (its fine if it points to nothing or
	///   to another bucket)
	pub async fn purge_global_bucket_alias(
		&self,
		bucket_id: Uuid,
		alias_name: &String,
	) -> Result<(), Error> {
		let mut bucket = self.bucket().get_internal_bucket(bucket_id).await?;

		let mut alias = self
			.0
			.bucket_alias_table
			.get(&EmptyKey, alias_name)
			.await?
			.ok_or_else(|| Error::NoSuchBucket(alias_name.to_string()))?;

		// Checks ok, remove alias
		let alias_ts = increment_logical_clock_2(
			alias.state.timestamp(),
			bucket
				.state
				.as_option()
				.map(|p| p.aliases.get_timestamp(alias_name))
				.unwrap_or(0),
		);

		// ---- timestamp-ensured causality barrier ----
		// writes are now done and all writes use timestamp alias_ts

		if alias.state.get() == &Some(bucket_id) {
			alias.state = Lww::raw(alias_ts, None);
			self.0.bucket_alias_table.insert(&alias).await?;
		}

		if let Some(bucket_state) = bucket.state.as_option_mut() {
			bucket_state.aliases = LwwMap::raw_item(alias_name.clone(), alias_ts, false);
			self.0.bucket_table.insert(&bucket).await?;
		}

		Ok(())
	}

	// ================================================
	//      local bucket aliases
	// ================================================

	/// Sets a new alias for a bucket in the local namespace of a key.
	/// This function fails if:
	/// - alias name is not valid according to S3 spec
	/// - bucket does not exist or is deleted
	/// - key does not exist or is deleted
	/// - alias already exists and points to another bucket
	pub async fn set_local_bucket_alias(
		&self,
		bucket_id: Uuid,
		key_id: &String,
		alias_name: &String,
	) -> Result<(), Error> {
		if !is_valid_bucket_name(alias_name, self.0.config.allow_punycode) {
			return Err(Error::InvalidBucketName(alias_name.to_string()));
		}

		let mut bucket = self.bucket().get_existing_bucket(bucket_id).await?;
		let mut key = self.key().get_existing_key(key_id).await?;

		let key_param = key.state.as_option_mut().unwrap();

		if let Some(Some(existing_alias)) = key_param.local_aliases.get(alias_name) {
			if *existing_alias != bucket_id {
				return Err(Error::BadRequest(format!("Alias {} already exists in namespace of key {} and points to different bucket: {:?}", alias_name,  key.key_id, existing_alias)));
			}
		}

		// Checks ok, add alias
		let bucket_p = bucket.state.as_option_mut().unwrap();
		let bucket_p_local_alias_key = (key.key_id.clone(), alias_name.clone());

		// Calculate the timestamp to assign to this aliasing in the two local_aliases maps
		// (the one from key to bucket, and the reverse one stored in the bucket itself)
		// so that merges on both maps in case of a concurrent operation resolve
		// to the same alias being set
		let alias_ts = increment_logical_clock_2(
			key_param.local_aliases.get_timestamp(alias_name),
			bucket_p
				.local_aliases
				.get_timestamp(&bucket_p_local_alias_key),
		);

		// ---- timestamp-ensured causality barrier ----
		// writes are now done and all writes use timestamp alias_ts

		key_param.local_aliases = LwwMap::raw_item(alias_name.clone(), alias_ts, Some(bucket_id));
		self.0.key_table.insert(&key).await?;

		bucket_p.local_aliases = LwwMap::raw_item(bucket_p_local_alias_key, alias_ts, true);
		self.0.bucket_table.insert(&bucket).await?;

		Ok(())
	}

	/// Unsets an alias for a bucket in the local namespace of a key.
	/// This function fails if:
	/// - bucket does not exist or is deleted
	/// - key does not exist or is deleted
	/// - alias does not exist or maps to another bucket (-> internal error)
	/// - bucket has no other aliases (global or local)
	pub async fn unset_local_bucket_alias(
		&self,
		bucket_id: Uuid,
		key_id: &String,
		alias_name: &String,
	) -> Result<(), Error> {
		let mut bucket = self.bucket().get_existing_bucket(bucket_id).await?;
		let mut key = self.key().get_existing_key(key_id).await?;

		let key_p = key.state.as_option().unwrap();
		let bucket_p = bucket.state.as_option_mut().unwrap();

		if key_p.local_aliases.get(alias_name).cloned().flatten() != Some(bucket_id) {
			return Err(GarageError::Message(format!(
				"Bucket {:?} does not have alias {} in namespace of key {}",
				bucket_id, alias_name, key_id
			))
			.into());
		}

		let has_other_global_aliases = bucket_p
			.aliases
			.items()
			.iter()
			.any(|(_, _, active)| *active);
		let has_other_local_aliases = bucket_p
			.local_aliases
			.items()
			.iter()
			.any(|((k, n), _, active)| (*k != key.key_id || n != alias_name) && *active);

		if !has_other_global_aliases && !has_other_local_aliases {
			return Err(Error::BadRequest(format!("Bucket {} doesn't have other aliases, please delete it instead of just unaliasing.", alias_name)));
		}

		// Checks ok, remove alias
		let bucket_p_local_alias_key = (key.key_id.clone(), alias_name.clone());

		let alias_ts = increment_logical_clock_2(
			key_p.local_aliases.get_timestamp(alias_name),
			bucket_p
				.local_aliases
				.get_timestamp(&bucket_p_local_alias_key),
		);

		// ---- timestamp-ensured causality barrier ----
		// writes are now done and all writes use timestamp alias_ts

		key.state.as_option_mut().unwrap().local_aliases =
			LwwMap::raw_item(alias_name.clone(), alias_ts, None);
		self.0.key_table.insert(&key).await?;

		bucket_p.local_aliases = LwwMap::raw_item(bucket_p_local_alias_key, alias_ts, false);
		self.0.bucket_table.insert(&bucket).await?;

		Ok(())
	}

	/// Ensures a bucket does not have a certain local alias.
	/// Contrarily to unset_local_bucket_alias, this does not
	/// fail on any condition other than:
	/// - bucket cannot be found (its fine if it is in deleted state)
	/// - key cannot be found (its fine if alias in key points to nothing
	///   or to another bucket)
	pub async fn purge_local_bucket_alias(
		&self,
		bucket_id: Uuid,
		key_id: &String,
		alias_name: &String,
	) -> Result<(), Error> {
		let mut bucket = self.bucket().get_internal_bucket(bucket_id).await?;
		let mut key = self.key().get_internal_key(key_id).await?;

		let bucket_p_local_alias_key = (key.key_id.clone(), alias_name.clone());

		let alias_ts = increment_logical_clock_2(
			key.state
				.as_option()
				.map(|p| p.local_aliases.get_timestamp(alias_name))
				.unwrap_or(0),
			bucket
				.state
				.as_option()
				.map(|p| p.local_aliases.get_timestamp(&bucket_p_local_alias_key))
				.unwrap_or(0),
		);

		// ---- timestamp-ensured causality barrier ----
		// writes are now done and all writes use timestamp alias_ts

		if let Some(kp) = key.state.as_option_mut() {
			kp.local_aliases = LwwMap::raw_item(alias_name.clone(), alias_ts, None);
			self.0.key_table.insert(&key).await?;
		}

		if let Some(bp) = bucket.state.as_option_mut() {
			bp.local_aliases = LwwMap::raw_item(bucket_p_local_alias_key, alias_ts, false);
			self.0.bucket_table.insert(&bucket).await?;
		}

		Ok(())
	}

	// ================================================
	//      permissions
	// ================================================

	/// Sets permissions for a key on a bucket.
	/// This function fails if:
	/// - bucket or key cannot be found at all (its ok if they are in deleted state)
	/// - bucket or key is in deleted state and we are trying to set
	///   permissions other than "deny all"
	pub async fn set_bucket_key_permissions(
		&self,
		bucket_id: Uuid,
		key_id: &String,
		mut perm: BucketKeyPerm,
	) -> Result<(), Error> {
		let mut bucket = self.bucket().get_internal_bucket(bucket_id).await?;
		let mut key = self.key().get_internal_key(key_id).await?;

		if let Some(bstate) = bucket.state.as_option() {
			if let Some(kp) = bstate.authorized_keys.get(key_id) {
				perm.timestamp = increment_logical_clock_2(perm.timestamp, kp.timestamp);
			}
		} else if perm.is_any() {
			return Err(Error::BadRequest(
				"Trying to give permissions on a deleted bucket".into(),
			));
		}

		if let Some(kstate) = key.state.as_option() {
			if let Some(bp) = kstate.authorized_buckets.get(&bucket_id) {
				perm.timestamp = increment_logical_clock_2(perm.timestamp, bp.timestamp);
			}
		} else if perm.is_any() {
			return Err(Error::BadRequest(
				"Trying to give permissions to a deleted key".into(),
			));
		}

		// ---- timestamp-ensured causality barrier ----

		if let Some(bstate) = bucket.state.as_option_mut() {
			bstate.authorized_keys = Map::put_mutator(key_id.clone(), perm);
			self.0.bucket_table.insert(&bucket).await?;
		}

		if let Some(kstate) = key.state.as_option_mut() {
			kstate.authorized_buckets = Map::put_mutator(bucket_id, perm);
			self.0.key_table.insert(&key).await?;
		}

		Ok(())
	}

	// ================================================
	//      keys
	// ================================================

	/// Deletes an API access key
	pub async fn delete_key(&self, key: &mut Key) -> Result<(), Error> {
		let state = key.state.as_option_mut().unwrap();

		// --- done checking, now commit ---

		// 1. Delete local aliases
		for (alias, _, to) in state.local_aliases.items().iter() {
			if let Some(bucket_id) = to {
				self.purge_local_bucket_alias(*bucket_id, &key.key_id, alias)
					.await?;
			}
		}

		// 2. Remove permissions on all authorized buckets
		for (ab_id, _auth) in state.authorized_buckets.items().iter() {
			self.set_bucket_key_permissions(*ab_id, &key.key_id, BucketKeyPerm::NO_PERMISSIONS)
				.await?;
		}

		// 3. Actually delete key
		key.state = Deletable::delete();
		self.0.key_table.insert(key).await?;

		Ok(())
	}

	// ================================================
	//      repair procedure
	// ================================================

	pub async fn repair_aliases(&self) -> Result<(), GarageError> {
		self.0.db.transaction(|tx| {
			info!("--- begin repair_aliases transaction ----");

			// 1. List all non-deleted buckets, so that we can fix bad aliases
			let mut all_buckets: HashSet<Uuid> = HashSet::new();

			for item in tx.range::<&[u8], _>(&self.0.bucket_table.data.store, ..)? {
				let bucket = self
					.0
					.bucket_table
					.data
					.decode_entry(&(item?.1))
					.map_err(db::TxError::Abort)?;
				if !bucket.is_deleted() {
					all_buckets.insert(bucket.id);
				}
			}

			info!("number of buckets: {}", all_buckets.len());

			// 2. List all aliases declared in bucket_alias_table and key_table
			//    Take note of aliases that point to non-existing buckets
			let mut global_aliases: HashMap<String, Uuid> = HashMap::new();

			{
				let mut delete_global = vec![];
				for item in tx.range::<&[u8], _>(&self.0.bucket_alias_table.data.store, ..)? {
					let mut alias = self
						.0
						.bucket_alias_table
						.data
						.decode_entry(&(item?.1))
						.map_err(db::TxError::Abort)?;
					if let Some(id) = alias.state.get() {
						if all_buckets.contains(id) {
							// keep aliases
							global_aliases.insert(alias.name().to_string(), *id);
						} else {
							// delete alias
							warn!(
								"global alias: remove {} -> {:?} (bucket is deleted)",
								alias.name(),
								id
							);
							alias.state.update(None);
							delete_global.push(alias);
						}
					}
				}

				info!("number of global aliases: {}", global_aliases.len());

				info!("global alias table: {} entries fixed", delete_global.len());
				for ga in delete_global {
					debug!("Enqueue update to global alias table: {:?}", ga);
					self.0.bucket_alias_table.queue_insert(tx, &ga)?;
				}
			}

			let mut local_aliases: HashMap<(String, String), Uuid> = HashMap::new();

			{
				let mut delete_local = vec![];

				for item in tx.range::<&[u8], _>(&self.0.key_table.data.store, ..)? {
					let mut key = self
						.0
						.key_table
						.data
						.decode_entry(&(item?.1))
						.map_err(db::TxError::Abort)?;
					let Some(p) = key.state.as_option_mut() else {
						continue;
					};
					let mut has_changes = false;
					for (name, _, to) in p.local_aliases.items().to_vec() {
						if let Some(id) = to {
							if all_buckets.contains(&id) {
								local_aliases.insert((key.key_id.clone(), name), id);
							} else {
								warn!(
									"local alias: remove ({}, {}) -> {:?} (bucket is deleted)",
									key.key_id, name, id
								);
								p.local_aliases.update_in_place(name, None);
								has_changes = true;
							}
						}
					}
					if has_changes {
						delete_local.push(key);
					}
				}

				info!("number of local aliases: {}", local_aliases.len());

				info!("key table: {} entries fixed", delete_local.len());
				for la in delete_local {
					debug!("Enqueue update to key table: {:?}", la);
					self.0.key_table.queue_insert(tx, &la)?;
				}
			}

			// 4. Reverse the alias maps to determine the aliases per-bucket
			let mut bucket_global: HashMap<Uuid, Vec<String>> = HashMap::new();
			let mut bucket_local: HashMap<Uuid, Vec<(String, String)>> = HashMap::new();

			for (name, bucket) in global_aliases {
				bucket_global.entry(bucket).or_default().push(name);
			}
			for ((key, name), bucket) in local_aliases {
				bucket_local.entry(bucket).or_default().push((key, name));
			}

			// 5. Fix the bucket table to ensure consistency
			let mut bucket_updates = vec![];

			for item in tx.range::<&[u8], _>(&self.0.bucket_table.data.store, ..)? {
				let bucket = self
					.0
					.bucket_table
					.data
					.decode_entry(&(item?.1))
					.map_err(db::TxError::Abort)?;
				let mut bucket2 = bucket.clone();
				let Some(param) = bucket2.state.as_option_mut() else {
					continue;
				};

				// fix global aliases
				{
					let ga = bucket_global.remove(&bucket.id).unwrap_or_default();
					for (name, _, active) in param.aliases.items().to_vec() {
						if active && !ga.contains(&name) {
							warn!("bucket {:?}: remove global alias {}", bucket.id, name);
							param.aliases.update_in_place(name, false);
						}
					}
					for name in ga {
						if param.aliases.get(&name).copied() != Some(true) {
							warn!("bucket {:?}: add global alias {}", bucket.id, name);
							param.aliases.update_in_place(name, true);
						}
					}
				}

				// fix local aliases
				{
					let la = bucket_local.remove(&bucket.id).unwrap_or_default();
					for (pair, _, active) in param.local_aliases.items().to_vec() {
						if active && !la.contains(&pair) {
							warn!("bucket {:?}: remove local alias {:?}", bucket.id, pair);
							param.local_aliases.update_in_place(pair, false);
						}
					}
					for pair in la {
						if param.local_aliases.get(&pair).copied() != Some(true) {
							warn!("bucket {:?}: add local alias {:?}", bucket.id, pair);
							param.local_aliases.update_in_place(pair, true);
						}
					}
				}

				if bucket2 != bucket {
					bucket_updates.push(bucket2);
				}
			}

			info!("bucket table: {} entries fixed", bucket_updates.len());
			for b in bucket_updates {
				debug!("Enqueue update to bucket table: {:?}", b);
				self.0.bucket_table.queue_insert(tx, &b)?;
			}

			info!("--- end repair_aliases transaction ----");

			Ok(())
		})?;

		info!("repair_aliases is done");

		Ok(())
	}
}
