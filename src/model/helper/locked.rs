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
		if !is_valid_bucket_name(alias_name) {
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
			None => BucketAlias::new(alias_name.clone(), alias_ts, Some(bucket_id))
				.ok_or_else(|| Error::InvalidBucketName(alias_name.clone()))?,
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
		let alias_ts = match bucket.state.as_option() {
			Some(bucket_state) => increment_logical_clock_2(
				alias.state.timestamp(),
				bucket_state.aliases.get_timestamp(alias_name),
			),
			None => increment_logical_clock(alias.state.timestamp()),
		};

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
		let key_helper = KeyHelper(self.0);

		if !is_valid_bucket_name(alias_name) {
			return Err(Error::InvalidBucketName(alias_name.to_string()));
		}

		let mut bucket = self.bucket().get_existing_bucket(bucket_id).await?;
		let mut key = key_helper.get_existing_key(key_id).await?;

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
		let key_helper = KeyHelper(self.0);

		let mut bucket = self.bucket().get_existing_bucket(bucket_id).await?;
		let mut key = key_helper.get_existing_key(key_id).await?;

		let bucket_p = bucket.state.as_option_mut().unwrap();

		if key
			.state
			.as_option()
			.unwrap()
			.local_aliases
			.get(alias_name)
			.cloned()
			.flatten()
			!= Some(bucket_id)
		{
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
			.any(|((k, n), _, active)| *k == key.key_id && n == alias_name && *active);
		if !has_other_global_aliases && !has_other_local_aliases {
			return Err(Error::BadRequest(format!("Bucket {} doesn't have other aliases, please delete it instead of just unaliasing.", alias_name)));
		}

		// Checks ok, remove alias
		let key_param = key.state.as_option_mut().unwrap();
		let bucket_p_local_alias_key = (key.key_id.clone(), alias_name.clone());

		let alias_ts = increment_logical_clock_2(
			key_param.local_aliases.get_timestamp(alias_name),
			bucket_p
				.local_aliases
				.get_timestamp(&bucket_p_local_alias_key),
		);

		// ---- timestamp-ensured causality barrier ----
		// writes are now done and all writes use timestamp alias_ts

		key_param.local_aliases = LwwMap::raw_item(alias_name.clone(), alias_ts, None);
		self.0.key_table.insert(&key).await?;

		bucket_p.local_aliases = LwwMap::raw_item(bucket_p_local_alias_key, alias_ts, false);
		self.0.bucket_table.insert(&bucket).await?;

		Ok(())
	}

	/// Sets permissions for a key on a bucket.
	/// This function fails if:
	/// - bucket or key cannot be found at all (its ok if they are in deleted state)
	/// - bucket or key is in deleted state and we are trying to set permissions other than "deny
	/// all"
	pub async fn set_bucket_key_permissions(
		&self,
		bucket_id: Uuid,
		key_id: &String,
		mut perm: BucketKeyPerm,
	) -> Result<(), Error> {
		let key_helper = KeyHelper(self.0);

		let mut bucket = self.bucket().get_internal_bucket(bucket_id).await?;
		let mut key = key_helper.get_internal_key(key_id).await?;

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

	// ----

	/// Deletes an API access key
	pub async fn delete_key(&self, key: &mut Key) -> Result<(), Error> {
		let state = key.state.as_option_mut().unwrap();

		// --- done checking, now commit ---
		// (the step at unset_local_bucket_alias will fail if a bucket
		// does not have another alias, the deletion will be
		// interrupted in the middle if that happens)

		// 1. Delete local aliases
		for (alias, _, to) in state.local_aliases.items().iter() {
			if let Some(bucket_id) = to {
				self.unset_local_bucket_alias(*bucket_id, &key.key_id, alias)
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
}
