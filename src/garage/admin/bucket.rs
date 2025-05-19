use std::collections::HashMap;
use std::fmt::Write;

use garage_util::crdt::*;
use garage_util::time::*;

use garage_table::*;

use garage_model::bucket_alias_table::*;
use garage_model::bucket_table::*;
use garage_model::helper::error::{Error, OkOrBadRequest};
use garage_model::permission::*;

use crate::cli::*;

use super::*;

impl AdminRpcHandler {
	pub(super) async fn handle_bucket_cmd(&self, cmd: &BucketOperation) -> Result<AdminRpc, Error> {
		match cmd {
			BucketOperation::List => self.handle_list_buckets().await,
			BucketOperation::Info(query) => self.handle_bucket_info(query).await,
			BucketOperation::Create(query) => self.handle_create_bucket(&query.name).await,
			BucketOperation::Delete(query) => self.handle_delete_bucket(query).await,
			BucketOperation::Alias(query) => self.handle_alias_bucket(query).await,
			BucketOperation::Unalias(query) => self.handle_unalias_bucket(query).await,
			BucketOperation::Allow(query) => self.handle_bucket_allow(query).await,
			BucketOperation::Deny(query) => self.handle_bucket_deny(query).await,
			BucketOperation::Website(query) => self.handle_bucket_website(query).await,
			BucketOperation::SetQuotas(query) => self.handle_bucket_set_quotas(query).await,
			BucketOperation::CleanupIncompleteUploads(query) => {
				self.handle_bucket_cleanup_incomplete_uploads(query).await
			}
		}
	}

	async fn handle_list_buckets(&self) -> Result<AdminRpc, Error> {
		let buckets = self
			.garage
			.bucket_table
			.get_range(
				&EmptyKey,
				None,
				Some(DeletedFilter::NotDeleted),
				10000,
				EnumerationOrder::Forward,
			)
			.await?;

		Ok(AdminRpc::BucketList(buckets))
	}

	async fn handle_bucket_info(&self, query: &BucketOpt) -> Result<AdminRpc, Error> {
		let bucket_id = self
			.garage
			.bucket_helper()
			.admin_get_existing_matching_bucket(&query.name)
			.await?;

		let bucket = self
			.garage
			.bucket_helper()
			.get_existing_bucket(bucket_id)
			.await?;

		let counters = self
			.garage
			.object_counter_table
			.table
			.get(&bucket_id, &EmptyKey)
			.await?
			.map(|x| x.filtered_values(&self.garage.system.cluster_layout()))
			.unwrap_or_default();

		let mpu_counters = self
			.garage
			.mpu_counter_table
			.table
			.get(&bucket_id, &EmptyKey)
			.await?
			.map(|x| x.filtered_values(&self.garage.system.cluster_layout()))
			.unwrap_or_default();

		let mut relevant_keys = HashMap::new();
		for (k, _) in bucket
			.state
			.as_option()
			.unwrap()
			.authorized_keys
			.items()
			.iter()
		{
			if let Some(key) = self
				.garage
				.key_table
				.get(&EmptyKey, k)
				.await?
				.filter(|k| !k.is_deleted())
			{
				relevant_keys.insert(k.clone(), key);
			}
		}
		for ((k, _), _, _) in bucket
			.state
			.as_option()
			.unwrap()
			.local_aliases
			.items()
			.iter()
		{
			if relevant_keys.contains_key(k) {
				continue;
			}
			if let Some(key) = self.garage.key_table.get(&EmptyKey, k).await? {
				relevant_keys.insert(k.clone(), key);
			}
		}

		Ok(AdminRpc::BucketInfo {
			bucket,
			relevant_keys,
			counters,
			mpu_counters,
		})
	}

	#[allow(clippy::ptr_arg)]
	async fn handle_create_bucket(&self, name: &String) -> Result<AdminRpc, Error> {
		if !is_valid_bucket_name(name, self.garage.config.allow_punycode) {
			return Err(Error::BadRequest(format!(
				"{}: {}",
				name, INVALID_BUCKET_NAME_MESSAGE
			)));
		}

		let helper = self.garage.locked_helper().await;

		if let Some(alias) = self.garage.bucket_alias_table.get(&EmptyKey, name).await? {
			if alias.state.get().is_some() {
				return Err(Error::BadRequest(format!("Bucket {} already exists", name)));
			}
		}

		// ---- done checking, now commit ----

		let bucket = Bucket::new();
		self.garage.bucket_table.insert(&bucket).await?;

		helper.set_global_bucket_alias(bucket.id, name).await?;

		Ok(AdminRpc::Ok(format!("Bucket {} was created.", name)))
	}

	async fn handle_delete_bucket(&self, query: &DeleteBucketOpt) -> Result<AdminRpc, Error> {
		let helper = self.garage.locked_helper().await;

		let bucket_id = helper
			.bucket()
			.admin_get_existing_matching_bucket(&query.name)
			.await?;

		// Get the alias, but keep in minde here the bucket name
		// given in parameter can also be directly the bucket's ID.
		// In that case bucket_alias will be None, and
		// we can still delete the bucket if it has zero aliases
		// (a condition which we try to prevent but that could still happen somehow).
		// We just won't try to delete an alias entry because there isn't one.
		let bucket_alias = self
			.garage
			.bucket_alias_table
			.get(&EmptyKey, &query.name)
			.await?;

		// Check bucket doesn't have other aliases
		let mut bucket = helper.bucket().get_existing_bucket(bucket_id).await?;
		let bucket_state = bucket.state.as_option().unwrap();
		if bucket_state
			.aliases
			.items()
			.iter()
			.filter(|(_, _, active)| *active)
			.any(|(name, _, _)| name != &query.name)
		{
			return Err(Error::BadRequest(format!("Bucket {} still has other global aliases. Use `bucket unalias` to delete them one by one.", query.name)));
		}
		if bucket_state
			.local_aliases
			.items()
			.iter()
			.any(|(_, _, active)| *active)
		{
			return Err(Error::BadRequest(format!("Bucket {} still has other local aliases. Use `bucket unalias` to delete them one by one.", query.name)));
		}

		// Check bucket is empty
		if !helper.bucket().is_bucket_empty(bucket_id).await? {
			return Err(Error::BadRequest(format!(
				"Bucket {} is not empty",
				query.name
			)));
		}

		if !query.yes {
			return Err(Error::BadRequest(
				"Add --yes flag to really perform this operation".to_string(),
			));
		}

		// --- done checking, now commit ---
		// 1. delete authorization from keys that had access
		for (key_id, _) in bucket.authorized_keys() {
			helper
				.set_bucket_key_permissions(bucket.id, key_id, BucketKeyPerm::NO_PERMISSIONS)
				.await?;
		}

		// 2. delete bucket alias
		if bucket_alias.is_some() {
			helper
				.purge_global_bucket_alias(bucket_id, &query.name)
				.await?;
		}

		// 3. delete bucket
		bucket.state = Deletable::delete();
		self.garage.bucket_table.insert(&bucket).await?;

		Ok(AdminRpc::Ok(format!("Bucket {} was deleted.", query.name)))
	}

	async fn handle_alias_bucket(&self, query: &AliasBucketOpt) -> Result<AdminRpc, Error> {
		let helper = self.garage.locked_helper().await;

		let bucket_id = helper
			.bucket()
			.admin_get_existing_matching_bucket(&query.existing_bucket)
			.await?;

		if let Some(key_pattern) = &query.local {
			let key = helper.key().get_existing_matching_key(key_pattern).await?;

			helper
				.set_local_bucket_alias(bucket_id, &key.key_id, &query.new_name)
				.await?;
			Ok(AdminRpc::Ok(format!(
				"Alias {} now points to bucket {:?} in namespace of key {}",
				query.new_name, bucket_id, key.key_id
			)))
		} else {
			helper
				.set_global_bucket_alias(bucket_id, &query.new_name)
				.await?;
			Ok(AdminRpc::Ok(format!(
				"Alias {} now points to bucket {:?}",
				query.new_name, bucket_id
			)))
		}
	}

	async fn handle_unalias_bucket(&self, query: &UnaliasBucketOpt) -> Result<AdminRpc, Error> {
		let helper = self.garage.locked_helper().await;

		if let Some(key_pattern) = &query.local {
			let key = helper.key().get_existing_matching_key(key_pattern).await?;

			let bucket_id = key
				.state
				.as_option()
				.unwrap()
				.local_aliases
				.get(&query.name)
				.cloned()
				.flatten()
				.ok_or_bad_request("Bucket not found")?;

			helper
				.unset_local_bucket_alias(bucket_id, &key.key_id, &query.name)
				.await?;

			Ok(AdminRpc::Ok(format!(
				"Alias {} no longer points to bucket {:?} in namespace of key {}",
				&query.name, bucket_id, key.key_id
			)))
		} else {
			let bucket_id = helper
				.bucket()
				.resolve_global_bucket_name(&query.name)
				.await?
				.ok_or_bad_request("Bucket not found")?;

			helper
				.unset_global_bucket_alias(bucket_id, &query.name)
				.await?;

			Ok(AdminRpc::Ok(format!(
				"Alias {} no longer points to bucket {:?}",
				&query.name, bucket_id
			)))
		}
	}

	async fn handle_bucket_allow(&self, query: &PermBucketOpt) -> Result<AdminRpc, Error> {
		let helper = self.garage.locked_helper().await;

		let bucket_id = helper
			.bucket()
			.admin_get_existing_matching_bucket(&query.bucket)
			.await?;
		let key = helper
			.key()
			.get_existing_matching_key(&query.key_pattern)
			.await?;

		let allow_read = query.read || key.allow_read(&bucket_id);
		let allow_write = query.write || key.allow_write(&bucket_id);
		let allow_owner = query.owner || key.allow_owner(&bucket_id);

		helper
			.set_bucket_key_permissions(
				bucket_id,
				&key.key_id,
				BucketKeyPerm {
					timestamp: now_msec(),
					allow_read,
					allow_write,
					allow_owner,
				},
			)
			.await?;

		Ok(AdminRpc::Ok(format!(
			"New permissions for {} on {}: read {}, write {}, owner {}.",
			&key.key_id, &query.bucket, allow_read, allow_write, allow_owner
		)))
	}

	async fn handle_bucket_deny(&self, query: &PermBucketOpt) -> Result<AdminRpc, Error> {
		let helper = self.garage.locked_helper().await;

		let bucket_id = helper
			.bucket()
			.admin_get_existing_matching_bucket(&query.bucket)
			.await?;
		let key = helper
			.key()
			.get_existing_matching_key(&query.key_pattern)
			.await?;

		let allow_read = !query.read && key.allow_read(&bucket_id);
		let allow_write = !query.write && key.allow_write(&bucket_id);
		let allow_owner = !query.owner && key.allow_owner(&bucket_id);

		helper
			.set_bucket_key_permissions(
				bucket_id,
				&key.key_id,
				BucketKeyPerm {
					timestamp: now_msec(),
					allow_read,
					allow_write,
					allow_owner,
				},
			)
			.await?;

		Ok(AdminRpc::Ok(format!(
			"New permissions for {} on {}: read {}, write {}, owner {}.",
			&key.key_id, &query.bucket, allow_read, allow_write, allow_owner
		)))
	}

	async fn handle_bucket_website(&self, query: &WebsiteOpt) -> Result<AdminRpc, Error> {
		let bucket_id = self
			.garage
			.bucket_helper()
			.admin_get_existing_matching_bucket(&query.bucket)
			.await?;

		let mut bucket = self
			.garage
			.bucket_helper()
			.get_existing_bucket(bucket_id)
			.await?;
		let bucket_state = bucket.state.as_option_mut().unwrap();

		if !(query.allow ^ query.deny) {
			return Err(Error::BadRequest(
				"You must specify exactly one flag, either --allow or --deny".to_string(),
			));
		}

		let website = if query.allow {
			Some(WebsiteConfig {
				index_document: query.index_document.clone(),
				error_document: query.error_document.clone(),
			})
		} else {
			None
		};

		bucket_state.website_config.update(website);
		self.garage.bucket_table.insert(&bucket).await?;

		let msg = if query.allow {
			format!("Website access allowed for {}", &query.bucket)
		} else {
			format!("Website access denied for {}", &query.bucket)
		};

		Ok(AdminRpc::Ok(msg))
	}

	async fn handle_bucket_set_quotas(&self, query: &SetQuotasOpt) -> Result<AdminRpc, Error> {
		let bucket_id = self
			.garage
			.bucket_helper()
			.admin_get_existing_matching_bucket(&query.bucket)
			.await?;

		let mut bucket = self
			.garage
			.bucket_helper()
			.get_existing_bucket(bucket_id)
			.await?;
		let bucket_state = bucket.state.as_option_mut().unwrap();

		if query.max_size.is_none() && query.max_objects.is_none() {
			return Err(Error::BadRequest(
				"You must specify either --max-size or --max-objects (or both) for this command to do something.".to_string(),
			));
		}

		let mut quotas = bucket_state.quotas.get().clone();

		match query.max_size.as_ref().map(String::as_ref) {
			Some("none") => quotas.max_size = None,
			Some(v) => {
				let bs = v
					.parse::<bytesize::ByteSize>()
					.ok_or_bad_request(format!("Invalid size specified: {}", v))?;
				quotas.max_size = Some(bs.as_u64());
			}
			_ => (),
		}

		match query.max_objects.as_ref().map(String::as_ref) {
			Some("none") => quotas.max_objects = None,
			Some(v) => {
				let mo = v
					.parse::<u64>()
					.ok_or_bad_request(format!("Invalid number specified: {}", v))?;
				quotas.max_objects = Some(mo);
			}
			_ => (),
		}

		bucket_state.quotas.update(quotas);
		self.garage.bucket_table.insert(&bucket).await?;

		Ok(AdminRpc::Ok(format!(
			"Quotas updated for {}",
			&query.bucket
		)))
	}

	async fn handle_bucket_cleanup_incomplete_uploads(
		&self,
		query: &CleanupIncompleteUploadsOpt,
	) -> Result<AdminRpc, Error> {
		let mut bucket_ids = vec![];
		for b in query.buckets.iter() {
			bucket_ids.push(
				self.garage
					.bucket_helper()
					.admin_get_existing_matching_bucket(b)
					.await?,
			);
		}

		let duration = parse_duration::parse::parse(&query.older_than)
			.ok_or_bad_request("Invalid duration passed for --older-than parameter")?;

		let mut ret = String::new();
		for bucket in bucket_ids {
			let count = self
				.garage
				.bucket_helper()
				.cleanup_incomplete_uploads(&bucket, duration)
				.await?;
			writeln!(
				&mut ret,
				"Bucket {:?}: {} incomplete uploads aborted",
				bucket, count
			)
			.unwrap();
		}

		Ok(AdminRpc::Ok(ret))
	}
}
