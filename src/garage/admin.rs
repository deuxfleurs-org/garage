use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use garage_util::crdt::*;
use garage_util::data::*;
use garage_util::error::*;
use garage_util::time::*;

use garage_table::replication::*;
use garage_table::*;

use garage_rpc::*;

use garage_model::bucket_alias_table::*;
use garage_model::bucket_table::*;
use garage_model::garage::Garage;
use garage_model::key_table::*;
use garage_model::permission::*;

use crate::cli::*;
use crate::repair::Repair;

pub const ADMIN_RPC_PATH: &str = "garage/admin_rpc.rs/Rpc";

#[derive(Debug, Serialize, Deserialize)]
pub enum AdminRpc {
	BucketOperation(BucketOperation),
	KeyOperation(KeyOperation),
	LaunchRepair(RepairOpt),
	Stats(StatsOpt),

	// Replies
	Ok(String),
	BucketList(Vec<BucketAlias>),
	BucketInfo(Bucket),
	KeyList(Vec<(String, String)>),
	KeyInfo(Key),
}

impl Rpc for AdminRpc {
	type Response = Result<AdminRpc, Error>;
}

pub struct AdminRpcHandler {
	garage: Arc<Garage>,
	endpoint: Arc<Endpoint<AdminRpc, Self>>,
}

impl AdminRpcHandler {
	pub fn new(garage: Arc<Garage>) -> Arc<Self> {
		let endpoint = garage.system.netapp.endpoint(ADMIN_RPC_PATH.into());
		let admin = Arc::new(Self { garage, endpoint });
		admin.endpoint.set_handler(admin.clone());
		admin
	}

	async fn handle_bucket_cmd(&self, cmd: &BucketOperation) -> Result<AdminRpc, Error> {
		match cmd {
			BucketOperation::List => self.handle_list_buckets().await,
			BucketOperation::Info(query) => {
				let bucket_id = self
					.garage
					.bucket_helper()
					.resolve_global_bucket_name(&query.name)
					.await?
					.ok_or_message("Bucket not found")?;
				let bucket = self
					.garage
					.bucket_helper()
					.get_existing_bucket(bucket_id)
					.await?;
				Ok(AdminRpc::BucketInfo(bucket))
			}
			BucketOperation::Create(query) => self.handle_create_bucket(&query.name).await,
			BucketOperation::Delete(query) => self.handle_delete_bucket(query).await,
			BucketOperation::Alias(query) => self.handle_alias_bucket(query).await,
			BucketOperation::Unalias(query) => self.handle_unalias_bucket(query).await,
			BucketOperation::Allow(query) => self.handle_bucket_allow(query).await,
			BucketOperation::Deny(query) => self.handle_bucket_deny(query).await,
			BucketOperation::Website(query) => self.handle_bucket_website(query).await,
		}
	}

	async fn handle_list_buckets(&self) -> Result<AdminRpc, Error> {
		let bucket_aliases = self
			.garage
			.bucket_alias_table
			.get_range(&EmptyKey, None, Some(DeletedFilter::NotDeleted), 10000)
			.await?;
		Ok(AdminRpc::BucketList(bucket_aliases))
	}

	#[allow(clippy::ptr_arg)]
	async fn handle_create_bucket(&self, name: &String) -> Result<AdminRpc, Error> {
		let mut bucket = Bucket::new();
		let alias = match self.garage.bucket_alias_table.get(&EmptyKey, name).await? {
			Some(mut alias) => {
				if !alias.state.get().is_deleted() {
					return Err(Error::BadRpc(format!("Bucket {} already exists", name)));
				}
				alias.state.update(Deletable::Present(AliasParams {
					bucket_id: bucket.id,
				}));
				alias
			}
			None => BucketAlias::new(name.clone(), bucket.id),
		};
		bucket
			.state
			.as_option_mut()
			.unwrap()
			.aliases
			.update_in_place(name.clone(), true);
		self.garage.bucket_table.insert(&bucket).await?;
		self.garage.bucket_alias_table.insert(&alias).await?;
		Ok(AdminRpc::Ok(format!("Bucket {} was created.", name)))
	}

	async fn handle_delete_bucket(&self, query: &DeleteBucketOpt) -> Result<AdminRpc, Error> {
		let mut bucket_alias = self
			.garage
			.bucket_alias_table
			.get(&EmptyKey, &query.name)
			.await?
			.filter(|a| !a.is_deleted())
			.ok_or_message(format!("Bucket {} does not exist", query.name))?;

		let bucket_id = bucket_alias.state.get().as_option().unwrap().bucket_id;

		// Check bucket doesn't have other aliases
		let mut bucket = self
			.garage
			.bucket_helper()
			.get_existing_bucket(bucket_id)
			.await?;
		let bucket_state = bucket.state.as_option().unwrap();
		if bucket_state
			.aliases
			.items()
			.iter()
			.filter(|(_, _, active)| *active)
			.any(|(name, _, _)| name != &query.name)
		{
			return Err(Error::Message(format!("Bucket {} still has other global aliases. Use `bucket unalias` to delete them one by one.", query.name)));
		}
		if bucket_state
			.local_aliases
			.items()
			.iter()
			.any(|(_, _, active)| *active)
		{
			return Err(Error::Message(format!("Bucket {} still has other local aliases. Use `bucket unalias` to delete them one by one.", query.name)));
		}

		// Check bucket is empty
		let objects = self
			.garage
			.object_table
			.get_range(&bucket_id, None, Some(DeletedFilter::NotDeleted), 10)
			.await?;
		if !objects.is_empty() {
			return Err(Error::BadRpc(format!("Bucket {} is not empty", query.name)));
		}

		if !query.yes {
			return Err(Error::BadRpc(
				"Add --yes flag to really perform this operation".to_string(),
			));
		}

		// --- done checking, now commit ---
		// 1. delete authorization from keys that had access
		for (key_id, _) in bucket.authorized_keys() {
			if let Some(key) = self.garage.key_table.get(&EmptyKey, key_id).await? {
				if !key.state.is_deleted() {
					self.update_key_bucket(&key, bucket.id, false, false, false)
						.await?;
				}
			} else {
				return Err(Error::Message(format!("Key not found: {}", key_id)));
			}
		}
		// 2. delete bucket alias
		bucket_alias.state.update(Deletable::Deleted);
		self.garage.bucket_alias_table.insert(&bucket_alias).await?;
		// 3. delete bucket alias
		bucket.state = Deletable::delete();
		self.garage.bucket_table.insert(&bucket).await?;

		Ok(AdminRpc::Ok(format!("Bucket {} was deleted.", query.name)))
	}

	async fn handle_alias_bucket(&self, query: &AliasBucketOpt) -> Result<AdminRpc, Error> {
		let bucket_id = self
			.garage
			.bucket_helper()
			.resolve_global_bucket_name(&query.existing_bucket)
			.await?
			.ok_or_message("Bucket not found")?;
		let mut bucket = self
			.garage
			.bucket_helper()
			.get_existing_bucket(bucket_id)
			.await?;

		if let Some(key_local) = &query.local {
			let mut key = self.get_existing_key(key_local).await?;
			let mut key_param = key.state.as_option_mut().unwrap();

			if let Some(Deletable::Present(existing_alias)) =
				key_param.local_aliases.get(&query.new_name)
			{
				if *existing_alias == bucket_id {
					return Ok(AdminRpc::Ok(format!(
						"Alias {} already points to bucket {:?} in namespace of key {}",
						query.new_name, bucket_id, key.key_id
					)));
				} else {
					return Err(Error::Message(format!("Alias {} already exists and points to different bucket: {:?} in namespace of key {}", query.new_name, existing_alias, key.key_id)));
				}
			}

			key_param.local_aliases = key_param
				.local_aliases
				.update_mutator(query.new_name.clone(), Deletable::present(bucket_id));
			self.garage.key_table.insert(&key).await?;

			let mut bucket_p = bucket.state.as_option_mut().unwrap();
			bucket_p.local_aliases = bucket_p
				.local_aliases
				.update_mutator((key.key_id.clone(), query.new_name.clone()), true);
			self.garage.bucket_table.insert(&bucket).await?;

			Ok(AdminRpc::Ok(format!(
				"Alias {} created to bucket {:?} in namespace of key {}",
				query.new_name, bucket_id, key.key_id
			)))
		} else {
			let mut alias = self
				.garage
				.bucket_alias_table
				.get(&EmptyKey, &query.new_name)
				.await?
				.unwrap_or(BucketAlias {
					name: query.new_name.clone(),
					state: Lww::new(Deletable::delete()),
				});

			if let Some(existing_alias) = alias.state.get().as_option() {
				if existing_alias.bucket_id == bucket_id {
					return Ok(AdminRpc::Ok(format!(
						"Alias {} already points to bucket {:?}",
						query.new_name, bucket_id
					)));
				} else {
					return Err(Error::Message(format!(
						"Alias {} already exists and points to different bucket: {:?}",
						query.new_name, existing_alias.bucket_id
					)));
				}
			}

			// Checks ok, add alias
			alias
				.state
				.update(Deletable::present(AliasParams { bucket_id }));
			self.garage.bucket_alias_table.insert(&alias).await?;

			let mut bucket_p = bucket.state.as_option_mut().unwrap();
			bucket_p.aliases = bucket_p
				.aliases
				.update_mutator(query.new_name.clone(), true);
			self.garage.bucket_table.insert(&bucket).await?;

			Ok(AdminRpc::Ok(format!(
				"Alias {} created to bucket {:?}",
				query.new_name, bucket_id
			)))
		}
	}

	async fn handle_unalias_bucket(&self, query: &UnaliasBucketOpt) -> Result<AdminRpc, Error> {
		if let Some(key_local) = &query.local {
			let mut key = self.get_existing_key(key_local).await?;

			let bucket_id = key
				.state
				.as_option()
				.unwrap()
				.local_aliases
				.get(&query.name)
				.map(|a| a.into_option())
				.flatten()
				.ok_or_message("Bucket not found")?;
			let mut bucket = self
				.garage
				.bucket_helper()
				.get_existing_bucket(bucket_id)
				.await?;
			let mut bucket_state = bucket.state.as_option_mut().unwrap();

			let has_other_aliases = bucket_state
				.aliases
				.items()
				.iter()
				.any(|(_, _, active)| *active)
				|| bucket_state
					.local_aliases
					.items()
					.iter()
					.any(|((k, n), _, active)| *k == key.key_id && *n == query.name && *active);
			if !has_other_aliases {
				return Err(Error::Message(format!("Bucket {} doesn't have other aliases, please delete it instead of just unaliasing.", query.name)));
			}

			let mut key_param = key.state.as_option_mut().unwrap();
			key_param.local_aliases = key_param
				.local_aliases
				.update_mutator(query.name.clone(), Deletable::delete());
			self.garage.key_table.insert(&key).await?;

			bucket_state.local_aliases = bucket_state
				.local_aliases
				.update_mutator((key.key_id.clone(), query.name.clone()), false);
			self.garage.bucket_table.insert(&bucket).await?;

			Ok(AdminRpc::Ok(format!(
				"Bucket alias {} deleted from namespace of key {}",
				query.name, key.key_id
			)))
		} else {
			let bucket_id = self
				.garage
				.bucket_helper()
				.resolve_global_bucket_name(&query.name)
				.await?
				.ok_or_message("Bucket not found")?;
			let mut bucket = self
				.garage
				.bucket_helper()
				.get_existing_bucket(bucket_id)
				.await?;
			let mut bucket_state = bucket.state.as_option_mut().unwrap();

			let has_other_aliases = bucket_state
				.aliases
				.items()
				.iter()
				.any(|(name, _, active)| *name != query.name && *active)
				|| bucket_state
					.local_aliases
					.items()
					.iter()
					.any(|(_, _, active)| *active);
			if !has_other_aliases {
				return Err(Error::Message(format!("Bucket {} doesn't have other aliases, please delete it instead of just unaliasing.", query.name)));
			}

			let mut alias = self
				.garage
				.bucket_alias_table
				.get(&EmptyKey, &query.name)
				.await?
				.ok_or_message("Internal error: alias not found")?;
			alias.state.update(Deletable::delete());
			self.garage.bucket_alias_table.insert(&alias).await?;

			bucket_state.aliases = bucket_state
				.aliases
				.update_mutator(query.name.clone(), false);
			self.garage.bucket_table.insert(&bucket).await?;

			Ok(AdminRpc::Ok(format!("Bucket alias {} deleted", query.name)))
		}
	}

	async fn handle_bucket_allow(&self, query: &PermBucketOpt) -> Result<AdminRpc, Error> {
		let bucket_id = self
			.garage
			.bucket_helper()
			.resolve_global_bucket_name(&query.bucket)
			.await?
			.ok_or_message("Bucket not found")?;
		let bucket = self
			.garage
			.bucket_helper()
			.get_existing_bucket(bucket_id)
			.await?;
		let key = self.get_existing_key(&query.key_pattern).await?;

		let allow_read = query.read || key.allow_read(&bucket_id);
		let allow_write = query.write || key.allow_write(&bucket_id);
		let allow_owner = query.owner || key.allow_owner(&bucket_id);

		let new_perm = self
			.update_key_bucket(&key, bucket_id, allow_read, allow_write, allow_owner)
			.await?;
		self.update_bucket_key(bucket, &key.key_id, new_perm)
			.await?;

		Ok(AdminRpc::Ok(format!(
			"New permissions for {} on {}: read {}, write {}, owner {}.",
			&key.key_id, &query.bucket, allow_read, allow_write, allow_owner
		)))
	}

	async fn handle_bucket_deny(&self, query: &PermBucketOpt) -> Result<AdminRpc, Error> {
		let bucket_id = self
			.garage
			.bucket_helper()
			.resolve_global_bucket_name(&query.bucket)
			.await?
			.ok_or_message("Bucket not found")?;
		let bucket = self
			.garage
			.bucket_helper()
			.get_existing_bucket(bucket_id)
			.await?;
		let key = self.get_existing_key(&query.key_pattern).await?;

		let allow_read = !query.read && key.allow_read(&bucket_id);
		let allow_write = !query.write && key.allow_write(&bucket_id);
		let allow_owner = !query.owner && key.allow_owner(&bucket_id);

		let new_perm = self
			.update_key_bucket(&key, bucket_id, allow_read, allow_write, allow_owner)
			.await?;
		self.update_bucket_key(bucket, &key.key_id, new_perm)
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
			.resolve_global_bucket_name(&query.bucket)
			.await?
			.ok_or_message("Bucket not found")?;

		let mut bucket = self
			.garage
			.bucket_helper()
			.get_existing_bucket(bucket_id)
			.await?;
		let bucket_state = bucket.state.as_option_mut().unwrap();

		if !(query.allow ^ query.deny) {
			return Err(Error::Message(
				"You must specify exactly one flag, either --allow or --deny".to_string(),
			));
		}

		bucket_state.website_access.update(query.allow);
		self.garage.bucket_table.insert(&bucket).await?;

		let msg = if query.allow {
			format!("Website access allowed for {}", &query.bucket)
		} else {
			format!("Website access denied for {}", &query.bucket)
		};

		Ok(AdminRpc::Ok(msg))
	}

	async fn handle_key_cmd(&self, cmd: &KeyOperation) -> Result<AdminRpc, Error> {
		match cmd {
			KeyOperation::List => self.handle_list_keys().await,
			KeyOperation::Info(query) => {
				let key = self.get_existing_key(&query.key_pattern).await?;
				Ok(AdminRpc::KeyInfo(key))
			}
			KeyOperation::New(query) => self.handle_create_key(query).await,
			KeyOperation::Rename(query) => self.handle_rename_key(query).await,
			KeyOperation::Delete(query) => self.handle_delete_key(query).await,
			KeyOperation::Import(query) => self.handle_import_key(query).await,
		}
	}

	async fn handle_list_keys(&self) -> Result<AdminRpc, Error> {
		let key_ids = self
			.garage
			.key_table
			.get_range(
				&EmptyKey,
				None,
				Some(KeyFilter::Deleted(DeletedFilter::NotDeleted)),
				10000,
			)
			.await?
			.iter()
			.map(|k| (k.key_id.to_string(), k.name.get().clone()))
			.collect::<Vec<_>>();
		Ok(AdminRpc::KeyList(key_ids))
	}

	async fn handle_create_key(&self, query: &KeyNewOpt) -> Result<AdminRpc, Error> {
		let key = Key::new(query.name.clone());
		self.garage.key_table.insert(&key).await?;
		Ok(AdminRpc::KeyInfo(key))
	}

	async fn handle_rename_key(&self, query: &KeyRenameOpt) -> Result<AdminRpc, Error> {
		let mut key = self.get_existing_key(&query.key_pattern).await?;
		key.name.update(query.new_name.clone());
		self.garage.key_table.insert(&key).await?;
		Ok(AdminRpc::KeyInfo(key))
	}

	async fn handle_delete_key(&self, query: &KeyDeleteOpt) -> Result<AdminRpc, Error> {
		let mut key = self.get_existing_key(&query.key_pattern).await?;
		if !query.yes {
			return Err(Error::BadRpc(
				"Add --yes flag to really perform this operation".to_string(),
			));
		}
		let state = key.state.as_option_mut().unwrap();

		// --- done checking, now commit ---
		// 1. Delete local aliases
		for (alias, _, to) in state.local_aliases.items().iter() {
			if let Deletable::Present(bucket_id) = to {
				if let Some(mut bucket) = self.garage.bucket_table.get(bucket_id, &EmptyKey).await?
				{
					if let Deletable::Present(bucket_state) = &mut bucket.state {
						bucket_state.local_aliases = bucket_state
							.local_aliases
							.update_mutator((key.key_id.to_string(), alias.to_string()), false);
						self.garage.bucket_table.insert(&bucket).await?;
					}
				} else {
					// ignore
				}
			}
		}
		// 2. Delete authorized buckets
		for (ab_id, auth) in state.authorized_buckets.items().iter() {
			if let Some(bucket) = self.garage.bucket_table.get(ab_id, &EmptyKey).await? {
				let new_perm = BucketKeyPerm {
					timestamp: increment_logical_clock(auth.timestamp),
					allow_read: false,
					allow_write: false,
					allow_owner: false,
				};
				if !bucket.is_deleted() {
					self.update_bucket_key(bucket, &key.key_id, new_perm)
						.await?;
				}
			} else {
				// ignore
			}
		}
		// 3. Actually delete key
		key.state = Deletable::delete();
		self.garage.key_table.insert(&key).await?;

		Ok(AdminRpc::Ok(format!(
			"Key {} was deleted successfully.",
			key.key_id
		)))
	}

	async fn handle_import_key(&self, query: &KeyImportOpt) -> Result<AdminRpc, Error> {
		let prev_key = self.garage.key_table.get(&EmptyKey, &query.key_id).await?;
		if prev_key.is_some() {
			return Err(Error::Message(format!("Key {} already exists in data store. Even if it is deleted, we can't let you create a new key with the same ID. Sorry.", query.key_id)));
		}
		let imported_key = Key::import(&query.key_id, &query.secret_key, &query.name);
		self.garage.key_table.insert(&imported_key).await?;
		Ok(AdminRpc::KeyInfo(imported_key))
	}

	async fn get_existing_key(&self, pattern: &str) -> Result<Key, Error> {
		let candidates = self
			.garage
			.key_table
			.get_range(
				&EmptyKey,
				None,
				Some(KeyFilter::Matches(pattern.to_string())),
				10,
			)
			.await?
			.into_iter()
			.filter(|k| !k.state.is_deleted())
			.collect::<Vec<_>>();
		if candidates.len() != 1 {
			Err(Error::Message(format!(
				"{} matching keys",
				candidates.len()
			)))
		} else {
			Ok(candidates.into_iter().next().unwrap())
		}
	}

	/// Update **key table** to inform of the new linked bucket
	async fn update_key_bucket(
		&self,
		key: &Key,
		bucket_id: Uuid,
		allow_read: bool,
		allow_write: bool,
		allow_owner: bool,
	) -> Result<BucketKeyPerm, Error> {
		let mut key = key.clone();
		let mut key_state = key.state.as_option_mut().unwrap();

		let perm = key_state
			.authorized_buckets
			.get(&bucket_id)
			.cloned()
			.map(|old_perm| BucketKeyPerm {
				timestamp: increment_logical_clock(old_perm.timestamp),
				allow_read,
				allow_write,
				allow_owner,
			})
			.unwrap_or(BucketKeyPerm {
				timestamp: now_msec(),
				allow_read,
				allow_write,
				allow_owner,
			});

		key_state.authorized_buckets = Map::put_mutator(bucket_id, perm);

		self.garage.key_table.insert(&key).await?;
		Ok(perm)
	}

	/// Update **bucket table** to inform of the new linked key
	async fn update_bucket_key(
		&self,
		mut bucket: Bucket,
		key_id: &str,
		new_perm: BucketKeyPerm,
	) -> Result<(), Error> {
		bucket.state.as_option_mut().unwrap().authorized_keys =
			Map::put_mutator(key_id.to_string(), new_perm);
		self.garage.bucket_table.insert(&bucket).await?;
		Ok(())
	}

	async fn handle_launch_repair(self: &Arc<Self>, opt: RepairOpt) -> Result<AdminRpc, Error> {
		if !opt.yes {
			return Err(Error::BadRpc(
				"Please provide the --yes flag to initiate repair operations.".to_string(),
			));
		}
		if opt.all_nodes {
			let mut opt_to_send = opt.clone();
			opt_to_send.all_nodes = false;

			let mut failures = vec![];
			let ring = self.garage.system.ring.borrow().clone();
			for node in ring.layout.node_ids().iter() {
				let node = (*node).into();
				let resp = self
					.endpoint
					.call(
						&node,
						&AdminRpc::LaunchRepair(opt_to_send.clone()),
						PRIO_NORMAL,
					)
					.await;
				if !matches!(resp, Ok(Ok(_))) {
					failures.push(node);
				}
			}
			if failures.is_empty() {
				Ok(AdminRpc::Ok("Repair launched on all nodes".to_string()))
			} else {
				Err(Error::Message(format!(
					"Could not launch repair on nodes: {:?} (launched successfully on other nodes)",
					failures
				)))
			}
		} else {
			let repair = Repair {
				garage: self.garage.clone(),
			};
			self.garage
				.system
				.background
				.spawn_worker("Repair worker".into(), move |must_exit| async move {
					repair.repair_worker(opt, must_exit).await
				});
			Ok(AdminRpc::Ok(format!(
				"Repair launched on {:?}",
				self.garage.system.id
			)))
		}
	}

	async fn handle_stats(&self, opt: StatsOpt) -> Result<AdminRpc, Error> {
		if opt.all_nodes {
			let mut ret = String::new();
			let ring = self.garage.system.ring.borrow().clone();

			for node in ring.layout.node_ids().iter() {
				let mut opt = opt.clone();
				opt.all_nodes = false;

				writeln!(&mut ret, "\n======================").unwrap();
				writeln!(&mut ret, "Stats for node {:?}:", node).unwrap();

				let node_id = (*node).into();
				match self
					.endpoint
					.call(&node_id, &AdminRpc::Stats(opt), PRIO_NORMAL)
					.await?
				{
					Ok(AdminRpc::Ok(s)) => writeln!(&mut ret, "{}", s).unwrap(),
					Ok(x) => writeln!(&mut ret, "Bad answer: {:?}", x).unwrap(),
					Err(e) => writeln!(&mut ret, "Error: {}", e).unwrap(),
				}
			}
			Ok(AdminRpc::Ok(ret))
		} else {
			Ok(AdminRpc::Ok(self.gather_stats_local(opt)))
		}
	}

	fn gather_stats_local(&self, opt: StatsOpt) -> String {
		let mut ret = String::new();
		writeln!(
			&mut ret,
			"\nGarage version: {}",
			option_env!("GIT_VERSION").unwrap_or(git_version::git_version!(
				prefix = "git:",
				cargo_prefix = "cargo:",
				fallback = "unknown"
			))
		)
		.unwrap();

		// Gather ring statistics
		let ring = self.garage.system.ring.borrow().clone();
		let mut ring_nodes = HashMap::new();
		for (_i, loc) in ring.partitions().iter() {
			for n in ring.get_nodes(loc, ring.replication_factor).iter() {
				if !ring_nodes.contains_key(n) {
					ring_nodes.insert(*n, 0usize);
				}
				*ring_nodes.get_mut(n).unwrap() += 1;
			}
		}
		writeln!(&mut ret, "\nRing nodes & partition count:").unwrap();
		for (n, c) in ring_nodes.iter() {
			writeln!(&mut ret, "  {:?} {}", n, c).unwrap();
		}

		self.gather_table_stats(&mut ret, &self.garage.bucket_table, &opt);
		self.gather_table_stats(&mut ret, &self.garage.key_table, &opt);
		self.gather_table_stats(&mut ret, &self.garage.object_table, &opt);
		self.gather_table_stats(&mut ret, &self.garage.version_table, &opt);
		self.gather_table_stats(&mut ret, &self.garage.block_ref_table, &opt);

		writeln!(&mut ret, "\nBlock manager stats:").unwrap();
		if opt.detailed {
			writeln!(
				&mut ret,
				"  number of RC entries (~= number of blocks): {}",
				self.garage.block_manager.rc_len()
			)
			.unwrap();
		}
		writeln!(
			&mut ret,
			"  resync queue length: {}",
			self.garage.block_manager.resync_queue_len()
		)
		.unwrap();

		ret
	}

	fn gather_table_stats<F, R>(&self, to: &mut String, t: &Arc<Table<F, R>>, opt: &StatsOpt)
	where
		F: TableSchema + 'static,
		R: TableReplication + 'static,
	{
		writeln!(to, "\nTable stats for {}", F::TABLE_NAME).unwrap();
		if opt.detailed {
			writeln!(to, "  number of items: {}", t.data.store.len()).unwrap();
			writeln!(
				to,
				"  Merkle tree size: {}",
				t.merkle_updater.merkle_tree_len()
			)
			.unwrap();
		}
		writeln!(
			to,
			"  Merkle updater todo queue length: {}",
			t.merkle_updater.todo_len()
		)
		.unwrap();
		writeln!(to, "  GC todo queue length: {}", t.data.gc_todo_len()).unwrap();
	}
}

#[async_trait]
impl EndpointHandler<AdminRpc> for AdminRpcHandler {
	async fn handle(
		self: &Arc<Self>,
		message: &AdminRpc,
		_from: NodeID,
	) -> Result<AdminRpc, Error> {
		match message {
			AdminRpc::BucketOperation(bo) => self.handle_bucket_cmd(bo).await,
			AdminRpc::KeyOperation(ko) => self.handle_key_cmd(ko).await,
			AdminRpc::LaunchRepair(opt) => self.handle_launch_repair(opt.clone()).await,
			AdminRpc::Stats(opt) => self.handle_stats(opt.clone()).await,
			_ => Err(Error::BadRpc("Invalid RPC".to_string())),
		}
	}
}
