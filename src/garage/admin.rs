use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use garage_util::crdt::*;
use garage_util::data::*;
use garage_util::error::Error as GarageError;
use garage_util::time::*;

use garage_table::replication::*;
use garage_table::*;

use garage_rpc::*;

use garage_model::bucket_alias_table::*;
use garage_model::bucket_table::*;
use garage_model::garage::Garage;
use garage_model::helper::error::{Error, OkOrBadRequest};
use garage_model::key_table::*;
use garage_model::migrate::Migrate;
use garage_model::permission::*;

use crate::cli::*;
use crate::repair::online::OnlineRepair;

pub const ADMIN_RPC_PATH: &str = "garage/admin_rpc.rs/Rpc";

#[derive(Debug, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum AdminRpc {
	BucketOperation(BucketOperation),
	KeyOperation(KeyOperation),
	LaunchRepair(RepairOpt),
	Migrate(MigrateOpt),
	Stats(StatsOpt),

	// Replies
	Ok(String),
	BucketList(Vec<Bucket>),
	BucketInfo {
		bucket: Bucket,
		relevant_keys: HashMap<String, Key>,
		counters: HashMap<String, i64>,
	},
	KeyList(Vec<(String, String)>),
	KeyInfo(Key, HashMap<Uuid, Bucket>),
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
			BucketOperation::Info(query) => self.handle_bucket_info(query).await,
			BucketOperation::Create(query) => self.handle_create_bucket(&query.name).await,
			BucketOperation::Delete(query) => self.handle_delete_bucket(query).await,
			BucketOperation::Alias(query) => self.handle_alias_bucket(query).await,
			BucketOperation::Unalias(query) => self.handle_unalias_bucket(query).await,
			BucketOperation::Allow(query) => self.handle_bucket_allow(query).await,
			BucketOperation::Deny(query) => self.handle_bucket_deny(query).await,
			BucketOperation::Website(query) => self.handle_bucket_website(query).await,
			BucketOperation::SetQuotas(query) => self.handle_bucket_set_quotas(query).await,
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
			.resolve_global_bucket_name(&query.name)
			.await?
			.ok_or_bad_request("Bucket not found")?;

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
			.map(|x| x.filtered_values(&self.garage.system.ring.borrow()))
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
		})
	}

	#[allow(clippy::ptr_arg)]
	async fn handle_create_bucket(&self, name: &String) -> Result<AdminRpc, Error> {
		if !is_valid_bucket_name(name) {
			return Err(Error::BadRequest(format!(
				"{}: {}",
				name, INVALID_BUCKET_NAME_MESSAGE
			)));
		}

		if let Some(alias) = self.garage.bucket_alias_table.get(&EmptyKey, name).await? {
			if alias.state.get().is_some() {
				return Err(Error::BadRequest(format!("Bucket {} already exists", name)));
			}
		}

		// ---- done checking, now commit ----

		let bucket = Bucket::new();
		self.garage.bucket_table.insert(&bucket).await?;

		self.garage
			.bucket_helper()
			.set_global_bucket_alias(bucket.id, name)
			.await?;

		Ok(AdminRpc::Ok(format!("Bucket {} was created.", name)))
	}

	async fn handle_delete_bucket(&self, query: &DeleteBucketOpt) -> Result<AdminRpc, Error> {
		let helper = self.garage.bucket_helper();

		let bucket_id = helper
			.resolve_global_bucket_name(&query.name)
			.await?
			.ok_or_bad_request("Bucket not found")?;

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
		let mut bucket = helper.get_existing_bucket(bucket_id).await?;
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
		if !helper.is_bucket_empty(bucket_id).await? {
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
		let helper = self.garage.bucket_helper();
		let key_helper = self.garage.key_helper();

		let bucket_id = helper
			.resolve_global_bucket_name(&query.existing_bucket)
			.await?
			.ok_or_bad_request("Bucket not found")?;

		if let Some(key_pattern) = &query.local {
			let key = key_helper.get_existing_matching_key(key_pattern).await?;

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
		let helper = self.garage.bucket_helper();
		let key_helper = self.garage.key_helper();

		if let Some(key_pattern) = &query.local {
			let key = key_helper.get_existing_matching_key(key_pattern).await?;

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
		let helper = self.garage.bucket_helper();
		let key_helper = self.garage.key_helper();

		let bucket_id = helper
			.resolve_global_bucket_name(&query.bucket)
			.await?
			.ok_or_bad_request("Bucket not found")?;
		let key = key_helper
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
		let helper = self.garage.bucket_helper();
		let key_helper = self.garage.key_helper();

		let bucket_id = helper
			.resolve_global_bucket_name(&query.bucket)
			.await?
			.ok_or_bad_request("Bucket not found")?;
		let key = key_helper
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
			.resolve_global_bucket_name(&query.bucket)
			.await?
			.ok_or_bad_request("Bucket not found")?;

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
			.resolve_global_bucket_name(&query.bucket)
			.await?
			.ok_or_bad_request("Bucket not found")?;

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

	async fn handle_key_cmd(&self, cmd: &KeyOperation) -> Result<AdminRpc, Error> {
		match cmd {
			KeyOperation::List => self.handle_list_keys().await,
			KeyOperation::Info(query) => self.handle_key_info(query).await,
			KeyOperation::New(query) => self.handle_create_key(query).await,
			KeyOperation::Rename(query) => self.handle_rename_key(query).await,
			KeyOperation::Delete(query) => self.handle_delete_key(query).await,
			KeyOperation::Allow(query) => self.handle_allow_key(query).await,
			KeyOperation::Deny(query) => self.handle_deny_key(query).await,
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
				EnumerationOrder::Forward,
			)
			.await?
			.iter()
			.map(|k| (k.key_id.to_string(), k.params().unwrap().name.get().clone()))
			.collect::<Vec<_>>();
		Ok(AdminRpc::KeyList(key_ids))
	}

	async fn handle_key_info(&self, query: &KeyOpt) -> Result<AdminRpc, Error> {
		let key = self
			.garage
			.key_helper()
			.get_existing_matching_key(&query.key_pattern)
			.await?;
		self.key_info_result(key).await
	}

	async fn handle_create_key(&self, query: &KeyNewOpt) -> Result<AdminRpc, Error> {
		let key = Key::new(&query.name);
		self.garage.key_table.insert(&key).await?;
		self.key_info_result(key).await
	}

	async fn handle_rename_key(&self, query: &KeyRenameOpt) -> Result<AdminRpc, Error> {
		let mut key = self
			.garage
			.key_helper()
			.get_existing_matching_key(&query.key_pattern)
			.await?;
		key.params_mut()
			.unwrap()
			.name
			.update(query.new_name.clone());
		self.garage.key_table.insert(&key).await?;
		self.key_info_result(key).await
	}

	async fn handle_delete_key(&self, query: &KeyDeleteOpt) -> Result<AdminRpc, Error> {
		let key_helper = self.garage.key_helper();

		let mut key = key_helper
			.get_existing_matching_key(&query.key_pattern)
			.await?;

		if !query.yes {
			return Err(Error::BadRequest(
				"Add --yes flag to really perform this operation".to_string(),
			));
		}

		key_helper.delete_key(&mut key).await?;

		Ok(AdminRpc::Ok(format!(
			"Key {} was deleted successfully.",
			key.key_id
		)))
	}

	async fn handle_allow_key(&self, query: &KeyPermOpt) -> Result<AdminRpc, Error> {
		let mut key = self
			.garage
			.key_helper()
			.get_existing_matching_key(&query.key_pattern)
			.await?;
		if query.create_bucket {
			key.params_mut().unwrap().allow_create_bucket.update(true);
		}
		self.garage.key_table.insert(&key).await?;
		self.key_info_result(key).await
	}

	async fn handle_deny_key(&self, query: &KeyPermOpt) -> Result<AdminRpc, Error> {
		let mut key = self
			.garage
			.key_helper()
			.get_existing_matching_key(&query.key_pattern)
			.await?;
		if query.create_bucket {
			key.params_mut().unwrap().allow_create_bucket.update(false);
		}
		self.garage.key_table.insert(&key).await?;
		self.key_info_result(key).await
	}

	async fn handle_import_key(&self, query: &KeyImportOpt) -> Result<AdminRpc, Error> {
		let prev_key = self.garage.key_table.get(&EmptyKey, &query.key_id).await?;
		if prev_key.is_some() {
			return Err(Error::BadRequest(format!("Key {} already exists in data store. Even if it is deleted, we can't let you create a new key with the same ID. Sorry.", query.key_id)));
		}
		let imported_key = Key::import(&query.key_id, &query.secret_key, &query.name);
		self.garage.key_table.insert(&imported_key).await?;

		self.key_info_result(imported_key).await
	}

	async fn key_info_result(&self, key: Key) -> Result<AdminRpc, Error> {
		let mut relevant_buckets = HashMap::new();

		for (id, _) in key
			.state
			.as_option()
			.unwrap()
			.authorized_buckets
			.items()
			.iter()
		{
			if let Some(b) = self.garage.bucket_table.get(&EmptyKey, id).await? {
				relevant_buckets.insert(*id, b);
			}
		}

		Ok(AdminRpc::KeyInfo(key, relevant_buckets))
	}

	async fn handle_migrate(self: &Arc<Self>, opt: MigrateOpt) -> Result<AdminRpc, Error> {
		if !opt.yes {
			return Err(Error::BadRequest(
				"Please provide the --yes flag to initiate migration operation.".to_string(),
			));
		}

		let m = Migrate {
			garage: self.garage.clone(),
		};
		match opt.what {
			MigrateWhat::Buckets050 => m.migrate_buckets050().await,
		}?;
		Ok(AdminRpc::Ok("Migration successfull.".into()))
	}

	async fn handle_launch_repair(self: &Arc<Self>, opt: RepairOpt) -> Result<AdminRpc, Error> {
		if !opt.yes {
			return Err(Error::BadRequest(
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
				Err(Error::BadRequest(format!(
					"Could not launch repair on nodes: {:?} (launched successfully on other nodes)",
					failures
				)))
			}
		} else {
			let repair = OnlineRepair {
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
			Ok(AdminRpc::Ok(self.gather_stats_local(opt)?))
		}
	}

	fn gather_stats_local(&self, opt: StatsOpt) -> Result<String, Error> {
		let mut ret = String::new();
		writeln!(
			&mut ret,
			"\nGarage version: {}",
			self.garage.system.garage_version(),
		)
		.unwrap();
		writeln!(&mut ret, "\nDatabase engine: {}", self.garage.db.engine()).unwrap();

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

		self.gather_table_stats(&mut ret, &self.garage.bucket_table, &opt)?;
		self.gather_table_stats(&mut ret, &self.garage.key_table, &opt)?;
		self.gather_table_stats(&mut ret, &self.garage.object_table, &opt)?;
		self.gather_table_stats(&mut ret, &self.garage.version_table, &opt)?;
		self.gather_table_stats(&mut ret, &self.garage.block_ref_table, &opt)?;

		writeln!(&mut ret, "\nBlock manager stats:").unwrap();
		if opt.detailed {
			writeln!(
				&mut ret,
				"  number of RC entries (~= number of blocks): {}",
				self.garage.block_manager.rc_len()?
			)
			.unwrap();
		}
		writeln!(
			&mut ret,
			"  resync queue length: {}",
			self.garage.block_manager.resync_queue_len()?
		)
		.unwrap();
		writeln!(
			&mut ret,
			"  blocks with resync errors: {}",
			self.garage.block_manager.resync_errors_len()?
		)
		.unwrap();

		Ok(ret)
	}

	fn gather_table_stats<F, R>(
		&self,
		to: &mut String,
		t: &Arc<Table<F, R>>,
		opt: &StatsOpt,
	) -> Result<(), Error>
	where
		F: TableSchema + 'static,
		R: TableReplication + 'static,
	{
		writeln!(to, "\nTable stats for {}", F::TABLE_NAME).unwrap();
		if opt.detailed {
			writeln!(
				to,
				"  number of items: {}",
				t.data.store.len().map_err(GarageError::from)?
			)
			.unwrap();
			writeln!(
				to,
				"  Merkle tree size: {}",
				t.merkle_updater.merkle_tree_len()?
			)
			.unwrap();
		}
		writeln!(
			to,
			"  Merkle updater todo queue length: {}",
			t.merkle_updater.todo_len()?
		)
		.unwrap();
		writeln!(to, "  GC todo queue length: {}", t.data.gc_todo_len()?).unwrap();

		Ok(())
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
			AdminRpc::Migrate(opt) => self.handle_migrate(opt.clone()).await,
			AdminRpc::LaunchRepair(opt) => self.handle_launch_repair(opt.clone()).await,
			AdminRpc::Stats(opt) => self.handle_stats(opt.clone()).await,
			m => Err(GarageError::unexpected_rpc_message(m).into()),
		}
	}
}
