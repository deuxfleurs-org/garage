use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use garage_util::error::Error;

use garage_table::crdt::CRDT;
use garage_table::replication::*;
use garage_table::*;

use garage_rpc::rpc_client::*;
use garage_rpc::rpc_server::*;

use garage_model::bucket_table::*;
use garage_model::garage::Garage;
use garage_model::key_table::*;

use crate::cli::*;
use crate::repair::Repair;
use crate::*;

pub const ADMIN_RPC_TIMEOUT: Duration = Duration::from_secs(30);
pub const ADMIN_RPC_PATH: &str = "_admin";

#[derive(Debug, Serialize, Deserialize)]
pub enum AdminRpc {
	BucketOperation(BucketOperation),
	KeyOperation(KeyOperation),
	LaunchRepair(RepairOpt),
	Stats(StatsOpt),

	// Replies
	Ok(String),
	BucketList(Vec<String>),
	BucketInfo(Bucket),
	KeyList(Vec<(String, String)>),
	KeyInfo(Key),
}

impl RpcMessage for AdminRpc {}

pub struct AdminRpcHandler {
	garage: Arc<Garage>,
	rpc_client: Arc<RpcClient<AdminRpc>>,
}

impl AdminRpcHandler {
	pub fn new(garage: Arc<Garage>) -> Arc<Self> {
		let rpc_client = garage.system.clone().rpc_client::<AdminRpc>(ADMIN_RPC_PATH);
		Arc::new(Self { garage, rpc_client })
	}

	pub fn register_handler(self: Arc<Self>, rpc_server: &mut RpcServer) {
		rpc_server.add_handler::<AdminRpc, _, _>(ADMIN_RPC_PATH.to_string(), move |msg, _addr| {
			let self2 = self.clone();
			async move {
				match msg {
					AdminRpc::BucketOperation(bo) => self2.handle_bucket_cmd(bo).await,
					AdminRpc::KeyOperation(ko) => self2.handle_key_cmd(ko).await,
					AdminRpc::LaunchRepair(opt) => self2.handle_launch_repair(opt).await,
					AdminRpc::Stats(opt) => self2.handle_stats(opt).await,
					_ => Err(Error::BadRPC("Invalid RPC".to_string())),
				}
			}
		});
	}

	async fn handle_bucket_cmd(&self, cmd: BucketOperation) -> Result<AdminRpc, Error> {
		match cmd {
			BucketOperation::List => {
				let bucket_names = self
					.garage
					.bucket_table
					.get_range(&EmptyKey, None, Some(DeletedFilter::NotDeleted), 10000)
					.await?
					.iter()
					.map(|b| b.name.to_string())
					.collect::<Vec<_>>();
				Ok(AdminRpc::BucketList(bucket_names))
			}
			BucketOperation::Info(query) => {
				let bucket = self.get_existing_bucket(&query.name).await?;
				Ok(AdminRpc::BucketInfo(bucket))
			}
			BucketOperation::Create(query) => {
				let bucket = match self.garage.bucket_table.get(&EmptyKey, &query.name).await? {
					Some(mut bucket) => {
						if !bucket.is_deleted() {
							return Err(Error::BadRPC(format!(
								"Bucket {} already exists",
								query.name
							)));
						}
						bucket
							.state
							.update(BucketState::Present(BucketParams::new()));
						bucket
					}
					None => Bucket::new(query.name.clone()),
				};
				self.garage.bucket_table.insert(&bucket).await?;
				Ok(AdminRpc::Ok(format!("Bucket {} was created.", query.name)))
			}
			BucketOperation::Delete(query) => {
				let mut bucket = self.get_existing_bucket(&query.name).await?;
				let objects = self
					.garage
					.object_table
					.get_range(&query.name, None, Some(DeletedFilter::NotDeleted), 10)
					.await?;
				if !objects.is_empty() {
					return Err(Error::BadRPC(format!("Bucket {} is not empty", query.name)));
				}
				if !query.yes {
					return Err(Error::BadRPC(
						"Add --yes flag to really perform this operation".to_string(),
					));
				}
				// --- done checking, now commit ---
				for (key_id, _, _) in bucket.authorized_keys() {
					if let Some(key) = self.garage.key_table.get(&EmptyKey, key_id).await? {
						if !key.deleted.get() {
							self.update_key_bucket(&key, &bucket.name, false, false)
								.await?;
						}
					} else {
						return Err(Error::Message(format!("Key not found: {}", key_id)));
					}
				}
				bucket.state.update(BucketState::Deleted);
				self.garage.bucket_table.insert(&bucket).await?;
				Ok(AdminRpc::Ok(format!("Bucket {} was deleted.", query.name)))
			}
			BucketOperation::Allow(query) => {
				let key = self.get_existing_key(&query.key_pattern).await?;
				let bucket = self.get_existing_bucket(&query.bucket).await?;
				let allow_read = query.read || key.allow_read(&query.bucket);
				let allow_write = query.write || key.allow_write(&query.bucket);
				self.update_key_bucket(&key, &query.bucket, allow_read, allow_write)
					.await?;
				self.update_bucket_key(bucket, &key.key_id, allow_read, allow_write)
					.await?;
				Ok(AdminRpc::Ok(format!(
					"New permissions for {} on {}: read {}, write {}.",
					&key.key_id, &query.bucket, allow_read, allow_write
				)))
			}
			BucketOperation::Deny(query) => {
				let key = self.get_existing_key(&query.key_pattern).await?;
				let bucket = self.get_existing_bucket(&query.bucket).await?;
				let allow_read = !query.read && key.allow_read(&query.bucket);
				let allow_write = !query.write && key.allow_write(&query.bucket);
				self.update_key_bucket(&key, &query.bucket, allow_read, allow_write)
					.await?;
				self.update_bucket_key(bucket, &key.key_id, allow_read, allow_write)
					.await?;
				Ok(AdminRpc::Ok(format!(
					"New permissions for {} on {}: read {}, write {}.",
					&key.key_id, &query.bucket, allow_read, allow_write
				)))
			}
			BucketOperation::Website(query) => {
				let mut bucket = self.get_existing_bucket(&query.bucket).await?;

				if !(query.allow ^ query.deny) {
					return Err(Error::Message(
						"You must specify exactly one flag, either --allow or --deny".to_string(),
					));
				}

				if let BucketState::Present(state) = bucket.state.get_mut() {
					state.website.update(query.allow);
					self.garage.bucket_table.insert(&bucket).await?;
					let msg = if query.allow {
						format!("Website access allowed for {}", &query.bucket)
					} else {
						format!("Website access denied for {}", &query.bucket)
					};

					Ok(AdminRpc::Ok(msg))
				} else {
					unreachable!();
				}
			}
		}
	}

	async fn handle_key_cmd(&self, cmd: KeyOperation) -> Result<AdminRpc, Error> {
		match cmd {
			KeyOperation::List => {
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
			KeyOperation::Info(query) => {
				let key = self.get_existing_key(&query.key_pattern).await?;
				Ok(AdminRpc::KeyInfo(key))
			}
			KeyOperation::New(query) => {
				let key = Key::new(query.name);
				self.garage.key_table.insert(&key).await?;
				Ok(AdminRpc::KeyInfo(key))
			}
			KeyOperation::Rename(query) => {
				let mut key = self.get_existing_key(&query.key_pattern).await?;
				key.name.update(query.new_name);
				self.garage.key_table.insert(&key).await?;
				Ok(AdminRpc::KeyInfo(key))
			}
			KeyOperation::Delete(query) => {
				let key = self.get_existing_key(&query.key_pattern).await?;
				if !query.yes {
					return Err(Error::BadRPC(
						"Add --yes flag to really perform this operation".to_string(),
					));
				}
				// --- done checking, now commit ---
				for (ab_name, _, _) in key.authorized_buckets.items().iter() {
					if let Some(bucket) = self.garage.bucket_table.get(&EmptyKey, ab_name).await? {
						if !bucket.is_deleted() {
							self.update_bucket_key(bucket, &key.key_id, false, false)
								.await?;
						}
					} else {
						return Err(Error::Message(format!("Bucket not found: {}", ab_name)));
					}
				}
				let del_key = Key::delete(key.key_id.to_string());
				self.garage.key_table.insert(&del_key).await?;
				Ok(AdminRpc::Ok(format!(
					"Key {} was deleted successfully.",
					key.key_id
				)))
			}
			KeyOperation::Import(query) => {
				let prev_key = self.garage.key_table.get(&EmptyKey, &query.key_id).await?;
				if prev_key.is_some() {
					return Err(Error::Message(format!("Key {} already exists in data store. Even if it is deleted, we can't let you create a new key with the same ID. Sorry.", query.key_id)));
				}
				let imported_key = Key::import(&query.key_id, &query.secret_key, &query.name);
				self.garage.key_table.insert(&imported_key).await?;
				Ok(AdminRpc::KeyInfo(imported_key))
			}
		}
	}

	#[allow(clippy::ptr_arg)]
	async fn get_existing_bucket(&self, bucket: &String) -> Result<Bucket, Error> {
		self.garage
			.bucket_table
			.get(&EmptyKey, bucket)
			.await?
			.filter(|b| !b.is_deleted())
			.map(Ok)
			.unwrap_or_else(|| Err(Error::BadRPC(format!("Bucket {} does not exist", bucket))))
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
			.filter(|k| !k.deleted.get())
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

	/// Update **bucket table** to inform of the new linked key
	async fn update_bucket_key(
		&self,
		mut bucket: Bucket,
		key_id: &str,
		allow_read: bool,
		allow_write: bool,
	) -> Result<(), Error> {
		if let BucketState::Present(params) = bucket.state.get_mut() {
			let ak = &mut params.authorized_keys;
			let old_ak = ak.take_and_clear();
			ak.merge(&old_ak.update_mutator(
				key_id.to_string(),
				PermissionSet {
					allow_read,
					allow_write,
				},
			));
		} else {
			return Err(Error::Message(
				"Bucket is deleted in update_bucket_key".to_string(),
			));
		}
		self.garage.bucket_table.insert(&bucket).await?;
		Ok(())
	}

	/// Update **key table** to inform of the new linked bucket
	async fn update_key_bucket(
		&self,
		key: &Key,
		bucket: &str,
		allow_read: bool,
		allow_write: bool,
	) -> Result<(), Error> {
		let mut key = key.clone();
		let old_map = key.authorized_buckets.take_and_clear();
		key.authorized_buckets.merge(&old_map.update_mutator(
			bucket.to_string(),
			PermissionSet {
				allow_read,
				allow_write,
			},
		));
		self.garage.key_table.insert(&key).await?;
		Ok(())
	}

	async fn handle_launch_repair(self: &Arc<Self>, opt: RepairOpt) -> Result<AdminRpc, Error> {
		if !opt.yes {
			return Err(Error::BadRPC(
				"Please provide the --yes flag to initiate repair operations.".to_string(),
			));
		}
		if opt.all_nodes {
			let mut opt_to_send = opt.clone();
			opt_to_send.all_nodes = false;

			let mut failures = vec![];
			let ring = self.garage.system.ring.borrow().clone();
			for node in ring.config.members.keys() {
				if self
					.rpc_client
					.call(
						*node,
						AdminRpc::LaunchRepair(opt_to_send.clone()),
						ADMIN_RPC_TIMEOUT,
					)
					.await
					.is_err()
				{
					failures.push(*node);
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

			for node in ring.config.members.keys() {
				let mut opt = opt.clone();
				opt.all_nodes = false;

				writeln!(&mut ret, "\n======================").unwrap();
				writeln!(&mut ret, "Stats for node {:?}:", node).unwrap();
				match self
					.rpc_client
					.call(*node, AdminRpc::Stats(opt), ADMIN_RPC_TIMEOUT)
					.await
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
			git_version::git_version!()
		)
		.unwrap();

		// Gather ring statistics
		let ring = self.garage.system.ring.borrow().clone();
		let mut ring_nodes = HashMap::new();
		for r in ring.ring.iter() {
			for n in r.nodes.iter() {
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
				"  number of blocks: {}",
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
		writeln!(to, "\nTable stats for {}", t.data.name).unwrap();
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
