use std::sync::Arc;

use serde::{Deserialize, Serialize};

use garage_util::error::Error;

use garage_table::crdt::CRDT;
use garage_table::*;

use garage_rpc::rpc_client::*;
use garage_rpc::rpc_server::*;

use garage_model::bucket_table::*;
use garage_model::garage::Garage;
use garage_model::key_table::*;

use crate::repair::Repair;
use crate::*;

pub const ADMIN_RPC_TIMEOUT: Duration = Duration::from_secs(30);
pub const ADMIN_RPC_PATH: &str = "_admin";

#[derive(Debug, Serialize, Deserialize)]
pub enum AdminRPC {
	BucketOperation(BucketOperation),
	KeyOperation(KeyOperation),
	LaunchRepair(RepairOpt),

	// Replies
	Ok(String),
	BucketList(Vec<String>),
	BucketInfo(Bucket),
	KeyList(Vec<(String, String)>),
	KeyInfo(Key),
}

impl RpcMessage for AdminRPC {}

pub struct AdminRpcHandler {
	garage: Arc<Garage>,
	rpc_client: Arc<RpcClient<AdminRPC>>,
}

impl AdminRpcHandler {
	pub fn new(garage: Arc<Garage>) -> Arc<Self> {
		let rpc_client = garage.system.clone().rpc_client::<AdminRPC>(ADMIN_RPC_PATH);
		Arc::new(Self { garage, rpc_client })
	}

	pub fn register_handler(self: Arc<Self>, rpc_server: &mut RpcServer) {
		rpc_server.add_handler::<AdminRPC, _, _>(ADMIN_RPC_PATH.to_string(), move |msg, _addr| {
			let self2 = self.clone();
			async move {
				match msg {
					AdminRPC::BucketOperation(bo) => self2.handle_bucket_cmd(bo).await,
					AdminRPC::KeyOperation(ko) => self2.handle_key_cmd(ko).await,
					AdminRPC::LaunchRepair(opt) => self2.handle_launch_repair(opt).await,
					_ => Err(Error::BadRPC(format!("Invalid RPC"))),
				}
			}
		});
	}

	async fn handle_bucket_cmd(&self, cmd: BucketOperation) -> Result<AdminRPC, Error> {
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
				Ok(AdminRPC::BucketList(bucket_names))
			}
			BucketOperation::Info(query) => {
				let bucket = self.get_existing_bucket(&query.name).await?;
				Ok(AdminRPC::BucketInfo(bucket))
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
				Ok(AdminRPC::Ok(format!("Bucket {} was created.", query.name)))
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
					return Err(Error::BadRPC(format!(
						"Add --yes flag to really perform this operation"
					)));
				}
				// --- done checking, now commit ---
				for (key_id, _, _) in bucket.authorized_keys() {
					if let Some(key) = self.garage.key_table.get(&EmptyKey, key_id).await? {
						if !key.deleted.get() {
							self.update_key_bucket(key, &bucket.name, false, false)
								.await?;
						}
					} else {
						return Err(Error::Message(format!("Key not found: {}", key_id)));
					}
				}
				bucket.state.update(BucketState::Deleted);
				self.garage.bucket_table.insert(&bucket).await?;
				Ok(AdminRPC::Ok(format!("Bucket {} was deleted.", query.name)))
			}
			BucketOperation::Allow(query) => {
				let key = self.get_existing_key(&query.key_id).await?;
				let bucket = self.get_existing_bucket(&query.bucket).await?;
				let allow_read = query.read || key.allow_read(&query.bucket);
				let allow_write = query.write || key.allow_write(&query.bucket);
				self.update_key_bucket(key, &query.bucket, allow_read, allow_write)
					.await?;
				self.update_bucket_key(bucket, &query.key_id, allow_read, allow_write)
					.await?;
				Ok(AdminRPC::Ok(format!(
					"New permissions for {} on {}: read {}, write {}.",
					&query.key_id, &query.bucket, allow_read, allow_write
				)))
			}
			BucketOperation::Deny(query) => {
				let key = self.get_existing_key(&query.key_id).await?;
				let bucket = self.get_existing_bucket(&query.bucket).await?;
				let allow_read = !query.read && key.allow_read(&query.bucket);
				let allow_write = !query.write && key.allow_write(&query.bucket);
				self.update_key_bucket(key, &query.bucket, allow_read, allow_write)
					.await?;
				self.update_bucket_key(bucket, &query.key_id, allow_read, allow_write)
					.await?;
				Ok(AdminRPC::Ok(format!(
					"New permissions for {} on {}: read {}, write {}.",
					&query.key_id, &query.bucket, allow_read, allow_write
				)))
			}
			BucketOperation::Website(query) => {
				let mut bucket = self.get_existing_bucket(&query.bucket).await?;

				if !(query.allow ^ query.deny) {
					return Err(Error::Message(format!(
						"You must specify exactly one flag, either --allow or --deny"
					)));
				}

				if let BucketState::Present(state) = bucket.state.get_mut() {
					state.website.update(query.allow);
					self.garage.bucket_table.insert(&bucket).await?;
					let msg = if query.allow {
						format!("Website access allowed for {}", &query.bucket)
					} else {
						format!("Website access denied for {}", &query.bucket)
					};

					Ok(AdminRPC::Ok(msg.to_string()))
				} else {
					return Err(Error::Message(format!(
						"Bucket is deleted in update_bucket_key"
					)));
				}
			}
		}
	}

	async fn handle_key_cmd(&self, cmd: KeyOperation) -> Result<AdminRPC, Error> {
		match cmd {
			KeyOperation::List => {
				let key_ids = self
					.garage
					.key_table
					.get_range(&EmptyKey, None, Some(DeletedFilter::NotDeleted), 10000)
					.await?
					.iter()
					.map(|k| (k.key_id.to_string(), k.name.get().clone()))
					.collect::<Vec<_>>();
				Ok(AdminRPC::KeyList(key_ids))
			}
			KeyOperation::Info(query) => {
				let key = self.get_existing_key(&query.key_id).await?;
				Ok(AdminRPC::KeyInfo(key))
			}
			KeyOperation::New(query) => {
				let key = Key::new(query.name);
				self.garage.key_table.insert(&key).await?;
				Ok(AdminRPC::KeyInfo(key))
			}
			KeyOperation::Rename(query) => {
				let mut key = self.get_existing_key(&query.key_id).await?;
				key.name.update(query.new_name);
				self.garage.key_table.insert(&key).await?;
				Ok(AdminRPC::KeyInfo(key))
			}
			KeyOperation::Delete(query) => {
				let key = self.get_existing_key(&query.key_id).await?;
				if !query.yes {
					return Err(Error::BadRPC(format!(
						"Add --yes flag to really perform this operation"
					)));
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
				let del_key = Key::delete(key.key_id);
				self.garage.key_table.insert(&del_key).await?;
				Ok(AdminRPC::Ok(format!(
					"Key {} was deleted successfully.",
					query.key_id
				)))
			}
		}
	}

	async fn get_existing_bucket(&self, bucket: &String) -> Result<Bucket, Error> {
		self.garage
			.bucket_table
			.get(&EmptyKey, bucket)
			.await?
			.filter(|b| !b.is_deleted())
			.map(Ok)
			.unwrap_or(Err(Error::BadRPC(format!(
				"Bucket {} does not exist",
				bucket
			))))
	}

	async fn get_existing_key(&self, id: &String) -> Result<Key, Error> {
		self.garage
			.key_table
			.get(&EmptyKey, id)
			.await?
			.filter(|k| !k.deleted.get())
			.map(Ok)
			.unwrap_or(Err(Error::BadRPC(format!("Key {} does not exist", id))))
	}

	/// Update **bucket table** to inform of the new linked key
	async fn update_bucket_key(
		&self,
		mut bucket: Bucket,
		key_id: &String,
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
			return Err(Error::Message(format!(
				"Bucket is deleted in update_bucket_key"
			)));
		}
		self.garage.bucket_table.insert(&bucket).await?;
		Ok(())
	}

	/// Update **key table** to inform of the new linked bucket
	async fn update_key_bucket(
		&self,
		mut key: Key,
		bucket: &String,
		allow_read: bool,
		allow_write: bool,
	) -> Result<(), Error> {
		let old_map = key.authorized_buckets.take_and_clear();
		key.authorized_buckets.merge(&old_map.update_mutator(
			bucket.clone(),
			PermissionSet {
				allow_read,
				allow_write,
			},
		));
		self.garage.key_table.insert(&key).await?;
		Ok(())
	}

	async fn handle_launch_repair(self: &Arc<Self>, opt: RepairOpt) -> Result<AdminRPC, Error> {
		if !opt.yes {
			return Err(Error::BadRPC(format!(
				"Please provide the --yes flag to initiate repair operations."
			)));
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
						AdminRPC::LaunchRepair(opt_to_send.clone()),
						ADMIN_RPC_TIMEOUT,
					)
					.await
					.is_err()
				{
					failures.push(node.clone());
				}
			}
			if failures.is_empty() {
				Ok(AdminRPC::Ok(format!("Repair launched on all nodes")))
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
				})
				.await;
			Ok(AdminRPC::Ok(format!(
				"Repair launched on {:?}",
				self.garage.system.id
			)))
		}
	}
}
