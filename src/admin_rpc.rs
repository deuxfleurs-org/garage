use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::data::*;
use crate::error::Error;
use crate::server::Garage;

use crate::table::*;

use crate::rpc::rpc_client::*;
use crate::rpc::rpc_server::*;

use crate::store::bucket_table::*;
use crate::store::repair::Repair;

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
					_ => Err(Error::BadRequest(format!("Invalid RPC"))),
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
					.get_range(&EmptyKey, None, Some(()), 10000)
					.await?
					.iter()
					.map(|b| b.name.to_string())
					.collect::<Vec<_>>();
				Ok(AdminRPC::BucketList(bucket_names))
			}
			BucketOperation::Info(query) => {
				let bucket = self
					.garage
					.bucket_table
					.get(&EmptyKey, &query.name)
					.await?
					.filter(|b| !b.deleted);
				match bucket {
					Some(b) => Ok(AdminRPC::BucketInfo(b)),
					None => Err(Error::BadRequest(format!(
						"Bucket {} not found",
						query.name
					))),
				}
			}
			BucketOperation::Create(query) => {
				let bucket = self.garage.bucket_table.get(&EmptyKey, &query.name).await?;
				if bucket.as_ref().filter(|b| !b.deleted).is_some() {
					return Err(Error::BadRequest(format!(
						"Bucket {} already exists",
						query.name
					)));
				}
				let new_time = match bucket {
					Some(b) => std::cmp::max(b.timestamp + 1, now_msec()),
					None => now_msec(),
				};
				self.garage
					.bucket_table
					.insert(&Bucket::new(query.name.clone(), new_time, false, vec![]))
					.await?;
				Ok(AdminRPC::Ok(format!("Bucket {} was created.", query.name)))
			}
			BucketOperation::Delete(query) => {
				let bucket = match self
					.garage
					.bucket_table
					.get(&EmptyKey, &query.name)
					.await?
					.filter(|b| !b.deleted)
				{
					None => {
						return Err(Error::BadRequest(format!(
							"Bucket {} does not exist",
							query.name
						)));
					}
					Some(b) => b,
				};
				let objects = self
					.garage
					.object_table
					.get_range(&query.name, None, Some(()), 10)
					.await?;
				if !objects.is_empty() {
					return Err(Error::BadRequest(format!(
						"Bucket {} is not empty",
						query.name
					)));
				}
				if !query.yes {
					return Err(Error::BadRequest(format!(
						"Add --yes flag to really perform this operation"
					)));
				}
				self.garage
					.bucket_table
					.insert(&Bucket::new(
						query.name.clone(),
						std::cmp::max(bucket.timestamp + 1, now_msec()),
						true,
						vec![],
					))
					.await?;
				Ok(AdminRPC::Ok(format!("Bucket {} was deleted.", query.name)))
			}
			_ => {
				// TODO
				Err(Error::Message(format!("Not implemented")))
			}
		}
	}

	async fn handle_key_cmd(&self, cmd: KeyOperation) -> Result<AdminRPC, Error> {
		Err(Error::Message(format!("Not implemented")))
	}

	async fn handle_launch_repair(self: &Arc<Self>, opt: RepairOpt) -> Result<AdminRPC, Error> {
		if !opt.yes {
			return Err(Error::BadRequest(format!(
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
