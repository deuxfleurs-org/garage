use std::sync::Arc;

use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use crate::data::*;
use crate::error::Error;
use crate::rpc_client::*;
use crate::rpc_server::*;
use crate::server::Garage;
use crate::table::*;
use crate::*;

use crate::block_ref_table::*;
use crate::bucket_table::*;
use crate::version_table::*;

pub const ADMIN_RPC_TIMEOUT: Duration = Duration::from_secs(30);
pub const ADMIN_RPC_PATH: &str = "_admin";

#[derive(Debug, Serialize, Deserialize)]
pub enum AdminRPC {
	BucketOperation(BucketOperation),
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
					.insert(&Bucket {
						name: query.name.clone(),
						timestamp: new_time,
						deleted: false,
						authorized_keys: vec![],
					})
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
					.insert(&Bucket {
						name: query.name.clone(),
						timestamp: std::cmp::max(bucket.timestamp + 1, now_msec()),
						deleted: true,
						authorized_keys: vec![],
					})
					.await?;
				Ok(AdminRPC::Ok(format!("Bucket {} was deleted.", query.name)))
			}
			_ => {
				// TODO
				Err(Error::Message(format!("Not implemented")))
			}
		}
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
			let self2 = self.clone();
			self.garage
				.system
				.background
				.spawn_worker("Repair worker".into(), move |must_exit| async move {
					self2.repair_worker(opt, must_exit).await
				})
				.await;
			Ok(AdminRPC::Ok(format!(
				"Repair launched on {:?}",
				self.garage.system.id
			)))
		}
	}

	async fn repair_worker(
		self: Arc<Self>,
		opt: RepairOpt,
		must_exit: watch::Receiver<bool>,
	) -> Result<(), Error> {
		let todo = |x| opt.what.as_ref().map(|y| *y == x).unwrap_or(true);

		if todo(RepairWhat::Tables) {
			info!("Launching a full sync of tables");
			self.garage
				.bucket_table
				.syncer
				.load_full()
				.unwrap()
				.add_full_scan()
				.await;
			self.garage
				.object_table
				.syncer
				.load_full()
				.unwrap()
				.add_full_scan()
				.await;
			self.garage
				.version_table
				.syncer
				.load_full()
				.unwrap()
				.add_full_scan()
				.await;
			self.garage
				.block_ref_table
				.syncer
				.load_full()
				.unwrap()
				.add_full_scan()
				.await;
		}

		// TODO: wait for full sync to finish before proceeding to the rest?

		if todo(RepairWhat::Versions) {
			info!("Repairing the versions table");
			self.repair_versions(&must_exit).await?;
		}

		if todo(RepairWhat::BlockRefs) {
			info!("Repairing the block refs table");
			self.repair_block_ref(&must_exit).await?;
		}

		if opt.what.is_none() {
			info!("Repairing the RC");
			self.repair_rc(&must_exit).await?;
		}

		if todo(RepairWhat::Blocks) {
			info!("Repairing the stored blocks");
			self.garage
				.block_manager
				.repair_data_store(&must_exit)
				.await?;
		}

		Ok(())
	}

	async fn repair_versions(&self, must_exit: &watch::Receiver<bool>) -> Result<(), Error> {
		let mut pos = vec![];

		while let Some((item_key, item_bytes)) = self.garage.version_table.store.get_gt(&pos)? {
			pos = item_key.to_vec();

			let version = rmp_serde::decode::from_read_ref::<_, Version>(item_bytes.as_ref())?;
			if version.deleted {
				continue;
			}
			let object = self
				.garage
				.object_table
				.get(&version.bucket, &version.key)
				.await?;
			let version_exists = match object {
				Some(o) => o.versions.iter().any(|x| x.uuid == version.uuid),
				None => {
					warn!(
						"Repair versions: object for version {:?} not found",
						version
					);
					false
				}
			};
			if !version_exists {
				info!("Repair versions: marking version as deleted: {:?}", version);
				self.garage
					.version_table
					.insert(&Version {
						uuid: version.uuid,
						deleted: true,
						blocks: vec![],
						bucket: version.bucket,
						key: version.key,
					})
					.await?;
			}

			if *must_exit.borrow() {
				break;
			}
		}
		Ok(())
	}

	async fn repair_block_ref(&self, must_exit: &watch::Receiver<bool>) -> Result<(), Error> {
		let mut pos = vec![];

		while let Some((item_key, item_bytes)) = self.garage.block_ref_table.store.get_gt(&pos)? {
			pos = item_key.to_vec();

			let block_ref = rmp_serde::decode::from_read_ref::<_, BlockRef>(item_bytes.as_ref())?;
			if block_ref.deleted {
				continue;
			}
			let version = self
				.garage
				.version_table
				.get(&block_ref.version, &EmptyKey)
				.await?;
			let ref_exists = match version {
				Some(v) => !v.deleted,
				None => {
					warn!(
						"Block ref repair: version for block ref {:?} not found",
						block_ref
					);
					false
				}
			};
			if !ref_exists {
				info!(
					"Repair block ref: marking block_ref as deleted: {:?}",
					block_ref
				);
				self.garage
					.block_ref_table
					.insert(&BlockRef {
						block: block_ref.block,
						version: block_ref.version,
						deleted: true,
					})
					.await?;
			}

			if *must_exit.borrow() {
				break;
			}
		}
		Ok(())
	}

	async fn repair_rc(&self, _must_exit: &watch::Receiver<bool>) -> Result<(), Error> {
		// TODO
		warn!("repair_rc: not implemented");
		Ok(())
	}
}
