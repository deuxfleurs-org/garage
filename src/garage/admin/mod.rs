use std::sync::Arc;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use garage_util::background::BackgroundRunner;
use garage_util::error::Error as GarageError;

use garage_rpc::*;

use garage_model::garage::Garage;
use garage_model::helper::error::Error;

use crate::cli::*;
use crate::repair::online::launch_online_repair;

pub const ADMIN_RPC_PATH: &str = "garage/admin_rpc.rs/Rpc";

#[derive(Debug, Serialize, Deserialize)]
#[allow(clippy::large_enum_variant)]
pub enum AdminRpc {
	LaunchRepair(RepairOpt),

	// Replies
	Ok(String),
}

impl Rpc for AdminRpc {
	type Response = Result<AdminRpc, Error>;
}

pub struct AdminRpcHandler {
	garage: Arc<Garage>,
	background: Arc<BackgroundRunner>,
	endpoint: Arc<Endpoint<AdminRpc, Self>>,
}

impl AdminRpcHandler {
	pub fn new(garage: Arc<Garage>, background: Arc<BackgroundRunner>) -> Arc<Self> {
		let endpoint = garage.system.netapp.endpoint(ADMIN_RPC_PATH.into());
		let admin = Arc::new(Self {
			garage,
			background,
			endpoint,
		});
		admin.endpoint.set_handler(admin.clone());
		admin
	}

	// ================ REPAIR COMMANDS ====================

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
			let all_nodes = self.garage.system.cluster_layout().all_nodes().to_vec();
			for node in all_nodes.iter() {
				let node = (*node).into();
				let resp = self
					.endpoint
					.call(
						&node,
						AdminRpc::LaunchRepair(opt_to_send.clone()),
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
			launch_online_repair(&self.garage, &self.background, opt).await?;
			Ok(AdminRpc::Ok(format!(
				"Repair launched on {:?}",
				self.garage.system.id
			)))
		}
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
			AdminRpc::LaunchRepair(opt) => self.handle_launch_repair(opt.clone()).await,
			m => Err(GarageError::unexpected_rpc_message(m).into()),
		}
	}
}
