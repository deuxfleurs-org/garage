pub mod util;

pub mod cluster;
pub mod layout;

use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::Duration;

use garage_util::error::*;

use garage_rpc::system::*;
use garage_rpc::*;

use garage_api::admin::api::*;
use garage_api::admin::EndpointHandler as AdminApiEndpoint;

use crate::admin::*;
use crate::cli as cli_v1;
use crate::cli::structs::*;
use crate::cli::Command;

pub struct Cli {
	pub system_rpc_endpoint: Arc<Endpoint<SystemRpc, ()>>,
	pub admin_rpc_endpoint: Arc<Endpoint<AdminRpc, ()>>,
	pub rpc_host: NodeID,
}

impl Cli {
	pub async fn handle(&self, cmd: Command) -> Result<(), Error> {
		match cmd {
			Command::Status => self.cmd_status().await,
			Command::Node(NodeOperation::Connect(connect_opt)) => {
				self.cmd_connect(connect_opt).await
			}
			Command::Layout(layout_opt) => self.layout_command_dispatch(layout_opt).await,

			// TODO
			Command::Bucket(bo) => cli_v1::cmd_admin(
				&self.admin_rpc_endpoint,
				self.rpc_host,
				AdminRpc::BucketOperation(bo),
			)
			.await
			.ok_or_message("xoxo"),
			Command::Key(ko) => cli_v1::cmd_admin(
				&self.admin_rpc_endpoint,
				self.rpc_host,
				AdminRpc::KeyOperation(ko),
			)
			.await
			.ok_or_message("xoxo"),
			Command::Repair(ro) => cli_v1::cmd_admin(
				&self.admin_rpc_endpoint,
				self.rpc_host,
				AdminRpc::LaunchRepair(ro),
			)
			.await
			.ok_or_message("xoxo"),
			Command::Stats(so) => {
				cli_v1::cmd_admin(&self.admin_rpc_endpoint, self.rpc_host, AdminRpc::Stats(so))
					.await
					.ok_or_message("xoxo")
			}
			Command::Worker(wo) => cli_v1::cmd_admin(
				&self.admin_rpc_endpoint,
				self.rpc_host,
				AdminRpc::Worker(wo),
			)
			.await
			.ok_or_message("xoxo"),
			Command::Block(bo) => cli_v1::cmd_admin(
				&self.admin_rpc_endpoint,
				self.rpc_host,
				AdminRpc::BlockOperation(bo),
			)
			.await
			.ok_or_message("xoxo"),
			Command::Meta(mo) => cli_v1::cmd_admin(
				&self.admin_rpc_endpoint,
				self.rpc_host,
				AdminRpc::MetaOperation(mo),
			)
			.await
			.ok_or_message("xoxo"),

			_ => unreachable!(),
		}
	}

	pub async fn api_request<T>(&self, req: T) -> Result<<T as AdminApiEndpoint>::Response, Error>
	where
		T: AdminApiEndpoint,
		AdminApiRequest: From<T>,
		<T as AdminApiEndpoint>::Response: TryFrom<TaggedAdminApiResponse>,
	{
		let req = AdminApiRequest::from(req);
		let req_name = req.name();
		match self
			.admin_rpc_endpoint
			.call(&self.rpc_host, AdminRpc::ApiRequest(req), PRIO_NORMAL)
			.await?
			.ok_or_message("xoxo")?
		{
			AdminRpc::ApiOkResponse(resp) => <T as AdminApiEndpoint>::Response::try_from(resp)
				.map_err(|_| Error::Message(format!("{} returned unexpected response", req_name))),
			AdminRpc::ApiErrorResponse {
				http_code,
				error_code,
				message,
			} => Err(Error::Message(format!(
				"{} returned {} ({}): {}",
				req_name, error_code, http_code, message
			))),
			m => Err(Error::unexpected_rpc_message(m)),
		}
	}
}
