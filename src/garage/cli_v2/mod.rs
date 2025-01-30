use std::collections::{HashMap, HashSet};
use std::convert::TryFrom;
use std::sync::Arc;
use std::time::Duration;

use format_table::format_table;
use garage_util::error::*;

use garage_rpc::layout::*;
use garage_rpc::system::*;
use garage_rpc::*;

use garage_api::admin::api::*;
use garage_api::admin::EndpointHandler as AdminApiEndpoint;

use crate::admin::*;
use crate::cli::*;

pub struct Cli {
	pub system_rpc_endpoint: Arc<Endpoint<SystemRpc, ()>>,
	pub admin_rpc_endpoint: Arc<Endpoint<AdminRpc, ()>>,
	pub rpc_host: NodeID,
}

impl Cli {
	pub async fn handle(&self, cmd: Command) -> Result<(), Error> {
		println!("{:?}", self.api_request(GetClusterStatusRequest).await?);
		Ok(())
		/*
		match cmd {
			_ => todo!(),
		}
		*/
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
