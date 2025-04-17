pub mod admin_token;
pub mod bucket;
pub mod cluster;
pub mod key;
pub mod layout;

pub mod block;
pub mod node;
pub mod worker;

use std::convert::TryFrom;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};

use garage_util::error::*;

use garage_rpc::*;

use garage_api_admin::api::*;
use garage_api_admin::api_server::{AdminRpc as ProxyRpc, AdminRpcResponse as ProxyRpcResponse};
use garage_api_admin::RequestHandler;

use crate::cli::structs::*;

pub struct Cli {
	pub proxy_rpc_endpoint: Arc<Endpoint<ProxyRpc, ()>>,
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
			Command::Bucket(bo) => self.cmd_bucket(bo).await,
			Command::AdminToken(to) => self.cmd_admin_token(to).await,
			Command::Key(ko) => self.cmd_key(ko).await,
			Command::Worker(wo) => self.cmd_worker(wo).await,
			Command::Block(bo) => self.cmd_block(bo).await,
			Command::Meta(mo) => self.cmd_meta(mo).await,
			Command::Stats(so) => self.cmd_stats(so).await,
			Command::Repair(ro) => self.cmd_repair(ro).await,
			Command::JsonApi { endpoint, payload } => self.cmd_json_api(endpoint, payload).await,

			_ => unreachable!(),
		}
	}

	pub async fn api_request<T>(&self, req: T) -> Result<<T as RequestHandler>::Response, Error>
	where
		T: RequestHandler,
		AdminApiRequest: From<T>,
		<T as RequestHandler>::Response: TryFrom<TaggedAdminApiResponse>,
	{
		let req = AdminApiRequest::from(req);
		let req_name = req.name();
		match self
			.proxy_rpc_endpoint
			.call(&self.rpc_host, ProxyRpc::Proxy(req), PRIO_NORMAL)
			.await??
		{
			ProxyRpcResponse::ProxyApiOkResponse(resp) => {
				<T as RequestHandler>::Response::try_from(resp).map_err(|_| {
					Error::Message(format!("{} returned unexpected response", req_name))
				})
			}
			ProxyRpcResponse::ApiErrorResponse {
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

	pub async fn local_api_request<T>(
		&self,
		req: T,
	) -> Result<<T as RequestHandler>::Response, Error>
	where
		T: RequestHandler,
		MultiRequest<T>: RequestHandler<Response = MultiResponse<<T as RequestHandler>::Response>>,
		AdminApiRequest: From<MultiRequest<T>>,
		<MultiRequest<T> as RequestHandler>::Response: TryFrom<TaggedAdminApiResponse>,
	{
		let req = MultiRequest {
			node: hex::encode(self.rpc_host),
			body: req,
		};
		let resp = self.api_request(req).await?;

		if let Some((_, e)) = resp.error.into_iter().next() {
			return Err(Error::Message(e));
		}
		if resp.success.len() != 1 {
			return Err(Error::Message(format!(
				"{} responses returned, expected 1",
				resp.success.len()
			)));
		}
		Ok(resp.success.into_iter().next().unwrap().1)
	}

	pub async fn cmd_json_api(&self, endpoint: String, payload: String) -> Result<(), Error> {
		let payload: serde_json::Value = if payload == "-" {
			serde_json::from_reader(&std::io::stdin())?
		} else {
			serde_json::from_str(&payload)?
		};

		let request: AdminApiRequest = serde_json::from_value(serde_json::json!({
			endpoint.clone(): payload,
		}))?;

		let resp = match self
			.proxy_rpc_endpoint
			.call(&self.rpc_host, ProxyRpc::Proxy(request), PRIO_NORMAL)
			.await??
		{
			ProxyRpcResponse::ProxyApiOkResponse(resp) => resp,
			ProxyRpcResponse::ApiErrorResponse {
				http_code,
				error_code,
				message,
			} => {
				return Err(Error::Message(format!(
					"{} ({}): {}",
					error_code, http_code, message
				)))
			}
			m => return Err(Error::unexpected_rpc_message(m)),
		};

		if let serde_json::Value::Object(map) = serde_json::to_value(&resp)? {
			if let Some(inner) = map.get(&endpoint) {
				serde_json::to_writer_pretty(std::io::stdout(), &inner)?;
				return Ok(());
			}
		}

		Err(Error::Message(format!(
			"Invalid response: {}",
			serde_json::to_string(&resp)?
		)))
	}
}

pub fn table_list_abbr<T: IntoIterator<Item = S>, S: AsRef<str>>(values: T) -> String {
	let mut iter = values.into_iter();

	match iter.next() {
		Some(first) => match iter.count() {
			0 => first.as_ref().to_string(),
			n => format!("{}, ... ({})", first.as_ref(), n + 1),
		},
		None => String::new(),
	}
}

pub fn parse_expires_in(expires_in: &Option<String>) -> Result<Option<DateTime<Utc>>, Error> {
	expires_in
		.as_ref()
		.map(|x| parse_duration::parse::parse(&x).map(|dur| Utc::now() + dur))
		.transpose()
		.ok_or_message("Invalid duration passed for --expires-in parameter")
}
