use garage_util::error::*;

use garage_rpc::system::*;
use garage_rpc::*;

use garage_model::helper::error::Error as HelperError;

use crate::admin::*;

pub async fn cmd_admin(
	rpc_cli: &Endpoint<AdminRpc, ()>,
	rpc_host: NodeID,
	args: AdminRpc,
) -> Result<(), HelperError> {
	match rpc_cli.call(&rpc_host, args, PRIO_NORMAL).await?? {
		AdminRpc::Ok(msg) => {
			println!("{}", msg);
		}
		r => {
			error!("Unexpected response: {:?}", r);
		}
	}
	Ok(())
}

// ---- utility ----

pub async fn fetch_status(
	rpc_cli: &Endpoint<SystemRpc, ()>,
	rpc_host: NodeID,
) -> Result<Vec<KnownNodeInfo>, Error> {
	match rpc_cli
		.call(&rpc_host, SystemRpc::GetKnownNodes, PRIO_NORMAL)
		.await??
	{
		SystemRpc::ReturnKnownNodes(nodes) => Ok(nodes),
		resp => Err(Error::unexpected_rpc_message(resp)),
	}
}
