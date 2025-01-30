use garage_util::error::*;

use garage_rpc::system::*;
use garage_rpc::*;

use garage_model::helper::error::Error as HelperError;

use crate::admin::*;
use crate::cli::*;

pub async fn cmd_admin(
	rpc_cli: &Endpoint<AdminRpc, ()>,
	rpc_host: NodeID,
	args: AdminRpc,
) -> Result<(), HelperError> {
	match rpc_cli.call(&rpc_host, args, PRIO_NORMAL).await?? {
		AdminRpc::Ok(msg) => {
			println!("{}", msg);
		}
		AdminRpc::WorkerList(wi, wlo) => {
			print_worker_list(wi, wlo);
		}
		AdminRpc::WorkerInfo(tid, wi) => {
			print_worker_info(tid, wi);
		}
		AdminRpc::BlockErrorList(el) => {
			print_block_error_list(el);
		}
		AdminRpc::BlockInfo {
			hash,
			refcount,
			versions,
			uploads,
		} => {
			print_block_info(hash, refcount, versions, uploads);
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
