#![recursion_limit = "1024"]
//! Garage CLI, used to interact with a running Garage instance, and to launch a Garage instance

#[macro_use]
extern crate log;

mod admin_rpc;
mod cli;
mod repair;
mod server;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use structopt::StructOpt;

use garage_util::config::TlsConfig;
use garage_util::error::Error;

use garage_rpc::membership::*;
use garage_rpc::rpc_client::*;

use admin_rpc::*;
use cli::*;

#[derive(StructOpt, Debug)]
#[structopt(name = "garage")]
struct Opt {
	/// RPC connect to this host to execute client operations
	#[structopt(short = "h", long = "rpc-host", default_value = "127.0.0.1:3901", parse(try_from_str = parse_address))]
	pub rpc_host: SocketAddr,

	#[structopt(long = "ca-cert")]
	pub ca_cert: Option<String>,
	#[structopt(long = "client-cert")]
	pub client_cert: Option<String>,
	#[structopt(long = "client-key")]
	pub client_key: Option<String>,

	#[structopt(subcommand)]
	cmd: Command,
}

#[tokio::main]
async fn main() {
	pretty_env_logger::init();

	let opt = Opt::from_args();

	let res = if let Command::Server(server_opt) = opt.cmd {
		// Abort on panic (same behavior as in Go)
		std::panic::set_hook(Box::new(|panic_info| {
			error!("{}", panic_info.to_string());
			std::process::abort();
		}));

		server::run_server(server_opt.config_file).await
	} else {
		cli_command(opt).await
	};

	if let Err(e) = res {
		error!("{}", e);
	}
}

async fn cli_command(opt: Opt) -> Result<(), Error> {
	let tls_config = match (opt.ca_cert, opt.client_cert, opt.client_key) {
		(Some(ca_cert), Some(client_cert), Some(client_key)) => Some(TlsConfig {
			ca_cert,
			node_cert: client_cert,
			node_key: client_key,
		}),
		(None, None, None) => None,
		_ => {
			warn!("Missing one of: --ca-cert, --node-cert, --node-key. Not using TLS.");
			None
		}
	};

	let rpc_http_cli =
		Arc::new(RpcHttpClient::new(8, &tls_config).expect("Could not create RPC client"));
	let membership_rpc_cli =
		RpcAddrClient::new(rpc_http_cli.clone(), MEMBERSHIP_RPC_PATH.to_string());
	let admin_rpc_cli = RpcAddrClient::new(rpc_http_cli.clone(), ADMIN_RPC_PATH.to_string());

	cli_cmd(opt.cmd, membership_rpc_cli, admin_rpc_cli, opt.rpc_host).await
}

fn parse_address(address: &str) -> Result<SocketAddr, String> {
	use std::net::ToSocketAddrs;
	address
		.to_socket_addrs()
		.map_err(|_| format!("Could not resolve {}", address))?
		.next()
		.ok_or_else(|| format!("Could not resolve {}", address))
}
