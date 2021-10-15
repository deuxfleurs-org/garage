#![recursion_limit = "1024"]
//! Garage CLI, used to interact with a running Garage instance, and to launch a Garage instance

#[macro_use]
extern crate log;

mod admin_rpc;
mod cli;
mod repair;
mod server;

use structopt::StructOpt;

use netapp::util::parse_peer_addr;
use netapp::NetworkKey;

use garage_util::error::Error;

use garage_rpc::system::*;
use garage_rpc::*;

use admin_rpc::*;
use cli::*;

#[derive(StructOpt, Debug)]
#[structopt(name = "garage")]
struct Opt {
	/// Host to connect to for admin operations, in the format:
	/// <public-key>@<ip>:<port>
	#[structopt(short = "h", long = "rpc-host")]
	pub rpc_host: Option<String>,

	/// RPC secret network key for admin operations
	#[structopt(short = "s", long = "rpc-secret")]
	pub rpc_secret: Option<String>,

	#[structopt(subcommand)]
	cmd: Command,
}

#[tokio::main]
async fn main() {
	pretty_env_logger::init();
	sodiumoxide::init().expect("Unable to init sodiumoxide");

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
	let net_key_hex_str = &opt.rpc_secret.expect("No RPC secret provided");
	let network_key = NetworkKey::from_slice(
		&hex::decode(net_key_hex_str).expect("Invalid RPC secret key (bad hex)")[..],
	)
	.expect("Invalid RPC secret provided (wrong length)");
	let (_pk, sk) = sodiumoxide::crypto::sign::ed25519::gen_keypair();

	let netapp = NetApp::new(network_key, sk);
	let (id, addr) =
		parse_peer_addr(&opt.rpc_host.expect("No RPC host provided")).expect("Invalid RPC host");
	netapp.clone().try_connect(addr, id).await?;

	let system_rpc_endpoint = netapp.endpoint::<SystemRpc, ()>(SYSTEM_RPC_PATH.into());
	let admin_rpc_endpoint = netapp.endpoint::<AdminRpc, ()>(ADMIN_RPC_PATH.into());

	cli_cmd(opt.cmd, &system_rpc_endpoint, &admin_rpc_endpoint, id).await
}
