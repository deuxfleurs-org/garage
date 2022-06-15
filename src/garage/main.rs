#![recursion_limit = "1024"]
//! Garage CLI, used to interact with a running Garage instance, and to launch a Garage instance

#[macro_use]
extern crate tracing;

mod admin;
mod cli;
mod repair;
mod server;
mod tracing_setup;

use std::net::SocketAddr;
use std::path::PathBuf;

use structopt::StructOpt;

use netapp::util::parse_and_resolve_peer_addr;
use netapp::NetworkKey;

use garage_util::error::*;

use garage_rpc::system::*;
use garage_rpc::*;

use garage_model::helper::error::Error as HelperError;

use admin::*;
use cli::*;

#[derive(StructOpt, Debug)]
#[structopt(name = "garage")]
struct Opt {
	/// Host to connect to for admin operations, in the format:
	/// <public-key>@<ip>:<port>
	#[structopt(short = "h", long = "rpc-host", env = "GARAGE_RPC_HOST")]
	pub rpc_host: Option<String>,

	/// RPC secret network key for admin operations
	#[structopt(short = "s", long = "rpc-secret", env = "GARAGE_RPC_SECRET")]
	pub rpc_secret: Option<String>,

	/// Configuration file (garage.toml)
	#[structopt(
		short = "c",
		long = "config",
		env = "GARAGE_CONFIG_FILE",
		default_value = "/etc/garage.toml"
	)]
	pub config_file: PathBuf,

	#[structopt(subcommand)]
	cmd: Command,
}

#[tokio::main]
async fn main() {
	if std::env::var("RUST_LOG").is_err() {
		std::env::set_var("RUST_LOG", "netapp=info,garage=info")
	}
	pretty_env_logger::init();
	sodiumoxide::init().expect("Unable to init sodiumoxide");

	// Abort on panic (same behavior as in Go)
	std::panic::set_hook(Box::new(|panic_info| {
		error!("{}", panic_info.to_string());
		std::process::abort();
	}));

	let opt = Opt::from_args();
	let res = match opt.cmd {
		Command::Server => server::run_server(opt.config_file).await,
		Command::OfflineRepair(repair_opt) => {
			repair::offline::offline_repair(opt.config_file, repair_opt).await
		}
		Command::Node(NodeOperation::NodeId(node_id_opt)) => {
			node_id_command(opt.config_file, node_id_opt.quiet)
		}
		_ => cli_command(opt).await,
	};

	if let Err(e) = res {
		eprintln!("Error: {}", e);
		std::process::exit(1);
	}
}

async fn cli_command(opt: Opt) -> Result<(), Error> {
	let config = if opt.rpc_secret.is_none() || opt.rpc_host.is_none() {
		Some(garage_util::config::read_config(opt.config_file.clone())
			.err_context(format!("Unable to read configuration file {}. Configuration file is needed because -h or -s is not provided on the command line.", opt.config_file.to_string_lossy()))?)
	} else {
		None
	};

	// Find and parse network RPC secret
	let net_key_hex_str = opt
		.rpc_secret
		.as_ref()
		.or_else(|| config.as_ref().map(|c| &c.rpc_secret))
		.ok_or("No RPC secret provided")?;
	let network_key = NetworkKey::from_slice(
		&hex::decode(net_key_hex_str).err_context("Invalid RPC secret key (bad hex)")?[..],
	)
	.ok_or("Invalid RPC secret provided (wrong length)")?;

	// Generate a temporary keypair for our RPC client
	let (_pk, sk) = sodiumoxide::crypto::sign::ed25519::gen_keypair();

	let netapp = NetApp::new(GARAGE_VERSION_TAG, network_key, sk);

	// Find and parse the address of the target host
	let (id, addr) = if let Some(h) = opt.rpc_host {
		let (id, addrs) = parse_and_resolve_peer_addr(&h).ok_or_else(|| format!("Invalid RPC remote node identifier: {}. Expected format is <pubkey>@<IP or hostname>:<port>.", h))?;
		(id, addrs[0])
	} else {
		let node_id = garage_rpc::system::read_node_id(&config.as_ref().unwrap().metadata_dir)
			.err_context(READ_KEY_ERROR)?;
		if let Some(a) = config.as_ref().and_then(|c| c.rpc_public_addr) {
			(node_id, a)
		} else {
			let default_addr = SocketAddr::new(
				"127.0.0.1".parse().unwrap(),
				config.as_ref().unwrap().rpc_bind_addr.port(),
			);
			warn!(
				"Trying to contact Garage node at default address {}",
				default_addr
			);
			warn!("If this doesn't work, consider adding rpc_public_addr in your config file or specifying the -h command line parameter.");
			(node_id, default_addr)
		}
	};

	// Connect to target host
	netapp.clone().try_connect(addr, id).await
		.err_context("Unable to connect to destination RPC host. Check that you are using the same value of rpc_secret as them, and that you have their correct public key.")?;

	let system_rpc_endpoint = netapp.endpoint::<SystemRpc, ()>(SYSTEM_RPC_PATH.into());
	let admin_rpc_endpoint = netapp.endpoint::<AdminRpc, ()>(ADMIN_RPC_PATH.into());

	match cli_command_dispatch(opt.cmd, &system_rpc_endpoint, &admin_rpc_endpoint, id).await {
		Err(HelperError::Internal(i)) => Err(Error::Message(format!("Internal error: {}", i))),
		Err(HelperError::BadRequest(b)) => Err(Error::Message(b)),
		Err(e) => Err(Error::Message(format!("{}", e))),
		Ok(x) => Ok(x),
	}
}
