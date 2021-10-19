#![recursion_limit = "1024"]
//! Garage CLI, used to interact with a running Garage instance, and to launch a Garage instance

#[macro_use]
extern crate log;

mod admin_rpc;
mod cli;
mod repair;
mod server;

use std::path::PathBuf;

use structopt::StructOpt;

use netapp::util::parse_and_resolve_peer_addr;
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

	/// Configuration file (garage.toml)
	#[structopt(short = "c", long = "config", default_value = "/etc/garage.toml")]
	pub config_file: PathBuf,

	#[structopt(subcommand)]
	cmd: Command,
}

#[tokio::main]
async fn main() {
	pretty_env_logger::init();
	sodiumoxide::init().expect("Unable to init sodiumoxide");

	let opt = Opt::from_args();

	let res = match opt.cmd {
		Command::Server => {
			// Abort on panic (same behavior as in Go)
			std::panic::set_hook(Box::new(|panic_info| {
				error!("{}", panic_info.to_string());
				std::process::abort();
			}));

			server::run_server(opt.config_file).await
		}
		Command::NodeId(node_id_opt) => node_id_command(opt.config_file, node_id_opt.quiet),
		_ => cli_command(opt).await,
	};

	if let Err(e) = res {
		error!("{}", e);
	}
}

async fn cli_command(opt: Opt) -> Result<(), Error> {
	let config = if opt.rpc_secret.is_none() || opt.rpc_host.is_none() {
		Some(garage_util::config::read_config(opt.config_file.clone())
			.map_err(|e| Error::Message(format!("Unable to read configuration file {}: {}. Configuration file is needed because -h or -s is not provided on the command line.", opt.config_file.to_string_lossy(), e)))?)
	} else {
		None
	};

	// Find and parse network RPC secret
	let net_key_hex_str = opt
		.rpc_secret
		.as_ref()
		.or_else(|| config.as_ref().map(|c| &c.rpc_secret))
		.expect("No RPC secret provided");
	let network_key = NetworkKey::from_slice(
		&hex::decode(net_key_hex_str).expect("Invalid RPC secret key (bad hex)")[..],
	)
	.expect("Invalid RPC secret provided (wrong length)");

	// Generate a temporary keypair for our RPC client
	let (_pk, sk) = sodiumoxide::crypto::sign::ed25519::gen_keypair();

	let netapp = NetApp::new(network_key, sk);

	// Find and parse the address of the target host
	let (id, addr) = if let Some(h) = opt.rpc_host {
		let (id, addrs) = parse_and_resolve_peer_addr(&h).expect("Invalid RPC host");
		(id, addrs[0])
	} else if let Some(a) = config.as_ref().map(|c| c.rpc_public_addr).flatten() {
		let node_key = garage_rpc::system::gen_node_key(&config.unwrap().metadata_dir)
			.map_err(|e| Error::Message(format!("Unable to read or generate node key: {}", e)))?;
		(node_key.public_key(), a)
	} else {
		return Err(Error::Message("No RPC host provided".into()));
	};

	// Connect to target host
	netapp.clone().try_connect(addr, id).await?;

	let system_rpc_endpoint = netapp.endpoint::<SystemRpc, ()>(SYSTEM_RPC_PATH.into());
	let admin_rpc_endpoint = netapp.endpoint::<AdminRpc, ()>(ADMIN_RPC_PATH.into());

	cli_cmd(opt.cmd, &system_rpc_endpoint, &admin_rpc_endpoint, id).await
}

fn node_id_command(config_file: PathBuf, quiet: bool) -> Result<(), Error> {
	let config = garage_util::config::read_config(config_file.clone()).map_err(|e| {
		Error::Message(format!(
			"Unable to read configuration file {}: {}",
			config_file.to_string_lossy(),
			e
		))
	})?;

	let node_key = garage_rpc::system::gen_node_key(&config.metadata_dir)
		.map_err(|e| Error::Message(format!("Unable to read or generate node key: {}", e)))?;

	let idstr = if let Some(addr) = config.rpc_public_addr {
		let idstr = format!("{}@{}", hex::encode(&node_key.public_key()), addr);
		println!("{}", idstr);
		idstr
	} else {
		let idstr = hex::encode(&node_key.public_key());
		println!("{}", idstr);

		if !quiet {
			eprintln!("WARNING: I don't know the public address to reach this node.");
			eprintln!("In all of the instructions below, replace 127.0.0.1:3901 by the appropriate address and port.");
		}

		format!("{}@127.0.0.1:3901", idstr)
	};

	if !quiet {
		eprintln!("");
		eprintln!(
			"To instruct a node to connect to this node, run the following command on that node:"
		);
		eprintln!("    garage [-c <config file path>] node connect {}", idstr);
		eprintln!("");
		eprintln!("Or instruct them to connect from here by running:");
		eprintln!(
			"    garage -c {} -h <remote node> node connect {}",
			config_file.to_string_lossy(),
			idstr
		);
		eprintln!(
			"where <remote_node> is their own node identifier in the format: <pubkey>@<ip>:<port>"
		);
		eprintln!("");
		eprintln!("This node identifier can also be added as a bootstrap node in other node's garage.toml files:");
		eprintln!("    bootstrap_peers = [");
		eprintln!("        \"{}\",", idstr);
		eprintln!("        ...");
		eprintln!("    ]");
	}

	Ok(())
}
