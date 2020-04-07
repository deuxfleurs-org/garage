mod error;
mod data;
mod proto;
mod membership;
mod server;
mod rpc_server;
mod rpc_client;
mod api_server;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use structopt::StructOpt;

use error::Error;
use rpc_client::RpcClient;
use data::*;
use proto::*;

#[derive(StructOpt, Debug)]
#[structopt(name = "garage")]
pub struct Opt {
	/// RPC connect to this host to execute client operations
	#[structopt(short = "h", long = "rpc-host", default_value = "127.0.0.1:3901")]
	rpc_host: SocketAddr,

	#[structopt(subcommand)]
	cmd: Command,
}

#[derive(StructOpt, Debug)]
pub enum Command {
	/// Run Garage server
	#[structopt(name = "server")]
	Server(ServerOpt),

	/// Get network status
	#[structopt(name = "status")]
	Status,

	/// Configure Garage node
	#[structopt(name = "configure")]
	Configure(ConfigureOpt),
}

#[derive(StructOpt, Debug)]
pub struct ServerOpt {
	/// Configuration file
	#[structopt(short = "c", long = "config", default_value = "./config.toml")]
	config_file: PathBuf,
}

#[derive(StructOpt, Debug)]
pub struct ConfigureOpt {
	/// Node to configure (prefix of hexadecimal node id)
	node_id: String,

	/// Number of tokens
	n_tokens: u32,
}


#[tokio::main]
async fn main() {
	let opt = Opt::from_args();

	let rpc_cli = RpcClient::new();

	let resp = match opt.cmd {
		Command::Server(server_opt) => {
			server::run_server(server_opt.config_file).await
		}
		Command::Status => {
			cmd_status(rpc_cli, opt.rpc_host).await
		}
		Command::Configure(configure_opt) => {
			cmd_configure(rpc_cli, opt.rpc_host, configure_opt).await
		}
	};

	if let Err(e) = resp {
		eprintln!("Error: {}", e);
	}
}

async fn cmd_status(rpc_cli: RpcClient, rpc_host: SocketAddr) -> Result<(), Error> {
	let status = match rpc_cli.call(&rpc_host, 
								    &Message::PullStatus,
									DEFAULT_TIMEOUT).await? {
		Message::AdvertiseNodesUp(nodes) => nodes,
		resp => return Err(Error::Message(format!("Invalid RPC response: {:?}", resp)))
	};
	let config = match rpc_cli.call(&rpc_host, 
								    &Message::PullConfig,
									DEFAULT_TIMEOUT).await? {
		Message::AdvertiseConfig(cfg) => cfg,
		resp => return Err(Error::Message(format!("Invalid RPC response: {:?}", resp)))
	};

	println!("Healthy nodes:");
	for adv in status.iter() {
		if let Some(cfg) = config.members.get(&adv.id) {
			println!("{}\t{}\t{}\t{}", hex::encode(adv.id), adv.addr, adv.datacenter, cfg.n_tokens);
		}
	}

	let status_keys = status.iter().map(|x| x.id.clone()).collect::<HashSet<_>>();
	if config.members.iter().any(|(id, _)| !status_keys.contains(id)) {
		println!("\nFailed nodes:");
		for (id, cfg) in config.members.iter() {
			if !status.iter().any(|x| x.id == *id) {
				println!("{}\t{}", hex::encode(id), cfg.n_tokens);
			}
		}
	}

	if status.iter().any(|adv| !config.members.contains_key(&adv.id)) {
		println!("\nUnconfigured nodes:");
		for adv in status.iter() {
			if !config.members.contains_key(&adv.id) {
				println!("{}\t{}\t{}", hex::encode(adv.id), adv.addr, adv.datacenter);
			}
		}
	}

	Ok(())
}

async fn cmd_configure(rpc_cli: RpcClient, rpc_host: SocketAddr, args: ConfigureOpt) -> Result<(), Error> {
	let status = match rpc_cli.call(&rpc_host, 
								    &Message::PullStatus,
									DEFAULT_TIMEOUT).await? {
		Message::AdvertiseNodesUp(nodes) => nodes,
		resp => return Err(Error::Message(format!("Invalid RPC response: {:?}", resp)))
	};

	let mut candidates = vec![];
	for adv in status.iter() {
		if hex::encode(adv.id).starts_with(&args.node_id) {
			candidates.push(adv.id.clone());
		}
	}
	if candidates.len() != 1 {
		return Err(Error::Message(format!("{} matching nodes", candidates.len())));
	}

	let mut config = match rpc_cli.call(&rpc_host, 
								    &Message::PullConfig,
									DEFAULT_TIMEOUT).await? {
		Message::AdvertiseConfig(cfg) => cfg,
		resp => return Err(Error::Message(format!("Invalid RPC response: {:?}", resp)))
	};

	config.members.insert(candidates[0].clone(),
						  NetworkConfigEntry{
							  n_tokens: args.n_tokens,
						  });
	config.version += 1;

	rpc_cli.call(&rpc_host,
				 &Message::AdvertiseConfig(config),
				 DEFAULT_TIMEOUT).await?;
	Ok(())
}
