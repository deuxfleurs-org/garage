#![recursion_limit = "1024"]

#[macro_use]
extern crate log;

mod admin_rpc;
mod repair;
mod server;

use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use structopt::StructOpt;

use garage_util::config::TlsConfig;
use garage_util::data::*;
use garage_util::error::Error;

use garage_rpc::membership::*;
use garage_rpc::ring::*;
use garage_rpc::rpc_client::*;

use admin_rpc::*;

#[derive(StructOpt, Debug)]
#[structopt(name = "garage")]
pub struct Opt {
	/// RPC connect to this host to execute client operations
	#[structopt(short = "h", long = "rpc-host", default_value = "127.0.0.1:3901")]
	rpc_host: SocketAddr,

	#[structopt(long = "ca-cert")]
	ca_cert: Option<String>,
	#[structopt(long = "client-cert")]
	client_cert: Option<String>,
	#[structopt(long = "client-key")]
	client_key: Option<String>,

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

	/// Garage node operations
	#[structopt(name = "node")]
	Node(NodeOperation),

	/// Bucket operations
	#[structopt(name = "bucket")]
	Bucket(BucketOperation),

	/// Key operations
	#[structopt(name = "key")]
	Key(KeyOperation),

	/// Start repair of node data
	#[structopt(name = "repair")]
	Repair(RepairOpt),

	/// Gather node statistics
	#[structopt(name = "stats")]
	Stats(StatsOpt),
}

#[derive(StructOpt, Debug)]
pub struct ServerOpt {
	/// Configuration file
	#[structopt(short = "c", long = "config", default_value = "./config.toml")]
	config_file: PathBuf,
}

#[derive(StructOpt, Debug)]
pub enum NodeOperation {
	/// Configure Garage node
	#[structopt(name = "configure")]
	Configure(ConfigureNodeOpt),

	/// Remove Garage node from cluster
	#[structopt(name = "remove")]
	Remove(RemoveNodeOpt),
}

#[derive(StructOpt, Debug)]
pub struct ConfigureNodeOpt {
	/// Node to configure (prefix of hexadecimal node id)
	node_id: String,

	/// Location (datacenter) of the node
	#[structopt(short = "d", long = "datacenter")]
	datacenter: Option<String>,

	/// Capacity (in relative terms, use 1 to represent your smallest server)
	#[structopt(short = "c", long = "capacity")]
	capacity: Option<u32>,

	/// Optionnal node tag
	#[structopt(short = "t", long = "tag")]
	tag: Option<String>,
}

#[derive(StructOpt, Debug)]
pub struct RemoveNodeOpt {
	/// Node to configure (prefix of hexadecimal node id)
	node_id: String,

	/// If this flag is not given, the node won't be removed
	#[structopt(long = "yes")]
	yes: bool,
}

#[derive(Serialize, Deserialize, StructOpt, Debug)]
pub enum BucketOperation {
	/// List buckets
	#[structopt(name = "list")]
	List,

	/// Get bucket info
	#[structopt(name = "info")]
	Info(BucketOpt),

	/// Create bucket
	#[structopt(name = "create")]
	Create(BucketOpt),

	/// Delete bucket
	#[structopt(name = "delete")]
	Delete(DeleteBucketOpt),

	/// Allow key to read or write to bucket
	#[structopt(name = "allow")]
	Allow(PermBucketOpt),

	/// Allow key to read or write to bucket
	#[structopt(name = "deny")]
	Deny(PermBucketOpt),

	/// Expose as website or not
	#[structopt(name = "website")]
	Website(WebsiteOpt),
}

#[derive(Serialize, Deserialize, StructOpt, Debug)]
pub struct WebsiteOpt {
	/// Create
	#[structopt(long = "allow")]
	pub allow: bool,

	/// Delete
	#[structopt(long = "deny")]
	pub deny: bool,

	/// Bucket name
	pub bucket: String,
}

#[derive(Serialize, Deserialize, StructOpt, Debug)]
pub struct BucketOpt {
	/// Bucket name
	pub name: String,
}

#[derive(Serialize, Deserialize, StructOpt, Debug)]
pub struct DeleteBucketOpt {
	/// Bucket name
	pub name: String,

	/// If this flag is not given, the bucket won't be deleted
	#[structopt(long = "yes")]
	pub yes: bool,
}

#[derive(Serialize, Deserialize, StructOpt, Debug)]
pub struct PermBucketOpt {
	/// Access key ID
	#[structopt(long = "key")]
	pub key_id: String,

	/// Allow/deny read operations
	#[structopt(long = "read")]
	pub read: bool,

	/// Allow/deny write operations
	#[structopt(long = "write")]
	pub write: bool,

	/// Bucket name
	pub bucket: String,
}

#[derive(Serialize, Deserialize, StructOpt, Debug)]
pub enum KeyOperation {
	/// List keys
	#[structopt(name = "list")]
	List,

	/// Get key info
	#[structopt(name = "info")]
	Info(KeyOpt),

	/// Create new key
	#[structopt(name = "new")]
	New(KeyNewOpt),

	/// Rename key
	#[structopt(name = "rename")]
	Rename(KeyRenameOpt),

	/// Delete key
	#[structopt(name = "delete")]
	Delete(KeyDeleteOpt),
}

#[derive(Serialize, Deserialize, StructOpt, Debug)]
pub struct KeyOpt {
	/// ID of the key
	key_id: String,
}

#[derive(Serialize, Deserialize, StructOpt, Debug)]
pub struct KeyNewOpt {
	/// Name of the key
	#[structopt(long = "name", default_value = "Unnamed key")]
	name: String,
}

#[derive(Serialize, Deserialize, StructOpt, Debug)]
pub struct KeyRenameOpt {
	/// ID of the key
	key_id: String,

	/// New name of the key
	new_name: String,
}

#[derive(Serialize, Deserialize, StructOpt, Debug)]
pub struct KeyDeleteOpt {
	/// ID of the key
	key_id: String,

	/// Confirm deletion
	#[structopt(long = "yes")]
	yes: bool,
}

#[derive(Serialize, Deserialize, StructOpt, Debug, Clone)]
pub struct RepairOpt {
	/// Launch repair operation on all nodes
	#[structopt(short = "a", long = "all-nodes")]
	pub all_nodes: bool,

	/// Confirm the launch of the repair operation
	#[structopt(long = "yes")]
	pub yes: bool,

	#[structopt(subcommand)]
	pub what: Option<RepairWhat>,
}

#[derive(Serialize, Deserialize, StructOpt, Debug, Eq, PartialEq, Clone)]
pub enum RepairWhat {
	/// Only do a full sync of metadata tables
	#[structopt(name = "tables")]
	Tables,
	/// Only repair (resync/rebalance) the set of stored blocks
	#[structopt(name = "blocks")]
	Blocks,
	/// Only redo the propagation of object deletions to the version table (slow)
	#[structopt(name = "versions")]
	Versions,
	/// Only redo the propagation of version deletions to the block ref table (extremely slow)
	#[structopt(name = "block_refs")]
	BlockRefs,
}

#[derive(Serialize, Deserialize, StructOpt, Debug, Clone)]
pub struct StatsOpt {
	/// Gather statistics from all nodes
	#[structopt(short = "a", long = "all-nodes")]
	pub all_nodes: bool,

	/// Gather detailed statistics (this can be long)
	#[structopt(short = "d", long = "detailed")]
	pub detailed: bool,
}

#[tokio::main]
async fn main() {
	pretty_env_logger::init();

	let opt = Opt::from_args();

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

	let resp = match opt.cmd {
		Command::Server(server_opt) => {
			// Abort on panic (same behavior as in Go)
			std::panic::set_hook(Box::new(|panic_info| {
				error!("{}", panic_info.to_string());
				std::process::abort();
			}));

			server::run_server(server_opt.config_file).await
		}
		Command::Status => cmd_status(membership_rpc_cli, opt.rpc_host).await,
		Command::Node(NodeOperation::Configure(configure_opt)) => {
			cmd_configure(membership_rpc_cli, opt.rpc_host, configure_opt).await
		}
		Command::Node(NodeOperation::Remove(remove_opt)) => {
			cmd_remove(membership_rpc_cli, opt.rpc_host, remove_opt).await
		}
		Command::Bucket(bo) => {
			cmd_admin(admin_rpc_cli, opt.rpc_host, AdminRPC::BucketOperation(bo)).await
		}
		Command::Key(bo) => {
			cmd_admin(admin_rpc_cli, opt.rpc_host, AdminRPC::KeyOperation(bo)).await
		}
		Command::Repair(ro) => {
			cmd_admin(admin_rpc_cli, opt.rpc_host, AdminRPC::LaunchRepair(ro)).await
		}
		Command::Stats(so) => {
			cmd_admin(admin_rpc_cli, opt.rpc_host, AdminRPC::Stats(so)).await
		}
	};

	if let Err(e) = resp {
		error!("Error: {}", e);
	}
}

async fn cmd_status(rpc_cli: RpcAddrClient<Message>, rpc_host: SocketAddr) -> Result<(), Error> {
	let status = match rpc_cli
		.call(&rpc_host, &Message::PullStatus, ADMIN_RPC_TIMEOUT)
		.await??
	{
		Message::AdvertiseNodesUp(nodes) => nodes,
		resp => return Err(Error::Message(format!("Invalid RPC response: {:?}", resp))),
	};
	let config = match rpc_cli
		.call(&rpc_host, &Message::PullConfig, ADMIN_RPC_TIMEOUT)
		.await??
	{
		Message::AdvertiseConfig(cfg) => cfg,
		resp => return Err(Error::Message(format!("Invalid RPC response: {:?}", resp))),
	};

	println!("Healthy nodes:");
	for adv in status.iter().filter(|x| x.is_up) {
		if let Some(cfg) = config.members.get(&adv.id) {
			println!(
				"{:?}\t{}\t{}\t[{}]\t{}\t{}",
				adv.id, adv.state_info.hostname, adv.addr, cfg.tag, cfg.datacenter, cfg.capacity
			);
		} else {
			println!(
				"{:?}\t{}\t{}\tUNCONFIGURED/REMOVED",
				adv.id, adv.state_info.hostname, adv.addr
			);
		}
	}

	let status_keys = status.iter().map(|x| x.id).collect::<HashSet<_>>();
	let failure_case_1 = status.iter().any(|x| !x.is_up);
	let failure_case_2 = config
		.members
		.iter()
		.any(|(id, _)| !status_keys.contains(id));
	if failure_case_1 || failure_case_2 {
		println!("\nFailed nodes:");
		for adv in status.iter().filter(|x| !x.is_up) {
			if let Some(cfg) = config.members.get(&adv.id) {
				println!(
					"{:?}\t{}\t{}\t[{}]\t{}\t{}\tlast seen: {}s ago",
					adv.id,
					adv.state_info.hostname,
					adv.addr,
					cfg.tag,
					cfg.datacenter,
					cfg.capacity,
					(now_msec() - adv.last_seen) / 1000,
				);
			}
		}
		for (id, cfg) in config.members.iter() {
			if !status.iter().any(|x| x.id == *id) {
				println!(
					"{:?}\t{}\t{}\t{}\tnever seen",
					id, cfg.tag, cfg.datacenter, cfg.capacity
				);
			}
		}
	}

	Ok(())
}

async fn cmd_configure(
	rpc_cli: RpcAddrClient<Message>,
	rpc_host: SocketAddr,
	args: ConfigureNodeOpt,
) -> Result<(), Error> {
	let status = match rpc_cli
		.call(&rpc_host, &Message::PullStatus, ADMIN_RPC_TIMEOUT)
		.await??
	{
		Message::AdvertiseNodesUp(nodes) => nodes,
		resp => return Err(Error::Message(format!("Invalid RPC response: {:?}", resp))),
	};

	let mut candidates = vec![];
	for adv in status.iter() {
		if hex::encode(&adv.id).starts_with(&args.node_id) {
			candidates.push(adv.id);
		}
	}
	if candidates.len() != 1 {
		return Err(Error::Message(format!(
			"{} matching nodes",
			candidates.len()
		)));
	}

	let mut config = match rpc_cli
		.call(&rpc_host, &Message::PullConfig, ADMIN_RPC_TIMEOUT)
		.await??
	{
		Message::AdvertiseConfig(cfg) => cfg,
		resp => return Err(Error::Message(format!("Invalid RPC response: {:?}", resp))),
	};

	let new_entry = match config.members.get(&candidates[0]) {
		None => NetworkConfigEntry {
			datacenter: args
				.datacenter
				.expect("Please specifiy a datacenter with the -d flag"),
			capacity: args
				.capacity
				.expect("Please specifiy a capacity with the -c flag"),
			tag: args.tag.unwrap_or("".to_string()),
		},
		Some(old) => NetworkConfigEntry {
			datacenter: args.datacenter.unwrap_or(old.datacenter.to_string()),
			capacity: args.capacity.unwrap_or(old.capacity),
			tag: args.tag.unwrap_or(old.tag.to_string()),
		},
	};

	config.members.insert(candidates[0].clone(), new_entry);
	config.version += 1;

	rpc_cli
		.call(
			&rpc_host,
			&Message::AdvertiseConfig(config),
			ADMIN_RPC_TIMEOUT,
		)
		.await??;
	Ok(())
}

async fn cmd_remove(
	rpc_cli: RpcAddrClient<Message>,
	rpc_host: SocketAddr,
	args: RemoveNodeOpt,
) -> Result<(), Error> {
	let mut config = match rpc_cli
		.call(&rpc_host, &Message::PullConfig, ADMIN_RPC_TIMEOUT)
		.await??
	{
		Message::AdvertiseConfig(cfg) => cfg,
		resp => return Err(Error::Message(format!("Invalid RPC response: {:?}", resp))),
	};

	let mut candidates = vec![];
	for (key, _) in config.members.iter() {
		if hex::encode(key).starts_with(&args.node_id) {
			candidates.push(*key);
		}
	}
	if candidates.len() != 1 {
		return Err(Error::Message(format!(
			"{} matching nodes",
			candidates.len()
		)));
	}

	if !args.yes {
		return Err(Error::Message(format!(
			"Add the flag --yes to really remove {:?} from the cluster",
			candidates[0]
		)));
	}

	config.members.remove(&candidates[0]);
	config.version += 1;

	rpc_cli
		.call(
			&rpc_host,
			&Message::AdvertiseConfig(config),
			ADMIN_RPC_TIMEOUT,
		)
		.await??;
	Ok(())
}

async fn cmd_admin(
	rpc_cli: RpcAddrClient<AdminRPC>,
	rpc_host: SocketAddr,
	args: AdminRPC,
) -> Result<(), Error> {
	match rpc_cli.call(&rpc_host, args, ADMIN_RPC_TIMEOUT).await?? {
		AdminRPC::Ok(msg) => {
			println!("{}", msg);
		}
		AdminRPC::BucketList(bl) => {
			println!("List of buckets:");
			for bucket in bl {
				println!("{}", bucket);
			}
		}
		AdminRPC::BucketInfo(bucket) => {
			println!("{:?}", bucket);
		}
		AdminRPC::KeyList(kl) => {
			println!("List of keys:");
			for key in kl {
				println!("{}\t{}", key.0, key.1);
			}
		}
		AdminRPC::KeyInfo(key) => {
			println!("{:?}", key);
		}
		r => {
			error!("Unexpected response: {:?}", r);
		}
	}
	Ok(())
}
