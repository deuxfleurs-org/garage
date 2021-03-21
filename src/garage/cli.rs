use std::collections::HashSet;
use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use structopt::StructOpt;

use garage_util::data::UUID;
use garage_util::error::Error;
use garage_util::time::*;

use garage_rpc::membership::*;
use garage_rpc::ring::*;
use garage_rpc::rpc_client::*;

use garage_model::bucket_table::*;
use garage_model::key_table::*;

use crate::admin_rpc::*;

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
	pub config_file: PathBuf,
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

	/// Optional node tag
	#[structopt(short = "t", long = "tag")]
	tag: Option<String>,

	/// Replaced node(s): list of node IDs that will be removed from the current cluster
	#[structopt(long = "replace")]
	replace: Vec<String>,
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
	/// Access key name or ID
	#[structopt(long = "key")]
	pub key_pattern: String,

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

	/// Import key
	#[structopt(name = "import")]
	Import(KeyImportOpt),
}

#[derive(Serialize, Deserialize, StructOpt, Debug)]
pub struct KeyOpt {
	/// ID or name of the key
	pub key_pattern: String,
}

#[derive(Serialize, Deserialize, StructOpt, Debug)]
pub struct KeyNewOpt {
	/// Name of the key
	#[structopt(long = "name", default_value = "Unnamed key")]
	pub name: String,
}

#[derive(Serialize, Deserialize, StructOpt, Debug)]
pub struct KeyRenameOpt {
	/// ID or name of the key
	pub key_pattern: String,

	/// New name of the key
	pub new_name: String,
}

#[derive(Serialize, Deserialize, StructOpt, Debug)]
pub struct KeyDeleteOpt {
	/// ID or name of the key
	pub key_pattern: String,

	/// Confirm deletion
	#[structopt(long = "yes")]
	pub yes: bool,
}

#[derive(Serialize, Deserialize, StructOpt, Debug)]
pub struct KeyImportOpt {
	/// Access key ID
	pub key_id: String,

	/// Secret access key
	pub secret_key: String,

	/// Key name
	#[structopt(short = "n", default_value = "Imported key")]
	pub name: String,
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

pub async fn cli_cmd(
	cmd: Command,
	membership_rpc_cli: RpcAddrClient<Message>,
	admin_rpc_cli: RpcAddrClient<AdminRPC>,
	rpc_host: SocketAddr,
) -> Result<(), Error> {
	match cmd {
		Command::Status => cmd_status(membership_rpc_cli, rpc_host).await,
		Command::Node(NodeOperation::Configure(configure_opt)) => {
			cmd_configure(membership_rpc_cli, rpc_host, configure_opt).await
		}
		Command::Node(NodeOperation::Remove(remove_opt)) => {
			cmd_remove(membership_rpc_cli, rpc_host, remove_opt).await
		}
		Command::Bucket(bo) => {
			cmd_admin(admin_rpc_cli, rpc_host, AdminRPC::BucketOperation(bo)).await
		}
		Command::Key(ko) => cmd_admin(admin_rpc_cli, rpc_host, AdminRPC::KeyOperation(ko)).await,
		Command::Repair(ro) => cmd_admin(admin_rpc_cli, rpc_host, AdminRPC::LaunchRepair(ro)).await,
		Command::Stats(so) => cmd_admin(admin_rpc_cli, rpc_host, AdminRPC::Stats(so)).await,
		_ => unreachable!(),
	}
}

pub async fn cmd_status(
	rpc_cli: RpcAddrClient<Message>,
	rpc_host: SocketAddr,
) -> Result<(), Error> {
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

pub fn find_matching_node(
	cand: impl std::iter::Iterator<Item = UUID>,
	pattern: &str,
) -> Result<UUID, Error> {
	let mut candidates = vec![];
	for c in cand {
		if hex::encode(&c).starts_with(&pattern) {
			candidates.push(c);
		}
	}
	if candidates.len() != 1 {
		Err(Error::Message(format!(
			"{} nodes match '{}'",
			candidates.len(),
			pattern,
		)))
	} else {
		Ok(candidates[0])
	}
}

pub async fn cmd_configure(
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

	let added_node = find_matching_node(status.iter().map(|x| x.id), &args.node_id)?;

	let mut config = match rpc_cli
		.call(&rpc_host, &Message::PullConfig, ADMIN_RPC_TIMEOUT)
		.await??
	{
		Message::AdvertiseConfig(cfg) => cfg,
		resp => return Err(Error::Message(format!("Invalid RPC response: {:?}", resp))),
	};

	for replaced in args.replace.iter() {
		let replaced_node = find_matching_node(config.members.keys().cloned(), replaced)?;
		if config.members.remove(&replaced_node).is_none() {
			return Err(Error::Message(format!(
				"Cannot replace node {:?} as it is not in current configuration",
				replaced_node
			)));
		}
	}

	let new_entry = match config.members.get(&added_node) {
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

	config.members.insert(added_node, new_entry);
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

pub async fn cmd_remove(
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

	let deleted_node = find_matching_node(config.members.keys().cloned(), &args.node_id)?;

	if !args.yes {
		return Err(Error::Message(format!(
			"Add the flag --yes to really remove {:?} from the cluster",
			deleted_node
		)));
	}

	config.members.remove(&deleted_node);
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

pub async fn cmd_admin(
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
			print_bucket_info(&bucket);
		}
		AdminRPC::KeyList(kl) => {
			println!("List of keys:");
			for key in kl {
				println!("{}\t{}", key.0, key.1);
			}
		}
		AdminRPC::KeyInfo(key) => {
			print_key_info(&key);
		}
		r => {
			error!("Unexpected response: {:?}", r);
		}
	}
	Ok(())
}

fn print_key_info(key: &Key) {
	println!("Key name: {}", key.name.get());
	println!("Key ID: {}", key.key_id);
	println!("Secret key: {}", key.secret_key);
	if key.deleted.get() {
		println!("Key is deleted.");
	} else {
		println!("Authorized buckets:");
		for (b, _, perm) in key.authorized_buckets.items().iter() {
			println!("- {} R:{} W:{}", b, perm.allow_read, perm.allow_write);
		}
	}
}

fn print_bucket_info(bucket: &Bucket) {
	println!("Bucket name: {}", bucket.name);
	match bucket.state.get() {
		BucketState::Deleted => println!("Bucket is deleted."),
		BucketState::Present(p) => {
			println!("Authorized keys:");
			for (k, _, perm) in p.authorized_keys.items().iter() {
				println!("- {} R:{} W:{}", k, perm.allow_read, perm.allow_write);
			}
			println!("Website access: {}", p.website.get());
		}
	};
}
