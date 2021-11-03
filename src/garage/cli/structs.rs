use serde::{Deserialize, Serialize};

use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub enum Command {
	/// Run Garage server
	#[structopt(name = "server")]
	Server,

	/// Print identifier (public key) of this garage node.
	/// Generates a new keypair if necessary.
	#[structopt(name = "node-id")]
	NodeId(NodeIdOpt),

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
pub enum NodeOperation {
	/// Connect to Garage node that is currently isolated from the system
	#[structopt(name = "connect")]
	Connect(ConnectNodeOpt),

	/// Configure Garage node
	#[structopt(name = "configure")]
	Configure(ConfigureNodeOpt),

	/// Remove Garage node from cluster
	#[structopt(name = "remove")]
	Remove(RemoveNodeOpt),
}

#[derive(StructOpt, Debug)]
pub struct NodeIdOpt {
	/// Do not print usage instructions to stderr
	#[structopt(short = "q", long = "quiet")]
	pub(crate) quiet: bool,
}

#[derive(StructOpt, Debug)]
pub struct ConnectNodeOpt {
	/// Node public key and address, in the format:
	/// `<public key hexadecimal>@<ip or hostname>:<port>`
	pub(crate) node: String,
}

#[derive(StructOpt, Debug)]
pub struct ConfigureNodeOpt {
	/// Node to configure (prefix of hexadecimal node id)
	pub(crate) node_id: String,

	/// Location (zone or datacenter) of the node
	#[structopt(short = "z", long = "zone")]
	pub(crate) zone: Option<String>,

	/// Capacity (in relative terms, use 1 to represent your smallest server)
	#[structopt(short = "c", long = "capacity")]
	pub(crate) capacity: Option<u32>,

	/// Gateway-only node
	#[structopt(short = "g", long = "gateway")]
	pub(crate) gateway: bool,

	/// Optional node tag
	#[structopt(short = "t", long = "tag")]
	pub(crate) tag: Option<String>,

	/// Replaced node(s): list of node IDs that will be removed from the current cluster
	#[structopt(long = "replace")]
	pub(crate) replace: Vec<String>,
}

#[derive(StructOpt, Debug)]
pub struct RemoveNodeOpt {
	/// Node to configure (prefix of hexadecimal node id)
	pub(crate) node_id: String,

	/// If this flag is not given, the node won't be removed
	#[structopt(long = "yes")]
	pub(crate) yes: bool,
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

	/// Deny key from reading or writing to bucket
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
	pub what: RepairWhat,
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
	/// Verify integrity of all blocks on disc (extremely slow, i/o intensive)
	#[structopt(name = "scrub")]
	Scrub {
		/// Tranquility factor (see tranquilizer documentation)
		#[structopt(name = "tranquility", default_value = "2")]
		tranquility: u32,
	},
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
