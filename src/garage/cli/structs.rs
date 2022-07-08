use serde::{Deserialize, Serialize};

use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub enum Command {
	/// Run Garage server
	#[structopt(name = "server")]
	Server,

	/// Get network status
	#[structopt(name = "status")]
	Status,

	/// Operations on individual Garage nodes
	#[structopt(name = "node")]
	Node(NodeOperation),

	/// Operations on the assignation of node roles in the cluster layout
	#[structopt(name = "layout")]
	Layout(LayoutOperation),

	/// Operations on buckets
	#[structopt(name = "bucket")]
	Bucket(BucketOperation),

	/// Operations on S3 access keys
	#[structopt(name = "key")]
	Key(KeyOperation),

	/// Run migrations from previous Garage version
	/// (DO NOT USE WITHOUT READING FULL DOCUMENTATION)
	#[structopt(name = "migrate")]
	Migrate(MigrateOpt),

	/// Start repair of node data on remote node
	#[structopt(name = "repair")]
	Repair(RepairOpt),

	/// Offline reparation of node data (these repairs must be run offline
	/// directly on the server node)
	#[structopt(name = "offline-repair")]
	OfflineRepair(OfflineRepairOpt),

	/// Gather node statistics
	#[structopt(name = "stats")]
	Stats(StatsOpt),

	/// Manage background workers
	#[structopt(name = "worker")]
	Worker(WorkerOpt),
}

#[derive(StructOpt, Debug)]
pub enum NodeOperation {
	/// Print identifier (public key) of this Garage node
	#[structopt(name = "id")]
	NodeId(NodeIdOpt),

	/// Connect to Garage node that is currently isolated from the system
	#[structopt(name = "connect")]
	Connect(ConnectNodeOpt),
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
pub enum LayoutOperation {
	/// Assign role to Garage node
	#[structopt(name = "assign")]
	Assign(AssignRoleOpt),

	/// Remove role from Garage cluster node
	#[structopt(name = "remove")]
	Remove(RemoveRoleOpt),

	/// Show roles currently assigned to nodes and changes staged for commit
	#[structopt(name = "show")]
	Show,

	/// Apply staged changes to cluster layout
	#[structopt(name = "apply")]
	Apply(ApplyLayoutOpt),

	/// Revert staged changes to cluster layout
	#[structopt(name = "revert")]
	Revert(RevertLayoutOpt),
}

#[derive(StructOpt, Debug)]
pub struct AssignRoleOpt {
	/// Node(s) to which to assign role (prefix of hexadecimal node id)
	#[structopt(required = true)]
	pub(crate) node_ids: Vec<String>,

	/// Location (zone or datacenter) of the node
	#[structopt(short = "z", long = "zone")]
	pub(crate) zone: Option<String>,

	/// Capacity (in relative terms, use 1 to represent your smallest server)
	#[structopt(short = "c", long = "capacity")]
	pub(crate) capacity: Option<u32>,

	/// Gateway-only node
	#[structopt(short = "g", long = "gateway")]
	pub(crate) gateway: bool,

	/// Optional tags to add to node
	#[structopt(short = "t", long = "tag")]
	pub(crate) tags: Vec<String>,

	/// Replaced node(s): list of node IDs that will be removed from the current cluster
	#[structopt(long = "replace")]
	pub(crate) replace: Vec<String>,
}

#[derive(StructOpt, Debug)]
pub struct RemoveRoleOpt {
	/// Node whose role to remove (prefix of hexadecimal node id)
	pub(crate) node_id: String,
}

#[derive(StructOpt, Debug)]
pub struct ApplyLayoutOpt {
	/// Version number of new configuration: this command will fail if
	/// it is not exactly 1 + the previous configuration's version
	#[structopt(long = "version")]
	pub(crate) version: Option<u64>,
}

#[derive(StructOpt, Debug)]
pub struct RevertLayoutOpt {
	/// Version number of old configuration to which to revert
	#[structopt(long = "version")]
	pub(crate) version: Option<u64>,
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

	/// Alias bucket under new name
	#[structopt(name = "alias")]
	Alias(AliasBucketOpt),

	/// Remove bucket alias
	#[structopt(name = "unalias")]
	Unalias(UnaliasBucketOpt),

	/// Allow key to read or write to bucket
	#[structopt(name = "allow")]
	Allow(PermBucketOpt),

	/// Deny key from reading or writing to bucket
	#[structopt(name = "deny")]
	Deny(PermBucketOpt),

	/// Expose as website or not
	#[structopt(name = "website")]
	Website(WebsiteOpt),

	/// Set the quotas for this bucket
	#[structopt(name = "set-quotas")]
	SetQuotas(SetQuotasOpt),
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

	/// Index document: the suffix appended to request paths ending by /
	#[structopt(short = "i", long = "index-document", default_value = "index.html")]
	pub index_document: String,

	/// Error document: the optionnal document returned when an error occurs
	#[structopt(short = "e", long = "error-document")]
	pub error_document: Option<String>,
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
pub struct AliasBucketOpt {
	/// Existing bucket name (its alias in global namespace or its full hex uuid)
	pub existing_bucket: String,

	/// New bucket name
	pub new_name: String,

	/// Make this alias local to the specified access key
	#[structopt(long = "local")]
	pub local: Option<String>,
}

#[derive(Serialize, Deserialize, StructOpt, Debug)]
pub struct UnaliasBucketOpt {
	/// Bucket name
	pub name: String,

	/// Unalias in bucket namespace local to this access key
	#[structopt(long = "local")]
	pub local: Option<String>,
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

	/// Allow/deny administrative operations operations
	/// (such as deleting bucket or changing bucket website configuration)
	#[structopt(long = "owner")]
	pub owner: bool,

	/// Bucket name
	pub bucket: String,
}

#[derive(Serialize, Deserialize, StructOpt, Debug)]
pub struct SetQuotasOpt {
	/// Bucket name
	pub bucket: String,

	/// Set a maximum size for the bucket (specify a size e.g. in MiB or GiB,
	/// or `none` for no size restriction)
	#[structopt(long = "max-size")]
	pub max_size: Option<String>,

	/// Set a maximum number of objects for the bucket (or `none` for no restriction)
	#[structopt(long = "max-objects")]
	pub max_objects: Option<String>,
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

	/// Set permission flags for key
	#[structopt(name = "allow")]
	Allow(KeyPermOpt),

	/// Unset permission flags for key
	#[structopt(name = "deny")]
	Deny(KeyPermOpt),

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
pub struct KeyPermOpt {
	/// ID or name of the key
	pub key_pattern: String,

	/// Flag that allows key to create buckets using S3's CreateBucket call
	#[structopt(long = "create-bucket")]
	pub create_bucket: bool,
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
pub struct MigrateOpt {
	/// Confirm the launch of the migrate operation
	#[structopt(long = "yes")]
	pub yes: bool,

	#[structopt(subcommand)]
	pub what: MigrateWhat,
}

#[derive(Serialize, Deserialize, StructOpt, Debug, Eq, PartialEq, Clone)]
pub enum MigrateWhat {
	/// Migrate buckets and permissions from v0.5.0
	#[structopt(name = "buckets050")]
	Buckets050,
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
		#[structopt(subcommand)]
		cmd: ScrubCmd,
	},
}

#[derive(Serialize, Deserialize, StructOpt, Debug, Eq, PartialEq, Clone)]
pub enum ScrubCmd {
	/// Start scrub
	#[structopt(name = "start")]
	Start,
	/// Pause scrub (it will resume automatically after 24 hours)
	#[structopt(name = "pause")]
	Pause,
	/// Resume paused scrub
	#[structopt(name = "resume")]
	Resume,
	/// Cancel scrub in progress
	#[structopt(name = "cancel")]
	Cancel,
	/// Set tranquility level for in-progress and future scrubs
	#[structopt(name = "set-tranquility")]
	SetTranquility {
		#[structopt()]
		tranquility: u32,
	},
}

#[derive(Serialize, Deserialize, StructOpt, Debug, Clone)]
pub struct OfflineRepairOpt {
	/// Confirm the launch of the repair operation
	#[structopt(long = "yes")]
	pub yes: bool,

	#[structopt(subcommand)]
	pub what: OfflineRepairWhat,
}

#[derive(Serialize, Deserialize, StructOpt, Debug, Eq, PartialEq, Clone)]
pub enum OfflineRepairWhat {
	/// Repair K2V item counters
	#[cfg(feature = "k2v")]
	#[structopt(name = "k2v_item_counters")]
	K2VItemCounters,
	/// Repair object counters
	#[structopt(name = "object_counters")]
	ObjectCounters,
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

#[derive(Serialize, Deserialize, StructOpt, Debug, Clone)]
pub struct WorkerOpt {
	#[structopt(subcommand)]
	pub cmd: WorkerCmd,
}

#[derive(Serialize, Deserialize, StructOpt, Debug, Eq, PartialEq, Clone)]
pub enum WorkerCmd {
	/// List all workers on Garage node
	#[structopt(name = "list")]
	List {
		#[structopt(flatten)]
		opt: WorkerListOpt,
	},
}

#[derive(Serialize, Deserialize, StructOpt, Debug, Eq, PartialEq, Clone, Copy)]
pub struct WorkerListOpt {
	/// Show only busy workers
	#[structopt(short = "b", long = "busy")]
	pub busy: bool,
	/// Show only workers with errors
	#[structopt(short = "e", long = "errors")]
	pub errors: bool,
}
