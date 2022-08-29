use serde::{Deserialize, Serialize};

use garage_util::version;
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
pub enum Command {
	/// Run Garage server
	#[structopt(name = "server", version = version::garage())]
	Server,

	/// Get network status
	#[structopt(name = "status", version = version::garage())]
	Status,

	/// Operations on individual Garage nodes
	#[structopt(name = "node", version = version::garage())]
	Node(NodeOperation),

	/// Operations on the assignation of node roles in the cluster layout
	#[structopt(name = "layout", version = version::garage())]
	Layout(LayoutOperation),

	/// Operations on buckets
	#[structopt(name = "bucket", version = version::garage())]
	Bucket(BucketOperation),

	/// Operations on S3 access keys
	#[structopt(name = "key", version = version::garage())]
	Key(KeyOperation),

	/// Run migrations from previous Garage version
	/// (DO NOT USE WITHOUT READING FULL DOCUMENTATION)
	#[structopt(name = "migrate", version = version::garage())]
	Migrate(MigrateOpt),

	/// Start repair of node data on remote node
	#[structopt(name = "repair", version = version::garage())]
	Repair(RepairOpt),

	/// Offline reparation of node data (these repairs must be run offline
	/// directly on the server node)
	#[structopt(name = "offline-repair", version = version::garage())]
	OfflineRepair(OfflineRepairOpt),

	/// Gather node statistics
	#[structopt(name = "stats", version = version::garage())]
	Stats(StatsOpt),

	/// Manage background workers
	#[structopt(name = "worker", version = version::garage())]
	Worker(WorkerOpt),
}

#[derive(StructOpt, Debug)]
pub enum NodeOperation {
	/// Print identifier (public key) of this Garage node
	#[structopt(name = "id", version = version::garage())]
	NodeId(NodeIdOpt),

	/// Connect to Garage node that is currently isolated from the system
	#[structopt(name = "connect", version = version::garage())]
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
	#[structopt(name = "assign", version = version::garage())]
	Assign(AssignRoleOpt),

	/// Remove role from Garage cluster node
	#[structopt(name = "remove", version = version::garage())]
	Remove(RemoveRoleOpt),

	/// Show roles currently assigned to nodes and changes staged for commit
	#[structopt(name = "show", version = version::garage())]
	Show,

	/// Apply staged changes to cluster layout
	#[structopt(name = "apply", version = version::garage())]
	Apply(ApplyLayoutOpt),

	/// Revert staged changes to cluster layout
	#[structopt(name = "revert", version = version::garage())]
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
	#[structopt(name = "list", version = version::garage())]
	List,

	/// Get bucket info
	#[structopt(name = "info", version = version::garage())]
	Info(BucketOpt),

	/// Create bucket
	#[structopt(name = "create", version = version::garage())]
	Create(BucketOpt),

	/// Delete bucket
	#[structopt(name = "delete", version = version::garage())]
	Delete(DeleteBucketOpt),

	/// Alias bucket under new name
	#[structopt(name = "alias", version = version::garage())]
	Alias(AliasBucketOpt),

	/// Remove bucket alias
	#[structopt(name = "unalias", version = version::garage())]
	Unalias(UnaliasBucketOpt),

	/// Allow key to read or write to bucket
	#[structopt(name = "allow", version = version::garage())]
	Allow(PermBucketOpt),

	/// Deny key from reading or writing to bucket
	#[structopt(name = "deny", version = version::garage())]
	Deny(PermBucketOpt),

	/// Expose as website or not
	#[structopt(name = "website", version = version::garage())]
	Website(WebsiteOpt),

	/// Set the quotas for this bucket
	#[structopt(name = "set-quotas", version = version::garage())]
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
	#[structopt(name = "list", version = version::garage())]
	List,

	/// Get key info
	#[structopt(name = "info", version = version::garage())]
	Info(KeyOpt),

	/// Create new key
	#[structopt(name = "new", version = version::garage())]
	New(KeyNewOpt),

	/// Rename key
	#[structopt(name = "rename", version = version::garage())]
	Rename(KeyRenameOpt),

	/// Delete key
	#[structopt(name = "delete", version = version::garage())]
	Delete(KeyDeleteOpt),

	/// Set permission flags for key
	#[structopt(name = "allow", version = version::garage())]
	Allow(KeyPermOpt),

	/// Unset permission flags for key
	#[structopt(name = "deny", version = version::garage())]
	Deny(KeyPermOpt),

	/// Import key
	#[structopt(name = "import", version = version::garage())]
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
	#[structopt(name = "buckets050", version = version::garage())]
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
	#[structopt(name = "tables", version = version::garage())]
	Tables,
	/// Only repair (resync/rebalance) the set of stored blocks
	#[structopt(name = "blocks", version = version::garage())]
	Blocks,
	/// Only redo the propagation of object deletions to the version table (slow)
	#[structopt(name = "versions", version = version::garage())]
	Versions,
	/// Only redo the propagation of version deletions to the block ref table (extremely slow)
	#[structopt(name = "block_refs", version = version::garage())]
	BlockRefs,
	/// Verify integrity of all blocks on disc (extremely slow, i/o intensive)
	#[structopt(name = "scrub", version = version::garage())]
	Scrub {
		#[structopt(subcommand)]
		cmd: ScrubCmd,
	},
}

#[derive(Serialize, Deserialize, StructOpt, Debug, Eq, PartialEq, Clone)]
pub enum ScrubCmd {
	/// Start scrub
	#[structopt(name = "start", version = version::garage())]
	Start,
	/// Pause scrub (it will resume automatically after 24 hours)
	#[structopt(name = "pause", version = version::garage())]
	Pause,
	/// Resume paused scrub
	#[structopt(name = "resume", version = version::garage())]
	Resume,
	/// Cancel scrub in progress
	#[structopt(name = "cancel", version = version::garage())]
	Cancel,
	/// Set tranquility level for in-progress and future scrubs
	#[structopt(name = "set-tranquility", version = version::garage())]
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
	#[structopt(name = "k2v_item_counters", version = version::garage())]
	K2VItemCounters,
	/// Repair object counters
	#[structopt(name = "object_counters", version = version::garage())]
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
	#[structopt(name = "list", version = version::garage())]
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
