use serde::{Deserialize, Serialize};
use structopt::StructOpt;

use garage_util::version::garage_version;

use crate::cli::convert_db;

#[derive(StructOpt, Debug)]
pub enum Command {
	/// Run Garage server
	#[structopt(name = "server", version = garage_version())]
	Server,

	/// Get network status
	#[structopt(name = "status", version = garage_version())]
	Status,

	/// Operations on individual Garage nodes
	#[structopt(name = "node", version = garage_version())]
	Node(NodeOperation),

	/// Operations on the assignation of node roles in the cluster layout
	#[structopt(name = "layout", version = garage_version())]
	Layout(LayoutOperation),

	/// Operations on buckets
	#[structopt(name = "bucket", version = garage_version())]
	Bucket(BucketOperation),

	/// Operations on S3 access keys
	#[structopt(name = "key", version = garage_version())]
	Key(KeyOperation),

	/// Run migrations from previous Garage version
	/// (DO NOT USE WITHOUT READING FULL DOCUMENTATION)
	#[structopt(name = "migrate", version = garage_version())]
	Migrate(MigrateOpt),

	/// Start repair of node data on remote node
	#[structopt(name = "repair", version = garage_version())]
	Repair(RepairOpt),

	/// Offline reparation of node data (these repairs must be run offline
	/// directly on the server node)
	#[structopt(name = "offline-repair", version = garage_version())]
	OfflineRepair(OfflineRepairOpt),

	/// Gather node statistics
	#[structopt(name = "stats", version = garage_version())]
	Stats(StatsOpt),

	/// Manage background workers
	#[structopt(name = "worker", version = garage_version())]
	Worker(WorkerOperation),

	/// Low-level debug operations on data blocks
	#[structopt(name = "block", version = garage_version())]
	Block(BlockOperation),

	/// Convert metadata db between database engine formats
	#[structopt(name = "convert-db", version = garage_version())]
	ConvertDb(convert_db::ConvertDbOpt),
}

#[derive(StructOpt, Debug)]
pub enum NodeOperation {
	/// Print identifier (public key) of this Garage node
	#[structopt(name = "id", version = garage_version())]
	NodeId(NodeIdOpt),

	/// Connect to Garage node that is currently isolated from the system
	#[structopt(name = "connect", version = garage_version())]
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
	#[structopt(name = "assign", version = garage_version())]
	Assign(AssignRoleOpt),

	/// Remove role from Garage cluster node
	#[structopt(name = "remove", version = garage_version())]
	Remove(RemoveRoleOpt),

	/// Show roles currently assigned to nodes and changes staged for commit
	#[structopt(name = "show", version = garage_version())]
	Show,

	/// Apply staged changes to cluster layout
	#[structopt(name = "apply", version = garage_version())]
	Apply(ApplyLayoutOpt),

	/// Revert staged changes to cluster layout
	#[structopt(name = "revert", version = garage_version())]
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
	#[structopt(name = "list", version = garage_version())]
	List,

	/// Get bucket info
	#[structopt(name = "info", version = garage_version())]
	Info(BucketOpt),

	/// Create bucket
	#[structopt(name = "create", version = garage_version())]
	Create(BucketOpt),

	/// Delete bucket
	#[structopt(name = "delete", version = garage_version())]
	Delete(DeleteBucketOpt),

	/// Alias bucket under new name
	#[structopt(name = "alias", version = garage_version())]
	Alias(AliasBucketOpt),

	/// Remove bucket alias
	#[structopt(name = "unalias", version = garage_version())]
	Unalias(UnaliasBucketOpt),

	/// Allow key to read or write to bucket
	#[structopt(name = "allow", version = garage_version())]
	Allow(PermBucketOpt),

	/// Deny key from reading or writing to bucket
	#[structopt(name = "deny", version = garage_version())]
	Deny(PermBucketOpt),

	/// Expose as website or not
	#[structopt(name = "website", version = garage_version())]
	Website(WebsiteOpt),

	/// Set the quotas for this bucket
	#[structopt(name = "set-quotas", version = garage_version())]
	SetQuotas(SetQuotasOpt),

	/// Clean up (abort) old incomplete multipart uploads
	#[structopt(name = "cleanup-incomplete-uploads", version = garage_version())]
	CleanupIncompleteUploads(CleanupIncompleteUploadsOpt),
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

	/// Error document: the optional document returned when an error occurs
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
pub struct CleanupIncompleteUploadsOpt {
	/// Abort multipart uploads older than this value
	#[structopt(long = "older-than", default_value = "1d")]
	pub older_than: String,

	/// Name of bucket(s) to clean up
	#[structopt(required = true)]
	pub buckets: Vec<String>,
}

#[derive(Serialize, Deserialize, StructOpt, Debug)]
pub enum KeyOperation {
	/// List keys
	#[structopt(name = "list", version = garage_version())]
	List,

	/// Get key info
	#[structopt(name = "info", version = garage_version())]
	Info(KeyOpt),

	/// Create new key
	#[structopt(name = "new", version = garage_version())]
	New(KeyNewOpt),

	/// Rename key
	#[structopt(name = "rename", version = garage_version())]
	Rename(KeyRenameOpt),

	/// Delete key
	#[structopt(name = "delete", version = garage_version())]
	Delete(KeyDeleteOpt),

	/// Set permission flags for key
	#[structopt(name = "allow", version = garage_version())]
	Allow(KeyPermOpt),

	/// Unset permission flags for key
	#[structopt(name = "deny", version = garage_version())]
	Deny(KeyPermOpt),

	/// Import key
	#[structopt(name = "import", version = garage_version())]
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
	#[structopt(name = "buckets050", version = garage_version())]
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
	#[structopt(name = "tables", version = garage_version())]
	Tables,
	/// Only repair (resync/rebalance) the set of stored blocks
	#[structopt(name = "blocks", version = garage_version())]
	Blocks,
	/// Only redo the propagation of object deletions to the version table (slow)
	#[structopt(name = "versions", version = garage_version())]
	Versions,
	/// Only redo the propagation of version deletions to the block ref table (extremely slow)
	#[structopt(name = "block_refs", version = garage_version())]
	BlockRefs,
	/// Verify integrity of all blocks on disc (extremely slow, i/o intensive)
	#[structopt(name = "scrub", version = garage_version())]
	Scrub {
		#[structopt(subcommand)]
		cmd: ScrubCmd,
	},
}

#[derive(Serialize, Deserialize, StructOpt, Debug, Eq, PartialEq, Clone)]
pub enum ScrubCmd {
	/// Start scrub
	#[structopt(name = "start", version = garage_version())]
	Start,
	/// Pause scrub (it will resume automatically after 24 hours)
	#[structopt(name = "pause", version = garage_version())]
	Pause,
	/// Resume paused scrub
	#[structopt(name = "resume", version = garage_version())]
	Resume,
	/// Cancel scrub in progress
	#[structopt(name = "cancel", version = garage_version())]
	Cancel,
	/// Set tranquility level for in-progress and future scrubs
	#[structopt(name = "set-tranquility", version = garage_version())]
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
	#[structopt(name = "k2v_item_counters", version = garage_version())]
	K2VItemCounters,
	/// Repair object counters
	#[structopt(name = "object_counters", version = garage_version())]
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

	/// Don't show global cluster stats (internal use in RPC)
	#[structopt(skip)]
	#[serde(default)]
	pub skip_global: bool,
}

#[derive(Serialize, Deserialize, StructOpt, Debug, Eq, PartialEq, Clone)]
pub enum WorkerOperation {
	/// List all workers on Garage node
	#[structopt(name = "list", version = garage_version())]
	List {
		#[structopt(flatten)]
		opt: WorkerListOpt,
	},
	/// Get detailed information about a worker
	#[structopt(name = "info", version = garage_version())]
	Info { tid: usize },
	/// Get worker parameter
	#[structopt(name = "get", version = garage_version())]
	Get {
		/// Gather variable values from all nodes
		#[structopt(short = "a", long = "all-nodes")]
		all_nodes: bool,
		/// Variable name to get, or none to get all variables
		variable: Option<String>,
	},
	/// Set worker parameter
	#[structopt(name = "set", version = garage_version())]
	Set {
		/// Set variable values on all nodes
		#[structopt(short = "a", long = "all-nodes")]
		all_nodes: bool,
		/// Variable node to set
		variable: String,
		/// Value to set the variable to
		value: String,
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

#[derive(Serialize, Deserialize, StructOpt, Debug, Eq, PartialEq, Clone)]
pub enum BlockOperation {
	/// List all blocks that currently have a resync error
	#[structopt(name = "list-errors", version = garage_version())]
	ListErrors,
	/// Get detailed information about a single block
	#[structopt(name = "info", version = garage_version())]
	Info {
		/// Hash of the block for which to retrieve information
		hash: String,
	},
	/// Retry now the resync of one or many blocks
	#[structopt(name = "retry-now", version = garage_version())]
	RetryNow {
		/// Retry all blocks that have a resync error
		#[structopt(long = "all")]
		all: bool,
		/// Hashes of the block to retry to resync now
		blocks: Vec<String>,
	},
	/// Delete all objects referencing a missing block
	#[structopt(name = "purge", version = garage_version())]
	Purge {
		/// Mandatory to confirm this operation
		#[structopt(long = "yes")]
		yes: bool,
		/// Hashes of the block to purge
		#[structopt(required = true)]
		blocks: Vec<String>,
	},
}
