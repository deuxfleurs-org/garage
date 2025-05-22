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

	/// Operations on the assignment of node roles in the cluster layout
	#[structopt(name = "layout", version = garage_version())]
	Layout(LayoutOperation),

	/// Operations on buckets
	#[structopt(name = "bucket", version = garage_version())]
	Bucket(BucketOperation),

	/// Operations on S3 access keys
	#[structopt(name = "key", version = garage_version())]
	Key(KeyOperation),

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

	/// Low-level node-local debug operations on data blocks
	#[structopt(name = "block", version = garage_version())]
	Block(BlockOperation),

	/// Operations on the metadata db
	#[structopt(name = "meta", version = garage_version())]
	Meta(MetaOperation),

	/// Convert metadata db between database engine formats
	#[structopt(name = "convert-db", version = garage_version())]
	ConvertDb(convert_db::ConvertDbOpt),
}

#[derive(StructOpt, Debug)]
pub enum NodeOperation {
	/// Print the full node ID (public key) of this Garage node, and its publicly reachable IP
	/// address and port if they are specified in config file under `rpc_public_addr`
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
	/// Full node ID (public key) and IP address and port, in the format:
	/// `<full node ID>@<ip or hostname>:<port>`.
	/// You can retrieve this information on the target node using `garage node id`.
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

	/// Configure parameters value for the layout computation
	#[structopt(name = "config", version = garage_version())]
	Config(ConfigLayoutOpt),

	/// Show roles currently assigned to nodes and changes staged for commit
	#[structopt(name = "show", version = garage_version())]
	Show,

	/// Apply staged changes to cluster layout
	#[structopt(name = "apply", version = garage_version())]
	Apply(ApplyLayoutOpt),

	/// Revert staged changes to cluster layout
	#[structopt(name = "revert", version = garage_version())]
	Revert(RevertLayoutOpt),

	/// View the history of layouts in the cluster
	#[structopt(name = "history", version = garage_version())]
	History,

	/// Skip dead nodes when awaiting for a new layout version to be synchronized
	#[structopt(name = "skip-dead-nodes", version = garage_version())]
	SkipDeadNodes(SkipDeadNodesOpt),
}

#[derive(StructOpt, Debug)]
pub struct AssignRoleOpt {
	/// Node(s) to which to assign role (prefix of hexadecimal node id)
	#[structopt(required = true)]
	pub(crate) node_ids: Vec<String>,

	/// Location (zone or datacenter) of the node
	#[structopt(short = "z", long = "zone")]
	pub(crate) zone: Option<String>,

	/// Storage capacity, in bytes (supported suffixes: B, KB, MB, GB, TB, PB)
	#[structopt(short = "c", long = "capacity")]
	pub(crate) capacity: Option<bytesize::ByteSize>,

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
pub struct ConfigLayoutOpt {
	/// Zone redundancy parameter ('none'/'max' or integer)
	#[structopt(short = "r", long = "redundancy")]
	pub(crate) redundancy: Option<String>,
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
	/// The revert operation will not be ran unless this flag is added
	#[structopt(long = "yes")]
	pub(crate) yes: bool,
}

#[derive(StructOpt, Debug)]
pub struct SkipDeadNodesOpt {
	/// Version number of the layout to assume is currently up-to-date.
	/// This will generally be the current layout version.
	#[structopt(long = "version")]
	pub(crate) version: u64,
	/// Allow the skip even if a quorum of nodes could not be found for
	/// the data among the remaining nodes
	#[structopt(long = "allow-missing-data")]
	pub(crate) allow_missing_data: bool,
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
	Info(KeyInfoOpt),

	/// Create new key
	#[structopt(name = "create", version = garage_version())]
	Create(KeyNewOpt),

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
pub struct KeyInfoOpt {
	/// ID or name of the key
	pub key_pattern: String,
	/// Whether to display the secret key
	#[structopt(long = "show-secret")]
	pub show_secret: bool,
}

#[derive(Serialize, Deserialize, StructOpt, Debug)]
pub struct KeyNewOpt {
	/// Name of the key
	#[structopt(default_value = "Unnamed key")]
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

	/// Confirm key import
	#[structopt(long = "yes")]
	pub yes: bool,
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
	/// Do a full sync of metadata tables
	#[structopt(name = "tables", version = garage_version())]
	Tables,
	/// Repair (resync/rebalance) the set of stored blocks in the cluster
	#[structopt(name = "blocks", version = garage_version())]
	Blocks,
	/// Repropagate object deletions to the version table
	#[structopt(name = "versions", version = garage_version())]
	Versions,
	/// Repropagate object deletions to the multipart upload table
	#[structopt(name = "mpu", version = garage_version())]
	MultipartUploads,
	/// Repropagate version deletions to the block ref table
	#[structopt(name = "block-refs", version = garage_version())]
	BlockRefs,
	/// Recalculate block reference counters
	#[structopt(name = "block-rc", version = garage_version())]
	BlockRc,
	/// Fix inconsistency in bucket aliases (WARNING: EXPERIMENTAL)
	#[structopt(name = "aliases", version = garage_version())]
	Aliases,
	/// Verify integrity of all blocks on disc
	#[structopt(name = "scrub", version = garage_version())]
	Scrub {
		#[structopt(subcommand)]
		cmd: ScrubCmd,
	},
	/// Rebalance data blocks among HDDs on individual nodes
	#[structopt(name = "rebalance", version = garage_version())]
	Rebalance,
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

#[derive(Serialize, Deserialize, StructOpt, Debug, Eq, PartialEq, Clone, Copy)]
pub enum MetaOperation {
	/// Save a snapshot of the metadata db file
	#[structopt(name = "snapshot", version = garage_version())]
	Snapshot {
		/// Run on all nodes instead of only local node
		#[structopt(long = "all")]
		all: bool,
	},
}
