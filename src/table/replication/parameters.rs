use garage_rpc::layout::*;
use garage_util::data::*;

/// Trait to describe how a table shall be replicated
pub trait TableReplication: Send + Sync + 'static {
	type WriteSets: AsRef<Vec<Vec<Uuid>>> + AsMut<Vec<Vec<Uuid>>> + Send + Sync + 'static;

	// See examples in table_sharded.rs and table_fullcopy.rs
	// To understand various replication methods

	/// The entire list of all nodes that store a partition
	fn storage_nodes(&self, hash: &Hash) -> Vec<Uuid>;

	/// Which nodes to send read requests to
	fn read_nodes(&self, hash: &Hash) -> Vec<Uuid>;
	/// Responses needed to consider a read successful
	fn read_quorum(&self) -> usize;

	/// Which nodes to send writes to
	fn write_sets(&self, hash: &Hash) -> Self::WriteSets;
	/// Responses needed to consider a write successful in each set
	fn write_quorum(&self) -> usize;

	// Accessing partitions, for Merkle tree & sync
	/// Get partition for data with given hash
	fn partition_of(&self, hash: &Hash) -> Partition;
	/// List of partitions and nodes to sync with in current layout
	fn sync_partitions(&self) -> SyncPartitions;
}

#[derive(Debug)]
pub struct SyncPartitions {
	pub layout_version: u64,
	pub partitions: Vec<SyncPartition>,
}

#[derive(Debug)]
pub struct SyncPartition {
	pub partition: Partition,
	pub first_hash: Hash,
	pub last_hash: Hash,
	pub storage_sets: Vec<Vec<Uuid>>,
}
