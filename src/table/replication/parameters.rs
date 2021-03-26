use garage_rpc::ring::*;

use garage_util::data::*;

/// Trait to describe how a table shall be replicated
pub trait TableReplication: Send + Sync {
	// See examples in table_sharded.rs and table_fullcopy.rs
	// To understand various replication methods

	/// Which nodes to send read requests to
	fn read_nodes(&self, hash: &Hash) -> Vec<UUID>;
	/// Responses needed to consider a read succesfull
	fn read_quorum(&self) -> usize;

	/// Which nodes to send writes to
	fn write_nodes(&self, hash: &Hash) -> Vec<UUID>;
	/// Responses needed to consider a write succesfull
	fn write_quorum(&self) -> usize;
	// this feels like its write_nodes().len() - write_quorum()
	fn max_write_errors(&self) -> usize;

	// Accessing partitions, for Merkle tree & sync
	/// Get partition for data with given hash
	fn partition_of(&self, hash: &Hash) -> Partition;
	/// List of existing partitions
	fn partitions(&self) -> Vec<(Partition, Hash)>;
}
