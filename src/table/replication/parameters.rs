use garage_rpc::ring::*;

use garage_util::data::*;

pub trait TableReplication: Send + Sync {
	// See examples in table_sharded.rs and table_fullcopy.rs
	// To understand various replication methods

	// Which nodes to send reads from
	fn read_nodes(&self, hash: &Hash) -> Vec<UUID>;
	fn read_quorum(&self) -> usize;

	// Which nodes to send writes to
	fn write_nodes(&self, hash: &Hash) -> Vec<UUID>;
	fn write_quorum(&self) -> usize;
	fn max_write_errors(&self) -> usize;

	// Accessing partitions, for Merkle tree & sync
	fn partition_of(&self, hash: &Hash) -> Partition;
	fn partitions(&self) -> Vec<(Partition, Hash)>;
}
