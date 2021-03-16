use garage_rpc::ring::Ring;

use garage_util::data::*;

pub trait TableReplication: Send + Sync {
	// See examples in table_sharded.rs and table_fullcopy.rs
	// To understand various replication methods

	// Partition number of data item (for Merkle tree)
	fn partition_of(&self, hash: &Hash) -> u16;

	// Which nodes to send reads from
	fn read_nodes(&self, hash: &Hash) -> Vec<UUID>;
	fn read_quorum(&self) -> usize;

	// Which nodes to send writes to
	fn write_nodes(&self, hash: &Hash) -> Vec<UUID>;
	fn write_quorum(&self) -> usize;
	fn max_write_errors(&self) -> usize;

	// Get partition boundaries
	fn split_points(&self, ring: &Ring) -> Vec<Hash>;
}
