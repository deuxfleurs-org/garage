use garage_rpc::membership::System;
use garage_rpc::ring::Ring;

use garage_util::data::*;

pub trait TableReplication: Send + Sync {
	// See examples in table_sharded.rs and table_fullcopy.rs
	// To understand various replication methods

	// Which nodes to send reads from
	fn read_nodes(&self, hash: &Hash, system: &System) -> Vec<UUID>;
	fn read_quorum(&self) -> usize;

	// Which nodes to send writes to
	fn write_nodes(&self, hash: &Hash, system: &System) -> Vec<UUID>;
	fn write_quorum(&self, system: &System) -> usize;
	fn max_write_errors(&self) -> usize;

	// Which are the nodes that do actually replicate the data
	fn replication_nodes(&self, hash: &Hash, ring: &Ring) -> Vec<UUID>;
	fn split_points(&self, ring: &Ring) -> Vec<Hash>;
}
