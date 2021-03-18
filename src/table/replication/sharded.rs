use std::sync::Arc;

use garage_rpc::membership::System;
use garage_rpc::ring::*;
use garage_util::data::*;

use crate::replication::*;

#[derive(Clone)]
pub struct TableShardedReplication {
	pub system: Arc<System>,
	pub replication_factor: usize,
	pub read_quorum: usize,
	pub write_quorum: usize,
}

impl TableReplication for TableShardedReplication {
	// Sharded replication schema:
	// - based on the ring of nodes, a certain set of neighbors
	//   store entries, given as a function of the position of the
	//   entry's hash in the ring
	// - reads are done on all of the nodes that replicate the data
	// - writes as well

	fn read_nodes(&self, hash: &Hash) -> Vec<UUID> {
		let ring = self.system.ring.borrow().clone();
		ring.walk_ring(&hash, self.replication_factor)
	}
	fn read_quorum(&self) -> usize {
		self.read_quorum
	}

	fn write_nodes(&self, hash: &Hash) -> Vec<UUID> {
		let ring = self.system.ring.borrow();
		ring.walk_ring(&hash, self.replication_factor)
	}
	fn write_quorum(&self) -> usize {
		self.write_quorum
	}
	fn max_write_errors(&self) -> usize {
		self.replication_factor - self.write_quorum
	}

	fn partition_of(&self, hash: &Hash) -> Partition {
		self.system.ring.borrow().partition_of(hash)
	}
	fn partitions(&self) -> Vec<(Partition, Hash)> {
		self.system.ring.borrow().partitions()
	}
}
