use std::sync::Arc;

use garage_rpc::ring::*;
use garage_rpc::system::System;
use garage_util::data::*;

use crate::replication::*;

/// Full replication schema: all nodes store everything
/// Writes are disseminated in an epidemic manner in the network
/// Advantage: do all reads locally, extremely fast
/// Inconvenient: only suitable to reasonably small tables
#[derive(Clone)]
pub struct TableFullReplication {
	/// The membership manager of this node
	pub system: Arc<System>,
	/// Max number of faults allowed while replicating a record
	pub max_faults: usize,
}

impl TableReplication for TableFullReplication {
	fn read_nodes(&self, _hash: &Hash) -> Vec<Uuid> {
		vec![self.system.id]
	}
	fn read_quorum(&self) -> usize {
		1
	}

	fn write_nodes(&self, _hash: &Hash) -> Vec<Uuid> {
		let ring = self.system.ring.borrow();
		ring.layout.node_ids().to_vec()
	}
	fn write_quorum(&self) -> usize {
		let nmembers = self.system.ring.borrow().layout.node_ids().len();
		if nmembers > self.max_faults {
			nmembers - self.max_faults
		} else {
			1
		}
	}
	fn max_write_errors(&self) -> usize {
		self.max_faults
	}

	fn partition_of(&self, _hash: &Hash) -> Partition {
		0u16
	}
	fn partitions(&self) -> Vec<(Partition, Hash)> {
		vec![(0u16, [0u8; 32].into())]
	}
}
