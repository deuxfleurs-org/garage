use std::sync::Arc;

use garage_rpc::membership::System;
use garage_rpc::ring::Ring;
use garage_util::data::*;

use crate::replication::*;

#[derive(Clone)]
pub struct TableFullReplication {
	pub system: Arc<System>,
	pub max_faults: usize,
}

impl TableReplication for TableFullReplication {
	// Full replication schema: all nodes store everything
	// Writes are disseminated in an epidemic manner in the network

	// Advantage: do all reads locally, extremely fast
	// Inconvenient: only suitable to reasonably small tables

	fn partition_of(&self, _hash: &Hash) -> u16 {
		0u16
	}

	fn read_nodes(&self, _hash: &Hash) -> Vec<UUID> {
		vec![self.system.id]
	}
	fn read_quorum(&self) -> usize {
		1
	}

	fn write_nodes(&self, _hash: &Hash) -> Vec<UUID> {
		let ring = self.system.ring.borrow();
		ring.config.members.keys().cloned().collect::<Vec<_>>()
	}
	fn write_quorum(&self) -> usize {
		let nmembers = self.system.ring.borrow().config.members.len();
		if nmembers > self.max_faults {
			nmembers - self.max_faults
		} else {
			1
		}
	}
	fn max_write_errors(&self) -> usize {
		self.max_faults
	}

	fn split_points(&self, _ring: &Ring) -> Vec<Hash> {
		let mut ret = vec![];
		ret.push([0u8; 32].into());
		ret
	}
}
