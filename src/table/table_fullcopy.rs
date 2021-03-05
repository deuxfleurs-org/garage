use std::sync::Arc;

use garage_rpc::membership::System;
use garage_rpc::ring::Ring;
use garage_util::data::*;

use crate::*;

#[derive(Clone)]
pub struct TableFullReplication {
	pub max_faults: usize,
}

#[derive(Clone)]
struct Neighbors {
	ring: Arc<Ring>,
	neighbors: Vec<UUID>,
}

impl TableFullReplication {
	pub fn new(max_faults: usize) -> Self {
		TableFullReplication { max_faults }
	}
}

impl TableReplication for TableFullReplication {
	// Full replication schema: all nodes store everything
	// Writes are disseminated in an epidemic manner in the network

	// Advantage: do all reads locally, extremely fast
	// Inconvenient: only suitable to reasonably small tables

	fn read_nodes(&self, _hash: &Hash, system: &System) -> Vec<UUID> {
		vec![system.id]
	}
	fn read_quorum(&self) -> usize {
		1
	}

	fn write_nodes(&self, hash: &Hash, system: &System) -> Vec<UUID> {
		self.replication_nodes(hash, system.ring.borrow().as_ref())
	}
	fn write_quorum(&self, system: &System) -> usize {
		system.ring.borrow().config.members.len() - self.max_faults
	}
	fn max_write_errors(&self) -> usize {
		self.max_faults
	}

	fn replication_nodes(&self, _hash: &Hash, ring: &Ring) -> Vec<UUID> {
		ring.config.members.keys().cloned().collect::<Vec<_>>()
	}
	fn split_points(&self, _ring: &Ring) -> Vec<Hash> {
		let mut ret = vec![];
		ret.push([0u8; 32].into());
		ret.push([0xFFu8; 32].into());
		ret
	}
}
