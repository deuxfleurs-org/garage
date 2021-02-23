use arc_swap::ArcSwapOption;
use std::sync::Arc;

use garage_rpc::membership::System;
use garage_rpc::ring::Ring;
use garage_util::data::*;

use crate::*;

#[derive(Clone)]
pub struct TableFullReplication {
	pub write_factor: usize,
	pub write_quorum: usize,

	neighbors: ArcSwapOption<Neighbors>,
}

#[derive(Clone)]
struct Neighbors {
	ring: Arc<Ring>,
	neighbors: Vec<UUID>,
}

impl TableFullReplication {
	pub fn new(write_factor: usize, write_quorum: usize) -> Self {
		TableFullReplication {
			write_factor,
			write_quorum,
			neighbors: ArcSwapOption::from(None),
		}
	}

	fn get_neighbors(&self, system: &System) -> Vec<UUID> {
		let neighbors = self.neighbors.load_full();
		if let Some(n) = neighbors {
			if Arc::ptr_eq(&n.ring, &system.ring.borrow()) {
				return n.neighbors.clone();
			}
		}

		// Recalculate neighbors
		let ring = system.ring.borrow().clone();
		let my_id = system.id;

		let mut nodes = vec![];
		for (node, _) in ring.config.members.iter() {
			let node_ranking = fasthash(&[node.as_slice(), my_id.as_slice()].concat());
			nodes.push((*node, node_ranking));
		}
		nodes.sort_by(|(_, rank1), (_, rank2)| rank1.cmp(rank2));
		let mut neighbors = nodes
			.drain(..)
			.map(|(node, _)| node)
			.filter(|node| *node != my_id)
			.take(self.write_factor)
			.collect::<Vec<_>>();

		neighbors.push(my_id);
		self.neighbors.swap(Some(Arc::new(Neighbors {
			ring,
			neighbors: neighbors.clone(),
		})));
		neighbors
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

	fn write_nodes(&self, _hash: &Hash, system: &System) -> Vec<UUID> {
		self.get_neighbors(system)
	}
	fn write_quorum(&self) -> usize {
		self.write_quorum
	}
	fn max_write_errors(&self) -> usize {
		self.write_factor - self.write_quorum
	}
	fn epidemic_writes(&self) -> bool {
		true
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
