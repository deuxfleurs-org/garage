//! Module containing types related to computing nodes which should receive a copy of data blocks
//! and metadata
use std::convert::TryInto;

use garage_util::data::*;

use crate::layout::ClusterLayout;

/// A partition id, which is stored on 16 bits
/// i.e. we have up to 2**16 partitions.
/// (in practice we have exactly 2**PARTITION_BITS partitions)
pub type Partition = u16;

// TODO: make this constant parametrizable in the config file
// For deployments with many nodes it might make sense to bump
// it up to 10.
// Maximum value : 16
/// How many bits from the hash are used to make partitions. Higher numbers means more fairness in
/// presence of numerous nodes, but exponentially bigger ring. Max 16
pub const PARTITION_BITS: usize = 8;

const PARTITION_MASK_U16: u16 = ((1 << PARTITION_BITS) - 1) << (16 - PARTITION_BITS);

/// A ring distributing fairly objects to nodes
#[derive(Clone)]
pub struct Ring {
	/// The replication factor for this ring
	pub replication_factor: usize,

	/// The network configuration used to generate this ring
	pub layout: ClusterLayout,

	// Internal order of nodes used to make a more compact representation of the ring
	nodes: Vec<Uuid>,

	// The list of entries in the ring
	ring: Vec<RingEntry>,
}

// Type to store compactly the id of a node in the system
// Change this to u16 the day we want to have more than 256 nodes in a cluster
pub type CompactNodeType = u8;

// The maximum number of times an object might get replicated
// This must be at least 3 because Garage supports 3-way replication
// Here we use 6 so that the size of a ring entry is 8 bytes
// (2 bytes partition id, 6 bytes node numbers as u8s)
const MAX_REPLICATION: usize = 6;

/// An entry in the ring
#[derive(Clone, Debug)]
struct RingEntry {
	// The two first bytes of the first hash that goes in this partition
	// (the next bytes are zeroes)
	hash_prefix: u16,
	// The nodes that store this partition, stored as a list of positions in the `nodes`
	// field of the Ring structure
	// Only items 0 up to ring.replication_factor - 1 are used, others are zeros
	nodes_buf: [CompactNodeType; MAX_REPLICATION],
}

impl Ring {
	pub(crate) fn new(layout: ClusterLayout, replication_factor: usize) -> Self {
		if replication_factor != layout.replication_factor {
			warn!("Could not build ring: replication factor does not match between local configuration and network role assignation.");
			return Self::empty(layout, replication_factor);
		}

		if layout.ring_assignation_data.len() != replication_factor * (1 << PARTITION_BITS) {
			warn!("Could not build ring: network role assignation data has invalid length");
			return Self::empty(layout, replication_factor);
		}

		let nodes = layout.node_id_vec.clone();
		let ring = (0..(1 << PARTITION_BITS))
			.map(|i| {
				let top = (i as u16) << (16 - PARTITION_BITS);
				let mut nodes_buf = [0u8; MAX_REPLICATION];
				nodes_buf[..replication_factor].copy_from_slice(
					&layout.ring_assignation_data
						[replication_factor * i..replication_factor * (i + 1)],
				);
				RingEntry {
					hash_prefix: top,
					nodes_buf,
				}
			})
			.collect::<Vec<_>>();

		Self {
			replication_factor,
			layout,
			nodes,
			ring,
		}
	}

	fn empty(layout: ClusterLayout, replication_factor: usize) -> Self {
		Self {
			replication_factor,
			layout,
			nodes: vec![],
			ring: vec![],
		}
	}

	/// Get the partition in which data would fall on
	pub fn partition_of(&self, position: &Hash) -> Partition {
		let top = u16::from_be_bytes(position.as_slice()[0..2].try_into().unwrap());
		top >> (16 - PARTITION_BITS)
	}

	/// Get the list of partitions and the first hash of a partition key that would fall in it
	pub fn partitions(&self) -> Vec<(Partition, Hash)> {
		let mut ret = vec![];

		for (i, entry) in self.ring.iter().enumerate() {
			let mut location = [0u8; 32];
			location[..2].copy_from_slice(&u16::to_be_bytes(entry.hash_prefix)[..]);
			ret.push((i as u16, location.into()));
		}
		if !ret.is_empty() {
			assert_eq!(ret[0].1, [0u8; 32].into());
		}

		ret
	}

	/// Walk the ring to find the n servers in which data should be replicated
	pub fn get_nodes(&self, position: &Hash, n: usize) -> Vec<Uuid> {
		if self.ring.len() != 1 << PARTITION_BITS {
			warn!("Ring not yet ready, read/writes will be lost!");
			return vec![];
		}

		let partition_idx = self.partition_of(position) as usize;
		let partition = &self.ring[partition_idx];

		let top = u16::from_be_bytes(position.as_slice()[0..2].try_into().unwrap());
		// Check that we haven't messed up our partition table, i.e. that this partition
		// table entrey indeed corresponds to the item we are storing
		assert_eq!(
			partition.hash_prefix & PARTITION_MASK_U16,
			top & PARTITION_MASK_U16
		);

		assert!(n <= self.replication_factor);
		partition.nodes_buf[..n]
			.iter()
			.map(|i| self.nodes[*i as usize])
			.collect::<Vec<_>>()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_ring_entry_size() {
		assert_eq!(std::mem::size_of::<RingEntry>(), 8);
	}
}
