//! Module containing types related to computing nodes which should receive a copy of data blocks
//! and metadata
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;

use serde::{Deserialize, Serialize};

use garage_util::data::*;

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

/// The user-defined configuration of the cluster's nodes
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkConfig {
	/// Map of each node's id to it's configuration
	pub members: HashMap<Uuid, NetworkConfigEntry>,
	/// Version of this config
	pub version: u64,
}

impl NetworkConfig {
	pub(crate) fn new() -> Self {
		Self {
			members: HashMap::new(),
			version: 0,
		}
	}
}

/// The overall configuration of one (possibly remote) node
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkConfigEntry {
	/// Datacenter at which this entry belong. This infromation might be used to perform a better
	/// geodistribution
	pub zone: String,
	/// The (relative) capacity of the node
	/// If this is set to None, the node does not participate in storing data for the system
	/// and is only active as an API gateway to other nodes
	pub capacity: Option<u32>,
	/// A tag to recognize the entry, not used for other things than display
	pub tag: String,
}

impl NetworkConfigEntry {
	pub fn capacity_string(&self) -> String {
		match self.capacity {
			Some(c) => format!("{}", c),
			None => "gateway".to_string(),
		}
	}
}

/// A ring distributing fairly objects to nodes
#[derive(Clone)]
pub struct Ring {
	/// The replication factor for this ring
	pub replication_factor: usize,

	/// The network configuration used to generate this ring
	pub config: NetworkConfig,

	// Internal order of nodes used to make a more compact representation of the ring
	nodes: Vec<Uuid>,

	// The list of entries in the ring
	ring: Vec<RingEntry>,
}

// Type to store compactly the id of a node in the system
// Change this to u16 the day we want to have more than 256 nodes in a cluster
type CompactNodeType = u8;

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
	// TODO this function MUST be refactored, it's 100 lines long, with a 50 lines loop, going up to 6
	// levels of imbrication. It is basically impossible to test, maintain, or understand for an
	// outsider.
	pub(crate) fn new(config: NetworkConfig, replication_factor: usize) -> Self {
		// Create a vector of partition indices (0 to 2**PARTITION_BITS-1)
		let partitions_idx = (0usize..(1usize << PARTITION_BITS)).collect::<Vec<_>>();

		let zones = config
			.members
			.iter()
			.filter(|(_id, info)| info.capacity.is_some())
			.map(|(_id, info)| info.zone.as_str())
			.collect::<HashSet<&str>>();
		let n_zones = zones.len();

		// Prepare ring
		let mut partitions: Vec<Vec<(&Uuid, &NetworkConfigEntry)>> = partitions_idx
			.iter()
			.map(|_i| Vec::new())
			.collect::<Vec<_>>();

		// Create MagLev priority queues for each node
		let mut queues = config
			.members
			.iter()
			.filter(|(_id, info)| info.capacity.is_some())
			.map(|(node_id, node_info)| {
				let mut parts = partitions_idx
					.iter()
					.map(|i| {
						let part_data =
							[&u16::to_be_bytes(*i as u16)[..], node_id.as_slice()].concat();
						(*i, fasthash(&part_data[..]))
					})
					.collect::<Vec<_>>();
				parts.sort_by_key(|(_i, h)| *h);
				let parts_i = parts.iter().map(|(i, _h)| *i).collect::<Vec<_>>();
				(node_id, node_info, parts_i, 0)
			})
			.collect::<Vec<_>>();

		let max_capacity = config
			.members
			.iter()
			.filter_map(|(_, node_info)| node_info.capacity)
			.fold(0, std::cmp::max);

		assert!(replication_factor <= MAX_REPLICATION);

		// Fill up ring
		for rep in 0..replication_factor {
			queues.sort_by_key(|(ni, _np, _q, _p)| {
				let queue_data = [&u16::to_be_bytes(rep as u16)[..], ni.as_slice()].concat();
				fasthash(&queue_data[..])
			});

			for (_, _, _, pos) in queues.iter_mut() {
				*pos = 0;
			}

			let mut remaining = partitions_idx.len();
			while remaining > 0 {
				let remaining0 = remaining;
				for i_round in 0..max_capacity {
					for (node_id, node_info, q, pos) in queues.iter_mut() {
						if i_round >= node_info.capacity.unwrap() {
							continue;
						}
						for (pos2, &qv) in q.iter().enumerate().skip(*pos) {
							if partitions[qv].len() != rep {
								continue;
							}
							let p_zns = partitions[qv]
								.iter()
								.map(|(_id, info)| info.zone.as_str())
								.collect::<HashSet<&str>>();
							if (p_zns.len() < n_zones
								&& !p_zns.contains(&node_info.zone.as_str()))
								|| (p_zns.len() == n_zones
									&& !partitions[qv].iter().any(|(id, _i)| id == node_id))
							{
								partitions[qv].push((node_id, node_info));
								remaining -= 1;
								*pos = pos2 + 1;
								break;
							}
						}
					}
				}
				if remaining == remaining0 {
					// No progress made, exit
					warn!("Could not build ring, not enough nodes configured.");
					return Self {
						replication_factor,
						config,
						nodes: vec![],
						ring: vec![],
					};
				}
			}
		}

		// Make a canonical order for nodes
		let nodes = config
			.members
			.iter()
			.filter(|(_id, info)| info.capacity.is_some())
			.map(|(id, _)| *id)
			.collect::<Vec<_>>();
		let nodes_rev = nodes
			.iter()
			.enumerate()
			.map(|(i, id)| (*id, i as CompactNodeType))
			.collect::<HashMap<Uuid, CompactNodeType>>();

		let ring = partitions
			.iter()
			.enumerate()
			.map(|(i, nodes)| {
				let top = (i as u16) << (16 - PARTITION_BITS);
				let nodes = nodes
					.iter()
					.map(|(id, _info)| *nodes_rev.get(id).unwrap())
					.collect::<Vec<CompactNodeType>>();
				assert!(nodes.len() == replication_factor);
				let mut nodes_buf = [0u8; MAX_REPLICATION];
				nodes_buf[..replication_factor].copy_from_slice(&nodes[..]);
				RingEntry {
					hash_prefix: top,
					nodes_buf,
				}
			})
			.collect::<Vec<_>>();

		Self {
			replication_factor,
			config,
			nodes,
			ring,
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
