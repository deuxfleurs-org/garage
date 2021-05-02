//! Module containing types related to computing nodes which should receive a copy of data blocks
//! and metadata
use std::collections::{HashMap, HashSet};
use std::convert::TryInto;

use serde::{Deserialize, Serialize};

use garage_util::data::*;

// A partition number is encoded on 16 bits,
// i.e. we have up to 2**16 partitions.
// (in practice we have exactly 2**PARTITION_BITS partitions)
/// A partition id, stored on 16 bits
pub type Partition = u16;

// TODO: make this constant parametrizable in the config file
// For deployments with many nodes it might make sense to bump
// it up to 10.
// Maximum value : 16
/// How many bits from the hash are used to make partitions. Higher numbers means more fairness in
/// presence of numerous nodes, but exponentially bigger ring. Max 16
pub const PARTITION_BITS: usize = 8;

const PARTITION_MASK_U16: u16 = ((1 << PARTITION_BITS) - 1) << (16 - PARTITION_BITS);

// TODO: make this constant paraetrizable in the config file
// (most deployments use a replication factor of 3, so...)
/// The maximum number of time an object might get replicated
pub const MAX_REPLICATION: usize = 3;

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
	pub datacenter: String,
	/// The (relative) capacity of the node
	pub capacity: u32,
	/// A tag to recognize the entry, not used for other things than display
	pub tag: String,
}

/// A ring distributing fairly objects to nodes
#[derive(Clone)]
pub struct Ring {
	/// The network configuration used to generate this ring
	pub config: NetworkConfig,
	/// The list of entries in the ring
	pub ring: Vec<RingEntry>,
}

/// An entry in the ring
#[derive(Clone, Debug)]
pub struct RingEntry {
	/// The prefix of the Hash of object which should use this entry
	pub location: Hash,
	/// The nodes in which a matching object should get stored
	pub nodes: [Uuid; MAX_REPLICATION],
}

impl Ring {
	// TODO this function MUST be refactored, it's 100 lines long, with a 50 lines loop, going up to 6
	// levels of imbrication. It is basically impossible to test, maintain, or understand for an
	// outsider.
	pub(crate) fn new(config: NetworkConfig) -> Self {
		// Create a vector of partition indices (0 to 2**PARTITION_BITS-1)
		let partitions_idx = (0usize..(1usize << PARTITION_BITS)).collect::<Vec<_>>();

		let datacenters = config
			.members
			.iter()
			.map(|(_id, info)| info.datacenter.as_str())
			.collect::<HashSet<&str>>();
		let n_datacenters = datacenters.len();

		// Prepare ring
		let mut partitions: Vec<Vec<(&Uuid, &NetworkConfigEntry)>> = partitions_idx
			.iter()
			.map(|_i| Vec::new())
			.collect::<Vec<_>>();

		// Create MagLev priority queues for each node
		let mut queues = config
			.members
			.iter()
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
			.map(|(_, node_info)| node_info.capacity)
			.fold(0, std::cmp::max);

		// Fill up ring
		for rep in 0..MAX_REPLICATION {
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
						if i_round >= node_info.capacity {
							continue;
						}
						for (pos2, &qv) in q.iter().enumerate().skip(*pos) {
							if partitions[qv].len() != rep {
								continue;
							}
							let p_dcs = partitions[qv]
								.iter()
								.map(|(_id, info)| info.datacenter.as_str())
								.collect::<HashSet<&str>>();
							if (p_dcs.len() < n_datacenters
								&& !p_dcs.contains(&node_info.datacenter.as_str()))
								|| (p_dcs.len() == n_datacenters
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
						config,
						ring: vec![],
					};
				}
			}
		}

		let ring = partitions
			.iter()
			.enumerate()
			.map(|(i, nodes)| {
				let top = (i as u16) << (16 - PARTITION_BITS);
				let mut hash = [0u8; 32];
				hash[0..2].copy_from_slice(&u16::to_be_bytes(top)[..]);
				let nodes = nodes.iter().map(|(id, _info)| **id).collect::<Vec<Uuid>>();
				RingEntry {
					location: hash.into(),
					nodes: nodes.try_into().unwrap(),
				}
			})
			.collect::<Vec<_>>();

		Self { config, ring }
	}

	/// Get the partition in which data would fall on
	pub fn partition_of(&self, from: &Hash) -> Partition {
		let top = u16::from_be_bytes(from.as_slice()[0..2].try_into().unwrap());
		top >> (16 - PARTITION_BITS)
	}

	/// Get the list of partitions and the first hash of a partition key that would fall in it
	pub fn partitions(&self) -> Vec<(Partition, Hash)> {
		let mut ret = vec![];

		for (i, entry) in self.ring.iter().enumerate() {
			ret.push((i as u16, entry.location));
		}
		if !ret.is_empty() {
			assert_eq!(ret[0].1, [0u8; 32].into());
		}

		ret
	}

	// TODO rename this function as it no longer walk the ring
	/// Walk the ring to find the n servers in which data should be replicated
	pub fn walk_ring(&self, from: &Hash, n: usize) -> Vec<Uuid> {
		if self.ring.len() != 1 << PARTITION_BITS {
			warn!("Ring not yet ready, read/writes will be lost!");
			return vec![];
		}

		let top = u16::from_be_bytes(from.as_slice()[0..2].try_into().unwrap());
		let partition_idx = (top >> (16 - PARTITION_BITS)) as usize;
		// TODO why computing two time in the same way and asserting?
		assert_eq!(partition_idx, self.partition_of(from) as usize);

		let partition = &self.ring[partition_idx];

		let partition_top =
			u16::from_be_bytes(partition.location.as_slice()[0..2].try_into().unwrap());
		// TODO is this an assertion on the validity of PARTITION_MASK_U16? If so, it should
		// probably be a test more than a runtime assertion
		assert_eq!(partition_top & PARTITION_MASK_U16, top & PARTITION_MASK_U16);

		assert!(n <= partition.nodes.len());
		partition.nodes[..n].to_vec()
	}
}
