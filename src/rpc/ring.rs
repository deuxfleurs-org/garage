use std::collections::{HashMap, HashSet};
use std::convert::TryInto;

use serde::{Deserialize, Serialize};

use garage_util::data::*;

// TODO: make this constant parametrizable in the config file
// For deployments with many nodes it might make sense to bump
// it up to 10.
// Maximum value : 16
pub const PARTITION_BITS: usize = 8;

const PARTITION_MASK_U16: u16 = ((1 << PARTITION_BITS) - 1) << (16 - PARTITION_BITS);

// TODO: make this constant paraetrizable in the config file
// (most deployments use a replication factor of 3, so...)
pub const MAX_REPLICATION: usize = 3;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkConfig {
	pub members: HashMap<UUID, NetworkConfigEntry>,
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

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkConfigEntry {
	pub datacenter: String,
	pub n_tokens: u32,
	pub tag: String,
}

#[derive(Clone)]
pub struct Ring {
	pub config: NetworkConfig,
	pub ring: Vec<RingEntry>,
}

#[derive(Clone, Debug)]
pub struct RingEntry {
	pub location: Hash,
	pub nodes: [UUID; MAX_REPLICATION],
}

impl Ring {
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
		let mut partitions: Vec<Vec<(&UUID, &NetworkConfigEntry)>> = partitions_idx
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

		let max_toktok = config
			.members
			.iter()
			.map(|(_, node_info)| node_info.n_tokens)
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
				for toktok in 0..max_toktok {
					for (node_id, node_info, q, pos) in queues.iter_mut() {
						if toktok >= node_info.n_tokens {
							continue;
						}
						for pos2 in *pos..q.len() {
							let qv = q[pos2];
							if partitions[qv].len() != rep {
								continue;
							}
							let p_dcs = partitions[qv]
								.iter()
								.map(|(_id, info)| info.datacenter.as_str())
								.collect::<HashSet<&str>>();
							if !partitions[qv]
								.iter()
								.any(|(_id, i)| *i.datacenter == node_info.datacenter)
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
				let nodes = nodes.iter().map(|(id, _info)| **id).collect::<Vec<UUID>>();
				RingEntry {
					location: hash.into(),
					nodes: nodes.try_into().unwrap(),
				}
			})
			.collect::<Vec<_>>();

		eprintln!("RING: --");
		for e in ring.iter() {
			eprintln!("{:?}", e);
		}
		eprintln!("END --");

		Self { config, ring }
	}

	pub fn walk_ring(&self, from: &Hash, n: usize) -> Vec<UUID> {
		if self.ring.len() != 1 << PARTITION_BITS {
			warn!("Ring not yet ready, read/writes will be lost");
			return vec![];
		}

		let top = u16::from_be_bytes(from.as_slice()[0..2].try_into().unwrap());

		let partition_idx = (top >> (16 - PARTITION_BITS)) as usize;
		let partition = &self.ring[partition_idx];

		let partition_top =
			u16::from_be_bytes(partition.location.as_slice()[0..2].try_into().unwrap());
		assert!(partition_top & PARTITION_MASK_U16 == top & PARTITION_MASK_U16);

		assert!(n <= partition.nodes.len());
		partition.nodes[..n].iter().cloned().collect::<Vec<_>>()
	}
}
