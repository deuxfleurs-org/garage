use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};

use garage_util::crdt::{AutoCrdt, Crdt, LwwMap};
use garage_util::data::*;

use crate::ring::*;

/// The layout of the cluster, i.e. the list of roles
/// which are assigned to each cluster node
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterLayout {
	pub version: u64,

	pub replication_factor: usize,
	pub roles: LwwMap<Uuid, NodeRoleV>,

	/// node_id_vec: a vector of node IDs with a role assigned
	/// in the system (this includes gateway nodes).
	/// The order here is different than the vec stored by `roles`, because:
	/// 1. non-gateway nodes are first so that they have lower numbers
	/// 2. nodes that don't have a role are excluded (but they need to
	///    stay in the CRDT as tombstones)
	pub node_id_vec: Vec<Uuid>,
	/// the assignation of data partitions to node, the values
	/// are indices in node_id_vec
	#[serde(with = "serde_bytes")]
	pub ring_assignation_data: Vec<CompactNodeType>,

	/// Role changes which are staged for the next version of the layout
	pub staging: LwwMap<Uuid, NodeRoleV>,
	pub staging_hash: Hash,
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct NodeRoleV(pub Option<NodeRole>);

impl AutoCrdt for NodeRoleV {
	const WARN_IF_DIFFERENT: bool = true;
}

/// The user-assigned roles of cluster nodes
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct NodeRole {
	/// Datacenter at which this entry belong. This information might be used to perform a better
	/// geodistribution
	pub zone: String,
	/// The (relative) capacity of the node
	/// If this is set to None, the node does not participate in storing data for the system
	/// and is only active as an API gateway to other nodes
	pub capacity: Option<u32>,
	/// A set of tags to recognize the node
	pub tags: Vec<String>,
}

impl NodeRole {
	pub fn capacity_string(&self) -> String {
		match self.capacity {
			Some(c) => format!("{}", c),
			None => "gateway".to_string(),
		}
	}
}

impl ClusterLayout {
	pub fn new(replication_factor: usize) -> Self {
		let empty_lwwmap = LwwMap::new();
		let empty_lwwmap_hash = blake2sum(&rmp_to_vec_all_named(&empty_lwwmap).unwrap()[..]);

		ClusterLayout {
			version: 0,
			replication_factor,
			roles: LwwMap::new(),
			node_id_vec: Vec::new(),
			ring_assignation_data: Vec::new(),
			staging: empty_lwwmap,
			staging_hash: empty_lwwmap_hash,
		}
	}

	pub fn merge(&mut self, other: &ClusterLayout) -> bool {
		match other.version.cmp(&self.version) {
			Ordering::Greater => {
				*self = other.clone();
				true
			}
			Ordering::Equal => {
				self.staging.merge(&other.staging);

				let new_staging_hash = blake2sum(&rmp_to_vec_all_named(&self.staging).unwrap()[..]);
				let changed = new_staging_hash != self.staging_hash;

				self.staging_hash = new_staging_hash;

				changed
			}
			Ordering::Less => false,
		}
	}

	/// Returns a list of IDs of nodes that currently have
	/// a role in the cluster
	pub fn node_ids(&self) -> &[Uuid] {
		&self.node_id_vec[..]
	}

	pub fn num_nodes(&self) -> usize {
		self.node_id_vec.len()
	}

	/// Returns the role of a node in the layout
	pub fn node_role(&self, node: &Uuid) -> Option<&NodeRole> {
		match self.roles.get(node) {
			Some(NodeRoleV(Some(v))) => Some(v),
			_ => None,
		}
	}

	/// Check a cluster layout for internal consistency
	/// returns true if consistent, false if error
	pub fn check(&self) -> bool {
		// Check that the hash of the staging data is correct
		let staging_hash = blake2sum(&rmp_to_vec_all_named(&self.staging).unwrap()[..]);
		if staging_hash != self.staging_hash {
			return false;
		}

		// Check that node_id_vec contains the correct list of nodes
		let mut expected_nodes = self
			.roles
			.items()
			.iter()
			.filter(|(_, _, v)| v.0.is_some())
			.map(|(id, _, _)| *id)
			.collect::<Vec<_>>();
		expected_nodes.sort();
		let mut node_id_vec = self.node_id_vec.clone();
		node_id_vec.sort();
		if expected_nodes != node_id_vec {
			return false;
		}

		// Check that the assignation data has the correct length
		if self.ring_assignation_data.len() != (1 << PARTITION_BITS) * self.replication_factor {
			return false;
		}

		// Check that the assigned nodes are correct identifiers
		// of nodes that are assigned a role
		// and that role is not the role of a gateway nodes
		for x in self.ring_assignation_data.iter() {
			if *x as usize >= self.node_id_vec.len() {
				return false;
			}
			let node = self.node_id_vec[*x as usize];
			match self.roles.get(&node) {
				Some(NodeRoleV(Some(x))) if x.capacity.is_some() => (),
				_ => return false,
			}
		}

		true
	}

	/// Calculate an assignation of partitions to nodes
	pub fn calculate_partition_assignation(&mut self) -> bool {
		let (configured_nodes, zones) = self.configured_nodes_and_zones();
		let n_zones = zones.len();

		println!("Calculating updated partition assignation, this may take some time...");
		println!();

		// Get old partition assignation
		let old_partitions = self.parse_assignation_data();

		// Create new partition assignation starting from old one
		let mut partitions = old_partitions.clone();

		// Cleanup steps in new partition assignation:
		let min_keep_nodes_per_part = (self.replication_factor + 1) / 2;
		for part in partitions.iter_mut() {
			// - remove from assignation nodes that don't have a role in the layout anymore
			part.nodes
				.retain(|(_, info)| info.map(|x| x.capacity.is_some()).unwrap_or(false));

			// - remove from assignation some nodes that are in the same datacenter
			//   if we can, so that the later steps can ensure datacenter variety
			//   as much as possible (but still under the constraint that each partition
			//   should not move from at least a certain number of nodes that is
			//   min_keep_nodes_per_part)
			'rmloop: while part.nodes.len() > min_keep_nodes_per_part {
				let mut zns_c = HashMap::<&str, usize>::new();
				for (_id, info) in part.nodes.iter() {
					*zns_c.entry(info.unwrap().zone.as_str()).or_insert(0) += 1;
				}
				for i in 0..part.nodes.len() {
					if zns_c[part.nodes[i].1.unwrap().zone.as_str()] > 1 {
						part.nodes.remove(i);
						continue 'rmloop;
					}
				}

				break;
			}
		}

		// When nodes are removed, or when bootstraping an assignation from
		// scratch for a new cluster, the old partitions will have holes (or be empty).
		// Here we add more nodes to make a complete (sub-optimal) assignation,
		// using an initial partition assignation that is calculated using the multi-dc maglev trick
		match self.initial_partition_assignation() {
			Some(initial_partitions) => {
				for (part, ipart) in partitions.iter_mut().zip(initial_partitions.iter()) {
					for (id, info) in ipart.nodes.iter() {
						if part.nodes.len() < self.replication_factor {
							part.add(part.nodes.len() + 1, n_zones, id, info.unwrap());
						}
					}
					assert!(part.nodes.len() == self.replication_factor);
				}
			}
			None => {
				// Not enough nodes in cluster to build a correct assignation.
				// Signal it by returning an error.
				return false;
			}
		}

		// Calculate how many partitions each node should ideally store,
		// and how many partitions they are storing with the current assignation
		// This defines our target for which we will optimize in the following loop.
		let total_capacity = configured_nodes
			.iter()
			.map(|(_, info)| info.capacity.unwrap_or(0))
			.sum::<u32>() as usize;
		let total_partitions = self.replication_factor * (1 << PARTITION_BITS);
		let target_partitions_per_node = configured_nodes
			.iter()
			.map(|(id, info)| {
				(
					*id,
					info.capacity.unwrap_or(0) as usize * total_partitions / total_capacity,
				)
			})
			.collect::<HashMap<&Uuid, usize>>();

		let mut partitions_per_node = self.partitions_per_node(&partitions[..]);

		println!("Target number of partitions per node:");
		for (node, npart) in target_partitions_per_node.iter() {
			println!("{:?}\t{}", node, npart);
		}
		println!();

		// Shuffle partitions between nodes so that nodes will reach (or better approach)
		// their target number of stored partitions
		loop {
			let mut option = None;
			for (i, part) in partitions.iter_mut().enumerate() {
				for (irm, (idrm, _)) in part.nodes.iter().enumerate() {
					let suprm = partitions_per_node.get(*idrm).cloned().unwrap_or(0) as i32
						- target_partitions_per_node.get(*idrm).cloned().unwrap_or(0) as i32;

					for (idadd, infoadd) in configured_nodes.iter() {
						// skip replacing a node by itself
						// and skip replacing by gateway nodes
						if idadd == idrm || infoadd.capacity.is_none() {
							continue;
						}

						let supadd = partitions_per_node.get(*idadd).cloned().unwrap_or(0) as i32
							- target_partitions_per_node.get(*idadd).cloned().unwrap_or(0) as i32;

						// We want to try replacing node idrm by node idadd
						// if that brings us close to our goal.
						let square = |i: i32| i * i;
						let oldcost = square(suprm) + square(supadd);
						let newcost = square(suprm - 1) + square(supadd + 1);
						if newcost >= oldcost {
							// not closer to our goal
							continue;
						}
						let gain = oldcost - newcost;

						let mut newpart = part.clone();

						newpart.nodes.remove(irm);
						if !newpart.add(newpart.nodes.len() + 1, n_zones, idadd, infoadd) {
							continue;
						}
						assert!(newpart.nodes.len() == self.replication_factor);

						if !old_partitions[i]
							.is_valid_transition_to(&newpart, self.replication_factor)
						{
							continue;
						}

						if option
							.as_ref()
							.map(|(old_gain, _, _, _, _)| gain > *old_gain)
							.unwrap_or(true)
						{
							option = Some((gain, i, idadd, idrm, newpart));
						}
					}
				}
			}
			if let Some((_gain, i, idadd, idrm, newpart)) = option {
				*partitions_per_node.entry(idadd).or_insert(0) += 1;
				*partitions_per_node.get_mut(idrm).unwrap() -= 1;
				partitions[i] = newpart;
			} else {
				break;
			}
		}

		// Check we completed the assignation correctly
		// (this is a set of checks for the algorithm's consistency)
		assert!(partitions.len() == (1 << PARTITION_BITS));
		assert!(partitions
			.iter()
			.all(|p| p.nodes.len() == self.replication_factor));

		let new_partitions_per_node = self.partitions_per_node(&partitions[..]);
		assert!(new_partitions_per_node == partitions_per_node);

		// Show statistics
		println!("New number of partitions per node:");
		for (node, npart) in partitions_per_node.iter() {
			println!("{:?}\t{}", node, npart);
		}
		println!();

		let mut diffcount = HashMap::new();
		for (oldpart, newpart) in old_partitions.iter().zip(partitions.iter()) {
			let nminus = oldpart.txtplus(newpart);
			let nplus = newpart.txtplus(oldpart);
			if nminus != "[...]" || nplus != "[...]" {
				let tup = (nminus, nplus);
				*diffcount.entry(tup).or_insert(0) += 1;
			}
		}
		if diffcount.is_empty() {
			println!("No data will be moved between nodes.");
		} else {
			let mut diffcount = diffcount.into_iter().collect::<Vec<_>>();
			diffcount.sort();
			println!("Number of partitions that move:");
			for ((nminus, nplus), npart) in diffcount {
				println!("\t{}\t{} -> {}", npart, nminus, nplus);
			}
		}
		println!();

		// Calculate and save new assignation data
		let (nodes, assignation_data) =
			self.compute_assignation_data(&configured_nodes[..], &partitions[..]);

		self.node_id_vec = nodes;
		self.ring_assignation_data = assignation_data;

		true
	}

	fn initial_partition_assignation(&self) -> Option<Vec<PartitionAss<'_>>> {
		let (configured_nodes, zones) = self.configured_nodes_and_zones();
		let n_zones = zones.len();

		// Create a vector of partition indices (0 to 2**PARTITION_BITS-1)
		let partitions_idx = (0usize..(1usize << PARTITION_BITS)).collect::<Vec<_>>();

		// Prepare ring
		let mut partitions: Vec<PartitionAss> = partitions_idx
			.iter()
			.map(|_i| PartitionAss::new())
			.collect::<Vec<_>>();

		// Create MagLev priority queues for each node
		let mut queues = configured_nodes
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

		let max_capacity = configured_nodes
			.iter()
			.filter_map(|(_, node_info)| node_info.capacity)
			.fold(0, std::cmp::max);

		// Fill up ring
		for rep in 0..self.replication_factor {
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
							if partitions[qv].add(rep + 1, n_zones, node_id, node_info) {
								remaining -= 1;
								*pos = pos2 + 1;
								break;
							}
						}
					}
				}
				if remaining == remaining0 {
					// No progress made, exit
					return None;
				}
			}
		}

		Some(partitions)
	}

	fn configured_nodes_and_zones(&self) -> (Vec<(&Uuid, &NodeRole)>, HashSet<&str>) {
		let configured_nodes = self
			.roles
			.items()
			.iter()
			.filter(|(_id, _, info)| info.0.is_some())
			.map(|(id, _, info)| (id, info.0.as_ref().unwrap()))
			.collect::<Vec<(&Uuid, &NodeRole)>>();

		let zones = configured_nodes
			.iter()
			.filter(|(_id, info)| info.capacity.is_some())
			.map(|(_id, info)| info.zone.as_str())
			.collect::<HashSet<&str>>();

		(configured_nodes, zones)
	}

	fn compute_assignation_data<'a>(
		&self,
		configured_nodes: &[(&'a Uuid, &'a NodeRole)],
		partitions: &[PartitionAss<'a>],
	) -> (Vec<Uuid>, Vec<CompactNodeType>) {
		assert!(partitions.len() == (1 << PARTITION_BITS));

		// Make a canonical order for nodes
		let mut nodes = configured_nodes
			.iter()
			.filter(|(_id, info)| info.capacity.is_some())
			.map(|(id, _)| **id)
			.collect::<Vec<_>>();
		let nodes_rev = nodes
			.iter()
			.enumerate()
			.map(|(i, id)| (*id, i as CompactNodeType))
			.collect::<HashMap<Uuid, CompactNodeType>>();

		let mut assignation_data = vec![];
		for partition in partitions.iter() {
			assert!(partition.nodes.len() == self.replication_factor);
			for (id, _) in partition.nodes.iter() {
				assignation_data.push(*nodes_rev.get(id).unwrap());
			}
		}

		nodes.extend(
			configured_nodes
				.iter()
				.filter(|(_id, info)| info.capacity.is_none())
				.map(|(id, _)| **id),
		);

		(nodes, assignation_data)
	}

	fn parse_assignation_data(&self) -> Vec<PartitionAss<'_>> {
		if self.ring_assignation_data.len() == self.replication_factor * (1 << PARTITION_BITS) {
			// If the previous assignation data is correct, use that
			let mut partitions = vec![];
			for i in 0..(1 << PARTITION_BITS) {
				let mut part = PartitionAss::new();
				for node_i in self.ring_assignation_data
					[i * self.replication_factor..(i + 1) * self.replication_factor]
					.iter()
				{
					let node_id = &self.node_id_vec[*node_i as usize];

					if let Some(NodeRoleV(Some(info))) = self.roles.get(node_id) {
						part.nodes.push((node_id, Some(info)));
					} else {
						part.nodes.push((node_id, None));
					}
				}
				partitions.push(part);
			}
			partitions
		} else {
			// Otherwise start fresh
			(0..(1 << PARTITION_BITS))
				.map(|_| PartitionAss::new())
				.collect()
		}
	}

	fn partitions_per_node<'a>(&self, partitions: &[PartitionAss<'a>]) -> HashMap<&'a Uuid, usize> {
		let mut partitions_per_node = HashMap::<&Uuid, usize>::new();
		for p in partitions.iter() {
			for (id, _) in p.nodes.iter() {
				*partitions_per_node.entry(*id).or_insert(0) += 1;
			}
		}
		partitions_per_node
	}
}

// ---- Internal structs for partition assignation in layout ----

#[derive(Clone)]
struct PartitionAss<'a> {
	nodes: Vec<(&'a Uuid, Option<&'a NodeRole>)>,
}

impl<'a> PartitionAss<'a> {
	fn new() -> Self {
		Self { nodes: Vec::new() }
	}

	fn nplus(&self, other: &PartitionAss<'a>) -> usize {
		self.nodes
			.iter()
			.filter(|x| !other.nodes.contains(x))
			.count()
	}

	fn txtplus(&self, other: &PartitionAss<'a>) -> String {
		let mut nodes = self
			.nodes
			.iter()
			.filter(|x| !other.nodes.contains(x))
			.map(|x| format!("{:?}", x.0))
			.collect::<Vec<_>>();
		nodes.sort();
		if self.nodes.iter().any(|x| other.nodes.contains(x)) {
			nodes.push("...".into());
		}
		format!("[{}]", nodes.join(" "))
	}

	fn is_valid_transition_to(&self, other: &PartitionAss<'a>, replication_factor: usize) -> bool {
		let min_keep_nodes_per_part = (replication_factor + 1) / 2;
		let n_removed = self.nplus(other);

		if self.nodes.len() <= min_keep_nodes_per_part {
			n_removed == 0
		} else {
			n_removed <= self.nodes.len() - min_keep_nodes_per_part
		}
	}

	fn add(
		&mut self,
		target_len: usize,
		n_zones: usize,
		node: &'a Uuid,
		role: &'a NodeRole,
	) -> bool {
		if self.nodes.len() != target_len - 1 {
			return false;
		}

		let p_zns = self
			.nodes
			.iter()
			.map(|(_id, info)| info.unwrap().zone.as_str())
			.collect::<HashSet<&str>>();
		if (p_zns.len() < n_zones && !p_zns.contains(&role.zone.as_str()))
			|| (p_zns.len() == n_zones && !self.nodes.iter().any(|(id, _)| *id == node))
		{
			self.nodes.push((node, Some(role)));
			true
		} else {
			false
		}
	}
}
