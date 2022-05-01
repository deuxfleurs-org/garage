use std::cmp::min;
use std::cmp::Ordering;
use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use garage_util::bipartite::*;
use garage_util::crdt::{AutoCrdt, Crdt, LwwMap};
use garage_util::data::*;

use rand::prelude::SliceRandom;

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

	/// This function calculates a new partition-to-node assignation.
	/// The computed assignation maximizes the capacity of a
	/// partition (assuming all partitions have the same size).
	/// Among such optimal assignation, it minimizes the distance to
	/// the former assignation (if any) to minimize the amount of
	/// data to be moved. A heuristic ensures node triplets
	/// dispersion (in garage_util::bipartite::optimize_matching()).
	pub fn calculate_partition_assignation(&mut self) -> bool {
		//The nodes might have been updated, some might have been deleted.
		//So we need to first update the list of nodes and retrieve the
		//assignation.
		let old_node_assignation = self.update_nodes_and_ring();

		let (node_zone, _) = self.get_node_zone_capacity();

		//We compute the optimal number of partition to assign to
		//every node and zone.
		if let Some((part_per_nod, part_per_zone)) = self.optimal_proportions() {
			//We collect part_per_zone in a vec to not rely on the
			//arbitrary order in which elements are iterated in
			//Hashmap::iter()
			let part_per_zone_vec = part_per_zone
				.iter()
				.map(|(x, y)| (x.clone(), *y))
				.collect::<Vec<(String, usize)>>();
			//We create an indexing of the zones
			let mut zone_id = HashMap::<String, usize>::new();
			for (i, ppz) in part_per_zone_vec.iter().enumerate() {
				zone_id.insert(ppz.0.clone(), i);
			}

			//We compute a candidate for the new partition to zone
			//assignation.
			let nb_zones = part_per_zone.len();
			let nb_nodes = part_per_nod.len();
			let nb_partitions = 1 << PARTITION_BITS;
			let left_cap_vec = vec![self.replication_factor as u32; nb_partitions];
			let right_cap_vec = part_per_zone_vec.iter().map(|(_, y)| *y as u32).collect();
			let mut zone_assignation = dinic_compute_matching(left_cap_vec, right_cap_vec);

			//We create the structure for the partition-to-node assignation.
			let mut node_assignation = vec![vec![None; self.replication_factor]; nb_partitions];
			//We will decrement part_per_nod to keep track of the number
			//of partitions that we still have to associate.
			let mut part_per_nod = part_per_nod;

			//We minimize the distance to the former assignation(if any)

			//We get the id of the zones of the former assignation
			//(and the id no_zone if there is no node assignated)
			let no_zone = part_per_zone_vec.len();
			let old_zone_assignation: Vec<Vec<usize>> = old_node_assignation
				.iter()
				.map(|x| {
					x.iter()
						.map(|id| match *id {
							Some(i) => zone_id[&node_zone[i]],
							None => no_zone,
						})
						.collect()
				})
				.collect();

			//We minimize the distance to the former zone assignation
			zone_assignation =
				optimize_matching(&old_zone_assignation, &zone_assignation, nb_zones + 1); //+1 for no_zone

			//We need to assign partitions to nodes in their zone
			//We first put the nodes assignation that can stay the same
			for i in 0..nb_partitions {
				for j in 0..self.replication_factor {
					if let Some(Some(former_node)) = old_node_assignation[i].iter().find(|x| {
						if let Some(id) = x {
							zone_id[&node_zone[*id]] == zone_assignation[i][j]
						} else {
							false
						}
					}) {
						if part_per_nod[*former_node] > 0 {
							node_assignation[i][j] = Some(*former_node);
							part_per_nod[*former_node] -= 1;
						}
					}
				}
			}

			//We complete the assignation of partitions to nodes
			let mut rng = rand::thread_rng();
			for i in 0..nb_partitions {
				for j in 0..self.replication_factor {
					if node_assignation[i][j] == None {
						let possible_nodes: Vec<usize> = (0..nb_nodes)
							.filter(|id| {
								zone_id[&node_zone[*id]] == zone_assignation[i][j]
									&& part_per_nod[*id] > 0
							})
							.collect();
						assert!(!possible_nodes.is_empty());
						//We randomly pick a node
						if let Some(nod) = possible_nodes.choose(&mut rng) {
							node_assignation[i][j] = Some(*nod);
							part_per_nod[*nod] -= 1;
						}
					}
				}
			}

			//We write the assignation in the 1D table
			self.ring_assignation_data = Vec::<CompactNodeType>::new();
			for ass in node_assignation {
				for nod in ass {
					if let Some(id) = nod {
						self.ring_assignation_data.push(id as CompactNodeType);
					} else {
						panic!()
					}
				}
			}

			true
		} else {
			false
		}
	}

	/// The LwwMap of node roles might have changed. This function updates the node_id_vec
	/// and returns the assignation given by ring, with the new indices of the nodes, and
	/// None of the node is not present anymore.
	/// We work with the assumption that only this function and calculate_new_assignation
	/// do modify assignation_ring and node_id_vec.
	fn update_nodes_and_ring(&mut self) -> Vec<Vec<Option<usize>>> {
		let nb_partitions = 1usize << PARTITION_BITS;
		let mut node_assignation = vec![vec![None; self.replication_factor]; nb_partitions];
		let rf = self.replication_factor;
		let ring = &self.ring_assignation_data;

		let new_node_id_vec: Vec<Uuid> = self.roles.items().iter().map(|(k, _, _)| *k).collect();

		if ring.len() == rf * nb_partitions {
			for i in 0..nb_partitions {
				for j in 0..self.replication_factor {
					node_assignation[i][j] = new_node_id_vec
						.iter()
						.position(|id| *id == self.node_id_vec[ring[i * rf + j] as usize]);
				}
			}
		}

		self.node_id_vec = new_node_id_vec;
		self.ring_assignation_data = vec![];
		node_assignation
	}

	///This function compute the number of partition to assign to
	///every node and zone, so that every partition is replicated
	///self.replication_factor times and the capacity of a partition
	///is maximized.
	fn optimal_proportions(&mut self) -> Option<(Vec<usize>, HashMap<String, usize>)> {
		let mut zone_capacity: HashMap<String, u32> = HashMap::new();

		let (node_zone, node_capacity) = self.get_node_zone_capacity();
		let nb_nodes = self.node_id_vec.len();

		for i in 0..nb_nodes {
			if zone_capacity.contains_key(&node_zone[i]) {
				zone_capacity.insert(
					node_zone[i].clone(),
					zone_capacity[&node_zone[i]] + node_capacity[i],
				);
			} else {
				zone_capacity.insert(node_zone[i].clone(), node_capacity[i]);
			}
		}

		//Compute the optimal number of partitions per zone
		let sum_capacities: u32 = zone_capacity.values().sum();

		if sum_capacities == 0 {
			println!("No storage capacity in the network.");
			return None;
		}

		let nb_partitions = 1 << PARTITION_BITS;

		//Initially we would like to use zones porportionally to
		//their capacity.
		//However, a large zone can be associated to at most
		//nb_partitions to ensure replication of the date.
		//So we take the min with nb_partitions:
		let mut part_per_zone: HashMap<String, usize> = zone_capacity
			.iter()
			.map(|(k, v)| {
				(
					k.clone(),
					min(
						nb_partitions,
						(self.replication_factor * nb_partitions * *v as usize)
							/ sum_capacities as usize,
					),
				)
			})
			.collect();

		//The replication_factor-1 upper bounds the number of
		//part_per_zones that are greater than nb_partitions
		for _ in 1..self.replication_factor {
			//The number of partitions that are not assignated to
			//a zone that takes nb_partitions.
			let sum_capleft: u32 = zone_capacity
				.keys()
				.filter(|k| part_per_zone[*k] < nb_partitions)
				.map(|k| zone_capacity[k])
				.sum();

			//The number of replication of the data that we need
			//to ensure.
			let repl_left = self.replication_factor
				- part_per_zone
					.values()
					.filter(|x| **x == nb_partitions)
					.count();
			if repl_left == 0 {
				break;
			}

			for k in zone_capacity.keys() {
				if part_per_zone[k] != nb_partitions {
					part_per_zone.insert(
						k.to_string(),
						min(
							nb_partitions,
							(nb_partitions * zone_capacity[k] as usize * repl_left)
								/ sum_capleft as usize,
						),
					);
				}
			}
		}

		//Now we divide the zone's partition share proportionally
		//between their nodes.

		let mut part_per_nod: Vec<usize> = (0..nb_nodes)
			.map(|i| {
				(part_per_zone[&node_zone[i]] * node_capacity[i] as usize)
					/ zone_capacity[&node_zone[i]] as usize
			})
			.collect();

		//We must update the part_per_zone to make it correspond to
		//part_per_nod (because of integer rounding)
		part_per_zone = part_per_zone.iter().map(|(k, _)| (k.clone(), 0)).collect();
		for i in 0..nb_nodes {
			part_per_zone.insert(
				node_zone[i].clone(),
				part_per_zone[&node_zone[i]] + part_per_nod[i],
			);
		}

		//Because of integer rounding, the total sum of part_per_nod
		//might not be replication_factor*nb_partitions.
		// We need at most to add 1 to every non maximal value of
		// part_per_nod. The capacity of a partition will be bounded
		// by the minimal value of
		// node_capacity_vec[i]/part_per_nod[i]
		// so we try to maximize this minimal value, keeping the
		// part_per_zone capped

		let discrepancy: usize =
			nb_partitions * self.replication_factor - part_per_nod.iter().sum::<usize>();

		//We use a stupid O(N^2) algorithm. If the number of nodes
		//is actually expected to be high, one should optimize this.

		for _ in 0..discrepancy {
			if let Some(idmax) = (0..nb_nodes)
				.filter(|i| part_per_zone[&node_zone[*i]] < nb_partitions)
				.max_by(|i, j| {
					(node_capacity[*i] * (part_per_nod[*j] + 1) as u32)
						.cmp(&(node_capacity[*j] * (part_per_nod[*i] + 1) as u32))
				}) {
				part_per_nod[idmax] += 1;
				part_per_zone.insert(
					node_zone[idmax].clone(),
					part_per_zone[&node_zone[idmax]] + 1,
				);
			}
		}

		//We check the algorithm consistency

		let discrepancy: usize =
			nb_partitions * self.replication_factor - part_per_nod.iter().sum::<usize>();
		assert!(discrepancy == 0);
		assert!(if let Some(v) = part_per_zone.values().max() {
			*v <= nb_partitions
		} else {
			false
		});

		Some((part_per_nod, part_per_zone))
	}

	//Returns vectors of zone and capacity; indexed by the same (temporary)
	//indices as node_id_vec.
	fn get_node_zone_capacity(&self) -> (Vec<String>, Vec<u32>) {
		let node_zone = self
			.node_id_vec
			.iter()
			.map(|id_nod| match self.node_role(id_nod) {
				Some(NodeRole {
					zone,
					capacity: _,
					tags: _,
				}) => zone.clone(),
				_ => "".to_string(),
			})
			.collect();

		let node_capacity = self
			.node_id_vec
			.iter()
			.map(|id_nod| match self.node_role(id_nod) {
				Some(NodeRole {
					zone: _,
					capacity: Some(c),
					tags: _,
				}) => {
						*c
				}
				_ => 0,
			})
			.collect();

		(node_zone, node_capacity)
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use itertools::Itertools;

	fn check_assignation(cl: &ClusterLayout) {
		//Check that input data has the right format
		let nb_partitions = 1usize << PARTITION_BITS;
		assert!([1, 2, 3].contains(&cl.replication_factor));
		assert!(cl.ring_assignation_data.len() == nb_partitions * cl.replication_factor);

		let (node_zone, node_capacity) = cl.get_node_zone_capacity();

		//Check that is is a correct assignation with zone redundancy
		let rf = cl.replication_factor;
		for i in 0..nb_partitions {
			assert!(
				rf == cl.ring_assignation_data[rf * i..rf * (i + 1)]
					.iter()
					.map(|nod| node_zone[*nod as usize].clone())
					.unique()
					.count()
			);
		}

		let nb_nodes = cl.node_id_vec.len();
		//Check optimality
		let node_nb_part = (0..nb_nodes)
			.map(|i| {
				cl.ring_assignation_data
					.iter()
					.filter(|x| **x == i as u8)
					.count()
			})
			.collect::<Vec<_>>();

		let zone_vec = node_zone.iter().unique().collect::<Vec<_>>();
		let zone_nb_part = zone_vec
			.iter()
			.map(|z| {
				cl.ring_assignation_data
					.iter()
					.filter(|x| node_zone[**x as usize] == **z)
					.count()
			})
			.collect::<Vec<_>>();

		//Check optimality of the zone assignation : would it be better for the
		//node_capacity/node_partitions ratio to change the assignation of a partition

		if let Some(idmin) = (0..nb_nodes).min_by(|i, j| {
			(node_capacity[*i] * node_nb_part[*j] as u32)
				.cmp(&(node_capacity[*j] * node_nb_part[*i] as u32))
		}) {
			if let Some(idnew) = (0..nb_nodes)
				.filter(|i| {
					if let Some(p) = zone_vec.iter().position(|z| **z == node_zone[*i]) {
						zone_nb_part[p] < nb_partitions
					} else {
						false
					}
				})
				.max_by(|i, j| {
					(node_capacity[*i] * (node_nb_part[*j] as u32 + 1))
						.cmp(&(node_capacity[*j] * (node_nb_part[*i] as u32 + 1)))
				}) {
				assert!(
					node_capacity[idmin] * (node_nb_part[idnew] as u32 + 1)
						>= node_capacity[idnew] * node_nb_part[idmin] as u32
				);
			}
		}

		//In every zone, check optimality of the nod assignation
		for z in zone_vec {
			let node_of_z_iter = (0..nb_nodes).filter(|id| node_zone[*id] == *z);
			if let Some(idmin) = node_of_z_iter.clone().min_by(|i, j| {
				(node_capacity[*i] * node_nb_part[*j] as u32)
					.cmp(&(node_capacity[*j] * node_nb_part[*i] as u32))
			}) {
				if let Some(idnew) = node_of_z_iter.min_by(|i, j| {
					(node_capacity[*i] * (node_nb_part[*j] as u32 + 1))
						.cmp(&(node_capacity[*j] * (node_nb_part[*i] as u32 + 1)))
				}) {
					assert!(
						node_capacity[idmin] * (node_nb_part[idnew] as u32 + 1)
							>= node_capacity[idnew] * node_nb_part[idmin] as u32
					);
				}
			}
		}
	}

	fn update_layout(
		cl: &mut ClusterLayout,
		node_id_vec: &Vec<u8>,
		node_capacity_vec: &Vec<u32>,
		node_zone_vec: &Vec<String>,
	) {
		for i in 0..node_id_vec.len() {
			if let Some(x) = FixedBytes32::try_from(&[i as u8; 32]) {
				cl.node_id_vec.push(x);
			}

			let update = cl.roles.update_mutator(
				cl.node_id_vec[i],
				NodeRoleV(Some(NodeRole {
					zone: (node_zone_vec[i].to_string()),
					capacity: (Some(node_capacity_vec[i])),
					tags: (vec![]),
				})),
			);
			cl.roles.merge(&update);
		}
	}

	#[test]
	fn test_assignation() {
		let mut node_id_vec = vec![1, 2, 3];
		let mut node_capacity_vec = vec![4000, 1000, 2000];
		let mut node_zone_vec = vec!["A", "B", "C"]
			.into_iter()
			.map(|x| x.to_string())
			.collect();

		let mut cl = ClusterLayout {
			node_id_vec: vec![],

			roles: LwwMap::new(),

			replication_factor: 3,
			ring_assignation_data: vec![],
			version: 0,
			staging: LwwMap::new(),
			staging_hash: sha256sum(&[1; 32]),
		};
		update_layout(&mut cl, &node_id_vec, &node_capacity_vec, &node_zone_vec);
		cl.calculate_partition_assignation();
		check_assignation(&cl);

		node_id_vec = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
		node_capacity_vec = vec![4000, 1000, 1000, 3000, 1000, 1000, 2000, 10000, 2000];
		node_zone_vec = vec!["A", "B", "C", "C", "C", "B", "G", "H", "I"]
			.into_iter()
			.map(|x| x.to_string())
			.collect();
		update_layout(&mut cl, &node_id_vec, &node_capacity_vec, &node_zone_vec);
		cl.calculate_partition_assignation();
		check_assignation(&cl);

		node_capacity_vec = vec![4000, 1000, 2000, 7000, 1000, 1000, 2000, 10000, 2000];
		update_layout(&mut cl, &node_id_vec, &node_capacity_vec, &node_zone_vec);
		cl.calculate_partition_assignation();
		check_assignation(&cl);

		node_capacity_vec = vec![4000, 4000, 2000, 7000, 1000, 9000, 2000, 10, 2000];
		update_layout(&mut cl, &node_id_vec, &node_capacity_vec, &node_zone_vec);
		cl.calculate_partition_assignation();
		check_assignation(&cl);
	}
}
