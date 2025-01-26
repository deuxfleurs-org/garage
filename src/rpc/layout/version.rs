use std::collections::HashMap;
use std::collections::HashSet;
use std::convert::TryInto;

use bytesize::ByteSize;
use itertools::Itertools;

use garage_util::crdt::{Crdt, LwwMap};
use garage_util::data::*;
use garage_util::error::*;

use super::graph_algo::*;
use super::*;

// The Message type will be used to collect information on the algorithm.
pub type Message = Vec<String>;

impl LayoutVersion {
	pub fn new(replication_factor: usize) -> Self {
		// We set the default zone redundancy to be Maximum, meaning that the maximum
		// possible value will be used depending on the cluster topology
		let parameters = LayoutParameters {
			zone_redundancy: ZoneRedundancy::Maximum,
		};

		LayoutVersion {
			version: 0,
			replication_factor,
			partition_size: 0,
			roles: LwwMap::new(),
			node_id_vec: Vec::new(),
			nongateway_node_count: 0,
			ring_assignment_data: Vec::new(),
			parameters,
		}
	}

	// ===================== accessors ======================

	/// Returns a list of IDs of nodes that have a role in this
	/// version of the cluster layout, including gateway nodes
	pub fn all_nodes(&self) -> &[Uuid] {
		&self.node_id_vec[..]
	}

	/// Returns a list of IDs of nodes that have a storage capacity
	/// assigned in this version of the cluster layout
	pub fn nongateway_nodes(&self) -> &[Uuid] {
		&self.node_id_vec[..self.nongateway_node_count]
	}

	/// Returns the role of a node in the layout, if it has one
	pub fn node_role(&self, node: &Uuid) -> Option<&NodeRole> {
		match self.roles.get(node) {
			Some(NodeRoleV(Some(v))) => Some(v),
			_ => None,
		}
	}

	/// Returns the capacity of a node in the layout, if it has one
	pub fn get_node_capacity(&self, uuid: &Uuid) -> Option<u64> {
		match self.node_role(uuid) {
			Some(NodeRole {
				capacity: Some(cap),
				zone: _,
				tags: _,
			}) => Some(*cap),
			_ => None,
		}
	}

	/// Given a node uuids, this function returns the label of its zone if it has one
	pub fn get_node_zone(&self, uuid: &Uuid) -> Option<&str> {
		match self.node_role(uuid) {
			Some(role) => Some(&role.zone),
			_ => None,
		}
	}

	/// Returns the number of partitions associated to this node in the ring
	pub fn get_node_usage(&self, uuid: &Uuid) -> Result<usize, Error> {
		for (i, id) in self.node_id_vec.iter().enumerate() {
			if id == uuid {
				let mut count = 0;
				for nod in self.ring_assignment_data.iter() {
					if i as u8 == *nod {
						count += 1
					}
				}
				return Ok(count);
			}
		}
		Err(Error::Message(
			"The Uuid does not correspond to a node present in the \
                    cluster or this node does not have a positive capacity."
				.into(),
		))
	}

	/// Get the partition in which data would fall on
	pub fn partition_of(&self, position: &Hash) -> Partition {
		let top = u16::from_be_bytes(position.as_slice()[0..2].try_into().unwrap());
		top >> (16 - PARTITION_BITS)
	}

	/// Get the list of partitions and the first hash of a partition key that would fall in it
	pub fn partitions(&self) -> impl Iterator<Item = (Partition, Hash)> + '_ {
		(0..(1 << PARTITION_BITS)).map(|i| {
			let top = (i as u16) << (16 - PARTITION_BITS);
			let mut location = [0u8; 32];
			location[..2].copy_from_slice(&u16::to_be_bytes(top)[..]);
			(i as u16, Hash::from(location))
		})
	}

	/// Return the n servers in which data for this hash should be replicated
	pub fn nodes_of(&self, position: &Hash, n: usize) -> impl Iterator<Item = Uuid> + '_ {
		assert_eq!(n, self.replication_factor);

		let data = &self.ring_assignment_data;

		let partition_nodes = if data.len() == self.replication_factor * (1 << PARTITION_BITS) {
			let partition_idx = self.partition_of(position) as usize;
			let partition_start = partition_idx * self.replication_factor;
			let partition_end = (partition_idx + 1) * self.replication_factor;
			&data[partition_start..partition_end]
		} else {
			warn!("Ring not yet ready, read/writes will be lost!");
			&[]
		};

		partition_nodes
			.iter()
			.map(move |i| self.node_id_vec[*i as usize])
	}

	// ===================== internal information extractors ======================

	pub(crate) fn expect_get_node_capacity(&self, uuid: &Uuid) -> u64 {
		self.get_node_capacity(uuid)
			.expect("non-gateway node with zero capacity")
	}

	pub(crate) fn expect_get_node_zone(&self, uuid: &Uuid) -> &str {
		self.get_node_zone(uuid).expect("node without a zone")
	}

	/// Returns the sum of capacities of non gateway nodes in the cluster
	fn get_total_capacity(&self) -> u64 {
		let mut total_capacity = 0;
		for uuid in self.nongateway_nodes() {
			total_capacity += self.expect_get_node_capacity(uuid);
		}
		total_capacity
	}

	/// Returns the effective value of the zone_redundancy parameter
	pub(crate) fn effective_zone_redundancy(&self) -> usize {
		match self.parameters.zone_redundancy {
			ZoneRedundancy::AtLeast(v) => v,
			ZoneRedundancy::Maximum => {
				let n_zones = self
					.roles
					.items()
					.iter()
					.filter_map(|(_, _, role)| role.0.as_ref().map(|x| x.zone.as_str()))
					.collect::<HashSet<&str>>()
					.len();
				std::cmp::min(n_zones, self.replication_factor)
			}
		}
	}

	/// Check a cluster layout for internal consistency
	/// (assignment, roles, parameters, partition size)
	/// returns true if consistent, false if error
	pub fn check(&self) -> Result<(), String> {
		// Check that the assignment data has the correct length
		let expected_assignment_data_len = (1 << PARTITION_BITS) * self.replication_factor;
		if self.ring_assignment_data.len() != expected_assignment_data_len {
			return Err(format!(
				"ring_assignment_data has incorrect length {} instead of {}",
				self.ring_assignment_data.len(),
				expected_assignment_data_len
			));
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
			return Err(format!("node_id_vec does not contain the correct set of nodes\nnode_id_vec: {:?}\nexpected: {:?}", node_id_vec, expected_nodes));
		}

		// Check that the assigned nodes are correct identifiers
		// of nodes that are assigned a role
		// and that role is not the role of a gateway nodes
		for x in self.ring_assignment_data.iter() {
			if *x as usize >= self.node_id_vec.len() {
				return Err(format!(
					"ring_assignment_data contains invalid node id {}",
					*x
				));
			}
			let node = self.node_id_vec[*x as usize];
			match self.roles.get(&node) {
				Some(NodeRoleV(Some(x))) if x.capacity.is_some() => (),
				_ => return Err("ring_assignment_data contains id of a gateway node".into()),
			}
		}

		// Check that every partition is associated to distinct nodes
		let zone_redundancy = self.effective_zone_redundancy();
		let rf = self.replication_factor;
		for p in 0..(1 << PARTITION_BITS) {
			let nodes_of_p = self.ring_assignment_data[rf * p..rf * (p + 1)].to_vec();
			if nodes_of_p.iter().unique().count() != rf {
				return Err(format!("partition does not contain {} unique node ids", rf));
			}
			// Check that every partition is spread over at least zone_redundancy zones.
			let zones_of_p = nodes_of_p
				.iter()
				.map(|n| self.expect_get_node_zone(&self.node_id_vec[*n as usize]))
				.collect::<Vec<_>>();
			if zones_of_p.iter().unique().count() < zone_redundancy {
				return Err(format!(
					"nodes of partition are in less than {} distinct zones",
					zone_redundancy
				));
			}
		}

		// Check that the nodes capacities is consistent with the stored partitions
		let mut node_usage = vec![0; MAX_NODE_NUMBER];
		for n in self.ring_assignment_data.iter() {
			node_usage[*n as usize] += 1;
		}
		for (n, usage) in node_usage.iter().enumerate() {
			if *usage > 0 {
				let uuid = self.node_id_vec[n];
				let partusage = usage * self.partition_size;
				let nodecap = self.expect_get_node_capacity(&uuid);
				if partusage > nodecap {
					return Err(format!(
						"node usage ({}) is bigger than node capacity ({})",
						usage * self.partition_size,
						nodecap
					));
				}
			}
		}

		// Check that the partition size stored is the one computed by the asignation
		// algorithm.
		let cl2 = self.clone();
		let (_, zone_to_id) = cl2.generate_nongateway_zone_ids().unwrap();
		match cl2.compute_optimal_partition_size(&zone_to_id, zone_redundancy) {
			Ok(s) if s != self.partition_size => {
				return Err(format!(
					"partition_size ({}) is different than optimal value ({})",
					self.partition_size, s
				))
			}
			Err(e) => return Err(format!("could not calculate optimal partition size: {}", e)),
			_ => (),
		}

		Ok(())
	}

	// ================== updates to layout, internals ===================

	pub(crate) fn calculate_next_version(
		mut self,
		staging: &LayoutStaging,
	) -> Result<(Self, Message), Error> {
		self.version += 1;

		self.roles.merge(&staging.roles);
		self.roles.retain(|(_, _, v)| v.0.is_some());
		self.parameters = *staging.parameters.get();

		let msg = self.calculate_partition_assignment()?;

		Ok((self, msg))
	}

	/// This function calculates a new partition-to-node assignment.
	/// The computed assignment respects the node replication factor
	/// and the zone redundancy parameter It maximizes the capacity of a
	/// partition (assuming all partitions have the same size).
	/// Among such optimal assignment, it minimizes the distance to
	/// the former assignment (if any) to minimize the amount of
	/// data to be moved.
	/// Staged role changes must be merged with nodes roles before calling this function,
	/// hence it must only be called from apply_staged_changes() and hence is not public.
	fn calculate_partition_assignment(&mut self) -> Result<Message, Error> {
		// We update the node ids, since the node role list might have changed with the
		// changes in the layout. We retrieve the old_assignment reframed with new ids
		let old_assignment_opt = self.update_node_id_vec()?;

		let zone_redundancy = self.effective_zone_redundancy();

		let mut msg = Message::new();
		msg.push("==== COMPUTATION OF A NEW PARTITION ASSIGNATION ====".into());
		msg.push("".into());
		msg.push(format!(
			"Partitions are \
        replicated {} times on at least {} distinct zones.",
			self.replication_factor, zone_redundancy
		));

		// We generate for once numerical ids for the zones of non gateway nodes,
		// to use them as indices in the flow graphs.
		let (id_to_zone, zone_to_id) = self.generate_nongateway_zone_ids()?;

		if self.nongateway_nodes().len() < self.replication_factor {
			return Err(Error::Message(format!(
				"The number of nodes with positive \
            capacity ({}) is smaller than the replication factor ({}).",
				self.nongateway_nodes().len(),
				self.replication_factor
			)));
		}
		if id_to_zone.len() < zone_redundancy {
			return Err(Error::Message(format!(
				"The number of zones with non-gateway \
            nodes ({}) is smaller than the redundancy parameter ({})",
				id_to_zone.len(),
				zone_redundancy
			)));
		}

		// We compute the optimal partition size
		// Capacities should be given in a unit so that partition size is at least 100.
		// In this case, integer rounding plays a marginal role in the percentages of
		// optimality.
		let partition_size = self.compute_optimal_partition_size(&zone_to_id, zone_redundancy)?;

		msg.push("".into());
		if old_assignment_opt.is_some() {
			msg.push(format!(
				"Optimal partition size:                     {} ({} in previous layout)",
				ByteSize::b(partition_size).to_string_as(false),
				ByteSize::b(self.partition_size).to_string_as(false)
			));
		} else {
			msg.push(format!(
				"Optimal partition size:                     {}",
				ByteSize::b(partition_size).to_string_as(false)
			));
		}
		// We write the partition size.
		self.partition_size = partition_size;

		if partition_size < 100 {
			msg.push(
				"WARNING: The partition size is low (< 100), make sure the capacities of your nodes are correct and are of at least a few MB"
					.into(),
			);
		}

		// We compute a first flow/assignment that is heuristically close to the previous
		// assignment
		let mut gflow =
			self.compute_candidate_assignment(&zone_to_id, &old_assignment_opt, zone_redundancy)?;
		if let Some(assoc) = &old_assignment_opt {
			// We minimize the distance to the previous assignment.
			self.minimize_rebalance_load(&mut gflow, &zone_to_id, assoc)?;
		}

		// We display statistics of the computation
		msg.extend(self.output_stat(&gflow, &old_assignment_opt, &zone_to_id, &id_to_zone)?);

		// We update the layout structure
		self.update_ring_from_flow(id_to_zone.len(), &gflow)?;

		if let Err(e) = self.check() {
			return Err(Error::Message(
				format!("Layout check returned an error: {}\nOriginal result of computation: <<<<\n{}\n>>>>", e, msg.join("\n"))
			));
		}

		Ok(msg)
	}

	/// The LwwMap of node roles might have changed. This function updates the node_id_vec
	/// and returns the assignment given by ring, with the new indices of the nodes, and
	/// None if the node is not present anymore.
	/// We work with the assumption that only this function and calculate_new_assignment
	/// do modify assignment_ring and node_id_vec.
	fn update_node_id_vec(&mut self) -> Result<Option<Vec<Vec<usize>>>, Error> {
		// (1) We compute the new node list
		// Non gateway nodes should be coded on 8bits, hence they must be first in the list
		// We build the new node ids
		let new_non_gateway_nodes: Vec<Uuid> = self
			.roles
			.items()
			.iter()
			.filter(|(_, _, v)| matches!(&v.0, Some(r) if r.capacity.is_some()))
			.map(|(k, _, _)| *k)
			.collect();

		if new_non_gateway_nodes.len() > MAX_NODE_NUMBER {
			return Err(Error::Message(format!(
				"There are more than {} non-gateway nodes in the new \
                            layout. This is not allowed.",
				MAX_NODE_NUMBER
			)));
		}

		let new_gateway_nodes: Vec<Uuid> = self
			.roles
			.items()
			.iter()
			.filter(|(_, _, v)| matches!(v, NodeRoleV(Some(r)) if r.capacity.is_none()))
			.map(|(k, _, _)| *k)
			.collect();

		let old_node_id_vec = std::mem::take(&mut self.node_id_vec);

		self.nongateway_node_count = new_non_gateway_nodes.len();
		self.node_id_vec.clear();
		self.node_id_vec.extend(new_non_gateway_nodes);
		self.node_id_vec.extend(new_gateway_nodes);

		let new_node_id_vec = &self.node_id_vec;

		// (2) We retrieve the old association
		// We rewrite the old association with the new indices. We only consider partition
		// to node assignments where the node is still in use.
		if self.ring_assignment_data.is_empty() {
			// This is a new association
			return Ok(None);
		}

		if self.ring_assignment_data.len() != NB_PARTITIONS * self.replication_factor {
			return Err(Error::Message(
				"The old assignment does not have a size corresponding to \
                the old replication factor or the number of partitions."
					.into(),
			));
		}

		// We build a translation table between the uuid and new ids
		let mut uuid_to_new_id = HashMap::<Uuid, usize>::new();

		// We add the indices of only the new non-gateway nodes that can be used in the
		// association ring
		for (i, uuid) in new_node_id_vec.iter().enumerate() {
			uuid_to_new_id.insert(*uuid, i);
		}

		let mut old_assignment = vec![Vec::<usize>::new(); NB_PARTITIONS];
		let rf = self.replication_factor;

		for (p, old_assign_p) in old_assignment.iter_mut().enumerate() {
			for old_id in &self.ring_assignment_data[p * rf..(p + 1) * rf] {
				let uuid = old_node_id_vec[*old_id as usize];
				if uuid_to_new_id.contains_key(&uuid) {
					old_assign_p.push(uuid_to_new_id[&uuid]);
				}
			}
		}

		// We clear the ring assignment data
		self.ring_assignment_data = Vec::<CompactNodeType>::new();

		Ok(Some(old_assignment))
	}

	/// This function generates ids for the zone of the nodes appearing in
	/// self.node_id_vec.
	pub(crate) fn generate_nongateway_zone_ids(
		&self,
	) -> Result<(Vec<String>, HashMap<String, usize>), Error> {
		let mut id_to_zone = Vec::<String>::new();
		let mut zone_to_id = HashMap::<String, usize>::new();

		for uuid in self.nongateway_nodes().iter() {
			let r = self.node_role(uuid).unwrap();
			if !zone_to_id.contains_key(&r.zone) && r.capacity.is_some() {
				zone_to_id.insert(r.zone.clone(), id_to_zone.len());
				id_to_zone.push(r.zone.clone());
			}
		}
		Ok((id_to_zone, zone_to_id))
	}

	/// This function computes by dichotomy the largest realizable partition size, given
	/// the layout roles and parameters.
	fn compute_optimal_partition_size(
		&self,
		zone_to_id: &HashMap<String, usize>,
		zone_redundancy: usize,
	) -> Result<u64, Error> {
		let empty_set = HashSet::<(usize, usize)>::new();
		let mut g = self.generate_flow_graph(1, zone_to_id, &empty_set, zone_redundancy)?;
		g.compute_maximal_flow()?;
		if g.get_flow_value()? < (NB_PARTITIONS * self.replication_factor) as i64 {
			return Err(Error::Message(
				"The storage capacity of he cluster is to small. It is \
                       impossible to store partitions of size 1."
					.into(),
			));
		}

		let mut s_down = 1;
		let mut s_up = self.get_total_capacity();
		while s_down + 1 < s_up {
			g = self.generate_flow_graph(
				(s_down + s_up) / 2,
				zone_to_id,
				&empty_set,
				zone_redundancy,
			)?;
			g.compute_maximal_flow()?;
			if g.get_flow_value()? < (NB_PARTITIONS * self.replication_factor) as i64 {
				s_up = (s_down + s_up) / 2;
			} else {
				s_down = (s_down + s_up) / 2;
			}
		}

		Ok(s_down)
	}

	fn generate_graph_vertices(nb_zones: usize, nb_nodes: usize) -> Vec<Vertex> {
		let mut vertices = vec![Vertex::Source, Vertex::Sink];
		for p in 0..NB_PARTITIONS {
			vertices.push(Vertex::Pup(p));
			vertices.push(Vertex::Pdown(p));
			for z in 0..nb_zones {
				vertices.push(Vertex::PZ(p, z));
			}
		}
		for n in 0..nb_nodes {
			vertices.push(Vertex::N(n));
		}
		vertices
	}

	/// Generates the graph to compute the maximal flow corresponding to the optimal
	/// partition assignment.
	/// exclude_assoc is the set of (partition, node) association that we are forbidden
	/// to use (hence we do not add the corresponding edge to the graph). This parameter
	/// is used to compute a first flow that uses only edges appearing in the previous
	/// assignment. This produces a solution that heuristically should be close to the
	/// previous one.
	fn generate_flow_graph(
		&self,
		partition_size: u64,
		zone_to_id: &HashMap<String, usize>,
		exclude_assoc: &HashSet<(usize, usize)>,
		zone_redundancy: usize,
	) -> Result<Graph<FlowEdge>, Error> {
		let vertices =
			LayoutVersion::generate_graph_vertices(zone_to_id.len(), self.nongateway_nodes().len());
		let mut g = Graph::<FlowEdge>::new(&vertices);
		let nb_zones = zone_to_id.len();
		for p in 0..NB_PARTITIONS {
			g.add_edge(Vertex::Source, Vertex::Pup(p), zone_redundancy as u64)?;
			g.add_edge(
				Vertex::Source,
				Vertex::Pdown(p),
				(self.replication_factor - zone_redundancy) as u64,
			)?;
			for z in 0..nb_zones {
				g.add_edge(Vertex::Pup(p), Vertex::PZ(p, z), 1)?;
				g.add_edge(
					Vertex::Pdown(p),
					Vertex::PZ(p, z),
					self.replication_factor as u64,
				)?;
			}
		}
		for n in 0..self.nongateway_nodes().len() {
			let node_capacity = self.expect_get_node_capacity(&self.node_id_vec[n]);
			let node_zone = zone_to_id[self.expect_get_node_zone(&self.node_id_vec[n])];
			g.add_edge(Vertex::N(n), Vertex::Sink, node_capacity / partition_size)?;
			for p in 0..NB_PARTITIONS {
				if !exclude_assoc.contains(&(p, n)) {
					g.add_edge(Vertex::PZ(p, node_zone), Vertex::N(n), 1)?;
				}
			}
		}
		Ok(g)
	}

	/// This function computes a first optimal assignment (in the form of a flow graph).
	fn compute_candidate_assignment(
		&self,
		zone_to_id: &HashMap<String, usize>,
		prev_assign_opt: &Option<Vec<Vec<usize>>>,
		zone_redundancy: usize,
	) -> Result<Graph<FlowEdge>, Error> {
		// We list the (partition,node) associations that are not used in the
		// previous assignment
		let mut exclude_edge = HashSet::<(usize, usize)>::new();
		if let Some(prev_assign) = prev_assign_opt {
			let nb_nodes = self.nongateway_nodes().len();
			for (p, prev_assign_p) in prev_assign.iter().enumerate() {
				for n in 0..nb_nodes {
					exclude_edge.insert((p, n));
				}
				for n in prev_assign_p.iter() {
					exclude_edge.remove(&(p, *n));
				}
			}
		}

		// We compute the best flow using only the edges used in the previous assignment
		let mut g = self.generate_flow_graph(
			self.partition_size,
			zone_to_id,
			&exclude_edge,
			zone_redundancy,
		)?;
		g.compute_maximal_flow()?;

		// We add the excluded edges and compute the maximal flow with the full graph.
		// The algorithm is such that it will start with the flow that we just computed
		// and find ameliorating paths from that.
		for (p, n) in exclude_edge.iter() {
			let node_zone = zone_to_id[self.expect_get_node_zone(&self.node_id_vec[*n])];
			g.add_edge(Vertex::PZ(*p, node_zone), Vertex::N(*n), 1)?;
		}
		g.compute_maximal_flow()?;
		Ok(g)
	}

	/// This function updates the flow graph gflow to minimize the distance between
	/// its corresponding assignment and the previous one
	fn minimize_rebalance_load(
		&self,
		gflow: &mut Graph<FlowEdge>,
		zone_to_id: &HashMap<String, usize>,
		prev_assign: &[Vec<usize>],
	) -> Result<(), Error> {
		// We define a cost function on the edges (pairs of vertices) corresponding
		// to the distance between the two assignments.
		let mut cost = CostFunction::new();
		for (p, assoc_p) in prev_assign.iter().enumerate() {
			for n in assoc_p.iter() {
				if let Some(&node_zone) =
					zone_to_id.get(self.expect_get_node_zone(&self.node_id_vec[*n]))
				{
					cost.insert((Vertex::PZ(p, node_zone), Vertex::N(*n)), -1);
				}
			}
		}

		// We compute the maximal length of a simple path in gflow. It is used in the
		// Bellman-Ford algorithm in optimize_flow_with_cost to set the number
		// of iterations.
		let nb_nodes = self.nongateway_nodes().len();
		let path_length = 4 * nb_nodes;
		gflow.optimize_flow_with_cost(&cost, path_length)?;

		Ok(())
	}

	/// This function updates the assignment ring from the flow graph.
	fn update_ring_from_flow(
		&mut self,
		nb_zones: usize,
		gflow: &Graph<FlowEdge>,
	) -> Result<(), Error> {
		self.ring_assignment_data = Vec::<CompactNodeType>::new();
		for p in 0..NB_PARTITIONS {
			for z in 0..nb_zones {
				let assoc_vertex = gflow.get_positive_flow_from(Vertex::PZ(p, z))?;
				for vertex in assoc_vertex.iter() {
					if let Vertex::N(n) = vertex {
						self.ring_assignment_data.push((*n).try_into().unwrap());
					}
				}
			}
		}

		if self.ring_assignment_data.len() != NB_PARTITIONS * self.replication_factor {
			return Err(Error::Message(
				"Critical Error : the association ring we produced does not \
                       have the right size."
					.into(),
			));
		}
		Ok(())
	}

	/// This function returns a message summing up the partition repartition of the new
	/// layout, and other statistics of the partition assignment computation.
	fn output_stat(
		&self,
		gflow: &Graph<FlowEdge>,
		prev_assign_opt: &Option<Vec<Vec<usize>>>,
		zone_to_id: &HashMap<String, usize>,
		id_to_zone: &[String],
	) -> Result<Message, Error> {
		let mut msg = Message::new();

		let used_cap = self.partition_size * NB_PARTITIONS as u64 * self.replication_factor as u64;
		let total_cap = self.get_total_capacity();
		let percent_cap = 100.0 * (used_cap as f32) / (total_cap as f32);
		msg.push(format!(
			"Usable capacity / total cluster capacity:   {} / {} ({:.1} %)",
			ByteSize::b(used_cap).to_string_as(false),
			ByteSize::b(total_cap).to_string_as(false),
			percent_cap
		));
		msg.push(format!(
			"Effective capacity (replication factor {}):  {}",
			self.replication_factor,
			ByteSize::b(used_cap / self.replication_factor as u64).to_string_as(false)
		));
		if percent_cap < 80. {
			msg.push("".into());
			msg.push(
				"If the percentage is too low, it might be that the \
            cluster topology and redundancy constraints are forcing the use of nodes/zones with small \
            storage capacities."
					.into(),
			);
			msg.push(
				"You might want to move storage capacity between zones or relax the redundancy constraint."
					.into(),
			);
			msg.push(
				"See the detailed statistics below and look for saturated nodes/zones.".into(),
			);
		}

		// We define and fill in the following tables
		let storing_nodes = self.nongateway_nodes();
		let mut new_partitions = vec![0; storing_nodes.len()];
		let mut stored_partitions = vec![0; storing_nodes.len()];

		let mut new_partitions_zone = vec![0; id_to_zone.len()];
		let mut stored_partitions_zone = vec![0; id_to_zone.len()];

		for p in 0..NB_PARTITIONS {
			for z in 0..id_to_zone.len() {
				let pz_nodes = gflow.get_positive_flow_from(Vertex::PZ(p, z))?;
				if !pz_nodes.is_empty() {
					stored_partitions_zone[z] += 1;
					if let Some(prev_assign) = prev_assign_opt {
						let mut old_zones_of_p = Vec::<usize>::new();
						for n in prev_assign[p].iter() {
							if let Some(&zone_id) =
								zone_to_id.get(self.expect_get_node_zone(&self.node_id_vec[*n]))
							{
								old_zones_of_p.push(zone_id);
							}
						}
						if !old_zones_of_p.contains(&z) {
							new_partitions_zone[z] += 1;
						}
					}
				}
				for vert in pz_nodes.iter() {
					if let Vertex::N(n) = *vert {
						stored_partitions[n] += 1;
						if let Some(prev_assign) = prev_assign_opt {
							if !prev_assign[p].contains(&n) {
								new_partitions[n] += 1;
							}
						}
					}
				}
			}
		}

		if prev_assign_opt.is_none() {
			new_partitions = stored_partitions.clone();
			//new_partitions_zone = stored_partitions_zone.clone();
		}

		// We display the statistics

		msg.push("".into());
		if prev_assign_opt.is_some() {
			let total_new_partitions: usize = new_partitions.iter().sum();
			msg.push(format!(
				"A total of {} new copies of partitions need to be \
                             transferred.",
				total_new_partitions
			));
			msg.push("".into());
		}

		let mut table = vec![];
		for z in 0..id_to_zone.len() {
			let mut nodes_of_z = Vec::<usize>::new();
			for n in 0..storing_nodes.len() {
				if self.expect_get_node_zone(&self.node_id_vec[n]) == id_to_zone[z] {
					nodes_of_z.push(n);
				}
			}
			let replicated_partitions: usize =
				nodes_of_z.iter().map(|n| stored_partitions[*n]).sum();
			table.push(format!(
				"{}\tTags\tPartitions\tCapacity\tUsable capacity",
				id_to_zone[z]
			));

			let available_cap_z: u64 = self.partition_size * replicated_partitions as u64;
			let mut total_cap_z = 0;
			for n in nodes_of_z.iter() {
				total_cap_z += self.expect_get_node_capacity(&self.node_id_vec[*n]);
			}
			let percent_cap_z = 100.0 * (available_cap_z as f32) / (total_cap_z as f32);

			for n in nodes_of_z.iter() {
				let available_cap_n = stored_partitions[*n] as u64 * self.partition_size;
				let total_cap_n = self.expect_get_node_capacity(&self.node_id_vec[*n]);
				let tags_n = (self.node_role(&self.node_id_vec[*n]).ok_or("<??>"))?.tags_string();
				table.push(format!(
					"  {:?}\t{}\t{} ({} new)\t{}\t{} ({:.1}%)",
					self.node_id_vec[*n],
					tags_n,
					stored_partitions[*n],
					new_partitions[*n],
					ByteSize::b(total_cap_n).to_string_as(false),
					ByteSize::b(available_cap_n).to_string_as(false),
					(available_cap_n as f32) / (total_cap_n as f32) * 100.0,
				));
			}

			table.push(format!(
				"  TOTAL\t\t{} ({} unique)\t{}\t{} ({:.1}%)",
				replicated_partitions,
				stored_partitions_zone[z],
				//new_partitions_zone[z],
				ByteSize::b(total_cap_z).to_string_as(false),
				ByteSize::b(available_cap_z).to_string_as(false),
				percent_cap_z
			));
			table.push("".into());
		}
		msg.push(format_table::format_table_to_string(table));

		Ok(msg)
	}
}
