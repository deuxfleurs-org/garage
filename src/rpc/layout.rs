use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;

use bytesize::ByteSize;
use itertools::Itertools;

use garage_util::crdt::{AutoCrdt, Crdt, Lww, LwwMap};
use garage_util::data::*;
use garage_util::encode::nonversioned_encode;
use garage_util::error::*;

use crate::graph_algo::*;

use crate::ring::*;

use std::convert::TryInto;

const NB_PARTITIONS: usize = 1usize << PARTITION_BITS;

// The Message type will be used to collect information on the algorithm.
type Message = Vec<String>;

mod v08 {
	use crate::ring::CompactNodeType;
	use garage_util::crdt::LwwMap;
	use garage_util::data::{Hash, Uuid};
	use serde::{Deserialize, Serialize};

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

	/// The user-assigned roles of cluster nodes
	#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
	pub struct NodeRole {
		/// Datacenter at which this entry belong. This information is used to
		/// perform a better geodistribution
		pub zone: String,
		/// The capacity of the node
		/// If this is set to None, the node does not participate in storing data for the system
		/// and is only active as an API gateway to other nodes
		pub capacity: Option<u64>,
		/// A set of tags to recognize the node
		pub tags: Vec<String>,
	}

	impl garage_util::migrate::InitialFormat for ClusterLayout {}
}

mod v09 {
	use super::v08;
	use crate::ring::CompactNodeType;
	use garage_util::crdt::{Lww, LwwMap};
	use garage_util::data::{Hash, Uuid};
	use serde::{Deserialize, Serialize};
	pub use v08::{NodeRole, NodeRoleV};

	/// The layout of the cluster, i.e. the list of roles
	/// which are assigned to each cluster node
	#[derive(Clone, Debug, Serialize, Deserialize)]
	pub struct ClusterLayout {
		pub version: u64,

		pub replication_factor: usize,

		/// This attribute is only used to retain the previously computed partition size,
		/// to know to what extent does it change with the layout update.
		pub partition_size: u64,
		/// Parameters used to compute the assignment currently given by
		/// ring_assignment_data
		pub parameters: LayoutParameters,

		pub roles: LwwMap<Uuid, NodeRoleV>,

		/// see comment in v08::ClusterLayout
		pub node_id_vec: Vec<Uuid>,
		/// see comment in v08::ClusterLayout
		#[serde(with = "serde_bytes")]
		pub ring_assignment_data: Vec<CompactNodeType>,

		/// Parameters to be used in the next partition assignment computation.
		pub staging_parameters: Lww<LayoutParameters>,
		/// Role changes which are staged for the next version of the layout
		pub staging_roles: LwwMap<Uuid, NodeRoleV>,
		pub staging_hash: Hash,
	}

	/// This struct is used to set the parameters to be used in the assignment computation
	/// algorithm. It is stored as a Crdt.
	#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug, Serialize, Deserialize)]
	pub struct LayoutParameters {
		pub zone_redundancy: usize,
	}

	impl garage_util::migrate::Migrate for ClusterLayout {
		const VERSION_MARKER: &'static [u8] = b"Glayout09";

		type Previous = v08::ClusterLayout;

		fn migrate(previous: Self::Previous) -> Self {
			use itertools::Itertools;
			use std::collections::HashSet;

			// In the old layout, capacities are in an arbitrary unit,
			// but in the new layout they are in bytes.
			// Here we arbitrarily multiply everything by 1G,
			// such that 1 old capacity unit = 1GB in the new units.
			// This is totally arbitrary and won't work for most users.
			let cap_mul = 1024 * 1024 * 1024;
			let roles = multiply_all_capacities(previous.roles, cap_mul);
			let staging_roles = multiply_all_capacities(previous.staging, cap_mul);
			let node_id_vec = previous.node_id_vec;

			// Determine partition size
			let mut tmp = previous.ring_assignation_data.clone();
			tmp.sort();
			let partition_size = tmp
				.into_iter()
				.dedup_with_count()
				.map(|(npart, node)| {
					roles
						.get(&node_id_vec[node as usize])
						.and_then(|p| p.0.as_ref().and_then(|r| r.capacity))
						.unwrap_or(0) / npart as u64
				})
				.min()
				.unwrap_or(0);

			// Determine zone redundancy parameter
			let zone_redundancy = std::cmp::min(
				previous.replication_factor,
				roles
					.items()
					.iter()
					.filter_map(|(_, _, r)| r.0.as_ref().map(|p| p.zone.as_str()))
					.collect::<HashSet<&str>>()
					.len(),
			);
			let parameters = LayoutParameters { zone_redundancy };

			let mut res = Self {
				version: previous.version,
				replication_factor: previous.replication_factor,
				partition_size,
				parameters,
				roles,
				node_id_vec,
				ring_assignment_data: previous.ring_assignation_data,
				staging_parameters: Lww::new(parameters),
				staging_roles,
				staging_hash: [0u8; 32].into(),
			};
			res.staging_hash = res.calculate_staging_hash();
			res
		}
	}

	fn multiply_all_capacities(
		old_roles: LwwMap<Uuid, NodeRoleV>,
		mul: u64,
	) -> LwwMap<Uuid, NodeRoleV> {
		let mut new_roles = LwwMap::new();
		for (node, ts, role) in old_roles.items() {
			let mut role = role.clone();
			if let NodeRoleV(Some(NodeRole {
				capacity: Some(ref mut cap),
				..
			})) = role
			{
				*cap = *cap * mul;
			}
			new_roles.merge_raw(node, *ts, &role);
		}
		new_roles
	}
}

pub use v09::*;

impl AutoCrdt for LayoutParameters {
	const WARN_IF_DIFFERENT: bool = true;
}

impl AutoCrdt for NodeRoleV {
	const WARN_IF_DIFFERENT: bool = true;
}

impl NodeRole {
	pub fn capacity_string(&self) -> String {
		match self.capacity {
			Some(c) => ByteSize::b(c).to_string_as(false),
			None => "gateway".to_string(),
		}
	}

	pub fn tags_string(&self) -> String {
		self.tags.join(",")
	}
}

// Implementation of the ClusterLayout methods unrelated to the assignment algorithm.
impl ClusterLayout {
	pub fn new(replication_factor: usize) -> Self {
		// We set the default zone redundancy to be equal to the replication factor,
		// i.e. as strict as possible.
		let parameters = LayoutParameters {
			zone_redundancy: replication_factor,
		};
		let staging_parameters = Lww::<LayoutParameters>::new(parameters.clone());

		let empty_lwwmap = LwwMap::new();

		let mut ret = ClusterLayout {
			version: 0,
			replication_factor,
			partition_size: 0,
			roles: LwwMap::new(),
			node_id_vec: Vec::new(),
			ring_assignment_data: Vec::new(),
			parameters,
			staging_parameters,
			staging_roles: empty_lwwmap,
			staging_hash: [0u8; 32].into(),
		};
		ret.staging_hash = ret.calculate_staging_hash();
		ret
	}

	fn calculate_staging_hash(&self) -> Hash {
		let hashed_tuple = (&self.staging_roles, &self.staging_parameters);
		blake2sum(&nonversioned_encode(&hashed_tuple).unwrap()[..])
	}

	pub fn merge(&mut self, other: &ClusterLayout) -> bool {
		match other.version.cmp(&self.version) {
			Ordering::Greater => {
				*self = other.clone();
				true
			}
			Ordering::Equal => {
				self.staging_parameters.merge(&other.staging_parameters);
				self.staging_roles.merge(&other.staging_roles);

				let new_staging_hash = self.calculate_staging_hash();
				let changed = new_staging_hash != self.staging_hash;

				self.staging_hash = new_staging_hash;

				changed
			}
			Ordering::Less => false,
		}
	}

	pub fn apply_staged_changes(mut self, version: Option<u64>) -> Result<(Self, Message), Error> {
		match version {
			None => {
				let error = r#"
Please pass the new layout version number to ensure that you are writing the correct version of the cluster layout.
To know the correct value of the new layout version, invoke `garage layout show` and review the proposed changes.
				"#;
				return Err(Error::Message(error.into()));
			}
			Some(v) => {
				if v != self.version + 1 {
					return Err(Error::Message("Invalid new layout version".into()));
				}
			}
		}

		self.roles.merge(&self.staging_roles);
		self.roles.retain(|(_, _, v)| v.0.is_some());
		self.parameters = self.staging_parameters.get().clone();

		self.staging_roles.clear();
		self.staging_hash = self.calculate_staging_hash();

		let msg = self.calculate_partition_assignment()?;

		self.version += 1;

		Ok((self, msg))
	}

	pub fn revert_staged_changes(mut self, version: Option<u64>) -> Result<Self, Error> {
		match version {
			None => {
				let error = r#"
Please pass the new layout version number to ensure that you are writing the correct version of the cluster layout.
To know the correct value of the new layout version, invoke `garage layout show` and review the proposed changes.
				"#;
				return Err(Error::Message(error.into()));
			}
			Some(v) => {
				if v != self.version + 1 {
					return Err(Error::Message("Invalid new layout version".into()));
				}
			}
		}

		self.staging_roles.clear();
		self.staging_parameters.update(self.parameters.clone());
		self.staging_hash = self.calculate_staging_hash();

		self.version += 1;

		Ok(self)
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

	/// Returns the uuids of the non_gateway nodes in self.node_id_vec.
	fn nongateway_nodes(&self) -> Vec<Uuid> {
		let mut result = Vec::<Uuid>::new();
		for uuid in self.node_id_vec.iter() {
			match self.node_role(uuid) {
				Some(role) if role.capacity != None => result.push(*uuid),
				_ => (),
			}
		}
		result
	}

	/// Given a node uuids, this function returns the label of its zone
	fn get_node_zone(&self, uuid: &Uuid) -> Result<String, Error> {
		match self.node_role(uuid) {
			Some(role) => Ok(role.zone.clone()),
			_ => Err(Error::Message(
				"The Uuid does not correspond to a node present in the cluster.".into(),
			)),
		}
	}

	/// Given a node uuids, this function returns its capacity or fails if it does not have any
	pub fn get_node_capacity(&self, uuid: &Uuid) -> Result<u64, Error> {
		match self.node_role(uuid) {
			Some(NodeRole {
				capacity: Some(cap),
				zone: _,
				tags: _,
			}) => Ok(*cap),
			_ => Err(Error::Message(
				"The Uuid does not correspond to a node present in the \
                    cluster or this node does not have a positive capacity."
					.into(),
			)),
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

	/// Returns the sum of capacities of non gateway nodes in the cluster
	fn get_total_capacity(&self) -> Result<u64, Error> {
		let mut total_capacity = 0;
		for uuid in self.nongateway_nodes().iter() {
			total_capacity += self.get_node_capacity(uuid)?;
		}
		Ok(total_capacity)
	}

	/// Check a cluster layout for internal consistency
	/// (assignment, roles, parameters, partition size)
	/// returns true if consistent, false if error
	pub fn check(&self) -> Result<(), String> {
		// Check that the hash of the staging data is correct
		let staging_hash = self.calculate_staging_hash();
		if staging_hash != self.staging_hash {
			return Err("staging_hash is incorrect".into());
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

		// Check that the assignment data has the correct length
		let expected_assignment_data_len = (1 << PARTITION_BITS) * self.replication_factor;
		if self.ring_assignment_data.len() != expected_assignment_data_len {
			return Err(format!(
				"ring_assignment_data has incorrect length {} instead of {}",
				self.ring_assignment_data.len(),
				expected_assignment_data_len
			));
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
		let rf = self.replication_factor;
		for p in 0..(1 << PARTITION_BITS) {
			let nodes_of_p = self.ring_assignment_data[rf * p..rf * (p + 1)].to_vec();
			if nodes_of_p.iter().unique().count() != rf {
				return Err(format!("partition does not contain {} unique node ids", rf));
			}
			// Check that every partition is spread over at least zone_redundancy zones.
			let zones_of_p = nodes_of_p
				.iter()
				.map(|n| {
					self.get_node_zone(&self.node_id_vec[*n as usize])
						.expect("Zone not found.")
				})
				.collect::<Vec<_>>();
			let redundancy = self.parameters.zone_redundancy;
			if zones_of_p.iter().unique().count() < redundancy {
				return Err(format!(
					"nodes of partition are in less than {} distinct zones",
					redundancy
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
				let nodecap = self.get_node_capacity(&uuid).unwrap();
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
		match cl2.compute_optimal_partition_size(&zone_to_id) {
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
}

// Implementation of the ClusterLayout methods related to the assignment algorithm.
impl ClusterLayout {
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

		let mut msg = Message::new();
		msg.push("==== COMPUTATION OF A NEW PARTITION ASSIGNATION ====".into());
		msg.push("".into());
		msg.push(format!(
			"Partitions are \
        replicated {} times on at least {} distinct zones.",
			self.replication_factor, self.parameters.zone_redundancy
		));

		// We generate for once numerical ids for the zones of non gateway nodes,
		// to use them as indices in the flow graphs.
		let (id_to_zone, zone_to_id) = self.generate_nongateway_zone_ids()?;

		let nb_nongateway_nodes = self.nongateway_nodes().len();
		if nb_nongateway_nodes < self.replication_factor {
			return Err(Error::Message(format!(
				"The number of nodes with positive \
            capacity ({}) is smaller than the replication factor ({}).",
				nb_nongateway_nodes, self.replication_factor
			)));
		}
		if id_to_zone.len() < self.parameters.zone_redundancy {
			return Err(Error::Message(format!(
				"The number of zones with non-gateway \
            nodes ({}) is smaller than the redundancy parameter ({})",
				id_to_zone.len(),
				self.parameters.zone_redundancy
			)));
		}

		// We compute the optimal partition size
		// Capacities should be given in a unit so that partition size is at least 100.
		// In this case, integer rounding plays a marginal role in the percentages of
		// optimality.
		let partition_size = self.compute_optimal_partition_size(&zone_to_id)?;

		if old_assignment_opt != None {
			msg.push(format!(
				"Optimal size of a partition: {} (was {} in the previous layout).",
				ByteSize::b(partition_size).to_string_as(false),
				ByteSize::b(self.partition_size).to_string_as(false)
			));
		} else {
			msg.push(format!(
				"Given the replication and redundancy constraints, the \
                optimal size of a partition is {}.",
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
		let mut gflow = self.compute_candidate_assignment(&zone_to_id, &old_assignment_opt)?;
		if let Some(assoc) = &old_assignment_opt {
			// We minimize the distance to the previous assignment.
			self.minimize_rebalance_load(&mut gflow, &zone_to_id, assoc)?;
		}

		// We display statistics of the computation
		msg.extend(self.output_stat(&gflow, &old_assignment_opt, &zone_to_id, &id_to_zone)?);
		msg.push("".to_string());

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
			.filter(|(_, _, v)| matches!(&v.0, Some(r) if r.capacity != None))
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
			.filter(|(_, _, v)| matches!(v, NodeRoleV(Some(r)) if r.capacity == None))
			.map(|(k, _, _)| *k)
			.collect();

		let mut new_node_id_vec = Vec::<Uuid>::new();
		new_node_id_vec.extend(new_non_gateway_nodes);
		new_node_id_vec.extend(new_gateway_nodes);

		let old_node_id_vec = self.node_id_vec.clone();
		self.node_id_vec = new_node_id_vec.clone();

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

		// We write the ring
		self.ring_assignment_data = Vec::<CompactNodeType>::new();

		Ok(Some(old_assignment))
	}

	/// This function generates ids for the zone of the nodes appearing in
	/// self.node_id_vec.
	fn generate_nongateway_zone_ids(&self) -> Result<(Vec<String>, HashMap<String, usize>), Error> {
		let mut id_to_zone = Vec::<String>::new();
		let mut zone_to_id = HashMap::<String, usize>::new();

		for uuid in self.nongateway_nodes().iter() {
			let r = self.node_role(uuid).unwrap();
			if !zone_to_id.contains_key(&r.zone) && r.capacity != None {
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
	) -> Result<u64, Error> {
		let empty_set = HashSet::<(usize, usize)>::new();
		let mut g = self.generate_flow_graph(1, zone_to_id, &empty_set)?;
		g.compute_maximal_flow()?;
		if g.get_flow_value()? < (NB_PARTITIONS * self.replication_factor) as i64 {
			return Err(Error::Message(
				"The storage capacity of he cluster is to small. It is \
                       impossible to store partitions of size 1."
					.into(),
			));
		}

		let mut s_down = 1;
		let mut s_up = self.get_total_capacity()?;
		while s_down + 1 < s_up {
			g = self.generate_flow_graph((s_down + s_up) / 2, zone_to_id, &empty_set)?;
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
	) -> Result<Graph<FlowEdge>, Error> {
		let vertices =
			ClusterLayout::generate_graph_vertices(zone_to_id.len(), self.nongateway_nodes().len());
		let mut g = Graph::<FlowEdge>::new(&vertices);
		let nb_zones = zone_to_id.len();
		let redundancy = self.parameters.zone_redundancy;
		for p in 0..NB_PARTITIONS {
			g.add_edge(Vertex::Source, Vertex::Pup(p), redundancy as u64)?;
			g.add_edge(
				Vertex::Source,
				Vertex::Pdown(p),
				(self.replication_factor - redundancy) as u64,
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
			let node_capacity = self.get_node_capacity(&self.node_id_vec[n])?;
			let node_zone = zone_to_id[&self.get_node_zone(&self.node_id_vec[n])?];
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
		let mut g = self.generate_flow_graph(self.partition_size, zone_to_id, &exclude_edge)?;
		g.compute_maximal_flow()?;

		// We add the excluded edges and compute the maximal flow with the full graph.
		// The algorithm is such that it will start with the flow that we just computed
		// and find ameliorating paths from that.
		for (p, n) in exclude_edge.iter() {
			let node_zone = zone_to_id[&self.get_node_zone(&self.node_id_vec[*n])?];
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
				let node_zone = zone_to_id[&self.get_node_zone(&self.node_id_vec[*n])?];
				cost.insert((Vertex::PZ(p, node_zone), Vertex::N(*n)), -1);
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
		let total_cap = self.get_total_capacity()?;
		let percent_cap = 100.0 * (used_cap as f32) / (total_cap as f32);
		msg.push("".into());
		msg.push(format!(
			"Usable capacity / Total cluster capacity: {} / {} ({:.1} %)",
			ByteSize::b(used_cap).to_string_as(false),
			ByteSize::b(total_cap).to_string_as(false),
			percent_cap
		));
		msg.push("".into());
		msg.push(
			"If the percentage is too low, it might be that the \
        replication/redundancy constraints force the use of nodes/zones with small \
        storage capacities. \
        You might want to rebalance the storage capacities or relax the constraints. \
        See the detailed statistics below and look for saturated nodes/zones."
				.into(),
		);
		msg.push(format!(
			"Recall that because of the replication factor, the actual available \
                         storage capacity is {} / {} = {}.",
			ByteSize::b(used_cap).to_string_as(false),
			self.replication_factor,
			ByteSize::b(used_cap / self.replication_factor as u64).to_string_as(false)
		));

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
							old_zones_of_p
								.push(zone_to_id[&self.get_node_zone(&self.node_id_vec[*n])?]);
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

		if *prev_assign_opt == None {
			new_partitions = stored_partitions.clone();
			new_partitions_zone = stored_partitions_zone.clone();
		}

		// We display the statistics

		msg.push("".into());
		if *prev_assign_opt != None {
			let total_new_partitions: usize = new_partitions.iter().sum();
			msg.push(format!(
				"A total of {} new copies of partitions need to be \
                             transferred.",
				total_new_partitions
			));
		}
		msg.push("".into());
		msg.push("==== DETAILED STATISTICS BY ZONES AND NODES ====".into());

		for z in 0..id_to_zone.len() {
			let mut nodes_of_z = Vec::<usize>::new();
			for n in 0..storing_nodes.len() {
				if self.get_node_zone(&self.node_id_vec[n])? == id_to_zone[z] {
					nodes_of_z.push(n);
				}
			}
			let replicated_partitions: usize =
				nodes_of_z.iter().map(|n| stored_partitions[*n]).sum();
			msg.push("".into());

			msg.push(format!(
				"Zone {}: {} distinct partitions stored ({} new, \
                {} partition copies) ",
				id_to_zone[z],
				stored_partitions_zone[z],
				new_partitions_zone[z],
				replicated_partitions
			));

			let available_cap_z: u64 = self.partition_size * replicated_partitions as u64;
			let mut total_cap_z = 0;
			for n in nodes_of_z.iter() {
				total_cap_z += self.get_node_capacity(&self.node_id_vec[*n])?;
			}
			let percent_cap_z = 100.0 * (available_cap_z as f32) / (total_cap_z as f32);
			msg.push(format!(
				"  Usable capacity / Total capacity: {} / {} ({:.1}%).",
				ByteSize::b(available_cap_z).to_string_as(false),
				ByteSize::b(total_cap_z).to_string_as(false),
				percent_cap_z
			));

			for n in nodes_of_z.iter() {
				let available_cap_n = stored_partitions[*n] as u64 * self.partition_size;
				let total_cap_n = self.get_node_capacity(&self.node_id_vec[*n])?;
				let tags_n = (self
					.node_role(&self.node_id_vec[*n])
					.ok_or("Node not found."))?
				.tags_string();
				msg.push(format!(
					"  Node {:?}: {} partitions ({} new) ; \
                                 usable/total capacity: {} / {} ({:.1}%) ; tags:{}",
					self.node_id_vec[*n],
					stored_partitions[*n],
					new_partitions[*n],
					ByteSize::b(available_cap_n).to_string_as(false),
					ByteSize::b(total_cap_n).to_string_as(false),
					(available_cap_n as f32) / (total_cap_n as f32) * 100.0,
					tags_n
				));
			}
		}

		Ok(msg)
	}
}

// ====================================================================================

#[cfg(test)]
mod tests {
	use super::{Error, *};
	use std::cmp::min;

	// This function checks that the partition size S computed is at least better than the
	// one given by a very naive algorithm. To do so, we try to run the naive algorithm
	// assuming a partion size of S+1. If we succed, it means that the optimal assignment
	// was not optimal. The naive algorithm is the following :
	// - we compute the max number of partitions associated to every node, capped at the
	// partition number. It gives the number of tokens of every node.
	// - every zone has a number of tokens equal to the sum of the tokens of its nodes.
	// - we cycle over the partitions and associate zone tokens while respecting the
	// zone redundancy constraint.
	// NOTE: the naive algorithm is not optimal. Counter example:
	// take nb_partition = 3  ; replication_factor = 5; redundancy = 4;
	// number of tokens by zone : (A, 4), (B,1), (C,4), (D, 4), (E, 2)
	// With these parameters, the naive algo fails, whereas there is a solution:
	// (A,A,C,D,E) , (A,B,C,D,D) (A,C,C,D,E)
	fn check_against_naive(cl: &ClusterLayout) -> Result<bool, Error> {
		let over_size = cl.partition_size + 1;
		let mut zone_token = HashMap::<String, usize>::new();

		let (zones, zone_to_id) = cl.generate_nongateway_zone_ids()?;

		if zones.is_empty() {
			return Ok(false);
		}

		for z in zones.iter() {
			zone_token.insert(z.clone(), 0);
		}
		for uuid in cl.nongateway_nodes().iter() {
			let z = cl.get_node_zone(uuid)?;
			let c = cl.get_node_capacity(uuid)?;
			zone_token.insert(
				z.clone(),
				zone_token[&z] + min(NB_PARTITIONS, (c / over_size) as usize),
			);
		}

		// For every partition, we count the number of zone already associated and
		// the name of the last zone associated

		let mut id_zone_token = vec![0; zones.len()];
		for (z, t) in zone_token.iter() {
			id_zone_token[zone_to_id[z]] = *t;
		}

		let mut nb_token = vec![0; NB_PARTITIONS];
		let mut last_zone = vec![zones.len(); NB_PARTITIONS];

		let mut curr_zone = 0;

		let redundancy = cl.parameters.zone_redundancy;

		for replic in 0..cl.replication_factor {
			for p in 0..NB_PARTITIONS {
				while id_zone_token[curr_zone] == 0
					|| (last_zone[p] == curr_zone
						&& redundancy - nb_token[p] <= cl.replication_factor - replic)
				{
					curr_zone += 1;
					if curr_zone >= zones.len() {
						return Ok(true);
					}
				}
				id_zone_token[curr_zone] -= 1;
				if last_zone[p] != curr_zone {
					nb_token[p] += 1;
					last_zone[p] = curr_zone;
				}
			}
		}

		return Ok(false);
	}

	fn show_msg(msg: &Message) {
		for s in msg.iter() {
			println!("{}", s);
		}
	}

	fn update_layout(
		cl: &mut ClusterLayout,
		node_id_vec: &Vec<u8>,
		node_capacity_vec: &Vec<u64>,
		node_zone_vec: &Vec<String>,
		zone_redundancy: usize,
	) {
		for i in 0..node_id_vec.len() {
			if let Some(x) = FixedBytes32::try_from(&[i as u8; 32]) {
				cl.node_id_vec.push(x);
			}

			let update = cl.staging_roles.update_mutator(
				cl.node_id_vec[i],
				NodeRoleV(Some(NodeRole {
					zone: (node_zone_vec[i].to_string()),
					capacity: (Some(node_capacity_vec[i])),
					tags: (vec![]),
				})),
			);
			cl.staging_roles.merge(&update);
		}
		cl.staging_parameters
			.update(LayoutParameters { zone_redundancy });
		cl.staging_hash = cl.calculate_staging_hash();
	}

	#[test]
	fn test_assignment() {
		let mut node_id_vec = vec![1, 2, 3];
		let mut node_capacity_vec = vec![4000, 1000, 2000];
		let mut node_zone_vec = vec!["A", "B", "C"]
			.into_iter()
			.map(|x| x.to_string())
			.collect();

		let mut cl = ClusterLayout::new(3);
		update_layout(&mut cl, &node_id_vec, &node_capacity_vec, &node_zone_vec, 3);
		let v = cl.version;
		let (mut cl, msg) = cl.apply_staged_changes(Some(v + 1)).unwrap();
		show_msg(&msg);
		assert_eq!(cl.check(), Ok(()));
		assert!(matches!(check_against_naive(&cl), Ok(true)));

		node_id_vec = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
		node_capacity_vec = vec![4000, 1000, 1000, 3000, 1000, 1000, 2000, 10000, 2000];
		node_zone_vec = vec!["A", "B", "C", "C", "C", "B", "G", "H", "I"]
			.into_iter()
			.map(|x| x.to_string())
			.collect();
		update_layout(&mut cl, &node_id_vec, &node_capacity_vec, &node_zone_vec, 2);
		let v = cl.version;
		let (mut cl, msg) = cl.apply_staged_changes(Some(v + 1)).unwrap();
		show_msg(&msg);
		assert_eq!(cl.check(), Ok(()));
		assert!(matches!(check_against_naive(&cl), Ok(true)));

		node_capacity_vec = vec![4000, 1000, 2000, 7000, 1000, 1000, 2000, 10000, 2000];
		update_layout(&mut cl, &node_id_vec, &node_capacity_vec, &node_zone_vec, 3);
		let v = cl.version;
		let (mut cl, msg) = cl.apply_staged_changes(Some(v + 1)).unwrap();
		show_msg(&msg);
		assert_eq!(cl.check(), Ok(()));
		assert!(matches!(check_against_naive(&cl), Ok(true)));

		node_capacity_vec = vec![
			4000000, 4000000, 2000000, 7000000, 1000000, 9000000, 2000000, 10000, 2000000,
		];
		update_layout(&mut cl, &node_id_vec, &node_capacity_vec, &node_zone_vec, 1);
		let v = cl.version;
		let (cl, msg) = cl.apply_staged_changes(Some(v + 1)).unwrap();
		show_msg(&msg);
		assert_eq!(cl.check(), Ok(()));
		assert!(matches!(check_against_naive(&cl), Ok(true)));
	}
}
