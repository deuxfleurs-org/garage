use std::cmp::Ordering;
use std::collections::HashMap;
use std::collections::HashSet;

use hex::ToHex;
use itertools::Itertools;

use serde::{Deserialize, Serialize};

use garage_util::crdt::{AutoCrdt, Crdt, LwwMap, Lww};
use garage_util::data::*;
use garage_util::error::*;

use crate::graph_algo::*;

use crate::ring::*;

use std::convert::TryInto;

//The Message type will be used to collect information on the algorithm.
type Message = Vec<String>;

/// The layout of the cluster, i.e. the list of roles
/// which are assigned to each cluster node
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClusterLayout {
	pub version: u64,

	pub replication_factor: usize,
  
    //This attribute is only used to retain the previously computed partition size, 
    //to know to what extent does it change with the layout update.
    pub partition_size: u32,
    pub parameters: LayoutParameters,

	pub roles: LwwMap<Uuid, NodeRoleV>,

	/// node_id_vec: a vector of node IDs with a role assigned
	/// in the system (this includes gateway nodes).
	/// The order here is different than the vec stored by `roles`, because:
	/// 1. non-gateway nodes are first so that they have lower numbers holding
    ///     in u8 (the number of non-gateway nodes is at most 256).
	/// 2. nodes that don't have a role are excluded (but they need to
	///    stay in the CRDT as tombstones)
	pub node_id_vec: Vec<Uuid>,
	/// the assignation of data partitions to node, the values
	/// are indices in node_id_vec
	#[serde(with = "serde_bytes")]
	pub ring_assignation_data: Vec<CompactNodeType>,

	/// Role changes which are staged for the next version of the layout
    pub staged_parameters: Lww<LayoutParameters>,
	pub staging: LwwMap<Uuid, NodeRoleV>,
	pub staging_hash: Hash,
}

///This struct is used to set the parameters to be used in the assignation computation
///algorithm. It is stored as a Crdt.
#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Debug, Serialize, Deserialize)]
pub struct LayoutParameters {
    pub zone_redundancy:usize,
}

impl AutoCrdt for LayoutParameters {
	const WARN_IF_DIFFERENT: bool = true;
}

const NB_PARTITIONS : usize = 1usize << PARTITION_BITS;

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

    pub fn tags_string(&self) -> String {
        let mut tags = String::new();
        if self.tags.is_empty() {
            return tags
        }
        tags.push_str(&self.tags[0].clone());
        for t in 1..self.tags.len(){
            tags.push(',');
            tags.push_str(&self.tags[t].clone());
        }
        tags
    }
}

impl ClusterLayout {
	pub fn new(replication_factor: usize) -> Self {
        
        //We set the default zone redundancy to be equal to the replication factor,
        //i.e. as strict as possible.
        let parameters = LayoutParameters{ zone_redundancy: replication_factor};
        let staged_parameters = Lww::<LayoutParameters>::new(parameters.clone());

		let empty_lwwmap = LwwMap::new();
		let empty_lwwmap_hash = blake2sum(&rmp_to_vec_all_named(&empty_lwwmap).unwrap()[..]);

		ClusterLayout {
			version: 0,
			replication_factor,
            partition_size: 0,
			roles: LwwMap::new(),
			node_id_vec: Vec::new(),
			ring_assignation_data: Vec::new(),
            parameters,
            staged_parameters,
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
                let param_changed = self.staged_parameters.get() != other.staged_parameters.get();
                self.staged_parameters.merge(&other.staged_parameters);
				self.staging.merge(&other.staging);


				let new_staging_hash = blake2sum(&rmp_to_vec_all_named(&self.staging).unwrap()[..]);
				let stage_changed = new_staging_hash != self.staging_hash;

				self.staging_hash = new_staging_hash;

				stage_changed || param_changed
			}
			Ordering::Less => false,
		}
	}

	pub fn apply_staged_changes(mut self, version: Option<u64>) -> Result<(Self,Message), Error> {
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

		self.roles.merge(&self.staging);
		self.roles.retain(|(_, _, v)| v.0.is_some());

        let msg = self.calculate_partition_assignation()?;

		self.staging.clear();
		self.staging_hash = blake2sum(&rmp_to_vec_all_named(&self.staging).unwrap()[..]);

		self.version += 1;

		Ok((self,msg))
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

		self.staging.clear();
		self.staging_hash = blake2sum(&rmp_to_vec_all_named(&self.staging).unwrap()[..]);

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

    ///Returns the uuids of the non_gateway nodes in self.node_id_vec.
    pub fn useful_nodes(&self) -> Vec<Uuid> {
        let mut result = Vec::<Uuid>::new();
        for uuid in self.node_id_vec.iter() {
            match self.node_role(uuid) {
                Some(role) if role.capacity != None => result.push(*uuid),
                _ => ()
            }
        }
        result
    }

    ///Given a node uuids, this function returns the label of its zone
    pub fn get_node_zone(&self, uuid : &Uuid) -> Result<String,Error> {
        match self.node_role(uuid) {
            Some(role) => Ok(role.zone.clone()),
            _ => Err(Error::Message("The Uuid does not correspond to a node present in the cluster.".into()))
        }
    }
    
    ///Given a node uuids, this function returns its capacity or fails if it does not have any
    pub fn get_node_capacity(&self, uuid : &Uuid) -> Result<u32,Error> {
        match self.node_role(uuid) {
            Some(NodeRole{capacity : Some(cap), zone: _, tags: _}) => Ok(*cap),
            _ => Err(Error::Message("The Uuid does not correspond to a node present in the \
                    cluster or this node does not have a positive capacity.".into()))
        }
    }

    ///Returns the sum of capacities of non gateway nodes in the cluster
    pub fn get_total_capacity(&self) -> Result<u32,Error> {
        let mut total_capacity = 0;
        for uuid in self.useful_nodes().iter() {
            total_capacity += self.get_node_capacity(uuid)?;
        }
        Ok(total_capacity)
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

        //Check that every partition is associated to distinct nodes
        let rf = self.replication_factor;
        for p in 0..(1 << PARTITION_BITS) {
            let nodes_of_p = self.ring_assignation_data[rf*p..rf*(p+1)].to_vec();
            if nodes_of_p.iter().unique().count() != rf {
                return false;
            }
            //Check that every partition is spread over at least zone_redundancy zones.
            let zones_of_p = nodes_of_p.iter()
                    .map(|n| self.get_node_zone(&self.node_id_vec[*n as usize])
                         .expect("Zone not found."));
            let redundancy = self.parameters.zone_redundancy;
            if zones_of_p.unique().count() < redundancy {
                return false;
            }
        }

        //Check that the nodes capacities is consistent with the stored partitions
        let mut node_usage = vec![0; MAX_NODE_NUMBER];
        for n in self.ring_assignation_data.iter() {
            node_usage[*n as usize] += 1;
        }
        for (n, usage) in node_usage.iter().enumerate(){
            if *usage > 0 {
                let uuid = self.node_id_vec[n];
                if usage*self.partition_size > self.get_node_capacity(&uuid)
                                .expect("Critical Error"){
                    return false;
                }
            }
        }

        //Check that the partition size stored is the one computed by the asignation
        //algorithm.
        let cl2 = self.clone();
        let (_ , zone_to_id) = cl2.generate_useful_zone_ids().expect("Critical Error");
        let partition_size = cl2.compute_optimal_partition_size(&zone_to_id).expect("Critical Error");
        if partition_size != self.partition_size {
            return false;
        }


		true
	}

}

impl ClusterLayout {
	/// This function calculates a new partition-to-node assignation.
	/// The computed assignation respects the node replication factor
    /// and the zone redundancy parameter It maximizes the capacity of a
	/// partition (assuming all partitions have the same size).
	/// Among such optimal assignation, it minimizes the distance to
	/// the former assignation (if any) to minimize the amount of
	/// data to be moved.
    /// Staged changes must be merged with nodes roles before calling this function.
	pub fn calculate_partition_assignation(&mut self) -> Result<Message,Error> {
		//The nodes might have been updated, some might have been deleted.
		//So we need to first update the list of nodes and retrieve the
		//assignation.
       
        //We update the node ids, since the node role list might have changed with the 
        //changes in the layout. We retrieve the old_assignation reframed with the new ids
        let old_assignation_opt = self.update_node_id_vec()?;
        
        let redundancy = self.staged_parameters.get().zone_redundancy;


        let mut msg = Message::new();
        msg.push(format!("Computation of a new cluster layout where partitions are \
        replicated {} times on at least {} distinct zones.", self.replication_factor, redundancy));

        //We generate for once numerical ids for the zones of non gateway nodes, 
        //to use them as indices in the flow graphs.
        let (id_to_zone , zone_to_id) = self.generate_useful_zone_ids()?;

        let nb_useful_nodes = self.useful_nodes().len();
        msg.push(format!("The cluster contains {} nodes spread over {} zones.", 
                         nb_useful_nodes, id_to_zone.len()));
        if nb_useful_nodes < self.replication_factor{
            return Err(Error::Message(format!("The number of nodes with positive \
            capacity ({}) is smaller than the replication factor ({}).",
            nb_useful_nodes, self.replication_factor)));
        }
        if id_to_zone.len() < redundancy {
            return Err(Error::Message(format!("The number of zones with non-gateway \
            nodes ({}) is smaller than the redundancy parameter ({})",
            id_to_zone.len() , redundancy)));
        }
       
        //We compute the optimal partition size
        //Capacities should be given in a unit so that partition size is at least 100.
        //In this case, integer rounding plays a marginal role in the percentages of 
        //optimality.
        let partition_size = self.compute_optimal_partition_size(&zone_to_id)?;

        if old_assignation_opt != None  {
            msg.push(format!("Given the replication and redundancy constraint, the \
                optimal size of a partition is {}. In the previous layout, it used to \
                be {} (the zone redundancy was {}).", partition_size, self.partition_size,
                self.parameters.zone_redundancy));
        }
        else {
            msg.push(format!("Given the replication and redundancy constraints, the \
                optimal size of a partition is {}.", partition_size));
        }
        self.partition_size = partition_size;
        self.parameters = self.staged_parameters.get().clone();

        if partition_size < 100 {
            msg.push("WARNING: The partition size is low (< 100), you might consider to \
            provide the nodes capacities in a smaller unit (e.g. Mb instead of Gb).".into());
        }

        //We compute a first flow/assignment that is heuristically close to the previous
        //assignment
        let mut gflow = self.compute_candidate_assignment( &zone_to_id, &old_assignation_opt)?;
        if let Some(assoc) = &old_assignation_opt {
            //We minimize the distance to the previous assignment.
            self.minimize_rebalance_load(&mut gflow, &zone_to_id, assoc)?;
        }

        msg.append(&mut self.output_stat(&gflow, &old_assignation_opt, &zone_to_id,&id_to_zone)?);
        msg.push("".to_string());

        //We update the layout structure
        self.update_ring_from_flow(id_to_zone.len() , &gflow)?;
        Ok(msg)
    }

	/// The LwwMap of node roles might have changed. This function updates the node_id_vec
	/// and returns the assignation given by ring, with the new indices of the nodes, and
	/// None if the node is not present anymore.
	/// We work with the assumption that only this function and calculate_new_assignation
	/// do modify assignation_ring and node_id_vec.
    fn update_node_id_vec(&mut self) -> Result< Option< Vec<Vec<usize> > > ,Error> {
        // (1) We compute the new node list
        //Non gateway nodes should be coded on 8bits, hence they must be first in the list
	    //We build the new node ids	 
		let mut new_non_gateway_nodes: Vec<Uuid> = self.roles.items().iter()
            .filter(|(_, _, v)| matches!(&v.0, Some(r) if r.capacity != None))
            .map(|(k, _, _)| *k).collect();
        
        if new_non_gateway_nodes.len() > MAX_NODE_NUMBER {
            return Err(Error::Message(format!("There are more than {} non-gateway nodes in the new \
                            layout. This is not allowed.", MAX_NODE_NUMBER) ));
        }

		let mut new_gateway_nodes: Vec<Uuid> = self.roles.items().iter()
            .filter(|(_, _, v)| matches!(v, NodeRoleV(Some(r)) if r.capacity == None)) 
            .map(|(k, _, _)| *k).collect();
        
        let mut new_node_id_vec = Vec::<Uuid>::new();
        new_node_id_vec.append(&mut new_non_gateway_nodes);
        new_node_id_vec.append(&mut new_gateway_nodes);
        
        let old_node_id_vec = self.node_id_vec.clone();
        self.node_id_vec = new_node_id_vec.clone();
        
        // (2) We retrieve the old association
        //We rewrite the old association with the new indices. We only consider partition
        //to node assignations where the node is still in use.
        let nb_partitions = 1usize << PARTITION_BITS;
        let mut old_assignation = vec![ Vec::<usize>::new() ; nb_partitions];
        
        if self.ring_assignation_data.is_empty() {
            //This is a new association
            return Ok(None);
        }
        if self.ring_assignation_data.len() != nb_partitions * self.replication_factor {
            return Err(Error::Message("The old assignation does not have a size corresponding to \
                the old replication factor or the number of partitions.".into()));
        }

        //We build a translation table between the uuid and new ids
        let mut uuid_to_new_id = HashMap::<Uuid, usize>::new();
        
        //We add the indices of only the new non-gateway nodes that can be used in the
        //association ring
        for (i, uuid) in new_node_id_vec.iter().enumerate() {
            uuid_to_new_id.insert(*uuid, i );
        }

        let rf= self.replication_factor;
        for (p, old_assign_p) in old_assignation.iter_mut().enumerate() {
            for old_id in &self.ring_assignation_data[p*rf..(p+1)*rf] {
                let uuid = old_node_id_vec[*old_id as usize];
                if uuid_to_new_id.contains_key(&uuid) {
                    old_assign_p.push(uuid_to_new_id[&uuid]);
                }
            }
        }

        //We write the ring
        self.ring_assignation_data = Vec::<CompactNodeType>::new();
        
        if !self.check() {
            return Err(Error::Message("Critical error: The computed layout happens to be incorrect".into()));
        }

        Ok(Some(old_assignation))
	}


    ///This function generates ids for the zone of the nodes appearing in 
    ///self.node_id_vec.
    fn generate_useful_zone_ids(&self) -> Result<(Vec<String>, HashMap<String, usize>),Error>{
        let mut id_to_zone = Vec::<String>::new();
        let mut zone_to_id = HashMap::<String,usize>::new();
        
        for uuid in self.useful_nodes().iter() {
            if self.roles.get(uuid) == None {
                return Err(Error::Message("The uuid was not found in the node roles (this should \
                    not happen, it might be a critical error).".into()));
            }
            if let Some(r) = self.node_role(uuid) {
                if !zone_to_id.contains_key(&r.zone) && r.capacity != None {
                        zone_to_id.insert(r.zone.clone() , id_to_zone.len());
                        id_to_zone.push(r.zone.clone());
                }
            }
        }
        Ok((id_to_zone, zone_to_id))
    }

    ///This function computes by dichotomy the largest realizable partition size, given
    ///the layout.
    fn compute_optimal_partition_size(&self, zone_to_id: &HashMap<String, usize>) -> Result<u32,Error>{
        let nb_partitions = 1usize << PARTITION_BITS;
        let empty_set = HashSet::<(usize,usize)>::new();
        let mut g = self.generate_flow_graph(1, zone_to_id, &empty_set)?;
        g.compute_maximal_flow()?;
        if g.get_flow_value()? < (nb_partitions*self.replication_factor).try_into().unwrap() {
            return Err(Error::Message("The storage capacity of he cluster is to small. It is \
                       impossible to store partitions of size 1.".into()));
        }

        let mut s_down = 1;
        let mut s_up = self.get_total_capacity()?;
        while s_down +1 < s_up {
            g = self.generate_flow_graph((s_down+s_up)/2, zone_to_id, &empty_set)?;
            g.compute_maximal_flow()?;
            if g.get_flow_value()? < (nb_partitions*self.replication_factor).try_into().unwrap() {
                s_up = (s_down+s_up)/2;
            }
            else {
                s_down = (s_down+s_up)/2;
            }
        }

        Ok(s_down)
    }
    
    fn generate_graph_vertices(nb_zones : usize, nb_nodes : usize) -> Vec<Vertex> {
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

    fn generate_flow_graph(&self, size: u32, zone_to_id: &HashMap<String, usize>, exclude_assoc : &HashSet<(usize,usize)>) -> Result<Graph<FlowEdge>, Error> {
        let vertices = ClusterLayout::generate_graph_vertices(zone_to_id.len(), 
                                                        self.useful_nodes().len());
        let mut g= Graph::<FlowEdge>::new(&vertices);
        let nb_zones = zone_to_id.len();
        let redundancy = self.staged_parameters.get().zone_redundancy;
        for p in 0..NB_PARTITIONS {
            g.add_edge(Vertex::Source, Vertex::Pup(p), redundancy as u32)?;
            g.add_edge(Vertex::Source, Vertex::Pdown(p), (self.replication_factor - redundancy) as u32)?;
            for z in 0..nb_zones {
                g.add_edge(Vertex::Pup(p) , Vertex::PZ(p,z) , 1)?;
                g.add_edge(Vertex::Pdown(p) , Vertex::PZ(p,z) , 
                            self.replication_factor as u32)?;
            }
        }
        for n in 0..self.useful_nodes().len() {
            let node_capacity = self.get_node_capacity(&self.node_id_vec[n])?;
            let node_zone = zone_to_id[&self.get_node_zone(&self.node_id_vec[n])?]; 
            g.add_edge(Vertex::N(n), Vertex::Sink, node_capacity/size)?;
            for p in 0..NB_PARTITIONS {
                if !exclude_assoc.contains(&(p,n))  {
                    g.add_edge(Vertex::PZ(p, node_zone), Vertex::N(n), 1)?;
                }
            }
        }
        Ok(g)
    }


    fn compute_candidate_assignment(&self, zone_to_id: &HashMap<String, usize>, 
        old_assoc_opt : &Option<Vec< Vec<usize> >>) -> Result<Graph<FlowEdge>, Error > {
        
        //We list the edges that are not used in the old association
        let mut exclude_edge = HashSet::<(usize,usize)>::new();
        if let Some(old_assoc) = old_assoc_opt {
            let nb_nodes = self.useful_nodes().len();
            for (p, old_assoc_p) in old_assoc.iter().enumerate() {
                for n in 0..nb_nodes {
                    exclude_edge.insert((p,n));
                }
                for n in old_assoc_p.iter() {
                    exclude_edge.remove(&(p,*n));
                }
            }
        }

        //We compute the best flow using only the edges used in the old assoc
        let mut g = self.generate_flow_graph(self.partition_size, zone_to_id, &exclude_edge )?;
        g.compute_maximal_flow()?;
        for (p,n) in exclude_edge.iter() {
            let node_zone = zone_to_id[&self.get_node_zone(&self.node_id_vec[*n])?]; 
            g.add_edge(Vertex::PZ(*p,node_zone), Vertex::N(*n), 1)?;
        }
        g.compute_maximal_flow()?;
        Ok(g)
    }

    fn minimize_rebalance_load(&self, gflow: &mut Graph<FlowEdge>, zone_to_id: &HashMap<String, usize>, old_assoc : &[Vec<usize> ]) -> Result<(), Error > {
        let mut cost = CostFunction::new();
        for (p, assoc_p) in old_assoc.iter().enumerate(){
            for n in assoc_p.iter() {
                let node_zone = zone_to_id[&self.get_node_zone(&self.node_id_vec[*n])?]; 
                cost.insert((Vertex::PZ(p,node_zone), Vertex::N(*n)), -1);
            }
        }
        let nb_nodes = self.useful_nodes().len();
        let path_length = 4*nb_nodes;
        gflow.optimize_flow_with_cost(&cost, path_length)?;

        Ok(())
    }

    fn update_ring_from_flow(&mut self, nb_zones : usize, gflow: &Graph<FlowEdge> ) -> Result<(), Error>{
        self.ring_assignation_data = Vec::<CompactNodeType>::new();
        for p in 0..NB_PARTITIONS {
            for z in 0..nb_zones {
                let assoc_vertex = gflow.get_positive_flow_from(Vertex::PZ(p,z))?;
                for vertex in assoc_vertex.iter() {
                    if let Vertex::N(n) = vertex {
                        self.ring_assignation_data.push((*n).try_into().unwrap());
                    }
                }
            }
        }

        if self.ring_assignation_data.len() != NB_PARTITIONS*self.replication_factor {
            return Err(Error::Message("Critical Error : the association ring we produced does not \
                       have the right size.".into()));
        }
        Ok(())
    }
     

    //This function returns a message summing up the partition repartition of the new
    //layout.
    fn output_stat(&self , gflow : &Graph<FlowEdge>, 
                    old_assoc_opt : &Option< Vec<Vec<usize>> >,
                    zone_to_id: &HashMap<String, usize>, 
                    id_to_zone : &[String]) -> Result<Message, Error>{
        let mut msg = Message::new();
        
		let nb_partitions = 1usize << PARTITION_BITS;
        let used_cap = self.partition_size * nb_partitions as u32 * 
                self.replication_factor as u32;
        let total_cap = self.get_total_capacity()?;
        let percent_cap = 100.0*(used_cap as f32)/(total_cap as f32);
        msg.push(format!("Available capacity / Total cluster capacity: {} / {} ({:.1} %)",
            used_cap , total_cap , percent_cap ));
        msg.push("".into());
        msg.push("If the percentage is to low, it might be that the \
        replication/redundancy constraints force the use of nodes/zones with small \
        storage capacities. \
        You might want to rebalance the storage capacities or relax the constraints. \
        See the detailed statistics below and look for saturated nodes/zones.".into());
        msg.push(format!("Recall that because of the replication factor, the actual available \
                         storage capacity is {} / {} = {}.", 
                        used_cap , self.replication_factor , 
                        used_cap/self.replication_factor as u32));
       
        //We define and fill in the following tables
        let storing_nodes = self.useful_nodes();
        let mut new_partitions = vec![0; storing_nodes.len()];
        let mut stored_partitions = vec![0; storing_nodes.len()];

        let mut new_partitions_zone = vec![0; id_to_zone.len()];
        let mut stored_partitions_zone = vec![0; id_to_zone.len()];

        for p in 0..nb_partitions {
            for z in 0..id_to_zone.len() {
                let pz_nodes = gflow.get_positive_flow_from(Vertex::PZ(p,z))?;
                if !pz_nodes.is_empty() {
                    stored_partitions_zone[z] += 1;
                    if let Some(old_assoc) = old_assoc_opt {
                        let mut old_zones_of_p = Vec::<usize>::new();
                        for n in old_assoc[p].iter() {
                            old_zones_of_p.push(
                                zone_to_id[&self.get_node_zone(&self.node_id_vec[*n])?]);
                        }
                        if !old_zones_of_p.contains(&z) {
                            new_partitions_zone[z] += 1;
                        }
                    }
                }
                for vert in pz_nodes.iter() {
                    if let Vertex::N(n) = *vert {
                        stored_partitions[n] += 1;
                        if let Some(old_assoc) = old_assoc_opt {
                            if !old_assoc[p].contains(&n) {
                                new_partitions[n] += 1;
                            }
                        }
                    }
                }
            }
        }

        if *old_assoc_opt == None {
            new_partitions = stored_partitions.clone();
            new_partitions_zone = stored_partitions_zone.clone();
        }
        
        //We display the statistics

        msg.push("".into());
        if *old_assoc_opt != None {
            let total_new_partitions : usize = new_partitions.iter().sum();
            msg.push(format!("A total of {} new copies of partitions need to be \
                             transferred.", total_new_partitions));
        }
        msg.push("".into());
        msg.push("==== DETAILED STATISTICS BY ZONES AND NODES ====".into());
        
        for z in 0..id_to_zone.len(){
            let mut nodes_of_z = Vec::<usize>::new();
            for n in 0..storing_nodes.len(){
                if self.get_node_zone(&self.node_id_vec[n])? == id_to_zone[z] {
                    nodes_of_z.push(n);
                }
            }
            let replicated_partitions : usize = nodes_of_z.iter()
                    .map(|n| stored_partitions[*n]).sum();
            msg.push("".into());
            
            msg.push(format!("Zone {}: {} distinct partitions stored ({} new, \
                {} partition copies) ", id_to_zone[z], stored_partitions_zone[z], 
                                 new_partitions_zone[z], replicated_partitions));
            
            let available_cap_z : u32 = self.partition_size*replicated_partitions as u32;
            let mut total_cap_z = 0;
            for n in nodes_of_z.iter() {
                total_cap_z += self.get_node_capacity(&self.node_id_vec[*n])?;
            }
            let percent_cap_z = 100.0*(available_cap_z as f32)/(total_cap_z as f32);
            msg.push(format!("  Available capacity / Total capacity: {}/{} ({:.1}%).",
                available_cap_z, total_cap_z, percent_cap_z));
            
            for n in nodes_of_z.iter() {
                let available_cap_n = stored_partitions[*n] as u32 *self.partition_size;
                let total_cap_n =self.get_node_capacity(&self.node_id_vec[*n])?;
                let tags_n = (self.node_role(&self.node_id_vec[*n])
                                .ok_or("Node not found."))?.tags_string();
                msg.push(format!("  Node {}: {} partitions ({} new) ; \
                                 available/total capacity: {} / {} ({:.1}%) ; tags:{}", 
                        &self.node_id_vec[*n].to_vec()[0..2].to_vec().encode_hex::<String>(), 
                        stored_partitions[*n], 
                        new_partitions[*n], available_cap_n, total_cap_n,
                        (available_cap_n as f32)/(total_cap_n as f32)*100.0 ,
                        tags_n));
            }
        }

        Ok(msg)
    }
    
}

//====================================================================================

#[cfg(test)]
mod tests {
	use super::{*,Error};
    use std::cmp::min;


    //This function checks that the partition size S computed is at least better than the
    //one given by a very naive algorithm. To do so, we try to run the naive algorithm
    //assuming a partion size of S+1. If we succed, it means that the optimal assignation
    //was not optimal. The naive algorithm is the following : 
    //- we compute the max number of partitions associated to every node, capped at the
    //partition number. It gives the number of tokens of every node.
    //- every zone has a number of tokens equal to the sum of the tokens of its nodes.
    //- we cycle over the partitions and associate zone tokens while respecting the 
    //zone redundancy constraint.
    //NOTE: the naive algorithm is not optimal. Counter example: 
    //take nb_partition = 3  ; replication_factor = 5; redundancy = 4; 
    //number of tokens by zone : (A, 4), (B,1), (C,4), (D, 4), (E, 2)
    //With these parameters, the naive algo fails, whereas there is a solution:
    //(A,A,C,D,E) , (A,B,C,D,D) (A,C,C,D,E)
    fn check_against_naive(cl: &ClusterLayout) -> Result<bool,Error> {
        let over_size = cl.partition_size +1;
        let mut zone_token = HashMap::<String, usize>::new();
        let nb_partitions = 1usize << PARTITION_BITS;
        
        let (zones, zone_to_id) = cl.generate_useful_zone_ids()?;
        
        if zones.is_empty() {
            return Ok(false);
        }

        for z in zones.iter() {
            zone_token.insert(z.clone(), 0);
        }
        for uuid in cl.useful_nodes().iter() {
            let z = cl.get_node_zone(uuid)?;
            let c = cl.get_node_capacity(uuid)?;
            zone_token.insert(z.clone(), zone_token[&z] + min(nb_partitions , (c/over_size) as usize));
        }
        
        //For every partition, we count the number of zone already associated and
        //the name of the last zone associated

        let mut id_zone_token = vec![0; zones.len()];
        for (z,t) in zone_token.iter() {
            id_zone_token[zone_to_id[z]] = *t;
        }

        let mut nb_token = vec![0; nb_partitions];
        let mut last_zone = vec![zones.len(); nb_partitions];
        
        let mut curr_zone = 0;

        let redundancy = cl.parameters.zone_redundancy;

        for replic in 0..cl.replication_factor {
            for p in 0..nb_partitions {
                while id_zone_token[curr_zone] == 0 || 
                        (last_zone[p] == curr_zone 
                         && redundancy - nb_token[p] <= cl.replication_factor - replic) {
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

    fn show_msg(msg  : &Message) {
        for s in msg.iter(){
            println!("{}",s);
        }
    }

	fn update_layout(
		cl: &mut ClusterLayout,
		node_id_vec: &Vec<u8>,
		node_capacity_vec: &Vec<u32>,
		node_zone_vec: &Vec<String>,
        zone_redundancy: usize
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
        cl.staged_parameters = Lww::<LayoutParameters>::new(LayoutParameters{zone_redundancy});
	}

	#[test]
	fn test_assignation() {
        let mut node_id_vec = vec![1, 2, 3];
		let mut node_capacity_vec = vec![4000, 1000, 2000];
		let mut node_zone_vec = vec!["A", "B", "C"]
			.into_iter()
			.map(|x| x.to_string())
			.collect();

		let mut cl = ClusterLayout::new(3);
		update_layout(&mut cl, &node_id_vec, &node_capacity_vec, &node_zone_vec, 3);
		show_msg(&cl.calculate_partition_assignation().unwrap());
		assert!(cl.check());
		assert!(matches!(check_against_naive(&cl), Ok(true)));

		node_id_vec = vec![1, 2, 3, 4, 5, 6, 7, 8, 9];
		node_capacity_vec = vec![4000, 1000, 1000, 3000, 1000, 1000, 2000, 10000, 2000];
		node_zone_vec = vec!["A", "B", "C", "C", "C", "B", "G", "H", "I"]
			.into_iter()
			.map(|x| x.to_string())
			.collect();
		update_layout(&mut cl, &node_id_vec, &node_capacity_vec, &node_zone_vec, 2);
		show_msg(&cl.calculate_partition_assignation().unwrap());
		assert!(cl.check());
		assert!(matches!(check_against_naive(&cl), Ok(true)));

		node_capacity_vec = vec![4000, 1000, 2000, 7000, 1000, 1000, 2000, 10000, 2000];
		update_layout(&mut cl, &node_id_vec, &node_capacity_vec, &node_zone_vec, 3);
		show_msg(&cl.calculate_partition_assignation().unwrap());
		assert!(cl.check());
		assert!(matches!(check_against_naive(&cl), Ok(true)));

		node_capacity_vec = vec![4000000, 4000000, 2000000, 7000000, 1000000, 9000000, 2000000, 10000, 2000000];
		update_layout(&mut cl, &node_id_vec, &node_capacity_vec, &node_zone_vec, 1);
		show_msg(&cl.calculate_partition_assignation().unwrap());
		assert!(cl.check());
		assert!(matches!(check_against_naive(&cl), Ok(true)));

	}
}
