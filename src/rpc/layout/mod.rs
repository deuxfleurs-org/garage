use bytesize::ByteSize;

use garage_util::crdt::{AutoCrdt, Crdt};
use garage_util::data::Uuid;

mod graph_algo;
mod helper;
mod history;
mod version;

#[cfg(test)]
mod test;

pub mod manager;

// ---- re-exports ----

pub use helper::{LayoutHelper, RpcLayoutDigest, SyncLayoutDigest};
pub use manager::WriteLock;
pub use version::*;

// ---- defines: partitions ----

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

const NB_PARTITIONS: usize = 1usize << PARTITION_BITS;

// ---- defines: nodes ----

// Type to store compactly the id of a node in the system
// Change this to u16 the day we want to have more than 256 nodes in a cluster
pub type CompactNodeType = u8;
pub const MAX_NODE_NUMBER: usize = 256;

// ======== actual data structures for the layout data ========
// ======== that is persisted to disk                  ========
// some small utility impls are at the end of this file,
// but most of the code that actually computes stuff is in
// version.rs, history.rs and helper.rs

mod v08 {
	use crate::layout::CompactNodeType;
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

		// see comments in v010::ClusterLayout
		pub node_id_vec: Vec<Uuid>,
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
	use crate::layout::CompactNodeType;
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

		// see comments in v010::ClusterLayout
		pub node_id_vec: Vec<Uuid>,
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
		pub zone_redundancy: ZoneRedundancy,
	}

	/// Zone redundancy: if set to AtLeast(x), the layout calculation will aim to store copies
	/// of each partition on at least that number of different zones.
	/// Otherwise, copies will be stored on the maximum possible number of zones.
	#[derive(PartialEq, Eq, PartialOrd, Ord, Clone, Copy, Debug, Serialize, Deserialize)]
	pub enum ZoneRedundancy {
		AtLeast(usize),
		Maximum,
	}

	impl garage_util::migrate::Migrate for ClusterLayout {
		const VERSION_MARKER: &'static [u8] = b"G09layout";

		type Previous = v08::ClusterLayout;

		fn migrate(previous: Self::Previous) -> Self {
			use itertools::Itertools;

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

			// By default, zone_redundancy is maximum possible value
			let parameters = LayoutParameters {
				zone_redundancy: ZoneRedundancy::Maximum,
			};

			Self {
				version: previous.version,
				replication_factor: previous.replication_factor,
				partition_size,
				parameters,
				roles,
				node_id_vec,
				ring_assignment_data: previous.ring_assignation_data,
				staging_parameters: Lww::new(parameters),
				staging_roles,
				staging_hash: [0u8; 32].into(), // will be set in the next migration
			}
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
				*cap *= mul;
			}
			new_roles.merge_raw(node, *ts, &role);
		}
		new_roles
	}
}

mod v010 {
	use super::v09;
	use crate::layout::CompactNodeType;
	use garage_util::crdt::{Lww, LwwMap};
	use garage_util::data::Uuid;
	use serde::{Deserialize, Serialize};
	use std::collections::BTreeMap;
	pub use v09::{LayoutParameters, NodeRole, NodeRoleV, ZoneRedundancy};

	/// Number of old (non-live) versions to keep, see LayoutHistory::old_versions
	pub const OLD_VERSION_COUNT: usize = 5;

	/// The history of cluster layouts, with trackers to keep a record
	/// of which nodes are up-to-date to current cluster data
	#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
	pub struct LayoutHistory {
		/// The versions currently in use in the cluster
		pub versions: Vec<LayoutVersion>,
		/// At most 5 of the previous versions, not used by the garage_table
		/// module, but useful for the garage_block module to find data blocks
		/// that have not yet been moved
		pub old_versions: Vec<LayoutVersion>,

		/// Update trackers
		pub update_trackers: UpdateTrackers,

		/// Staged changes for the next version
		pub staging: Lww<LayoutStaging>,
	}

	/// A version of the layout of the cluster, i.e. the list of roles
	/// which are assigned to each cluster node
	#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
	pub struct LayoutVersion {
		/// The number of this version
		pub version: u64,

		/// Roles assigned to nodes in this version
		pub roles: LwwMap<Uuid, NodeRoleV>,
		/// Parameters used to compute the assignment currently given by
		/// ring_assignment_data
		pub parameters: LayoutParameters,

		/// The number of replicas for each data partition
		pub replication_factor: usize,
		/// This attribute is only used to retain the previously computed partition size,
		/// to know to what extent does it change with the layout update.
		pub partition_size: u64,

		/// node_id_vec: a vector of node IDs with a role assigned
		/// in the system (this includes gateway nodes).
		/// The order here is different than the vec stored by `roles`, because:
		/// 1. non-gateway nodes are first so that they have lower numbers
		/// 2. nodes that don't have a role are excluded (but they need to
		///    stay in the CRDT as tombstones)
		pub node_id_vec: Vec<Uuid>,
		/// number of non-gateway nodes, which are the first ids in node_id_vec
		pub nongateway_node_count: usize,
		/// The assignation of data partitions to nodes, the values
		/// are indices in node_id_vec
		#[serde(with = "serde_bytes")]
		pub ring_assignment_data: Vec<CompactNodeType>,
	}

	/// The staged changes for the next layout version
	#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
	pub struct LayoutStaging {
		/// Parameters to be used in the next partition assignment computation.
		pub parameters: Lww<LayoutParameters>,
		/// Role changes which are staged for the next version of the layout
		pub roles: LwwMap<Uuid, NodeRoleV>,
	}

	/// The tracker of acknowlegments and data syncs around the cluster
	#[derive(Clone, Debug, Serialize, Deserialize, Default, PartialEq)]
	pub struct UpdateTrackers {
		/// The highest layout version number each node has ack'ed
		pub ack_map: UpdateTracker,
		/// The highest layout version number each node has synced data for
		pub sync_map: UpdateTracker,
		/// The highest layout version number each node has
		/// ack'ed that all other nodes have synced data for
		pub sync_ack_map: UpdateTracker,
	}

	/// Generic update tracker struct
	#[derive(Clone, Debug, Serialize, Deserialize, Default, PartialEq)]
	pub struct UpdateTracker(pub BTreeMap<Uuid, u64>);

	impl garage_util::migrate::Migrate for LayoutHistory {
		const VERSION_MARKER: &'static [u8] = b"G010lh";

		type Previous = v09::ClusterLayout;

		fn migrate(previous: Self::Previous) -> Self {
			let nongateway_node_count = previous
				.node_id_vec
				.iter()
				.enumerate()
				.filter(|(_, uuid)| {
					let role = previous.roles.get(uuid);
					matches!(role, Some(NodeRoleV(Some(role))) if role.capacity.is_some())
				})
				.map(|(i, _)| i + 1)
				.max()
				.unwrap_or(0);

			let version = LayoutVersion {
				version: previous.version,
				replication_factor: previous.replication_factor,
				partition_size: previous.partition_size,
				parameters: previous.parameters,
				roles: previous.roles,
				node_id_vec: previous.node_id_vec,
				nongateway_node_count,
				ring_assignment_data: previous.ring_assignment_data,
			};
			let update_tracker = UpdateTracker(
				version
					.nongateway_nodes()
					.iter()
					.copied()
					.map(|x| (x, version.version))
					.collect::<BTreeMap<Uuid, u64>>(),
			);
			let staging = LayoutStaging {
				parameters: previous.staging_parameters,
				roles: previous.staging_roles,
			};
			Self {
				versions: vec![version],
				old_versions: vec![],
				update_trackers: UpdateTrackers {
					ack_map: update_tracker.clone(),
					sync_map: update_tracker.clone(),
					sync_ack_map: update_tracker,
				},
				staging: Lww::raw(previous.version, staging),
			}
		}
	}
}

pub use v010::*;

// ---- utility functions ----

impl AutoCrdt for LayoutParameters {
	const WARN_IF_DIFFERENT: bool = true;
}

impl AutoCrdt for NodeRoleV {
	const WARN_IF_DIFFERENT: bool = true;
}

impl Crdt for LayoutStaging {
	fn merge(&mut self, other: &LayoutStaging) {
		self.parameters.merge(&other.parameters);
		self.roles.merge(&other.roles);
	}
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

impl UpdateTracker {
	fn merge(&mut self, other: &UpdateTracker) -> bool {
		let mut changed = false;
		for (k, v) in other.0.iter() {
			if let Some(v_mut) = self.0.get_mut(k) {
				if *v > *v_mut {
					*v_mut = *v;
					changed = true;
				}
			} else {
				self.0.insert(*k, *v);
				changed = true;
			}
		}
		changed
	}

	/// This bumps the update tracker for a given node up to the specified value.
	/// This has potential impacts on the correctness of Garage and should only
	/// be used in very specific circumstances.
	pub fn set_max(&mut self, peer: Uuid, value: u64) -> bool {
		match self.0.get_mut(&peer) {
			Some(e) if *e < value => {
				*e = value;
				true
			}
			None => {
				self.0.insert(peer, value);
				true
			}
			_ => false,
		}
	}

	fn min_among(&self, storage_nodes: &[Uuid], min_version: u64) -> u64 {
		storage_nodes
			.iter()
			.map(|x| self.get(x, min_version))
			.min()
			.unwrap_or(min_version)
	}

	pub fn get(&self, node: &Uuid, min_version: u64) -> u64 {
		self.0.get(node).copied().unwrap_or(min_version)
	}
}

impl UpdateTrackers {
	pub(crate) fn merge(&mut self, other: &UpdateTrackers) -> bool {
		let c1 = self.ack_map.merge(&other.ack_map);
		let c2 = self.sync_map.merge(&other.sync_map);
		let c3 = self.sync_ack_map.merge(&other.sync_ack_map);
		c1 || c2 || c3
	}
}
