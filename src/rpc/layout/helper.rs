use std::collections::HashMap;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};

use garage_util::data::*;

use super::*;
use crate::replication_mode::*;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct RpcLayoutDigest {
	/// Cluster layout version
	pub current_version: u64,
	/// Number of active layout versions
	pub active_versions: usize,
	/// Hash of cluster layout update trackers
	pub trackers_hash: Hash,
	/// Hash of cluster layout staging data
	pub staging_hash: Hash,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct SyncLayoutDigest {
	current: u64,
	ack_map_min: u64,
	min_stored: u64,
}

pub struct LayoutHelper {
	replication_factor: ReplicationFactor,
	consistency_mode: ConsistencyMode,
	layout: Option<LayoutHistory>,

	// cached values
	ack_map_min: u64,
	sync_map_min: u64,

	all_nodes: Vec<Uuid>,
	all_nongateway_nodes: Vec<Uuid>,

	trackers_hash: Hash,
	staging_hash: Hash,

	// ack lock: counts in-progress write operations for each
	// layout version ; we don't increase the ack update tracker
	// while this lock is nonzero
	pub(crate) ack_lock: HashMap<u64, AtomicUsize>,
}

impl Deref for LayoutHelper {
	type Target = LayoutHistory;
	fn deref(&self) -> &LayoutHistory {
		self.layout()
	}
}

impl LayoutHelper {
	pub fn new(
		replication_factor: ReplicationFactor,
		consistency_mode: ConsistencyMode,
		mut layout: LayoutHistory,
		mut ack_lock: HashMap<u64, AtomicUsize>,
	) -> Self {
		// In the new() function of the helper, we do a bunch of cleanup
		// and calculations on the layout history to make sure things are
		// correct and we have rapid access to important values such as
		// the layout versions to use when reading to ensure consistency.

		if consistency_mode != ConsistencyMode::Consistent {
			// Fast path for when no consistency is required.
			// In this case we only need to keep the last version of the layout,
			// we don't care about coordinating stuff in the cluster.
			layout.keep_current_version_only();
		}

		layout.cleanup_old_versions();

		let all_nodes = layout.get_all_nodes();
		let all_nongateway_nodes = layout.get_all_nongateway_nodes();

		layout.clamp_update_trackers(&all_nodes);

		let min_version = layout.min_stored();

		// ack_map_min is the minimum value of ack_map among all nodes
		// in the cluster (gateway, non-gateway, current and previous layouts).
		// It is the highest layout version which all of these nodes have
		// acknowledged, indicating that they are aware of it and are no
		// longer processing write operations that did not take it into account.
		let ack_map_min = layout
			.update_trackers
			.ack_map
			.min_among(&all_nodes, min_version);

		// sync_map_min is the minimum value of sync_map among storage nodes
		// in the cluster (non-gateway nodes only, current and previous layouts).
		// It is the highest layout version for which we know that all relevant
		// storage nodes have fullfilled a sync, and therefore it is safe to
		// use a read quorum within that layout to ensure consistency.
		// Gateway nodes are excluded here because they hold no relevant data
		// (they store the bucket and access key tables, but we don't have
		// consistency on those).
		// This value is calculated using quorums to allow progress even
		// if not all nodes have successfully completed a sync.
		let sync_map_min =
			layout.calculate_sync_map_min_with_quorum(replication_factor, &all_nongateway_nodes);

		let trackers_hash = layout.calculate_trackers_hash();
		let staging_hash = layout.calculate_staging_hash();

		ack_lock.retain(|_, cnt| *cnt.get_mut() > 0);
		ack_lock
			.entry(layout.current().version)
			.or_insert(AtomicUsize::new(0));

		LayoutHelper {
			replication_factor,
			consistency_mode,
			layout: Some(layout),
			ack_map_min,
			sync_map_min,
			all_nodes,
			all_nongateway_nodes,
			trackers_hash,
			staging_hash,
			ack_lock,
		}
	}

	// ------------------ single updating function --------------

	fn layout(&self) -> &LayoutHistory {
		self.layout.as_ref().unwrap()
	}

	pub(crate) fn update<F>(&mut self, f: F) -> bool
	where
		F: FnOnce(&mut LayoutHistory) -> bool,
	{
		let changed = f(self.layout.as_mut().unwrap());
		if changed {
			*self = Self::new(
				self.replication_factor,
				self.consistency_mode,
				self.layout.take().unwrap(),
				std::mem::take(&mut self.ack_lock),
			);
		}
		changed
	}

	// ------------------ read helpers ---------------

	pub fn all_nodes(&self) -> &[Uuid] {
		&self.all_nodes
	}

	pub fn all_nongateway_nodes(&self) -> &[Uuid] {
		&self.all_nongateway_nodes
	}

	pub fn ack_map_min(&self) -> u64 {
		self.ack_map_min
	}

	pub fn sync_map_min(&self) -> u64 {
		self.sync_map_min
	}

	pub fn sync_digest(&self) -> SyncLayoutDigest {
		SyncLayoutDigest {
			current: self.layout().current().version,
			ack_map_min: self.ack_map_min(),
			min_stored: self.layout().min_stored(),
		}
	}

	pub fn read_nodes_of(&self, position: &Hash) -> Vec<Uuid> {
		let sync_min = self.sync_map_min;
		let version = self
			.layout()
			.versions
			.iter()
			.find(|x| x.version == sync_min)
			.or(self.layout().versions.last())
			.unwrap();
		version
			.nodes_of(position, version.replication_factor)
			.collect()
	}

	pub fn storage_sets_of(&self, position: &Hash) -> Vec<Vec<Uuid>> {
		self.layout()
			.versions
			.iter()
			.map(|x| x.nodes_of(position, x.replication_factor).collect())
			.collect()
	}

	pub fn storage_nodes_of(&self, position: &Hash) -> Vec<Uuid> {
		let mut ret = vec![];
		for version in self.layout().versions.iter() {
			ret.extend(version.nodes_of(position, version.replication_factor));
		}
		ret.sort();
		ret.dedup();
		ret
	}

	pub fn trackers_hash(&self) -> Hash {
		self.trackers_hash
	}

	pub fn staging_hash(&self) -> Hash {
		self.staging_hash
	}

	pub fn digest(&self) -> RpcLayoutDigest {
		RpcLayoutDigest {
			current_version: self.current().version,
			active_versions: self.versions.len(),
			trackers_hash: self.trackers_hash,
			staging_hash: self.staging_hash,
		}
	}

	// ------------------ helpers for update tracking ---------------

	pub(crate) fn update_trackers(&mut self, local_node_id: Uuid) {
		// Ensure trackers for this node's values are up-to-date

		// 1. Acknowledge the last layout version which is not currently
		//    locked by an in-progress write operation
		self.ack_max_free(local_node_id);

		// 2. Assume the data on this node is sync'ed up at least to
		//    the first layout version in the history
		self.sync_first(local_node_id);

		// 3. Acknowledge everyone has synced up to min(self.sync_map)
		self.sync_ack(local_node_id);

		debug!("ack_map: {:?}", self.update_trackers.ack_map);
		debug!("sync_map: {:?}", self.update_trackers.sync_map);
		debug!("sync_ack_map: {:?}", self.update_trackers.sync_ack_map);
	}

	fn sync_first(&mut self, local_node_id: Uuid) {
		let first_version = self.min_stored();
		self.update(|layout| {
			layout
				.update_trackers
				.sync_map
				.set_max(local_node_id, first_version)
		});
	}

	fn sync_ack(&mut self, local_node_id: Uuid) {
		let sync_map_min = self.sync_map_min;
		self.update(|layout| {
			layout
				.update_trackers
				.sync_ack_map
				.set_max(local_node_id, sync_map_min)
		});
	}

	pub(crate) fn ack_max_free(&mut self, local_node_id: Uuid) -> bool {
		let max_ack = self.max_free_ack();
		let changed = self.update(|layout| {
			layout
				.update_trackers
				.ack_map
				.set_max(local_node_id, max_ack)
		});
		if changed {
			info!("ack_until updated to {}", max_ack);
		}
		changed
	}

	pub(crate) fn max_free_ack(&self) -> u64 {
		self.layout()
			.versions
			.iter()
			.map(|x| x.version)
			.skip_while(|v| {
				self.ack_lock
					.get(v)
					.map(|x| x.load(Ordering::Relaxed) == 0)
					.unwrap_or(true)
			})
			.next()
			.unwrap_or(self.current().version)
	}
}
