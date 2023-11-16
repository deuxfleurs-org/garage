use std::collections::HashMap;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};

use garage_util::data::*;

use super::schema::*;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct LayoutDigest {
	/// Cluster layout version
	pub current_version: u64,
	/// Number of active layout versions
	pub active_versions: usize,
	/// Hash of cluster layout update trackers
	pub trackers_hash: Hash,
	/// Hash of cluster layout staging data
	pub staging_hash: Hash,
}

pub struct LayoutHelper {
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
	pub fn new(mut layout: LayoutHistory, mut ack_lock: HashMap<u64, AtomicUsize>) -> Self {
		layout.cleanup_old_versions();

		let all_nongateway_nodes = layout.get_all_nongateway_nodes();
		layout.clamp_update_trackers(&all_nongateway_nodes);

		let min_version = layout.min_stored();
		let ack_map_min = layout
			.update_trackers
			.ack_map
			.min(&all_nongateway_nodes, min_version);
		let sync_map_min = layout
			.update_trackers
			.sync_map
			.min(&all_nongateway_nodes, min_version);

		let all_nodes = layout.get_all_nodes();
		let trackers_hash = layout.calculate_trackers_hash();
		let staging_hash = layout.calculate_staging_hash();

		ack_lock.retain(|_, cnt| *cnt.get_mut() > 0);
		ack_lock
			.entry(layout.current().version)
			.or_insert(AtomicUsize::new(0));

		LayoutHelper {
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
		let changed = f(&mut self.layout.as_mut().unwrap());
		if changed {
			*self = Self::new(
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

	pub fn all_ack(&self) -> u64 {
		self.ack_map_min
	}

	pub fn sync_versions(&self) -> (u64, u64, u64) {
		(
			self.layout().current().version,
			self.all_ack(),
			self.layout().min_stored(),
		)
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

	pub(crate) fn write_sets_of(&self, position: &Hash) -> Vec<Vec<Uuid>> {
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

	pub fn digest(&self) -> LayoutDigest {
		LayoutDigest {
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

		info!("ack_map: {:?}", self.update_trackers.ack_map);
		info!("sync_map: {:?}", self.update_trackers.sync_map);
		info!("sync_ack_map: {:?}", self.update_trackers.sync_ack_map);
	}

	fn sync_first(&mut self, local_node_id: Uuid) {
		let first_version = self.versions.first().as_ref().unwrap().version;
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
			.take_while(|v| {
				self.ack_lock
					.get(v)
					.map(|x| x.load(Ordering::Relaxed) == 0)
					.unwrap_or(true)
			})
			.max()
			.unwrap_or(self.min_stored())
	}
}
