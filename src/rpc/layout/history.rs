use std::collections::HashMap;
use std::collections::HashSet;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};

use garage_util::crdt::{Crdt, Lww, LwwMap};
use garage_util::data::*;
use garage_util::encode::nonversioned_encode;
use garage_util::error::*;

use super::schema::*;
use super::*;

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

// ----

impl LayoutHistory {
	pub fn new(replication_factor: usize) -> Self {
		let version = LayoutVersion::new(replication_factor);

		let staging = LayoutStaging {
			parameters: Lww::<LayoutParameters>::new(version.parameters),
			roles: LwwMap::new(),
		};

		LayoutHistory {
			versions: vec![version],
			update_trackers: Default::default(),
			staging: Lww::raw(0, staging),
		}
	}

	// ------------------ who stores what now? ---------------

	pub fn current(&self) -> &LayoutVersion {
		self.versions.last().as_ref().unwrap()
	}

	pub fn min_stored(&self) -> u64 {
		self.versions.first().as_ref().unwrap().version
	}

	pub fn get_all_nodes(&self) -> Vec<Uuid> {
		if self.versions.len() == 1 {
			self.versions[0].all_nodes().to_vec()
		} else {
			let set = self
				.versions
				.iter()
				.map(|x| x.all_nodes())
				.flatten()
				.collect::<HashSet<_>>();
			set.into_iter().copied().collect::<Vec<_>>()
		}
	}

	fn get_all_nongateway_nodes(&self) -> Vec<Uuid> {
		if self.versions.len() == 1 {
			self.versions[0].nongateway_nodes().to_vec()
		} else {
			let set = self
				.versions
				.iter()
				.map(|x| x.nongateway_nodes())
				.flatten()
				.collect::<HashSet<_>>();
			set.into_iter().copied().collect::<Vec<_>>()
		}
	}

	// ---- housekeeping (all invoked by LayoutHelper) ----

	fn cleanup_old_versions(&mut self) {
		loop {
			let all_nongateway_nodes = self.get_all_nongateway_nodes();
			let min_version = self.min_stored();
			let sync_ack_map_min = self
				.update_trackers
				.sync_ack_map
				.min(&all_nongateway_nodes, min_version);
			if self.min_stored() < sync_ack_map_min {
				let removed = self.versions.remove(0);
				info!("Layout history: pruning old version {}", removed.version);
			} else {
				break;
			}
		}
	}

	fn clamp_update_trackers(&mut self, nodes: &[Uuid]) {
		let min_v = self.min_stored();
		for node in nodes {
			self.update_trackers.ack_map.set_max(*node, min_v);
			self.update_trackers.sync_map.set_max(*node, min_v);
			self.update_trackers.sync_ack_map.set_max(*node, min_v);
		}
	}

	fn calculate_trackers_hash(&self) -> Hash {
		blake2sum(&nonversioned_encode(&self.update_trackers).unwrap()[..])
	}

	fn calculate_staging_hash(&self) -> Hash {
		blake2sum(&nonversioned_encode(&self.staging).unwrap()[..])
	}

	// ================== updates to layout, public interface ===================

	pub fn merge(&mut self, other: &LayoutHistory) -> bool {
		let mut changed = false;

		// Add any new versions to history
		for v2 in other.versions.iter() {
			if let Some(v1) = self.versions.iter().find(|v| v.version == v2.version) {
				if v1 != v2 {
					error!("Inconsistent layout histories: different layout compositions for version {}. Your cluster will be broken as long as this layout version is not replaced.", v2.version);
				}
			} else if self.versions.iter().all(|v| v.version != v2.version - 1) {
				error!(
					"Cannot receive new layout version {}, version {} is missing",
					v2.version,
					v2.version - 1
				);
			} else {
				self.versions.push(v2.clone());
				changed = true;
			}
		}

		// Merge trackers
		if self.update_trackers != other.update_trackers {
			let c = self.update_trackers.merge(&other.update_trackers);
			changed = changed || c;
		}

		// If there are invalid versions before valid versions, remove them,
		// and increment update trackers
		if self.versions.len() > 1 && self.current().check().is_ok() {
			while self.versions.first().unwrap().check().is_err() {
				self.versions.remove(0);
				changed = true;
			}
		}

		// Merge staged layout changes
		if self.staging != other.staging {
			self.staging.merge(&other.staging);
			changed = true;
		}

		changed
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
				if v != self.current().version + 1 {
					return Err(Error::Message("Invalid new layout version".into()));
				}
			}
		}

		// Compute new version and add it to history
		let (new_version, msg) = self
			.current()
			.clone()
			.calculate_next_version(&self.staging.get())?;

		self.versions.push(new_version);
		if self.current().check().is_ok() {
			while self.versions.first().unwrap().check().is_err() {
				self.versions.remove(0);
			}
		}

		// Reset the staged layout changes
		self.staging.update(LayoutStaging {
			parameters: self.staging.get().parameters.clone(),
			roles: LwwMap::new(),
		});

		Ok((self, msg))
	}

	pub fn revert_staged_changes(mut self) -> Result<Self, Error> {
		self.staging.update(LayoutStaging {
			parameters: Lww::new(self.current().parameters.clone()),
			roles: LwwMap::new(),
		});

		Ok(self)
	}

	pub fn check(&self) -> Result<(), String> {
		for version in self.versions.iter() {
			version.check()?;
		}

		// TODO: anything more ?
		Ok(())
	}
}
