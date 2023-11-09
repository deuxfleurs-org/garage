use std::collections::HashSet;

use garage_util::crdt::{Crdt, Lww, LwwMap};
use garage_util::data::*;
use garage_util::encode::nonversioned_encode;
use garage_util::error::*;

use super::schema::*;
use super::*;

impl LayoutHistory {
	pub fn new(replication_factor: usize) -> Self {
		let version = LayoutVersion::new(replication_factor);

		let staging = LayoutStaging {
			parameters: Lww::<LayoutParameters>::new(version.parameters),
			roles: LwwMap::new(),
		};

		let mut ret = LayoutHistory {
			versions: vec![version].into_boxed_slice().into(),
			update_trackers: Default::default(),
			trackers_hash: [0u8; 32].into(),
			staging: Lww::raw(0, staging),
			staging_hash: [0u8; 32].into(),
		};
		ret.update_hashes();
		ret
	}

	pub fn current(&self) -> &LayoutVersion {
		self.versions.last().as_ref().unwrap()
	}

	pub fn update_hashes(&mut self) {
		self.trackers_hash = self.calculate_trackers_hash();
		self.staging_hash = self.calculate_staging_hash();
	}

	pub(crate) fn calculate_trackers_hash(&self) -> Hash {
		blake2sum(&nonversioned_encode(&self.update_trackers).unwrap()[..])
	}

	pub(crate) fn calculate_staging_hash(&self) -> Hash {
		blake2sum(&nonversioned_encode(&self.staging).unwrap()[..])
	}

	// ------------------ who stores what now? ---------------

	pub fn max_ack(&self) -> u64 {
		self.calculate_global_min(&self.update_trackers.ack_map)
	}

	pub fn all_storage_nodes(&self) -> HashSet<Uuid> {
		// TODO: cache this
		self.versions
			.iter()
			.map(|x| x.nongateway_nodes())
			.flatten()
			.collect::<HashSet<_>>()
	}

	pub fn read_nodes_of(&self, position: &Hash) -> Vec<Uuid> {
		let sync_min = self.calculate_global_min(&self.update_trackers.sync_map);
		let version = self
			.versions
			.iter()
			.find(|x| x.version == sync_min)
			.or(self.versions.last())
			.unwrap();
		version.nodes_of(position, version.replication_factor)
	}

	pub fn write_sets_of(&self, position: &Hash) -> Vec<Vec<Uuid>> {
		self.versions
			.iter()
			.map(|x| x.nodes_of(position, x.replication_factor))
			.collect::<Vec<_>>()
	}

	// ------------------ update tracking ---------------

	pub(crate) fn update_trackers(&mut self, node_id: Uuid) {
		// Ensure trackers for this node's values are up-to-date

		// 1. Acknowledge the last layout version in the history
		self.ack_last(node_id);

		// 2. Assume the data on this node is sync'ed up at least to
		//    the first layout version in the history
		self.sync_first(node_id);

		// 3. Acknowledge everyone has synced up to min(self.sync_map)
		self.sync_ack(node_id);

		// 4. Cleanup layout versions that are not needed anymore
		self.cleanup_old_versions();

		info!("ack_map: {:?}", self.update_trackers.ack_map);
		info!("sync_map: {:?}", self.update_trackers.sync_map);
		info!("sync_ack_map: {:?}", self.update_trackers.sync_ack_map);

		// Finally, update hashes
		self.update_hashes();
	}

	pub(crate) fn ack_last(&mut self, node: Uuid) {
		let last_version = self.current().version;
		self.update_trackers.ack_map.set_max(node, last_version);
	}

	pub(crate) fn sync_first(&mut self, node: Uuid) {
		let first_version = self.versions.first().as_ref().unwrap().version;
		self.update_trackers.sync_map.set_max(node, first_version);
	}

	pub(crate) fn sync_ack(&mut self, node: Uuid) {
		self.update_trackers.sync_ack_map.set_max(
			node,
			self.calculate_global_min(&self.update_trackers.sync_map),
		);
	}

	pub(crate) fn cleanup_old_versions(&mut self) {
		let min_sync_ack = self.calculate_global_min(&self.update_trackers.sync_ack_map);
		while self.versions.first().as_ref().unwrap().version < min_sync_ack {
			self.versions.remove(0);
		}
	}

	pub(crate) fn calculate_global_min(&self, tracker: &UpdateTracker) -> u64 {
		let storage_nodes = self.all_storage_nodes();
		storage_nodes
			.iter()
			.map(|x| tracker.0.get(x).copied().unwrap_or(0))
			.min()
			.unwrap_or(0)
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
		let mut new_version = self.current().clone();
		new_version.version += 1;

		new_version.roles.merge(&self.staging.get().roles);
		new_version.roles.retain(|(_, _, v)| v.0.is_some());
		new_version.parameters = *self.staging.get().parameters.get();

		let msg = new_version.calculate_partition_assignment()?;
		self.versions.push(new_version);

		// Reset the staged layout changes
		self.staging.update(LayoutStaging {
			parameters: self.staging.get().parameters.clone(),
			roles: LwwMap::new(),
		});
		self.update_hashes();

		Ok((self, msg))
	}

	pub fn revert_staged_changes(mut self) -> Result<Self, Error> {
		self.staging.update(LayoutStaging {
			parameters: Lww::new(self.current().parameters.clone()),
			roles: LwwMap::new(),
		});
		self.update_hashes();

		Ok(self)
	}

	pub fn check(&self) -> Result<(), String> {
		// Check that the hash of the staging data is correct
		if self.trackers_hash != self.calculate_trackers_hash() {
			return Err("trackers_hash is incorrect".into());
		}
		if self.staging_hash != self.calculate_staging_hash() {
			return Err("staging_hash is incorrect".into());
		}

		for version in self.versions.iter() {
			version.check()?;
		}

		// TODO: anythign more ?
		Ok(())
	}
}
