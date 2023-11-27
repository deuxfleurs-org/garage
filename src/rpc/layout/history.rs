use std::collections::HashSet;

use garage_util::crdt::{Crdt, Lww, LwwMap};
use garage_util::data::*;
use garage_util::encode::nonversioned_encode;
use garage_util::error::*;

use super::*;

impl LayoutHistory {
	pub fn new(replication_factor: usize) -> Self {
		let version = LayoutVersion::new(replication_factor);

		let staging = LayoutStaging {
			parameters: Lww::<LayoutParameters>::new(version.parameters),
			roles: LwwMap::new(),
		};

		LayoutHistory {
			versions: vec![version],
			old_versions: vec![],
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

	pub(crate) fn get_all_nongateway_nodes(&self) -> Vec<Uuid> {
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

	pub(crate) fn cleanup_old_versions(&mut self) {
		// If there are invalid versions before valid versions, remove them
		if self.versions.len() > 1 && self.current().check().is_ok() {
			while self.versions.len() > 1 && self.versions.first().unwrap().check().is_err() {
				let removed = self.versions.remove(0);
				info!(
					"Layout history: pruning old invalid version {}",
					removed.version
				);
			}
		}

		// If there are old versions that no one is reading from anymore,
		// remove them
		while self.versions.len() > 1 {
			let all_nongateway_nodes = self.get_all_nongateway_nodes();
			let min_version = self.min_stored();
			let sync_ack_map_min = self
				.update_trackers
				.sync_ack_map
				.min(&all_nongateway_nodes, min_version);
			if self.min_stored() < sync_ack_map_min {
				let removed = self.versions.remove(0);
				info!(
					"Layout history: moving version {} to old_versions",
					removed.version
				);
				self.old_versions.push(removed);
			} else {
				break;
			}
		}

		while self.old_versions.len() > OLD_VERSION_COUNT {
			let removed = self.old_versions.remove(0);
			info!("Layout history: removing old_version {}", removed.version);
		}
	}

	pub(crate) fn clamp_update_trackers(&mut self, nodes: &[Uuid]) {
		let min_v = self.min_stored();
		for node in nodes {
			self.update_trackers.ack_map.set_max(*node, min_v);
			self.update_trackers.sync_map.set_max(*node, min_v);
			self.update_trackers.sync_ack_map.set_max(*node, min_v);
		}
	}

	pub(crate) fn calculate_trackers_hash(&self) -> Hash {
		blake2sum(&nonversioned_encode(&self.update_trackers).unwrap()[..])
	}

	pub(crate) fn calculate_staging_hash(&self) -> Hash {
		blake2sum(&nonversioned_encode(&self.staging).unwrap()[..])
	}

	// ================== updates to layout, public interface ===================

	pub fn merge(&mut self, other: &LayoutHistory) -> bool {
		let mut changed = false;

		// Add any new versions to history
		for v2 in other.versions.iter() {
			if let Some(v1) = self.versions.iter().find(|v| v.version == v2.version) {
				// Version is already present, check consistency
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
		let c = self.update_trackers.merge(&other.update_trackers);
		changed = changed || c;

		// Merge staged layout changes
		if self.staging != other.staging {
			let prev_staging = self.staging.clone();
			self.staging.merge(&other.staging);
			changed = changed || self.staging != prev_staging;
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
		self.cleanup_old_versions();

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
		// TODO: anything more ?
		self.current().check()
	}
}
