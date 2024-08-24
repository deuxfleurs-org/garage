use std::collections::HashSet;

use garage_util::crdt::{Crdt, Lww, LwwMap};
use garage_util::data::*;
use garage_util::encode::nonversioned_encode;
use garage_util::error::*;

use super::*;
use crate::replication_mode::*;

impl LayoutHistory {
	pub fn new(replication_factor: ReplicationFactor) -> Self {
		let version = LayoutVersion::new(replication_factor.into());

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

	/// Returns the layout version with the highest number
	pub fn current(&self) -> &LayoutVersion {
		self.versions.last().as_ref().unwrap()
	}

	/// Returns the version number of the oldest layout version still active
	pub fn min_stored(&self) -> u64 {
		self.versions.first().as_ref().unwrap().version
	}

	/// Calculate the set of all nodes that have a role (gateway or storage)
	/// in one of the currently active layout versions
	pub fn get_all_nodes(&self) -> Vec<Uuid> {
		if self.versions.len() == 1 {
			self.versions[0].all_nodes().to_vec()
		} else {
			let set = self
				.versions
				.iter()
				.flat_map(|x| x.all_nodes())
				.collect::<HashSet<_>>();
			set.into_iter().copied().collect::<Vec<_>>()
		}
	}

	/// Calculate the set of all nodes that are configured to store data
	/// in one of the currently active layout versions
	pub(crate) fn get_all_nongateway_nodes(&self) -> Vec<Uuid> {
		if self.versions.len() == 1 {
			self.versions[0].nongateway_nodes().to_vec()
		} else {
			let set = self
				.versions
				.iter()
				.flat_map(|x| x.nongateway_nodes())
				.collect::<HashSet<_>>();
			set.into_iter().copied().collect::<Vec<_>>()
		}
	}

	// ---- housekeeping (all invoked by LayoutHelper) ----

	pub(crate) fn keep_current_version_only(&mut self) {
		while self.versions.len() > 1 {
			let removed = self.versions.remove(0);
			self.old_versions.push(removed);
		}
	}

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
		// remove them (keep them in self.old_versions).
		// ASSUMPTION: we only care about where nodes in the current layout version
		// are reading from, as we assume older nodes are being discarded.
		let current_nodes = &self.current().node_id_vec;
		let min_version = self.min_stored();
		let sync_ack_map_min = self
			.update_trackers
			.sync_ack_map
			.min_among(current_nodes, min_version);
		while self.min_stored() < sync_ack_map_min {
			assert!(self.versions.len() > 1);
			let removed = self.versions.remove(0);
			info!(
				"Layout history: moving version {} to old_versions",
				removed.version
			);
			self.old_versions.push(removed);
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

	pub(crate) fn calculate_sync_map_min_with_quorum(
		&self,
		replication_factor: ReplicationFactor,
		all_nongateway_nodes: &[Uuid],
	) -> u64 {
		// This function calculates the minimum layout version from which
		// it is safe to read if we want to maintain read-after-write consistency.
		// In the general case the computation can be a bit expensive so
		// we try to optimize it in several ways.

		// If there is only one layout version, we know that's the one
		// we need to read from.
		if self.versions.len() == 1 {
			return self.current().version;
		}

		let quorum = replication_factor.write_quorum(ConsistencyMode::Consistent);

		let min_version = self.min_stored();
		let global_min = self
			.update_trackers
			.sync_map
			.min_among(all_nongateway_nodes, min_version);

		// If the write quorums are equal to the total number of nodes,
		// i.e. no writes can succeed while they are not written to all nodes,
		// then we must in all case wait for all nodes to complete a sync.
		// This is represented by reading from the layout with version
		// number global_min, the smallest layout version for which all nodes
		// have completed a sync.
		if quorum == self.current().replication_factor {
			return global_min;
		}

		// In the general case, we need to look at all write sets for all partitions,
		// and find a safe layout version to read for that partition. We then
		// take the minimum value among all partition as the safe layout version
		// to read in all cases (the layout version to which all reads are directed).
		let mut current_min = self.current().version;
		let mut sets_done = HashSet::<Vec<Uuid>>::new();

		for (_, p_hash) in self.current().partitions() {
			for v in self.versions.iter() {
				if v.version == self.current().version {
					// We don't care about whether nodes in the latest layout version
					// have completed a sync or not, as the sync is push-only
					// and by definition nodes in the latest layout version do not
					// hold data that must be pushed to nodes in the latest layout
					// version, since that's the same version (any data that's
					// already in the latest version is assumed to have been written
					// by an operation that ensured a quorum of writes within
					// that version).
					continue;
				}

				// Determine set of nodes for partition p in layout version v.
				// Sort the node set to avoid duplicate computations.
				let mut set = v
					.nodes_of(&p_hash, v.replication_factor)
					.collect::<Vec<Uuid>>();
				set.sort();

				// If this set was already processed, skip it.
				if sets_done.contains(&set) {
					continue;
				}

				// Find the value of the sync update trackers that is the
				// highest possible minimum within a quorum of nodes.
				let mut sync_values = set
					.iter()
					.map(|x| self.update_trackers.sync_map.get(x, min_version))
					.collect::<Vec<_>>();
				sync_values.sort();
				let set_min = sync_values[sync_values.len() - quorum];
				if set_min < current_min {
					current_min = set_min;
				}
				// defavorable case, we know we are at the smallest possible version,
				// so we can stop early
				assert!(current_min >= global_min);
				if current_min == global_min {
					return current_min;
				}

				// Add set to already processed sets
				sets_done.insert(set);
			}
		}

		current_min
	}

	pub(crate) fn calculate_trackers_hash(&self) -> Hash {
		blake2sum(&nonversioned_encode(&self.update_trackers).unwrap()[..])
	}

	pub(crate) fn calculate_staging_hash(&self) -> Hash {
		blake2sum(&nonversioned_encode(&self.staging).unwrap()[..])
	}

	// ================== updates to layout, public interface ===================

	pub fn merge(&mut self, other: &LayoutHistory) -> bool {
		// If our current layout version is completely out-of-date,
		// forget everything we know and replace it by incoming layout data.
		if self.current().version < other.min_stored() {
			*self = other.clone();
			return true;
		}

		let mut changed = false;

		// Add any new versions to history
		for v2 in other.versions.iter() {
			if v2.version == self.current().version + 1 {
				// This is the next version, add it to our version list
				self.versions.push(v2.clone());
				changed = true;
			} else if let Some(v1) = self.versions.iter().find(|v| v.version == v2.version) {
				// Version is already present, check consistency
				if v1 != v2 {
					error!("Inconsistent layout histories: different layout compositions for version {}. Your cluster will be broken as long as this layout version is not replaced.", v2.version);
				}
			} else {
				// This is an older version
				assert!(v2.version < self.min_stored());
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
			.calculate_next_version(self.staging.get())?;

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
			parameters: Lww::new(self.current().parameters),
			roles: LwwMap::new(),
		});

		Ok(self)
	}

	pub fn check(&self) -> Result<(), String> {
		// TODO: anything more ?
		self.current().check()
	}
}
