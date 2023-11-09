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
			staging: Lww::raw(0, staging),
			staging_hash: [0u8; 32].into(),
		};
		ret.staging_hash = ret.calculate_staging_hash();
		ret
	}

	pub fn current(&self) -> &LayoutVersion {
		self.versions.last().as_ref().unwrap()
	}

	pub(crate) fn calculate_staging_hash(&self) -> Hash {
		blake2sum(&nonversioned_encode(&self.staging).unwrap()[..])
	}

	// ================== updates to layout, public interface ===================

	pub fn merge(&mut self, other: &LayoutHistory) -> bool {
		let mut changed = false;

		// Merge staged layout changes
		if self.staging != other.staging {
			changed = true;
		}
		self.staging.merge(&other.staging);

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
		self.update_trackers.merge(&other.update_trackers);

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
		self.staging_hash = self.calculate_staging_hash();

		Ok((self, msg))
	}

	pub fn revert_staged_changes(mut self) -> Result<Self, Error> {
		self.staging.update(LayoutStaging {
			parameters: Lww::new(self.current().parameters.clone()),
			roles: LwwMap::new(),
		});
		self.staging_hash = self.calculate_staging_hash();

		Ok(self)
	}

	pub fn check(&self) -> Result<(), String> {
		// Check that the hash of the staging data is correct
		let staging_hash = self.calculate_staging_hash();
		if staging_hash != self.staging_hash {
			return Err("staging_hash is incorrect".into());
		}

		// TODO: anythign more ?

		self.current().check()
	}
}
