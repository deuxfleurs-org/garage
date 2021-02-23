use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use garage_util::data::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkConfig {
	pub members: HashMap<UUID, NetworkConfigEntry>,
	pub version: u64,
}

impl NetworkConfig {
	pub(crate) fn new() -> Self {
		Self {
			members: HashMap::new(),
			version: 0,
		}
	}
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkConfigEntry {
	pub datacenter: String,
	pub n_tokens: u32,
	pub tag: String,
}

#[derive(Clone)]
pub struct Ring {
	pub config: NetworkConfig,
	pub ring: Vec<RingEntry>,
	pub n_datacenters: usize,
}

#[derive(Clone, Debug)]
pub struct RingEntry {
	pub location: Hash,
	pub node: UUID,
	datacenter: usize,
}

impl Ring {
	pub(crate) fn rebuild_ring(&mut self) {
		let mut new_ring = vec![];
		let mut datacenters = vec![];

		for (id, config) in self.config.members.iter() {
			let datacenter = &config.datacenter;

			if !datacenters.contains(datacenter) {
				datacenters.push(datacenter.to_string());
			}
			let datacenter_idx = datacenters
				.iter()
				.enumerate()
				.find(|(_, dc)| *dc == datacenter)
				.unwrap()
				.0;

			for i in 0..config.n_tokens {
				let location = sha256sum(format!("{} {}", hex::encode(&id), i).as_bytes());

				new_ring.push(RingEntry {
					location: location.into(),
					node: *id,
					datacenter: datacenter_idx,
				})
			}
		}

		new_ring.sort_unstable_by(|x, y| x.location.cmp(&y.location));
		self.ring = new_ring;
		self.n_datacenters = datacenters.len();

		// eprintln!("RING: --");
		// for e in self.ring.iter() {
		// 	eprintln!("{:?}", e);
		// }
		// eprintln!("END --");
	}

	pub fn walk_ring(&self, from: &Hash, n: usize) -> Vec<UUID> {
		if n >= self.config.members.len() {
			return self.config.members.keys().cloned().collect::<Vec<_>>();
		}

		let start = match self.ring.binary_search_by(|x| x.location.cmp(from)) {
			Ok(i) => i,
			Err(i) => {
				if i == 0 {
					self.ring.len() - 1
				} else {
					i - 1
				}
			}
		};

		self.walk_ring_from_pos(start, n)
	}

	fn walk_ring_from_pos(&self, start: usize, n: usize) -> Vec<UUID> {
		if n >= self.config.members.len() {
			return self.config.members.keys().cloned().collect::<Vec<_>>();
		}

		let mut ret = vec![];
		let mut datacenters = vec![];

		let mut delta = 0;
		while ret.len() < n {
			let i = (start + delta) % self.ring.len();
			delta += 1;

			if !datacenters.contains(&self.ring[i].datacenter) {
				ret.push(self.ring[i].node);
				datacenters.push(self.ring[i].datacenter);
			} else if datacenters.len() == self.n_datacenters && !ret.contains(&self.ring[i].node) {
				ret.push(self.ring[i].node);
			}
		}

		ret
	}
}
