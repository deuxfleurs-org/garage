use super::*;

impl UpdateTracker {
	fn merge(&mut self, other: &UpdateTracker) {
		for (k, v) in other.0.iter() {
			if let Some(v_mut) = self.0.get_mut(k) {
				*v_mut = std::cmp::max(*v_mut, *v);
			} else {
				self.0.insert(*k, *v);
			}
		}
	}
}

impl UpdateTrackers {
	pub(crate) fn merge(&mut self, other: &UpdateTrackers) {
		self.ack_map.merge(&other.ack_map);
		self.sync_map.merge(&other.sync_map);
		self.sync_ack_map.merge(&other.sync_ack_map);
	}
}
