//! Implements a CausalContext, which is a set of timestamps for each
//! node -- a vector clock --, indicating that the versions with
//! timestamps <= these numbers have been seen and can be
//! overwritten by a subsequent write.
//!
//! The textual representation of a CausalContext, which we call a
//! "causality token", is used in the API and must be sent along with
//! each write or delete operation to indicate the previously seen
//! versions that we want to overwrite or delete.
use base64::prelude::*;

use std::collections::BTreeMap;
use std::convert::TryInto;

use serde::{Deserialize, Serialize};

use garage_util::data::*;

/// Node IDs used in K2V are u64 integers that are the abbreviation
/// of full Garage node IDs which are 256-bit UUIDs.
pub type K2VNodeId = u64;

pub type VectorClock = BTreeMap<K2VNodeId, u64>;

pub fn make_node_id(node_id: Uuid) -> K2VNodeId {
	let mut tmp = [0u8; 8];
	tmp.copy_from_slice(&node_id.as_slice()[..8]);
	u64::from_be_bytes(tmp)
}

pub fn vclock_gt(a: &VectorClock, b: &VectorClock) -> bool {
	a.iter().any(|(n, ts)| ts > b.get(n).unwrap_or(&0))
}

pub fn vclock_max(a: &VectorClock, b: &VectorClock) -> VectorClock {
	let mut ret = a.clone();
	for (n, ts) in b.iter() {
		let ent = ret.entry(*n).or_insert(0);
		*ent = std::cmp::max(*ts, *ent);
	}
	ret
}

#[derive(PartialEq, Eq, Debug, Serialize, Deserialize, Default)]
pub struct CausalContext {
	pub vector_clock: VectorClock,
}

impl CausalContext {
	/// Empty causality context
	pub fn new() -> Self {
		Self::default()
	}

	/// Make binary representation and encode in base64
	pub fn serialize(&self) -> String {
		let mut ints = Vec::with_capacity(2 * self.vector_clock.len());
		for (node, time) in self.vector_clock.iter() {
			ints.push(*node);
			ints.push(*time);
		}
		let checksum = ints.iter().fold(0, |acc, v| acc ^ *v);

		let mut bytes = u64::to_be_bytes(checksum).to_vec();
		for i in ints {
			bytes.extend(u64::to_be_bytes(i));
		}

		BASE64_URL_SAFE_NO_PAD.encode(bytes)
	}

	/// Parse from base64-encoded binary representation.
	/// Returns None on error.
	pub fn parse(s: &str) -> Option<Self> {
		let bytes = BASE64_URL_SAFE_NO_PAD.decode(s).ok()?;
		if bytes.len() % 16 != 8 || bytes.len() < 8 {
			return None;
		}

		let checksum = u64::from_be_bytes(bytes[..8].try_into().unwrap());
		let mut ret = CausalContext {
			vector_clock: BTreeMap::new(),
		};

		for i in 0..(bytes.len() / 16) {
			let node_id = u64::from_be_bytes(bytes[8 + i * 16..16 + i * 16].try_into().unwrap());
			let time = u64::from_be_bytes(bytes[16 + i * 16..24 + i * 16].try_into().unwrap());
			ret.vector_clock.insert(node_id, time);
		}

		let check = ret.vector_clock.iter().fold(0, |acc, (n, t)| acc ^ *n ^ *t);

		if check != checksum {
			return None;
		}

		Some(ret)
	}

	/// Check if this causal context contains newer items than another one
	pub fn is_newer_than(&self, other: &Self) -> bool {
		vclock_gt(&self.vector_clock, &other.vector_clock)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_causality_token_serialization() {
		let ct = CausalContext {
			vector_clock: [(4, 42), (1928131023, 76), (0xefc0c1c47f9de433, 2)]
				.iter()
				.cloned()
				.collect(),
		};

		assert_eq!(CausalContext::parse(&ct.serialize()).unwrap(), ct);
	}
}
