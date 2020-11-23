use serde::{Deserialize, Serialize};

use garage_util::data::*;

/// Conflict-free replicated data type (CRDT)
///
/// CRDT are a type of data structures that do not require coordination.
/// In other words, we can edit them in parallel, we will always
/// find a way to merge it.
///
/// A general example is a counter. Its initial value is 0.
/// Alice and Bob get a copy of the counter.
/// Alice does +1 on her copy, she reads 1.
/// Bob does +3 on his copy, he reads 3.
/// Now, it is easy to merge their counters, order does not count:
/// we always get 4.
///
/// Learn more about CRDT [on Wikipedia](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type)
pub trait CRDT {
	/// Merge the two datastructures according to the CRDT rules
	///
	/// # Arguments
	///
	/// * `other` - the other copy of the CRDT
	fn merge(&mut self, other: &Self);
}

impl<T> CRDT for T
where
	T: Ord + Clone,
{
	fn merge(&mut self, other: &Self) {
		if other > self {
			*self = other.clone();
		}
	}
}

// ---- LWW Register ----

/// Last Write Win (LWW)
///
/// LWW is a very simple
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LWW<T> {
	ts: u64,
	v: T,
}

impl<T> LWW<T>
where
	T: CRDT,
{
	pub fn new(value: T) -> Self {
		Self {
			ts: now_msec(),
			v: value,
		}
	}
	pub fn migrate_from_raw(ts: u64, value: T) -> Self {
		Self { ts, v: value }
	}
	pub fn update(&mut self, new_value: T) {
		self.ts = std::cmp::max(self.ts + 1, now_msec());
		self.v = new_value;
	}
	pub fn get(&self) -> &T {
		&self.v
	}
	pub fn get_mut(&mut self) -> &mut T {
		&mut self.v
	}
}

impl<T> CRDT for LWW<T>
where
	T: Clone + CRDT,
{
	fn merge(&mut self, other: &Self) {
		if other.ts > self.ts {
			self.ts = other.ts;
			self.v = other.v.clone();
		} else if other.ts == self.ts {
			self.v.merge(&other.v);
		}
	}
}

/// Boolean
///
/// with True as absorbing state
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub struct Bool(bool);

impl Bool {
	pub fn new(b: bool) -> Self {
		Self(b)
	}
	pub fn set(&mut self) {
		self.0 = true;
	}
	pub fn get(&self) -> bool {
		self.0
	}
}

impl CRDT for Bool {
	fn merge(&mut self, other: &Self) {
		self.0 = self.0 || other.0;
	}
}

/// Last Write Win Map
///
///
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LWWMap<K, V> {
	vals: Vec<(K, u64, V)>,
}

impl<K, V> LWWMap<K, V>
where
	K: Ord,
	V: CRDT,
{
	pub fn new() -> Self {
		Self { vals: vec![] }
	}
	pub fn migrate_from_raw_item(k: K, ts: u64, v: V) -> Self {
		Self {
			vals: vec![(k, ts, v)],
		}
	}
	pub fn take_and_clear(&mut self) -> Self {
		let vals = std::mem::replace(&mut self.vals, vec![]);
		Self { vals }
	}
	pub fn clear(&mut self) {
		self.vals.clear();
	}
	pub fn update_mutator(&self, k: K, new_v: V) -> Self {
		let new_vals = match self.vals.binary_search_by(|(k2, _, _)| k2.cmp(&k)) {
			Ok(i) => {
				let (_, old_ts, _) = self.vals[i];
				let new_ts = std::cmp::max(old_ts + 1, now_msec());
				vec![(k, new_ts, new_v)]
			}
			Err(_) => vec![(k, now_msec(), new_v)],
		};
		Self { vals: new_vals }
	}
	pub fn get(&self, k: &K) -> Option<&V> {
		match self.vals.binary_search_by(|(k2, _, _)| k2.cmp(&k)) {
			Ok(i) => Some(&self.vals[i].2),
			Err(_) => None,
		}
	}
	pub fn items(&self) -> &[(K, u64, V)] {
		&self.vals[..]
	}
}

impl<K, V> CRDT for LWWMap<K, V>
where
	K: Clone + Ord,
	V: Clone + CRDT,
{
	fn merge(&mut self, other: &Self) {
		for (k, ts2, v2) in other.vals.iter() {
			match self.vals.binary_search_by(|(k2, _, _)| k2.cmp(&k)) {
				Ok(i) => {
					let (_, ts1, _v1) = &self.vals[i];
					if ts2 > ts1 {
						self.vals[i].1 = *ts2;
						self.vals[i].2 = v2.clone();
					} else if ts1 == ts2 {
						self.vals[i].2.merge(&v2);
					}
				}
				Err(i) => {
					self.vals.insert(i, (k.clone(), *ts2, v2.clone()));
				}
			}
		}
	}
}
