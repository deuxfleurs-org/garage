use serde::{Deserialize, Serialize};

use crate::crdt::crdt::*;

/// Simple CRDT Map
///
/// This types defines a CRDT for a map from keys to values. Values are CRDT types which
/// can have their own updating logic.
///
/// Internally, the map is stored as a vector of keys and values, sorted by ascending key order.
/// This is why the key type `K` must implement `Ord` (and also to ensure a unique serialization,
/// such that two values can be compared for equality based on their hashes). As a consequence,
/// insertions take `O(n)` time. This means that Map should be used for reasonably small maps.
/// However, note that even if we were using a more efficient data structure such as a `BTreeMap`,
/// the serialization cost `O(n)` would still have to be paid at each modification, so we are
/// actually not losing anything here.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Map<K, V> {
	vals: Vec<(K, V)>,
}

impl<K, V> Map<K, V>
where
	K: Clone + Ord,
	V: Clone + CRDT,
{
	/// Create a new empty map CRDT
	pub fn new() -> Self {
		Self { vals: vec![] }
	}

	/// Returns a map that contains a single mapping from the specified key to the specified value.
	/// This can be used to build a delta-mutator:
	/// when merged with another map, the value will be added or CRDT-merged if a previous
	/// value already exists.
	pub fn put_mutator(k: K, v: V) -> Self {
		Self { vals: vec![(k, v)] }
	}

	/// Add a value to the map
	pub fn put(&mut self, k: K, v: V) {
		self.merge(&Self::put_mutator(k, v));
	}

	/// Removes all values from the map
	pub fn clear(&mut self) {
		self.vals.clear();
	}

	/// Get a reference to the value assigned to a key
	pub fn get(&self, k: &K) -> Option<&V> {
		match self.vals.binary_search_by(|(k2, _)| k2.cmp(&k)) {
			Ok(i) => Some(&self.vals[i].1),
			Err(_) => None,
		}
	}
	/// Gets a reference to all of the items, as a slice. Usefull to iterate on all map values.
	pub fn items(&self) -> &[(K, V)] {
		&self.vals[..]
	}
	/// Returns the number of items in the map
	pub fn len(&self) -> usize {
		self.vals.len()
	}
}

impl<K, V> CRDT for Map<K, V>
where
	K: Clone + Ord,
	V: Clone + CRDT,
{
	fn merge(&mut self, other: &Self) {
		for (k, v2) in other.vals.iter() {
			match self.vals.binary_search_by(|(k2, _)| k2.cmp(&k)) {
				Ok(i) => {
					self.vals[i].1.merge(&v2);
				}
				Err(i) => {
					self.vals.insert(i, (k.clone(), v2.clone()));
				}
			}
		}
	}
}
