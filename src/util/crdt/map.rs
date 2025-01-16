use std::iter::{FromIterator, IntoIterator};

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
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Map<K, V> {
	vals: Vec<(K, V)>,
}

impl<K, V> Map<K, V>
where
	K: Clone + Ord,
	V: Clone + Crdt,
{
	/// Create a new empty map CRDT
	pub fn new() -> Self {
		Self { vals: vec![] }
	}

	/// Returns a map that contains a single mapping from the specified key to the specified value.
	/// This can be used to build a delta-mutator:
	/// when merged with another map, the value will be added or CRDT-merged if a previous
	/// value already exists.
	#[must_use = "CRDT mutators are meant to be merged into a CRDT and not ignored."]
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
		match self.vals.binary_search_by(|(k2, _)| k2.cmp(k)) {
			Ok(i) => Some(&self.vals[i].1),
			Err(_) => None,
		}
	}
	/// Gets a reference to all of the items, as a slice. Useful to iterate on all map values.
	pub fn items(&self) -> &[(K, V)] {
		&self.vals[..]
	}
	/// Returns the number of items in the map
	pub fn len(&self) -> usize {
		self.vals.len()
	}

	/// Returns true if the map is empty
	pub fn is_empty(&self) -> bool {
		self.len() == 0
	}
}

impl<K, V> Crdt for Map<K, V>
where
	K: Clone + Ord,
	V: Clone + Crdt,
{
	fn merge(&mut self, other: &Self) {
		for (k, v2) in other.vals.iter() {
			match self.vals.binary_search_by(|(k2, _)| k2.cmp(k)) {
				Ok(i) => {
					self.vals[i].1.merge(v2);
				}
				Err(i) => {
					self.vals.insert(i, (k.clone(), v2.clone()));
				}
			}
		}
	}
}

impl<K, V> Default for Map<K, V>
where
	K: Clone + Ord,
	V: Clone + Crdt,
{
	fn default() -> Self {
		Self::new()
	}
}

/// A crdt map can be created from an iterator of key-value pairs.
/// Note that all keys in the iterator must be distinct:
/// this function will throw a panic if it is not the case.
impl<K, V> FromIterator<(K, V)> for Map<K, V>
where
	K: Clone + Ord,
	V: Clone + Crdt,
{
	fn from_iter<T: IntoIterator<Item = (K, V)>>(iter: T) -> Self {
		let mut vals: Vec<(K, V)> = iter.into_iter().collect();
		vals.sort_by_cached_key(|tup| tup.0.clone());

		// sanity check
		for i in 1..vals.len() {
			if vals[i - 1].0 == vals[i].0 {
				panic!("Duplicate key in crdt::Map resulting from .from_iter() or .collect()");
			}
		}

		Self { vals }
	}
}
