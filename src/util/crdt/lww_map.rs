use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::time::now_msec;

use crate::crdt::crdt::*;

/// Last Write Win Map
///
/// This types defines a CRDT for a map from keys to values.
/// The values have an associated timestamp, such that the last written value
/// takes precedence over previous ones. As for the simpler `LWW` type, the value
/// type `V` is also required to implement the CRDT trait.
/// We do not encourage mutating the values associated with a given key
/// without updating the timestamp, in fact at the moment we do not provide a `.get_mut()`
/// method that would allow that.
///
/// Internally, the map is stored as a vector of keys and values, sorted by ascending key order.
/// This is why the key type `K` must implement `Ord` (and also to ensure a unique serialization,
/// such that two values can be compared for equality based on their hashes). As a consequence,
/// insertions take `O(n)` time. This means that LWWMap should be used for reasonably small maps.
/// However, note that even if we were using a more efficient data structure such as a `BTreeMap`,
/// the serialization cost `O(n)` would still have to be paid at each modification, so we are
/// actually not losing anything here.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LwwMap<K, V> {
	vals: Vec<(K, u64, V)>,
}

impl<K, V> LwwMap<K, V>
where
	K: Clone + Ord,
	V: Clone + Crdt,
{
	/// Create a new empty map CRDT
	pub fn new() -> Self {
		Self { vals: vec![] }
	}

	/// This produces a map that contains a single item with the specified timestamp.
	///
	/// Used to migrate from a map defined in an incompatible format. Do this as many
	/// times as you have items to migrate, and put them all together using the
	/// CRDT merge operator.
	pub fn raw_item(k: K, ts: u64, v: V) -> Self {
		Self {
			vals: vec![(k, ts, v)],
		}
	}
	/// Returns a map that contains a single mapping from the specified key to the specified value.
	/// This map is a mutator, or a delta-CRDT, such that when it is merged with the original map,
	/// the previous value will be replaced with the one specified here.
	/// The timestamp in the provided mutator is set to the maximum of the current system's clock
	/// and 1 + the previous value's timestamp (if there is one), so that the new value will always
	/// take precedence (LWW rule).
	///
	/// Typically, to update the value associated to a key in the map, you would do the following:
	///
	/// ```ignore
	/// let my_update = my_crdt.update_mutator(key_to_modify, new_value);
	/// my_crdt.merge(&my_update);
	/// ```
	///
	/// However extracting the mutator on its own and only sending that on the network is very
	/// interesting as it is much smaller than the whole map.
	#[must_use = "CRDT mutators are meant to be merged into a CRDT and not ignored."]
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

	/// Updates a value in place in the map (this generates
	/// a new timestamp)
	pub fn update_in_place(&mut self, k: K, new_v: V) {
		self.merge(&self.update_mutator(k, new_v));
	}

	/// Updates a value in place in the map, from a
	/// (key, timestamp, value) triple, only if the given
	/// timestamp is larger than the timestamp currently
	/// in the map
	pub fn merge_raw(&mut self, k: &K, ts2: u64, v2: &V) {
		match self.vals.binary_search_by(|(k2, _, _)| k2.cmp(k)) {
			Ok(i) => {
				let (_, ts1, _v1) = &self.vals[i];
				match ts2.cmp(ts1) {
					Ordering::Greater => {
						self.vals[i].1 = ts2;
						self.vals[i].2 = v2.clone();
					}
					Ordering::Equal => {
						self.vals[i].2.merge(v2);
					}
					Ordering::Less => (),
				}
			}
			Err(i) => {
				self.vals.insert(i, (k.clone(), ts2, v2.clone()));
			}
		}
	}

	/// Takes all of the values of the map and returns them. The current map is reset to the
	/// empty map. This is very useful to produce in-place a new map that contains only a delta
	/// that modifies a certain value:
	///
	/// ```ignore
	/// let mut a = get_my_crdt_value();
	/// let old_a = a.take_and_clear();
	/// a.merge(&old_a.update_mutator(key_to_modify, new_value));
	/// put_my_crdt_value(a);
	/// ```
	///
	/// Of course in this simple example we could have written simply
	/// `pyt_my_crdt_value(a.update_mutator(key_to_modify, new_value))`,
	/// but in the case where the map is a field in a struct for instance (as is always the case),
	/// this becomes very handy:
	///
	/// ```ignore
	/// let mut a = get_my_crdt_value();
	/// let old_a_map = a.map_field.take_and_clear();
	/// a.map_field.merge(&old_a_map.update_mutator(key_to_modify, new_value));
	/// put_my_crdt_value(a);
	/// ```
	pub fn take_and_clear(&mut self) -> Self {
		let vals = std::mem::take(&mut self.vals);
		Self { vals }
	}

	/// Removes all values from the map
	pub fn clear(&mut self) {
		self.vals.clear();
	}

	/// Retain only values that match a certain predicate
	pub fn retain(&mut self, pred: impl FnMut(&(K, u64, V)) -> bool) {
		self.vals.retain(pred);
	}

	/// Get a reference to the value assigned to a key
	pub fn get(&self, k: &K) -> Option<&V> {
		match self.vals.binary_search_by(|(k2, _, _)| k2.cmp(k)) {
			Ok(i) => Some(&self.vals[i].2),
			Err(_) => None,
		}
	}

	/// Get the timestamp of the value assigned to a key, or 0 if
	/// no value is assigned
	pub fn get_timestamp(&self, k: &K) -> u64 {
		match self.vals.binary_search_by(|(k2, _, _)| k2.cmp(k)) {
			Ok(i) => self.vals[i].1,
			Err(_) => 0,
		}
	}

	/// Gets a reference to all of the items, as a slice. Useful to iterate on all map values.
	/// In most case you will want to ignore the timestamp (second item of the tuple).
	pub fn items(&self) -> &[(K, u64, V)] {
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

impl<K, V> Crdt for LwwMap<K, V>
where
	K: Clone + Ord,
	V: Clone + Crdt,
{
	fn merge(&mut self, other: &Self) {
		for (k, ts2, v2) in other.vals.iter() {
			self.merge_raw(k, *ts2, v2);
		}
	}
}

impl<K, V> Default for LwwMap<K, V>
where
	K: Clone + Ord,
	V: Clone + Crdt,
{
	fn default() -> Self {
		Self::new()
	}
}
