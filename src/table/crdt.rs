//! This package provides a simple implementation of conflict-free replicated data types (CRDTs)
//!
//! CRDTs are a type of data structures that do not require coordination.  In other words, we can
//! edit them in parallel, we will always find a way to merge it.
//!
//! A general example is a counter. Its initial value is 0.  Alice and Bob get a copy of the
//! counter.  Alice does +1 on her copy, she reads 1.  Bob does +3 on his copy, he reads 3.  Now,
//! it is easy to merge their counters, order does not count: we always get 4.
//!
//! Learn more about CRDT [on Wikipedia](https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type)

use serde::{Deserialize, Serialize};

use garage_util::data::*;

/// Definition of a CRDT - all CRDT Rust types implement this.
///
/// A CRDT is defined as a merge operator that respects a certain set of axioms.
///
/// In particular, the merge operator must be commutative, associative,
/// idempotent, and monotonic.
/// In other words, if `a`, `b` and `c` are CRDTs, and `⊔` denotes the merge operator,
/// the following axioms must apply:
///
/// ```text
/// a ⊔ b = b ⊔ a                   (commutativity)
/// (a ⊔ b) ⊔ c = a ⊔ (b ⊔ c)       (associativity)
/// (a ⊔ b) ⊔ b = a ⊔ b             (idempotence)
/// ```
///
/// Moreover, the relationship `≥` defined by `a ≥ b ⇔ ∃c. a = b ⊔ c` must be a partial order.
/// This implies a few properties such as: if `a ⊔ b ≠ a`, then there is no `c` such that `(a ⊔ b) ⊔ c = a`,
/// as this would imply a cycle in the partial order.
pub trait CRDT {
	/// Merge the two datastructures according to the CRDT rules.
	/// `self` is modified to contain the merged CRDT value. `other` is not modified.
	///
	/// # Arguments
	///
	/// * `other` - the other CRDT we wish to merge with
	fn merge(&mut self, other: &Self);
}

/// All types that implement `Ord` (a total order) also implement a trivial CRDT
/// defined by the merge rule: `a ⊔ b = max(a, b)`.
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
/// An LWW CRDT associates a timestamp with a value, in order to implement a
/// time-based reconciliation rule: the most recent write wins.
/// For completeness, the LWW reconciliation rule must also be defined for two LWW CRDTs
/// with the same timestamp but different values.
///
/// In our case, we add the constraint that the value that is wrapped inside the LWW CRDT must
/// itself be a CRDT: in the case when the timestamp does not allow us to decide on which value to
/// keep, the merge rule of the inner CRDT is applied on the wrapped values.  (Note that all types
/// that implement the `Ord` trait get a default CRDT implemetnation that keeps the maximum value.
/// This enables us to use LWW directly with primitive data types such as numbers or strings. It is
/// generally desirable in this case to never explicitly produce LWW values with the same timestamp
/// but different inner values, as the rule to keep the maximum value isn't generally the desired
/// semantics.)
///
/// As multiple computers clocks are always desynchronized,
/// when operations are close enough, it is equivalent to
/// take one copy and drop the other one.
///
/// Given that clocks are not too desynchronized, this assumption
/// is enough for most cases, as there is few chance that two humans
/// coordonate themself faster than the time difference between two NTP servers.
///
/// As a more concret example, let's suppose you want to upload a file
/// with the same key (path) in the same bucket at the very same time.
/// For each request, the file will be timestamped by the receiving server
/// and may differ from what you observed with your atomic clock!
///
/// This scheme is used by AWS S3 or Soundcloud and often without knowing
/// in entreprise when reconciliating databases with ad-hoc scripts.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LWW<T> {
	ts: u64,
	v: T,
}

impl<T> LWW<T>
where
	T: CRDT,
{
	/// Creates a new CRDT
	///
	/// CRDT's internal timestamp is set with current node's clock.
	pub fn new(value: T) -> Self {
		Self {
			ts: now_msec(),
			v: value,
		}
	}

	/// Build a new CRDT from a previous non-compatible one
	///
	/// Compared to new, the CRDT's timestamp is not set to now
	/// but must be set to the previous, non-compatible, CRDT's timestamp.
	pub fn migrate_from_raw(ts: u64, value: T) -> Self {
		Self { ts, v: value }
	}

	/// Update the LWW CRDT while keeping some causal ordering.
	///
	/// The timestamp of the LWW CRDT is updated to be the current node's clock
	/// at time of update, or the previous timestamp + 1 if that's bigger,
	/// so that the new timestamp is always strictly larger than the previous one.
	/// This ensures that merging the update with the old value will result in keeping
	/// the updated value.
	pub fn update(&mut self, new_value: T) {
		self.ts = std::cmp::max(self.ts + 1, now_msec());
		self.v = new_value;
	}

	/// Get the CRDT value
	pub fn get(&self) -> &T {
		&self.v
	}

	/// Get a mutable reference to the CRDT's value
	///
	/// This is usefull to mutate the inside value without changing the LWW timestamp.
	/// When such mutation is done, the merge between two LWW values is done using the inner
	/// CRDT's merge operation. This is usefull in the case where the inner CRDT is a large
	/// data type, such as a map, and we only want to change a single item in the map.
	/// To do this, we can produce a "CRDT delta", i.e. a LWW that contains only the modification.
	/// This delta consists in a LWW with the same timestamp, and the map
	/// inside only contains the updated value.
	/// The advantage of such a delta is that it is much smaller than the whole map.
	///
	/// Avoid using this if the inner data type is a primitive type such as a number or a string,
	/// as you will then rely on the merge function defined on `Ord` types by keeping the maximum
	/// of both values.
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

/// Boolean, where `true` is an absorbing state
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq)]
pub struct Bool(bool);

impl Bool {
	/// Create a new boolean with the specified value
	pub fn new(b: bool) -> Self {
		Self(b)
	}
	/// Set the boolean to true
	pub fn set(&mut self) {
		self.0 = true;
	}
	/// Get the boolean value
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
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LWWMap<K, V> {
	vals: Vec<(K, u64, V)>,
}

impl<K, V> LWWMap<K, V>
where
	K: Ord,
	V: CRDT,
{
	/// Create a new empty map CRDT
	pub fn new() -> Self {
		Self { vals: vec![] }
	}
	/// Used to migrate from a map defined in an incompatible format. This produces
	/// a map that contains a single item with the specified timestamp (copied from
	/// the incompatible format). Do this as many times as you have items to migrate,
	/// and put them all together using the CRDT merge operator.
	pub fn migrate_from_raw_item(k: K, ts: u64, v: V) -> Self {
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
	/// Takes all of the values of the map and returns them. The current map is reset to the
	/// empty map. This is very usefull to produce in-place a new map that contains only a delta
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
		let vals = std::mem::replace(&mut self.vals, vec![]);
		Self { vals }
	}
	/// Removes all values from the map
	pub fn clear(&mut self) {
		self.vals.clear();
	}
	/// Get a reference to the value assigned to a key
	pub fn get(&self, k: &K) -> Option<&V> {
		match self.vals.binary_search_by(|(k2, _, _)| k2.cmp(&k)) {
			Ok(i) => Some(&self.vals[i].2),
			Err(_) => None,
		}
	}
	/// Gets a reference to all of the items, as a slice. Usefull to iterate on all map values.
	/// In most case you will want to ignore the timestamp (second item of the tuple).
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
