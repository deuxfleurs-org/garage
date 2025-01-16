use std::cmp::Ordering;

use serde::{Deserialize, Serialize};

use crate::time::now_msec;

use crate::crdt::crdt::*;

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
/// that implement the `Ord` trait get a default CRDT implementation that keeps the maximum value.
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
/// coordinate themself faster than the time difference between two NTP servers.
///
/// As a more concrete example, let's suppose you want to upload a file
/// with the same key (path) in the same bucket at the very same time.
/// For each request, the file will be timestamped by the receiving server
/// and may differ from what you observed with your atomic clock!
///
/// This scheme is used by AWS S3 or Soundcloud and often without knowing
/// in enterprise when reconciliating databases with ad-hoc scripts.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Lww<T> {
	ts: u64,
	v: T,
}

impl<T> Lww<T>
where
	T: Crdt,
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

	/// Build a new LWW CRDT from its raw pieces: a timestamp and the value
	pub fn raw(ts: u64, value: T) -> Self {
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

	/// Get the timestamp currently associated with the value
	pub fn timestamp(&self) -> u64 {
		self.ts
	}

	/// Get the CRDT value
	pub fn get(&self) -> &T {
		&self.v
	}

	/// Take the value inside the CRDT (discards the timestamp)
	pub fn take(self) -> T {
		self.v
	}

	/// Get a mutable reference to the CRDT's value
	///
	/// This is useful to mutate the inside value without changing the LWW timestamp.
	/// When such mutation is done, the merge between two LWW values is done using the inner
	/// CRDT's merge operation. This is useful in the case where the inner CRDT is a large
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

impl<T> Crdt for Lww<T>
where
	T: Clone + Crdt,
{
	fn merge(&mut self, other: &Self) {
		match other.ts.cmp(&self.ts) {
			Ordering::Greater => {
				self.ts = other.ts;
				self.v = other.v.clone();
			}
			Ordering::Equal => {
				self.v.merge(&other.v);
			}
			Ordering::Less => (),
		}
	}
}

impl<T> Default for Lww<T>
where
	T: Default,
{
	fn default() -> Self {
		Self {
			ts: 0,
			v: T::default(),
		}
	}
}
