use serde::{Deserialize, Serialize};

use crate::crdt::Crdt;

#[derive(Serialize, Deserialize, Clone, Default, Copy, PartialEq, Eq, Debug)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(transparent)]
pub struct CancelingOption<T>(pub Option<T>);

/// `CancelingOption<T>` implements Crdt for any type T, even if T doesn't implement CRDT itself: when
/// different values are detected, they are always merged to None.  This can be used for value
/// types which shoulnd't be merged, instead of trying to merge things when we know we don't want
/// to merge them (which is what the `AutoCrdt` trait is used for most of the time). This cases
/// arises very often, for example with a Lww or a `LwwMap`: the value type has to be a CRDT so that
/// we have a rule for what to do when timestamps aren't enough to disambiguate (in a distributed
/// system, anything can happen!), and with `AutoCrdt` the rule is to make an arbitrary (but
/// deterministic) choice between the two.  When using an `CancelingOption<T>` instead with this impl, ambiguity
/// cases are explicitly stored as None, which allows us to detect the ambiguity and handle it in
/// the way we want. (this can only work if we are happy with losing the value when an ambiguity
/// arises)
impl<T> Crdt for CancelingOption<T>
where
	T: Eq + Clone,
{
	fn merge(&mut self, other: &Self) {
		if self.0 != other.0 {
			self.0 = None;
		}
	}
}

impl<T> CancelingOption<T> {
	pub fn inner(&self) -> Option<&T> {
		self.0.as_ref()
	}

	pub fn into_inner(self) -> Option<T> {
		self.0
	}

	pub fn map<U>(self, f: impl FnOnce(T) -> U) -> CancelingOption<U> {
		CancelingOption(self.0.map(f))
	}
}

impl<T> From<Option<T>> for CancelingOption<T> {
	fn from(x: Option<T>) -> Self {
		Self(x)
	}
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, Copy, Default)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
#[serde(transparent)]
pub struct MergingOption<T>(pub Option<T>);

/// `MergingOption<T>` implements `Crdt` when `T` implements `Crdt`:
/// None is a bottom value, and different Some values get merged according
/// to their Crdt operator.
impl<T> Crdt for MergingOption<T>
where
	T: Crdt + Clone,
{
	fn merge(&mut self, other: &Self) {
		if let (Some(a), Some(b)) = (self.0.as_mut(), other.0.as_ref()) {
			a.merge(b);
		} else {
			self.0 = self.0.take().or_else(|| other.0.clone());
		}
	}
}

impl<T> MergingOption<T> {
	pub fn inner(&self) -> Option<&T> {
		self.0.as_ref()
	}

	pub fn into_inner(self) -> Option<T> {
		self.0
	}

	pub fn map<U>(self, f: impl FnOnce(T) -> U) -> MergingOption<U> {
		MergingOption(self.0.map(f))
	}
}

impl<T> From<Option<T>> for MergingOption<T> {
	fn from(x: Option<T>) -> Self {
		Self(x)
	}
}
