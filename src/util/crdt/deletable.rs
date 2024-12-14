use serde::{Deserialize, Serialize};

use crate::crdt::crdt::*;

/// Deletable object (once deleted, cannot go back)
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum Deletable<T> {
	Present(T),
	Deleted,
}

impl<T> Deletable<T> {
	/// Map value, used for migrations
	pub fn map<U, F: FnOnce(T) -> U>(self, f: F) -> Deletable<U> {
		match self {
			Self::Present(x) => Deletable::<U>::Present(f(x)),
			Self::Deleted => Deletable::<U>::Deleted,
		}
	}
}

impl<T: Crdt> Deletable<T> {
	/// Create a new deletable object that isn't deleted
	pub fn present(v: T) -> Self {
		Self::Present(v)
	}
	/// Create a new deletable object that is deleted
	pub fn delete() -> Self {
		Self::Deleted
	}
	/// As option
	pub fn as_option(&self) -> Option<&T> {
		match self {
			Self::Present(v) => Some(v),
			Self::Deleted => None,
		}
	}
	/// As option, mutable
	pub fn as_option_mut(&mut self) -> Option<&mut T> {
		match self {
			Self::Present(v) => Some(v),
			Self::Deleted => None,
		}
	}
	/// Into option
	pub fn into_option(self) -> Option<T> {
		match self {
			Self::Present(v) => Some(v),
			Self::Deleted => None,
		}
	}
	/// Is object deleted?
	pub fn is_deleted(&self) -> bool {
		matches!(self, Self::Deleted)
	}
}

impl<T> From<Option<T>> for Deletable<T> {
	fn from(v: Option<T>) -> Self {
		v.map(Self::Present).unwrap_or(Self::Deleted)
	}
}

impl<T> From<Deletable<T>> for Option<T> {
	fn from(v: Deletable<T>) -> Option<T> {
		match v {
			Deletable::Present(v) => Some(v),
			Deletable::Deleted => None,
		}
	}
}

impl<T: Crdt> Crdt for Deletable<T> {
	fn merge(&mut self, other: &Self) {
		if let Deletable::Present(v) = self {
			match other {
				Deletable::Deleted => *self = Deletable::Deleted,
				Deletable::Present(v2) => v.merge(v2),
			}
		}
	}
}
