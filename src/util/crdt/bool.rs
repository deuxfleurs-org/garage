use serde::{Deserialize, Serialize};

use crate::crdt::crdt::*;

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

impl From<bool> for Bool {
	fn from(b: bool) -> Bool {
		Bool::new(b)
	}
}

impl Crdt for Bool {
	fn merge(&mut self, other: &Self) {
		self.0 = self.0 || other.0;
	}
}
