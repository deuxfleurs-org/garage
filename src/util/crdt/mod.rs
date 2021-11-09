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

mod bool;
#[allow(clippy::module_inception)]
mod crdt;
mod lww;
mod lww_map;
mod map;

pub use self::bool::*;
pub use crdt::*;
pub use lww::*;
pub use lww_map::*;
pub use map::*;
