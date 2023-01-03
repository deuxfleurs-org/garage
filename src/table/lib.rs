#![recursion_limit = "1024"]
#![allow(clippy::comparison_chain)]

#[macro_use]
extern crate tracing;

pub mod schema;
pub mod util;

pub mod data;
pub mod replication;
pub mod table;

mod gc;
mod merkle;
mod metrics;
mod queue;
mod sync;

pub use schema::*;
pub use table::*;
pub use util::*;

pub mod crdt {
	pub use garage_util::crdt::*;
}
