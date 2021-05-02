#![recursion_limit = "1024"]
#![allow(clippy::comparison_chain)]

#[macro_use]
extern crate log;

pub mod crdt;
pub mod schema;
pub mod util;

pub mod data;
mod gc;
mod merkle;
pub mod replication;
mod sync;
pub mod table;

pub use schema::*;
pub use table::*;
pub use util::*;
