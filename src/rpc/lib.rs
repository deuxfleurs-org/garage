//! Crate containing rpc related functions and types used in Garage

#[macro_use]
extern crate log;

mod consul;

pub mod ring;
pub mod system;

pub mod rpc_helper;

pub use rpc_helper::*;
