//! Crate containing rpc related functions and types used in Garage

#[macro_use]
extern crate tracing;

#[cfg(feature = "consul-discovery")]
mod consul;
#[cfg(feature = "kubernetes-discovery")]
mod kubernetes;

pub mod layout;
pub mod ring;
pub mod system;

mod metrics;
pub mod rpc_helper;

pub use rpc_helper::*;
