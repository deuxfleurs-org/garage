//! Crate containing rpc related functions and types used in Garage

#[macro_use]
extern crate tracing;

mod metrics;
mod system_metrics;

#[cfg(feature = "consul-discovery")]
mod consul;
#[cfg(feature = "consul-service-discovery")]
mod consul_services;
#[cfg(feature = "kubernetes-discovery")]
mod kubernetes;

pub mod layout;
pub mod replication_mode;
pub mod ring;
pub mod system;

pub mod rpc_helper;

pub use rpc_helper::*;
