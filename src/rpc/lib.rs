//! Crate containing rpc related functions and types used in Garage

#[macro_use]
extern crate log;

mod consul;
pub(crate) mod tls_util;

pub mod membership;
pub mod ring;

pub mod rpc_client;
pub mod rpc_server;
