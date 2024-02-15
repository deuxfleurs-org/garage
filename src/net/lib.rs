//! Netapp is a Rust library that takes care of a few common tasks in distributed software:
//!
//! - establishing secure connections
//! - managing connection lifetime, reconnecting on failure
//! - checking peer's state
//! - peer discovery
//! - query/response message passing model for communications
//! - multiplexing transfers over a connection
//! - overlay networks: full mesh, and soon other methods
//!
//! Of particular interest, read the documentation for the `netapp::NetApp` type,
//! the `message::Message` trait, and `proto::RequestPriority` to learn more
//! about message priorization.
//! Also check out the examples to learn how to use this crate.

pub mod bytes_buf;
pub mod error;
pub mod stream;
pub mod util;

pub mod endpoint;
pub mod message;

mod client;
mod recv;
mod send;
mod server;

pub mod netapp;
pub mod peering;

pub use crate::netapp::*;

#[cfg(test)]
mod test;
