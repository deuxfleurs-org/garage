//! Crate for serving a S3 compatible API
#[macro_use]
extern crate tracing;

pub mod error;
pub use error::Error;

mod encoding;
mod generic_server;
pub mod helpers;
mod router_macros;
/// This mode is public only to help testing. Don't expect stability here
pub mod signature;

#[cfg(feature = "k2v")]
pub mod k2v;
pub mod s3;
