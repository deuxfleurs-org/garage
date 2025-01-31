//! Crate for serving a S3 compatible API
#[macro_use]
extern crate tracing;

pub mod common_error;

pub mod encoding;
pub mod generic_server;
pub mod helpers;
pub mod router_macros;
/// This mode is public only to help testing. Don't expect stability here
pub mod signature;
