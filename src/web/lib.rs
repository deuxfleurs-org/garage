//! Crate for handling web serving of s3 bucket
#[macro_use]
extern crate tracing;

mod error;
pub use error::Error;

mod web_server;
pub use web_server::WebServer;
