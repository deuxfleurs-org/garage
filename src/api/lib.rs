//! Crate for serving a S3 compatible API
#[macro_use]
extern crate log;

mod error;
pub use error::Error;

mod encoding;

mod api_server;
pub use api_server::run_api_server;

mod signature;

pub mod helpers;
mod s3_bucket;
mod s3_copy;
mod s3_delete;
pub mod s3_get;
mod s3_list;
mod s3_put;
mod s3_router;
mod s3_xml;
