//! Crate for serving a S3 compatible API
#[macro_use]
extern crate log;

pub mod error;
pub use error::Error;

mod encoding;

mod api_server;
pub use api_server::run_api_server;

mod signature;

pub mod helpers;
mod s3_bucket;
mod s3_copy;
pub mod s3_cors;
mod s3_delete;
pub mod s3_get;
mod s3_list;
mod s3_post_object;
mod s3_put;
mod s3_router;
mod s3_website;
mod s3_xml;
