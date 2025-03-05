#[macro_use]
extern crate tracing;

pub mod api_server;
pub mod error;

mod bucket;
mod copy;
pub mod cors;
mod delete;
pub mod get;
mod lifecycle;
mod list;
mod multipart;
mod post_object;
mod put;
pub mod website;

mod encryption;
mod router;
pub mod xml;
