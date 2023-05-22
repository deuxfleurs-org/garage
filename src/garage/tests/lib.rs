#[macro_use]
mod common;

mod admin;
mod bucket;

mod s3;

#[cfg(feature = "k2v")]
mod k2v;
#[cfg(feature = "k2v")]
mod k2v_client;
