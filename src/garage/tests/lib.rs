#[macro_use]
mod common;

mod admin;
mod bucket;

#[cfg(feature = "k2v")]
mod k2v;

mod s3;
