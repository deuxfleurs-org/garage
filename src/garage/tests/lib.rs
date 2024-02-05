#[macro_use]
mod common;

mod admin;
mod bucket;

mod s3;

#[cfg(feature = "k2v")]
mod k2v;
#[cfg(feature = "k2v")]
mod k2v_client;

use hyper::{body::HttpBody, Body, Response};

pub async fn json_body(res: Response<Body>) -> serde_json::Value {
	let body = res.into_body().collect().await.unwrap().to_bytes();
	let res_body: serde_json::Value = serde_json::from_slice(&body).unwrap();
	res_body
}
