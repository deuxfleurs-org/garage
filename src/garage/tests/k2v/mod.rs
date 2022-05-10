pub mod batch;
pub mod errorcodes;
pub mod item;
pub mod poll;
pub mod simple;

use hyper::{Body, Response};

pub async fn json_body(res: Response<Body>) -> serde_json::Value {
	let res_body: serde_json::Value = serde_json::from_slice(
		&hyper::body::to_bytes(res.into_body())
			.await
			.unwrap()
			.to_vec()[..],
	)
	.unwrap();
	res_body
}
