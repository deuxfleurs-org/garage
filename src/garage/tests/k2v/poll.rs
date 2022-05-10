use hyper::Method;
use std::time::Duration;

use crate::common;

#[tokio::test]
async fn test_poll() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("test-k2v-poll");

	// Write initial value
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.method(Method::PUT)
		.path("root")
		.query_param("sort_key", Some("test1"))
		.body(b"Initial value".to_vec())
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), 200);

	// Retrieve initial value to get its causality token
	let res2 = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.path("root")
		.query_param("sort_key", Some("test1"))
		.signed_header("accept", "application/octet-stream")
		.send()
		.await
		.unwrap();
	assert_eq!(res2.status(), 200);
	let ct = res2
		.headers()
		.get("x-garage-causality-token")
		.unwrap()
		.to_str()
		.unwrap()
		.to_string();

	let res2_body = hyper::body::to_bytes(res2.into_body())
		.await
		.unwrap()
		.to_vec();
	assert_eq!(res2_body, b"Initial value");

	// Start poll operation
	let poll = {
		let bucket = bucket.clone();
		let ct = ct.clone();
		tokio::spawn(async move {
			let ctx = common::context();
			ctx.k2v
				.request
				.builder(bucket.clone())
				.path("root")
				.query_param("sort_key", Some("test1"))
				.query_param("causality_token", Some(ct))
				.query_param("timeout", Some("10"))
				.signed_header("accept", "application/octet-stream")
				.send()
				.await
		})
	};

	// Write new value that supersedes initial one
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.method(Method::PUT)
		.path("root")
		.query_param("sort_key", Some("test1"))
		.signed_header("x-garage-causality-token", ct)
		.body(b"New value".to_vec())
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), 200);

	// Check poll finishes with correct value
	let poll_res = tokio::select! {
		_ = tokio::time::sleep(Duration::from_secs(10)) => panic!("poll did not terminate in time"),
		res = poll => res.unwrap().unwrap(),
	};

	assert_eq!(poll_res.status(), 200);

	let poll_res_body = hyper::body::to_bytes(poll_res.into_body())
		.await
		.unwrap()
		.to_vec();
	assert_eq!(poll_res_body, b"New value");
}
