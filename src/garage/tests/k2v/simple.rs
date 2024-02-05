use crate::common;

use http_body_util::BodyExt;
use hyper::{Method, StatusCode};

#[tokio::test]
async fn test_simple() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("test-k2v-simple");

	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.method(Method::PUT)
		.path("root")
		.query_param("sort_key", Some("test1"))
		.body(b"Hello, world!".to_vec())
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), StatusCode::NO_CONTENT);

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
	assert_eq!(res2.status(), StatusCode::OK);

	let res2_body = res2.into_body().collect().await.unwrap().to_bytes();
	assert_eq!(res2_body, b"Hello, world!"[..]);
}
