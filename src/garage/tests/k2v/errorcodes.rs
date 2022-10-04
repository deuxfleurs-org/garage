use crate::common;

use hyper::Method;

#[tokio::test]
async fn test_error_codes() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("test-k2v-error-codes");

	// Regular insert should work (code 200)
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
	assert_eq!(res.status(), 200);

	// Insert with trash causality token: invalid request
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.method(Method::PUT)
		.path("root")
		.query_param("sort_key", Some("test1"))
		.signed_header("x-garage-causality-token", "tra$sh")
		.body(b"Hello, world!".to_vec())
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), 400);

	// Search without partition key: invalid request
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.query_param("search", Option::<&str>::None)
		.body(
			br#"[
	{},
		]"#
			.to_vec(),
		)
		.method(Method::POST)
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), 400);

	// Search with start that is not in prefix: invalid request
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.query_param("search", Option::<&str>::None)
		.body(
			br#"[
	{"partition_key": "root", "prefix": "a", "start": "bx"},
		]"#
			.to_vec(),
		)
		.method(Method::POST)
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), 400);

	// Search with invalid json: 400
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.query_param("search", Option::<&str>::None)
		.body(
			br#"[
	{"partition_key": "root"
		]"#
			.to_vec(),
		)
		.method(Method::POST)
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), 400);

	// Batch insert with invalid causality token: 400
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.body(
			br#"[
	{"pk": "root", "sk": "a", "ct": "tra$h", "v": "aGVsbG8sIHdvcmxkCg=="}
		]"#
			.to_vec(),
		)
		.method(Method::POST)
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), 400);

	// Batch insert with invalid data: 400
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.body(
			br#"[
	{"pk": "root", "sk": "a", "ct": null, "v": "aGVsbG8sIHdvcmx$Cg=="}
		]"#
			.to_vec(),
		)
		.method(Method::POST)
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), 400);

	// Poll with invalid causality token: 400
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.path("root")
		.query_param("sort_key", Some("test1"))
		.query_param("causality_token", Some("tra$h"))
		.query_param("timeout", Some("10"))
		.signed_header("accept", "application/octet-stream")
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), 400);
}
