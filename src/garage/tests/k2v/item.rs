use std::time::Duration;

use crate::common;

use assert_json_diff::assert_json_eq;
use base64::prelude::*;
use serde_json::json;

use crate::json_body;
use http_body_util::BodyExt;
use hyper::{Method, StatusCode};

#[tokio::test]
async fn test_items_and_indices() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("test-k2v-item-and-index");

	// ReadIndex -- there should be nothing
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.send()
		.await
		.unwrap();
	let res_body = json_body(res).await;
	assert_json_eq!(
		res_body,
		json!({
			"prefix": null,
			"start": null,
			"end": null,
			"limit": null,
			"reverse": false,
			"partitionKeys": [],
			"more": false,
			"nextStart": null
		})
	);

	let content2_len = "_: hello universe".len();
	let content3_len = "_: concurrent value".len();

	for (i, sk) in ["a", "b", "c", "d"].iter().enumerate() {
		let content = format!("{}: hello world", sk).into_bytes();
		let content2 = format!("{}: hello universe", sk).into_bytes();
		let content3 = format!("{}: concurrent value", sk).into_bytes();
		eprintln!("test iteration {}: {}", i, sk);

		// Put initially, no causality token
		let res = ctx
			.k2v
			.request
			.builder(bucket.clone())
			.path("root")
			.query_param("sort_key", Some(sk))
			.body(content.clone())
			.method(Method::PUT)
			.send()
			.await
			.unwrap();
		assert_eq!(res.status(), StatusCode::NO_CONTENT);

		// Get value back
		let res = ctx
			.k2v
			.request
			.builder(bucket.clone())
			.path("root")
			.query_param("sort_key", Some(sk))
			.signed_header("accept", "*/*")
			.send()
			.await
			.unwrap();
		assert_eq!(res.status(), StatusCode::OK);
		assert_eq!(
			res.headers().get("content-type").unwrap().to_str().unwrap(),
			"application/octet-stream"
		);
		let ct = res
			.headers()
			.get("x-garage-causality-token")
			.unwrap()
			.to_str()
			.unwrap()
			.to_string();
		let res_body = res.into_body().collect().await.unwrap().to_bytes();
		assert_eq!(res_body, content);

		// ReadIndex -- now there should be some stuff
		tokio::time::sleep(Duration::from_millis(100)).await;
		let res = ctx
			.k2v
			.request
			.builder(bucket.clone())
			.send()
			.await
			.unwrap();
		let res_body = json_body(res).await;
		assert_json_eq!(
			res_body,
			json!({
				"prefix": null,
				"start": null,
				"end": null,
				"limit": null,
				"reverse": false,
				"partitionKeys": [
				{
					"pk": "root",
					"entries": i+1,
					"conflicts": i,
					"values": i+i+1,
					"bytes": i*(content2.len() + content3.len()) + content.len(),
				}
				],
				"more": false,
				"nextStart": null
			})
		);

		// Put again, this time with causality token
		let res = ctx
			.k2v
			.request
			.builder(bucket.clone())
			.path("root")
			.query_param("sort_key", Some(sk))
			.signed_header("x-garage-causality-token", ct.clone())
			.body(content2.clone())
			.method(Method::PUT)
			.send()
			.await
			.unwrap();
		assert_eq!(res.status(), StatusCode::NO_CONTENT);

		// Get value back
		let res = ctx
			.k2v
			.request
			.builder(bucket.clone())
			.path("root")
			.query_param("sort_key", Some(sk))
			.signed_header("accept", "*/*")
			.send()
			.await
			.unwrap();
		assert_eq!(res.status(), StatusCode::OK);
		assert_eq!(
			res.headers().get("content-type").unwrap().to_str().unwrap(),
			"application/octet-stream"
		);
		let res_body = res.into_body().collect().await.unwrap().to_bytes();
		assert_eq!(res_body, content2);

		// ReadIndex -- now there should be some stuff
		tokio::time::sleep(Duration::from_millis(100)).await;
		let res = ctx
			.k2v
			.request
			.builder(bucket.clone())
			.send()
			.await
			.unwrap();
		let res_body = json_body(res).await;
		assert_json_eq!(
			res_body,
			json!({
				"prefix": null,
				"start": null,
				"end": null,
				"limit": null,
				"reverse": false,
				"partitionKeys": [
				{
					"pk": "root",
					"entries": i+1,
					"conflicts": i,
					"values": i+i+1,
					"bytes": i*content3.len() + (i+1)*content2.len(),
				}
				],
				"more": false,
				"nextStart": null
			})
		);

		// Put again with same CT, now we have concurrent values
		let res = ctx
			.k2v
			.request
			.builder(bucket.clone())
			.path("root")
			.query_param("sort_key", Some(sk))
			.signed_header("x-garage-causality-token", ct.clone())
			.body(content3.clone())
			.method(Method::PUT)
			.send()
			.await
			.unwrap();
		assert_eq!(res.status(), StatusCode::NO_CONTENT);

		// Get value back
		let res = ctx
			.k2v
			.request
			.builder(bucket.clone())
			.path("root")
			.query_param("sort_key", Some(sk))
			.signed_header("accept", "*/*")
			.send()
			.await
			.unwrap();
		assert_eq!(res.status(), StatusCode::OK);
		assert_eq!(
			res.headers().get("content-type").unwrap().to_str().unwrap(),
			"application/json"
		);
		let res_json = json_body(res).await;
		assert_json_eq!(
			res_json,
			[
				BASE64_STANDARD.encode(&content2),
				BASE64_STANDARD.encode(&content3)
			]
		);

		// ReadIndex -- now there should be some stuff
		tokio::time::sleep(Duration::from_millis(100)).await;
		let res = ctx
			.k2v
			.request
			.builder(bucket.clone())
			.send()
			.await
			.unwrap();
		let res_body = json_body(res).await;
		assert_json_eq!(
			res_body,
			json!({
				"prefix": null,
				"start": null,
				"end": null,
				"limit": null,
				"reverse": false,
				"partitionKeys": [
				{
					"pk": "root",
					"entries": i+1,
					"conflicts": i+1,
					"values": 2*(i+1),
					"bytes": (i+1)*(content2.len() + content3.len()),
				}
				],
				"more": false,
				"nextStart": null
			})
		);
	}

	// Now delete things
	for (i, sk) in ["a", "b", "c", "d"].iter().enumerate() {
		// Get value back (we just need the CT)
		let res = ctx
			.k2v
			.request
			.builder(bucket.clone())
			.path("root")
			.query_param("sort_key", Some(sk))
			.signed_header("accept", "*/*")
			.send()
			.await
			.unwrap();
		assert_eq!(res.status(), StatusCode::OK);
		let ct = res
			.headers()
			.get("x-garage-causality-token")
			.unwrap()
			.to_str()
			.unwrap()
			.to_string();

		// Delete it
		let res = ctx
			.k2v
			.request
			.builder(bucket.clone())
			.method(Method::DELETE)
			.path("root")
			.query_param("sort_key", Some(sk))
			.signed_header("x-garage-causality-token", ct)
			.send()
			.await
			.unwrap();
		assert_eq!(res.status(), StatusCode::NO_CONTENT);

		// ReadIndex -- now there should be some stuff
		tokio::time::sleep(Duration::from_millis(100)).await;
		let res = ctx
			.k2v
			.request
			.builder(bucket.clone())
			.send()
			.await
			.unwrap();
		let res_body = json_body(res).await;
		if i < 3 {
			assert_json_eq!(
				res_body,
				json!({
					"prefix": null,
					"start": null,
					"end": null,
					"limit": null,
					"reverse": false,
					"partitionKeys": [
					{
						"pk": "root",
						"entries": 3-i,
						"conflicts": 3-i,
						"values": 2*(3-i),
						"bytes": (3-i)*(content2_len + content3_len),
					}
					],
					"more": false,
					"nextStart": null
				})
			);
		} else {
			assert_json_eq!(
				res_body,
				json!({
					"prefix": null,
					"start": null,
					"end": null,
					"limit": null,
					"reverse": false,
					"partitionKeys": [],
					"more": false,
					"nextStart": null
				})
			);
		}
	}
}

#[tokio::test]
async fn test_item_return_format() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("test-k2v-item-return-format");

	let single_value = b"A single value".to_vec();
	let concurrent_value = b"A concurrent value".to_vec();

	// -- Test with a single value --
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.path("root")
		.query_param("sort_key", Some("v1"))
		.body(single_value.clone())
		.method(Method::PUT)
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), StatusCode::NO_CONTENT);

	// f0: either
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.path("root")
		.query_param("sort_key", Some("v1"))
		.signed_header("accept", "*/*")
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), StatusCode::OK);
	assert_eq!(
		res.headers().get("content-type").unwrap().to_str().unwrap(),
		"application/octet-stream"
	);
	let ct = res
		.headers()
		.get("x-garage-causality-token")
		.unwrap()
		.to_str()
		.unwrap()
		.to_string();
	let res_body = res.into_body().collect().await.unwrap().to_bytes();
	assert_eq!(res_body, single_value);

	// f1: not specified
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.path("root")
		.query_param("sort_key", Some("v1"))
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), StatusCode::OK);
	assert_eq!(
		res.headers().get("content-type").unwrap().to_str().unwrap(),
		"application/json"
	);
	let res_body = json_body(res).await;
	assert_json_eq!(res_body, json!([BASE64_STANDARD.encode(&single_value)]));

	// f2: binary
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.path("root")
		.query_param("sort_key", Some("v1"))
		.signed_header("accept", "application/octet-stream")
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), StatusCode::OK);
	assert_eq!(
		res.headers().get("content-type").unwrap().to_str().unwrap(),
		"application/octet-stream"
	);
	let res_body = res.into_body().collect().await.unwrap().to_bytes();
	assert_eq!(res_body, single_value);

	// f3: json
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.path("root")
		.query_param("sort_key", Some("v1"))
		.signed_header("accept", "application/json")
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), StatusCode::OK);
	assert_eq!(
		res.headers().get("content-type").unwrap().to_str().unwrap(),
		"application/json"
	);
	let res_body = json_body(res).await;
	assert_json_eq!(res_body, json!([BASE64_STANDARD.encode(&single_value)]));

	// -- Test with a second, concurrent value --
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.path("root")
		.query_param("sort_key", Some("v1"))
		.body(concurrent_value.clone())
		.method(Method::PUT)
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), StatusCode::NO_CONTENT);

	// f0: either
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.path("root")
		.query_param("sort_key", Some("v1"))
		.signed_header("accept", "*/*")
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), StatusCode::OK);
	assert_eq!(
		res.headers().get("content-type").unwrap().to_str().unwrap(),
		"application/json"
	);
	let res_body = json_body(res).await;
	assert_json_eq!(
		res_body,
		json!([
			BASE64_STANDARD.encode(&single_value),
			BASE64_STANDARD.encode(&concurrent_value)
		])
	);

	// f1: not specified
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.path("root")
		.query_param("sort_key", Some("v1"))
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), StatusCode::OK);
	assert_eq!(
		res.headers().get("content-type").unwrap().to_str().unwrap(),
		"application/json"
	);
	let res_body = json_body(res).await;
	assert_json_eq!(
		res_body,
		json!([
			BASE64_STANDARD.encode(&single_value),
			BASE64_STANDARD.encode(&concurrent_value)
		])
	);

	// f2: binary
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.path("root")
		.query_param("sort_key", Some("v1"))
		.signed_header("accept", "application/octet-stream")
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), StatusCode::CONFLICT); // CONFLICT

	// f3: json
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.path("root")
		.query_param("sort_key", Some("v1"))
		.signed_header("accept", "application/json")
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), StatusCode::OK);
	assert_eq!(
		res.headers().get("content-type").unwrap().to_str().unwrap(),
		"application/json"
	);
	let res_body = json_body(res).await;
	assert_json_eq!(
		res_body,
		json!([
			BASE64_STANDARD.encode(&single_value),
			BASE64_STANDARD.encode(&concurrent_value)
		])
	);

	// -- Delete first value, concurrently with second insert --
	// -- (we now have a concurrent value and a deletion) --
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.path("root")
		.query_param("sort_key", Some("v1"))
		.method(Method::DELETE)
		.signed_header("x-garage-causality-token", ct)
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), StatusCode::NO_CONTENT);

	// f0: either
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.path("root")
		.query_param("sort_key", Some("v1"))
		.signed_header("accept", "*/*")
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), StatusCode::OK);
	assert_eq!(
		res.headers().get("content-type").unwrap().to_str().unwrap(),
		"application/json"
	);
	let res_body = json_body(res).await;
	assert_json_eq!(
		res_body,
		json!([BASE64_STANDARD.encode(&concurrent_value), null])
	);

	// f1: not specified
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.path("root")
		.query_param("sort_key", Some("v1"))
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), StatusCode::OK);
	assert_eq!(
		res.headers().get("content-type").unwrap().to_str().unwrap(),
		"application/json"
	);
	let ct = res
		.headers()
		.get("x-garage-causality-token")
		.unwrap()
		.to_str()
		.unwrap()
		.to_string();
	let res_body = json_body(res).await;
	assert_json_eq!(
		res_body,
		json!([BASE64_STANDARD.encode(&concurrent_value), null])
	);

	// f2: binary
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.path("root")
		.query_param("sort_key", Some("v1"))
		.signed_header("accept", "application/octet-stream")
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), StatusCode::CONFLICT); // CONFLICT

	// f3: json
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.path("root")
		.query_param("sort_key", Some("v1"))
		.signed_header("accept", "application/json")
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), StatusCode::OK);
	assert_eq!(
		res.headers().get("content-type").unwrap().to_str().unwrap(),
		"application/json"
	);
	let res_body = json_body(res).await;
	assert_json_eq!(
		res_body,
		json!([BASE64_STANDARD.encode(&concurrent_value), null])
	);

	// -- Delete everything --
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.path("root")
		.query_param("sort_key", Some("v1"))
		.method(Method::DELETE)
		.signed_header("x-garage-causality-token", ct)
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), StatusCode::NO_CONTENT);

	// f0: either
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.path("root")
		.query_param("sort_key", Some("v1"))
		.signed_header("accept", "*/*")
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), StatusCode::NO_CONTENT); // NO CONTENT

	// f1: not specified
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.path("root")
		.query_param("sort_key", Some("v1"))
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), StatusCode::OK);
	assert_eq!(
		res.headers().get("content-type").unwrap().to_str().unwrap(),
		"application/json"
	);
	let res_body = json_body(res).await;
	assert_json_eq!(res_body, json!([null]));

	// f2: binary
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.path("root")
		.query_param("sort_key", Some("v1"))
		.signed_header("accept", "application/octet-stream")
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), StatusCode::NO_CONTENT); // NO CONTENT

	// f3: json
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.path("root")
		.query_param("sort_key", Some("v1"))
		.signed_header("accept", "application/json")
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), StatusCode::OK);
	assert_eq!(
		res.headers().get("content-type").unwrap().to_str().unwrap(),
		"application/json"
	);
	let res_body = json_body(res).await;
	assert_json_eq!(res_body, json!([null]));
}
