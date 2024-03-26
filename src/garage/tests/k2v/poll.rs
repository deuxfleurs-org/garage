use base64::prelude::*;
use http_body_util::BodyExt;
use hyper::{Method, StatusCode};
use std::time::Duration;

use assert_json_diff::assert_json_eq;
use serde_json::json;

use crate::common;
use crate::json_body;

#[tokio::test]
#[ignore = "currently broken"]
async fn test_poll_item() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("test-k2v-poll-item");

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
	assert_eq!(res.status(), StatusCode::NO_CONTENT);

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
	assert_eq!(res2.status(), StatusCode::OK);
	let ct = res2
		.headers()
		.get("x-garage-causality-token")
		.unwrap()
		.to_str()
		.unwrap()
		.to_string();

	let res2_body = res2.into_body().collect().await.unwrap().to_bytes();
	assert_eq!(res2_body, b"Initial value"[..]);

	// Start poll operation
	let poll = {
		let bucket = bucket.clone();
		let ct = ct.clone();
		let ctx = ctx.clone();
		tokio::spawn(async move {
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
	assert_eq!(res.status(), StatusCode::NO_CONTENT);

	// Check poll finishes with correct value
	let poll_res = tokio::select! {
		_ = tokio::time::sleep(Duration::from_secs(10)) => panic!("poll did not terminate in time"),
		res = poll => res.unwrap().unwrap(),
	};

	assert_eq!(poll_res.status(), StatusCode::OK);

	let poll_res_body = poll_res.into_body().collect().await.unwrap().to_bytes();
	assert_eq!(poll_res_body, b"New value"[..]);
}

#[tokio::test]
#[ignore = "currently broken"]
async fn test_poll_range() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("test-k2v-poll-range");

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
	assert_eq!(res.status(), StatusCode::NO_CONTENT);

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
	assert_eq!(res2.status(), StatusCode::OK);
	let ct = res2
		.headers()
		.get("x-garage-causality-token")
		.unwrap()
		.to_str()
		.unwrap()
		.to_string();

	// Initial poll range, retrieve single item and first seen_marker
	let res2 = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.method(Method::POST)
		.path("root")
		.query_param("poll_range", None::<String>)
		.body(b"{}".to_vec())
		.send()
		.await
		.unwrap();
	assert_eq!(res2.status(), StatusCode::OK);
	let json_res = json_body(res2).await;
	let seen_marker = json_res["seenMarker"].as_str().unwrap().to_string();
	assert_json_eq!(
		json_res,
		json!(
			{
				"items": [
				  {"sk": "test1", "ct": ct, "v": [BASE64_STANDARD.encode(b"Initial value")]},
				],
				"seenMarker": seen_marker,
			}
		)
	);

	// Second poll range, which will complete later
	let poll = {
		let bucket = bucket.clone();
		let ctx = ctx.clone();
		tokio::spawn(async move {
			ctx.k2v
				.request
				.builder(bucket.clone())
				.method(Method::POST)
				.path("root")
				.query_param("poll_range", None::<String>)
				.body(format!(r#"{{"seenMarker": "{}"}}"#, seen_marker).into_bytes())
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
	assert_eq!(res.status(), StatusCode::NO_CONTENT);

	// Check poll finishes with correct value
	let poll_res = tokio::select! {
		_ = tokio::time::sleep(Duration::from_secs(10)) => panic!("poll did not terminate in time"),
		res = poll => res.unwrap().unwrap(),
	};

	assert_eq!(poll_res.status(), StatusCode::OK);
	let json_res = json_body(poll_res).await;
	let seen_marker = json_res["seenMarker"].as_str().unwrap().to_string();
	assert_eq!(json_res["items"].as_array().unwrap().len(), 1);
	assert_json_eq!(&json_res["items"][0]["sk"], json!("test1"));
	assert_json_eq!(
		&json_res["items"][0]["v"],
		json!([BASE64_STANDARD.encode(b"New value")])
	);

	// Now we will add a value on a different key
	// Start a new poll operation
	let poll = {
		let bucket = bucket.clone();
		let ctx = ctx.clone();
		tokio::spawn(async move {
			ctx.k2v
				.request
				.builder(bucket.clone())
				.method(Method::POST)
				.path("root")
				.query_param("poll_range", None::<String>)
				.body(format!(r#"{{"seenMarker": "{}"}}"#, seen_marker).into_bytes())
				.send()
				.await
		})
	};

	// Write value on different key
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.method(Method::PUT)
		.path("root")
		.query_param("sort_key", Some("test2"))
		.body(b"Other value".to_vec())
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), StatusCode::NO_CONTENT);

	// Check poll finishes with correct value
	let poll_res = tokio::select! {
		_ = tokio::time::sleep(Duration::from_secs(10)) => panic!("poll did not terminate in time"),
		res = poll => res.unwrap().unwrap(),
	};

	assert_eq!(poll_res.status(), StatusCode::OK);
	let json_res = json_body(poll_res).await;
	assert_eq!(json_res["items"].as_array().unwrap().len(), 1);
	assert_json_eq!(&json_res["items"][0]["sk"], json!("test2"));
	assert_json_eq!(
		&json_res["items"][0]["v"],
		json!([BASE64_STANDARD.encode(b"Other value")])
	);
}
