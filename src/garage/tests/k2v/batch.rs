use std::collections::HashMap;

use crate::common;

use assert_json_diff::assert_json_eq;
use serde_json::json;

use super::json_body;
use hyper::Method;

#[tokio::test]
async fn test_batch() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("test-k2v-batch");

	let mut values = HashMap::new();
	values.insert("a", "initial test 1");
	values.insert("b", "initial test 2");
	values.insert("c", "initial test 3");
	values.insert("d.1", "initial test 4");
	values.insert("d.2", "initial test 5");
	values.insert("e", "initial test 6");
	let mut ct = HashMap::new();

	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.body(
			format!(
				r#"[
	{{"pk": "root", "sk": "a", "ct": null, "v": "{}"}},
	{{"pk": "root", "sk": "b", "ct": null, "v": "{}"}},
	{{"pk": "root", "sk": "c", "ct": null, "v": "{}"}},
	{{"pk": "root", "sk": "d.1", "ct": null, "v": "{}"}},
	{{"pk": "root", "sk": "d.2", "ct": null, "v": "{}"}},
	{{"pk": "root", "sk": "e", "ct": null, "v": "{}"}}
		]"#,
				base64::encode(values.get(&"a").unwrap()),
				base64::encode(values.get(&"b").unwrap()),
				base64::encode(values.get(&"c").unwrap()),
				base64::encode(values.get(&"d.1").unwrap()),
				base64::encode(values.get(&"d.2").unwrap()),
				base64::encode(values.get(&"e").unwrap()),
			)
			.into_bytes(),
		)
		.method(Method::POST)
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), 200);

	for sk in ["a", "b", "c", "d.1", "d.2", "e"] {
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
		assert_eq!(res.status(), 200);
		assert_eq!(
			res.headers().get("content-type").unwrap().to_str().unwrap(),
			"application/octet-stream"
		);
		ct.insert(
			sk,
			res.headers()
				.get("x-garage-causality-token")
				.unwrap()
				.to_str()
				.unwrap()
				.to_string(),
		);
		let res_body = hyper::body::to_bytes(res.into_body())
			.await
			.unwrap()
			.to_vec();
		assert_eq!(res_body, values.get(sk).unwrap().as_bytes());
	}

	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.query_param("search", Option::<&str>::None)
		.body(
			br#"[
	{"partitionKey": "root"},
	{"partitionKey": "root", "start": "c"},
	{"partitionKey": "root", "start": "c", "reverse": true, "end": "a"},
	{"partitionKey": "root", "limit": 1},
	{"partitionKey": "root", "prefix": "d"}
		]"#
			.to_vec(),
		)
		.method(Method::POST)
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), 200);
	let json_res = json_body(res).await;
	assert_json_eq!(
		json_res,
		json!([
			{
				"partitionKey": "root",
				"prefix": null,
				"start": null,
				"end": null,
				"limit": null,
				"reverse": false,
				"conflictsOnly": false,
				"tombstones": false,
				"singleItem": false,
				"items": [
				  {"sk": "a", "ct": ct.get("a").unwrap(), "v": [base64::encode(values.get("a").unwrap())]},
				  {"sk": "b", "ct": ct.get("b").unwrap(), "v": [base64::encode(values.get("b").unwrap())]},
				  {"sk": "c", "ct": ct.get("c").unwrap(), "v": [base64::encode(values.get("c").unwrap())]},
				  {"sk": "d.1", "ct": ct.get("d.1").unwrap(), "v": [base64::encode(values.get("d.1").unwrap())]},
				  {"sk": "d.2", "ct": ct.get("d.2").unwrap(), "v": [base64::encode(values.get("d.2").unwrap())]},
				  {"sk": "e", "ct": ct.get("e").unwrap(), "v": [base64::encode(values.get("e").unwrap())]}
				],
				"more": false,
				"nextStart": null,
			},
			{
				"partitionKey": "root",
				"prefix": null,
				"start": "c",
				"end": null,
				"limit": null,
				"reverse": false,
				"conflictsOnly": false,
				"tombstones": false,
				"singleItem": false,
				"items": [
				  {"sk": "c", "ct": ct.get("c").unwrap(), "v": [base64::encode(values.get("c").unwrap())]},
				  {"sk": "d.1", "ct": ct.get("d.1").unwrap(), "v": [base64::encode(values.get("d.1").unwrap())]},
				  {"sk": "d.2", "ct": ct.get("d.2").unwrap(), "v": [base64::encode(values.get("d.2").unwrap())]},
				  {"sk": "e", "ct": ct.get("e").unwrap(), "v": [base64::encode(values.get("e").unwrap())]}
				],
				"more": false,
				"nextStart": null,
			},
			{
				"partitionKey": "root",
				"prefix": null,
				"start": "c",
				"end": "a",
				"limit": null,
				"reverse": true,
				"conflictsOnly": false,
				"tombstones": false,
				"singleItem": false,
				"items": [
				  {"sk": "c", "ct": ct.get("c").unwrap(), "v": [base64::encode(values.get("c").unwrap())]},
				  {"sk": "b", "ct": ct.get("b").unwrap(), "v": [base64::encode(values.get("b").unwrap())]},
				],
				"more": false,
				"nextStart": null,
			},
			{
				"partitionKey": "root",
				"prefix": null,
				"start": null,
				"end": null,
				"limit": 1,
				"reverse": false,
				"conflictsOnly": false,
				"tombstones": false,
				"singleItem": false,
				"items": [
				  {"sk": "a", "ct": ct.get("a").unwrap(), "v": [base64::encode(values.get("a").unwrap())]}
				],
				"more": true,
				"nextStart": "b",
			},
			{
				"partitionKey": "root",
				"prefix": "d",
				"start": null,
				"end": null,
				"limit": null,
				"reverse": false,
				"conflictsOnly": false,
				"tombstones": false,
				"singleItem": false,
				"items": [
				  {"sk": "d.1", "ct": ct.get("d.1").unwrap(), "v": [base64::encode(values.get("d.1").unwrap())]},
				  {"sk": "d.2", "ct": ct.get("d.2").unwrap(), "v": [base64::encode(values.get("d.2").unwrap())]}
				],
				"more": false,
				"nextStart": null,
			},
		])
	);

	// Insert some new values
	values.insert("c'", "new test 3");
	values.insert("d.1'", "new test 4");
	values.insert("d.2'", "new test 5");

	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.body(
			format!(
				r#"[
	{{"pk": "root", "sk": "b", "ct": "{}", "v": null}},
	{{"pk": "root", "sk": "c", "ct": null, "v": "{}"}},
	{{"pk": "root", "sk": "d.1", "ct": "{}", "v": "{}"}},
	{{"pk": "root", "sk": "d.2", "ct": null, "v": "{}"}}
		]"#,
				ct.get(&"b").unwrap(),
				base64::encode(values.get(&"c'").unwrap()),
				ct.get(&"d.1").unwrap(),
				base64::encode(values.get(&"d.1'").unwrap()),
				base64::encode(values.get(&"d.2'").unwrap()),
			)
			.into_bytes(),
		)
		.method(Method::POST)
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), 200);

	for sk in ["b", "c", "d.1", "d.2"] {
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
		if sk == "b" {
			assert_eq!(res.status(), 204);
		} else {
			assert_eq!(res.status(), 200);
		}
		ct.insert(
			sk,
			res.headers()
				.get("x-garage-causality-token")
				.unwrap()
				.to_str()
				.unwrap()
				.to_string(),
		);
	}

	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.query_param("search", Option::<&str>::None)
		.body(
			br#"[
	{"partitionKey": "root"},
	{"partitionKey": "root", "prefix": "d"},
	{"partitionKey": "root", "prefix": "d.", "end": "d.2"},
	{"partitionKey": "root", "prefix": "d.", "limit": 1},
	{"partitionKey": "root", "prefix": "d.", "start": "d.2", "limit": 1},
	{"partitionKey": "root", "prefix": "d.", "reverse": true},
	{"partitionKey": "root", "prefix": "d.", "start": "d.2", "reverse": true},
	{"partitionKey": "root", "prefix": "d.", "limit": 2}
		]"#
			.to_vec(),
		)
		.method(Method::POST)
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), 200);
	let json_res = json_body(res).await;
	assert_json_eq!(
		json_res,
		json!([
			{
				"partitionKey": "root",
				"prefix": null,
				"start": null,
				"end": null,
				"limit": null,
				"reverse": false,
				"conflictsOnly": false,
				"tombstones": false,
				"singleItem": false,
				"items": [
				  {"sk": "a", "ct": ct.get("a").unwrap(), "v": [base64::encode(values.get("a").unwrap())]},
				  {"sk": "c", "ct": ct.get("c").unwrap(), "v": [base64::encode(values.get("c").unwrap()), base64::encode(values.get("c'").unwrap())]},
				  {"sk": "d.1", "ct": ct.get("d.1").unwrap(), "v": [base64::encode(values.get("d.1'").unwrap())]},
				  {"sk": "d.2", "ct": ct.get("d.2").unwrap(), "v": [base64::encode(values.get("d.2").unwrap()), base64::encode(values.get("d.2'").unwrap())]},
				  {"sk": "e", "ct": ct.get("e").unwrap(), "v": [base64::encode(values.get("e").unwrap())]}
				],
				"more": false,
				"nextStart": null,
			},
			{
				"partitionKey": "root",
				"prefix": "d",
				"start": null,
				"end": null,
				"limit": null,
				"reverse": false,
				"conflictsOnly": false,
				"tombstones": false,
				"singleItem": false,
				"items": [
				  {"sk": "d.1", "ct": ct.get("d.1").unwrap(), "v": [base64::encode(values.get("d.1'").unwrap())]},
				  {"sk": "d.2", "ct": ct.get("d.2").unwrap(), "v": [base64::encode(values.get("d.2").unwrap()), base64::encode(values.get("d.2'").unwrap())]},
				],
				"more": false,
				"nextStart": null,
			},
			{
				"partitionKey": "root",
				"prefix": "d.",
				"start": null,
				"end": "d.2",
				"limit": null,
				"reverse": false,
				"conflictsOnly": false,
				"tombstones": false,
				"singleItem": false,
				"items": [
				  {"sk": "d.1", "ct": ct.get("d.1").unwrap(), "v": [base64::encode(values.get("d.1'").unwrap())]},
				],
				"more": false,
				"nextStart": null,
			},
			{
				"partitionKey": "root",
				"prefix": "d.",
				"start": null,
				"end": null,
				"limit": 1,
				"reverse": false,
				"conflictsOnly": false,
				"tombstones": false,
				"singleItem": false,
				"items": [
				  {"sk": "d.1", "ct": ct.get("d.1").unwrap(), "v": [base64::encode(values.get("d.1'").unwrap())]},
				],
				"more": true,
				"nextStart": "d.2",
			},
			{
				"partitionKey": "root",
				"prefix": "d.",
				"start": "d.2",
				"end": null,
				"limit": 1,
				"reverse": false,
				"conflictsOnly": false,
				"tombstones": false,
				"singleItem": false,
				"items": [
				  {"sk": "d.2", "ct": ct.get("d.2").unwrap(), "v": [base64::encode(values.get("d.2").unwrap()), base64::encode(values.get("d.2'").unwrap())]},
				],
				"more": false,
				"nextStart": null,
			},
			{
				"partitionKey": "root",
				"prefix": "d.",
				"start": null,
				"end": null,
				"limit": null,
				"reverse": true,
				"conflictsOnly": false,
				"tombstones": false,
				"singleItem": false,
				"items": [
				  {"sk": "d.2", "ct": ct.get("d.2").unwrap(), "v": [base64::encode(values.get("d.2").unwrap()), base64::encode(values.get("d.2'").unwrap())]},
				  {"sk": "d.1", "ct": ct.get("d.1").unwrap(), "v": [base64::encode(values.get("d.1'").unwrap())]},
				],
				"more": false,
				"nextStart": null,
			},
			{
				"partitionKey": "root",
				"prefix": "d.",
				"start": "d.2",
				"end": null,
				"limit": null,
				"reverse": true,
				"conflictsOnly": false,
				"tombstones": false,
				"singleItem": false,
				"items": [
				  {"sk": "d.2", "ct": ct.get("d.2").unwrap(), "v": [base64::encode(values.get("d.2").unwrap()), base64::encode(values.get("d.2'").unwrap())]},
				  {"sk": "d.1", "ct": ct.get("d.1").unwrap(), "v": [base64::encode(values.get("d.1'").unwrap())]},
				],
				"more": false,
				"nextStart": null,
			},
			{
				"partitionKey": "root",
				"prefix": "d.",
				"start": null,
				"end": null,
				"limit": 2,
				"reverse": false,
				"conflictsOnly": false,
				"tombstones": false,
				"singleItem": false,
				"items": [
				  {"sk": "d.1", "ct": ct.get("d.1").unwrap(), "v": [base64::encode(values.get("d.1'").unwrap())]},
				  {"sk": "d.2", "ct": ct.get("d.2").unwrap(), "v": [base64::encode(values.get("d.2").unwrap()), base64::encode(values.get("d.2'").unwrap())]},
				],
				"more": false,
				"nextStart": null,
			},
		])
	);

	// Test DeleteBatch
	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.query_param("delete", Option::<&str>::None)
		.body(
			br#"[
	{"partitionKey": "root", "start": "a", "end": "c"},
	{"partitionKey": "root", "prefix": "d"}
		]"#
			.to_vec(),
		)
		.method(Method::POST)
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), 200);
	let json_res = json_body(res).await;
	assert_json_eq!(
		json_res,
		json!([
			{
				"partitionKey": "root",
				"prefix": null,
				"start": "a",
				"end": "c",
				"singleItem": false,
				"deletedItems": 1,
			},
			{
				"partitionKey": "root",
				"prefix": "d",
				"start": null,
				"end": null,
				"singleItem": false,
				"deletedItems": 2,
			},
		])
	);

	let res = ctx
		.k2v
		.request
		.builder(bucket.clone())
		.query_param("search", Option::<&str>::None)
		.body(
			br#"[
	{"partitionKey": "root"},
	{"partitionKey": "root", "reverse": true}
		]"#
			.to_vec(),
		)
		.method(Method::POST)
		.send()
		.await
		.unwrap();
	assert_eq!(res.status(), 200);
	let json_res = json_body(res).await;
	assert_json_eq!(
		json_res,
		json!([
			{
				"partitionKey": "root",
				"prefix": null,
				"start": null,
				"end": null,
				"limit": null,
				"reverse": false,
				"conflictsOnly": false,
				"tombstones": false,
				"singleItem": false,
				"items": [
				  {"sk": "c", "ct": ct.get("c").unwrap(), "v": [base64::encode(values.get("c").unwrap()), base64::encode(values.get("c'").unwrap())]},
				  {"sk": "e", "ct": ct.get("e").unwrap(), "v": [base64::encode(values.get("e").unwrap())]}
				],
				"more": false,
				"nextStart": null,
			},
			{
				"partitionKey": "root",
				"prefix": null,
				"start": null,
				"end": null,
				"limit": null,
				"reverse": true,
				"conflictsOnly": false,
				"tombstones": false,
				"singleItem": false,
				"items": [
				  {"sk": "e", "ct": ct.get("e").unwrap(), "v": [base64::encode(values.get("e").unwrap())]},
				  {"sk": "c", "ct": ct.get("c").unwrap(), "v": [base64::encode(values.get("c").unwrap()), base64::encode(values.get("c'").unwrap())]},
				],
				"more": false,
				"nextStart": null,
			},
		])
	);
}
