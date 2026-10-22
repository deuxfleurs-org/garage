use crate::common;

use aws_sdk_s3::presigning::PresigningConfig;
use bytes::Bytes;
use http::{Request, StatusCode};
use http_body_util::Full;
use std::time::Duration;

#[tokio::test]
async fn test_signature_encoding() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("signature-encoding");

	let obj_key = "key@good~.txt";
	let obj_content = "hello world of special characters";

	// ---- basic put/get test

	let _put_obj_info = ctx
		.client
		.put_object()
		.bucket(&bucket)
		.key(obj_key)
		.body(obj_content.as_bytes().to_vec().into())
		.send()
		.await
		.expect("failed to put object");

	let _get_obj = ctx
		.client
		.get_object()
		.bucket(&bucket)
		.key(obj_key)
		.send()
		.await
		.expect("failed to get object");

	// ---- test url with different encoding -> should still find object

	let presign_config = PresigningConfig::builder()
		.expires_in(Duration::from_secs(100))
		.build()
		.expect("failed to build presigning config");
	let presigned_request = ctx
		.client
		.get_object()
		.bucket(&bucket)
		.key(obj_key)
		.presigned(presign_config)
		.await
		.expect("failed to construct presigned request");

	let altered_url = presigned_request
		.uri()
		.replace("%40", "@")
		.replace("~", "%7E");

	let client = ctx.custom_request.client();
	let req_builder = Request::builder()
		.method(presigned_request.method())
		.uri(altered_url);
	let req = presigned_request
		.headers()
		.fold(req_builder, |req_builder, (key, value)| {
			req_builder.header(key, value)
		})
		.body(Full::new(Bytes::new()))
		.expect("failed to construct request from presigned_request");

	let res = client
		.request(req)
		.await
		.expect("failed to execute presigned request");

	assert_eq!(res.status(), StatusCode::OK);

	// -- test url with badly encoded characters -> should return 400 bad request

	let client = ctx.custom_request.client();
	let req = Request::builder()
        .method(http::Method::GET)
        .uri(format!("{}/%C0", ctx.garage.s3_uri()))
        .header("x-amz-date", chrono::Utc::now().format("%Y%m%dT%H%M%SZ").to_string())
        .header("x-amz-content-sha256", "UNSIGNED-PAYLOAD")
        .header("Authorization", "AWS4-HMAC-SHA256 Credential=GKdoesnotexist/20261017/garage/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=00")
		.body(Full::new(Bytes::new()))
		.expect("failed to construct request from presigned_request");

	let res = client
		.request(req)
		.await
		.expect("failed to execute presigned request");

	assert_eq!(res.status(), StatusCode::BAD_REQUEST);
}
