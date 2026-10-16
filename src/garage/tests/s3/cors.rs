use aws_sdk_s3::types::{CorsConfiguration, CorsRule};
use hyper::{Method, StatusCode};

use crate::common;

const REQUEST_ORIGIN: &str = "https://app.example.test";
const SECOND_ALLOWED_ORIGIN: &str = "https://admin.example.test";
const OBJECT_KEY: &str = "probe.txt";
const BODY: &[u8] = b"hello from integration repro\n";

async fn send_preflight(
	ctx: &common::Context,
	bucket: &str,
	origin: &str,
) -> hyper::Response<common::custom_requester::Body> {
	ctx.custom_request
		.builder(bucket.to_string())
		.method(Method::OPTIONS)
		.path(OBJECT_KEY)
		.unsigned_header("origin", origin)
		.unsigned_header("access-control-request-method", "PUT")
		.unsigned_header(
			"access-control-request-headers",
			"content-type,x-amz-meta-demo",
		)
		.body(vec![])
		.send()
		.await
		.unwrap()
}

async fn send_put(
	ctx: &common::Context,
	bucket: &str,
	origin: &str,
) -> hyper::Response<common::custom_requester::Body> {
	ctx.custom_request
		.builder(bucket.to_string())
		.method(Method::PUT)
		.path(OBJECT_KEY)
		.signed_header("content-type", "text/plain")
		.signed_header("x-amz-meta-demo", "1")
		.unsigned_header("origin", origin)
		.body(BODY.to_vec())
		.send()
		.await
		.unwrap()
}

async fn apply_bucket_cors(ctx: &common::Context, bucket: &str, allowed_origins: &[&str]) {
	let rule = allowed_origins.iter().fold(
		CorsRule::builder()
			.allowed_headers("*")
			.allowed_methods("PUT")
			.expose_headers("ETag"),
		|rule, origin| rule.allowed_origins(*origin),
	);

	let cors = CorsConfiguration::builder()
		.cors_rules(rule.build().unwrap())
		.build()
		.unwrap();

	ctx.client
		.put_bucket_cors()
		.bucket(bucket)
		.cors_configuration(cors)
		.send()
		.await
		.unwrap();
}

#[tokio::test]
async fn test_s3_api_cors_reflects_request_origin() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("s3-cors-direct");
	ctx.bucket_allow_owner(&bucket);

	apply_bucket_cors(&ctx, &bucket, &[REQUEST_ORIGIN]).await;

	let control_preflight = send_preflight(&ctx, &bucket, REQUEST_ORIGIN).await;
	assert_eq!(control_preflight.status(), StatusCode::OK);
	assert_eq!(
		control_preflight
			.headers()
			.get("access-control-allow-origin")
			.unwrap(),
		REQUEST_ORIGIN
	);

	let control_put = send_put(&ctx, &bucket, REQUEST_ORIGIN).await;
	assert_eq!(control_put.status(), StatusCode::OK);
	assert_eq!(
		control_put
			.headers()
			.get("access-control-allow-origin")
			.unwrap(),
		REQUEST_ORIGIN
	);

	apply_bucket_cors(&ctx, &bucket, &[REQUEST_ORIGIN, SECOND_ALLOWED_ORIGIN]).await;

	let repro_preflight = send_preflight(&ctx, &bucket, REQUEST_ORIGIN).await;
	assert_eq!(repro_preflight.status(), StatusCode::OK);
	assert_eq!(
		repro_preflight
			.headers()
			.get("access-control-allow-origin")
			.unwrap(),
		REQUEST_ORIGIN
	);

	let repro_put = send_put(&ctx, &bucket, REQUEST_ORIGIN).await;
	assert_eq!(repro_put.status(), StatusCode::OK);
	assert_eq!(
		repro_put
			.headers()
			.get("access-control-allow-origin")
			.unwrap(),
		REQUEST_ORIGIN
	);
}
