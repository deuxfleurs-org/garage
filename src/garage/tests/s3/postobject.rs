use base64::prelude::*;
use bytes::Bytes;
use chrono::{Duration, Utc};
use hmac::Mac;
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper::{header, Method, Request, Response, StatusCode};

use garage_api_common::signature;

use crate::common;

const UTF8_KEY: &str = "uploads/test/копия файла.jpg";
const UTF8_FILENAME: &str = "café-日本語.txt";
const UTF8_CONTENT_DISPOSITION: &str = "attachment; filename=\"копия файла.jpg\"";
const REGION: &str = "garage-integ-test";
const BOUNDARY: &str = "boundary-garage-integ-test";

async fn send_post_object(
	ctx: &common::Context,
	bucket: &str,
	key_field: &str,
	filename: &str,
	extra_fields: &[(&str, &str)],
	file_body: &str,
) -> Response<Incoming> {
	let now = Utc::now();
	let scope = signature::compute_scope(&now, REGION, "s3");
	let credential = format!("{}/{}", ctx.key.id, scope);
	let date = now.format(signature::LONG_DATETIME).to_string();
	let expiration = (now + Duration::hours(1)).to_rfc3339();

	let mut conditions = vec![
		serde_json::json!({ "bucket": bucket }),
		serde_json::json!(["starts-with", "$key", ""]),
		serde_json::json!({ "x-amz-algorithm": "AWS4-HMAC-SHA256" }),
		serde_json::json!({ "x-amz-credential": &credential }),
		serde_json::json!({ "x-amz-date": &date }),
	];
	for (name, value) in extra_fields {
		conditions.push(serde_json::json!(["eq", format!("${}", name), value]));
	}
	let policy = serde_json::json!({
		"expiration": expiration,
		"conditions": conditions,
	})
	.to_string();

	let policy_b64 = BASE64_STANDARD.encode(policy.as_bytes());

	let mut signer = signature::signing_hmac(&now, &ctx.key.secret, REGION, "s3").unwrap();
	signer.update(policy_b64.as_bytes());
	let x_amz_signature = hex::encode(signer.finalize().into_bytes());

	let mut fields = vec![
		("key".to_string(), key_field.to_string()),
		("x-amz-algorithm".into(), "AWS4-HMAC-SHA256".into()),
		("x-amz-credential".into(), credential),
		("x-amz-date".into(), date),
		("policy".into(), policy_b64),
		("x-amz-signature".into(), x_amz_signature),
	];
	for (name, value) in extra_fields {
		fields.push((name.to_string(), value.to_string()));
	}

	let mut body = String::new();
	for (name, value) in &fields {
		body.push_str(&format!(
			"--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
		));
	}

	body.push_str(&format!(
		"--{BOUNDARY}\r\nContent-Disposition: form-data; name=\"file\"; filename=\"{filename}\"\r\nContent-Type: text/plain\r\n\r\n{file_body}\r\n--{BOUNDARY}--\r\n"
	));

	let req = Request::builder()
		.method(Method::POST)
		.uri(format!("{}{}", ctx.garage.s3_uri(), bucket))
		.header(header::HOST, "s3.garage")
		.header(
			header::CONTENT_TYPE,
			format!("multipart/form-data; boundary={BOUNDARY}"),
		)
		.body(Full::new(Bytes::from(body.into_bytes())))
		.unwrap();

	ctx.custom_request.client().request(req).await.unwrap()
}

#[tokio::test]
async fn test_post_object_utf8_key() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("post-object-utf8-key");

	let res = send_post_object(
		&ctx,
		&bucket,
		UTF8_KEY,
		"копия файла.jpg",
		&[("success_action_status", "201")],
		"hello",
	)
	.await;
	assert_eq!(res.status(), StatusCode::CREATED);

	let location = format!(
		"http://{}.s3.garage/{}",
		bucket,
		garage_api_common::encoding::uri_encode(UTF8_KEY, false)
	);
	assert_eq!(
		res.headers()
			.get(http::header::LOCATION)
			.unwrap()
			.to_str()
			.unwrap(),
		&location
	);
	let body = res.into_body().collect().await.unwrap().to_bytes();
	let body = std::str::from_utf8(&body).unwrap();
	assert!(body.contains(&format!("<Location>{}</Location>", location)));

	let obj = ctx
		.client
		.get_object()
		.bucket(&bucket)
		.key(UTF8_KEY)
		.send()
		.await
		.unwrap();
	assert_bytes_eq!(obj.body, b"hello");
}

#[tokio::test]
async fn test_post_object_utf8_filename_substitution() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("post-object-utf8-filename");

	let res = send_post_object(
		&ctx,
		&bucket,
		"uploads/${filename}",
		UTF8_FILENAME,
		&[],
		"bonjour",
	)
	.await;
	assert_eq!(res.status(), StatusCode::NO_CONTENT);

	let obj = ctx
		.client
		.get_object()
		.bucket(&bucket)
		.key(format!("uploads/{}", UTF8_FILENAME))
		.send()
		.await
		.unwrap();
	assert_bytes_eq!(obj.body, b"bonjour");
}

#[tokio::test]
async fn test_post_object_utf8_content_disposition_metadata() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("post-object-utf8-contdisp");

	let res = send_post_object(
		&ctx,
		&bucket,
		"ascii-key.jpg",
		"file.jpg",
		&[("content-disposition", UTF8_CONTENT_DISPOSITION)],
		"data",
	)
	.await;
	assert_eq!(res.status(), StatusCode::NO_CONTENT);

	let obj = ctx
		.client
		.get_object()
		.bucket(&bucket)
		.key("ascii-key.jpg")
		.send()
		.await
		.unwrap();
	assert_eq!(
		obj.content_disposition.as_deref(),
		Some(UTF8_CONTENT_DISPOSITION)
	);
}
