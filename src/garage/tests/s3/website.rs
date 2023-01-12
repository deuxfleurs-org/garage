use crate::common;
use crate::common::ext::*;
use crate::k2v::json_body;

use assert_json_diff::assert_json_eq;
use aws_sdk_s3::{
	model::{CorsConfiguration, CorsRule, ErrorDocument, IndexDocument, WebsiteConfiguration},
	types::ByteStream,
};
use http::{Request, StatusCode};
use hyper::{
	body::{to_bytes, Body},
	Client,
};
use serde_json::json;

const BODY: &[u8; 16] = b"<h1>bonjour</h1>";
const BODY_ERR: &[u8; 6] = b"erreur";

#[tokio::test]
async fn test_website() {
	const BCKT_NAME: &str = "my-website";
	let ctx = common::context();
	let bucket = ctx.create_bucket(BCKT_NAME);

	let data = ByteStream::from_static(BODY);

	ctx.client
		.put_object()
		.bucket(&bucket)
		.key("index.html")
		.body(data)
		.send()
		.await
		.unwrap();

	let client = Client::new();

	let req = || {
		Request::builder()
			.method("GET")
			.uri(format!("http://127.0.0.1:{}/", ctx.garage.web_port))
			.header("Host", format!("{}.web.garage", BCKT_NAME))
			.body(Body::empty())
			.unwrap()
	};

	let mut resp = client.request(req()).await.unwrap();

	assert_eq!(resp.status(), StatusCode::NOT_FOUND);
	assert_ne!(
		to_bytes(resp.body_mut()).await.unwrap().as_ref(),
		BODY.as_ref()
	); /* check that we do not leak body */

	let admin_req = || {
		Request::builder()
			.method("GET")
			.uri(format!("http://127.0.0.1:{}/check", ctx.garage.admin_port))
			.header("domain", format!("{}", BCKT_NAME))
			.body(Body::empty())
			.unwrap()
	};

	let admin_resp = client.request(admin_req()).await.unwrap();
	assert_eq!(admin_resp.status(), StatusCode::BAD_REQUEST);
	let res_body = json_body(admin_resp).await;
	assert_json_eq!(
		res_body,
		json!({
			"code": "InvalidRequest",
			"message": "Bad request: Bucket is not authorized for website hosting",
			"region": "garage-integ-test",
			"path": "/check",
		})
	);

	ctx.garage
		.command()
		.args(["bucket", "website", "--allow", BCKT_NAME])
		.quiet()
		.expect_success_status("Could not allow website on bucket");

	resp = client.request(req()).await.unwrap();
	assert_eq!(resp.status(), StatusCode::OK);
	assert_eq!(
		to_bytes(resp.body_mut()).await.unwrap().as_ref(),
		BODY.as_ref()
	);

	let admin_req = || {
		Request::builder()
			.method("GET")
			.uri(format!("http://127.0.0.1:{}/check", ctx.garage.admin_port))
			.header("domain", format!("{}", BCKT_NAME))
			.body(Body::empty())
			.unwrap()
	};

	let mut admin_resp = client.request(admin_req()).await.unwrap();
	assert_eq!(admin_resp.status(), StatusCode::OK);
	assert_eq!(
		to_bytes(admin_resp.body_mut()).await.unwrap().as_ref(),
		b"Bucket authorized for website hosting"
	);

	ctx.garage
		.command()
		.args(["bucket", "website", "--deny", BCKT_NAME])
		.quiet()
		.expect_success_status("Could not deny website on bucket");

	resp = client.request(req()).await.unwrap();
	assert_eq!(resp.status(), StatusCode::NOT_FOUND);
	assert_ne!(
		to_bytes(resp.body_mut()).await.unwrap().as_ref(),
		BODY.as_ref()
	); /* check that we do not leak body */

	let admin_req = || {
		Request::builder()
			.method("GET")
			.uri(format!("http://127.0.0.1:{}/check", ctx.garage.admin_port))
			.header("domain", format!("{}", BCKT_NAME))
			.body(Body::empty())
			.unwrap()
	};

	let admin_resp = client.request(admin_req()).await.unwrap();
	assert_eq!(admin_resp.status(), StatusCode::BAD_REQUEST);
	let res_body = json_body(admin_resp).await;
	assert_json_eq!(
		res_body,
		json!({
			"code": "InvalidRequest",
			"message": "Bad request: Bucket is not authorized for website hosting",
			"region": "garage-integ-test",
			"path": "/check",
		})
	);
}

#[tokio::test]
async fn test_website_s3_api() {
	const BCKT_NAME: &str = "my-cors";
	let ctx = common::context();
	let bucket = ctx.create_bucket(BCKT_NAME);

	let data = ByteStream::from_static(BODY);

	ctx.client
		.put_object()
		.bucket(&bucket)
		.key("site/home.html")
		.body(data)
		.send()
		.await
		.unwrap();

	ctx.client
		.put_object()
		.bucket(&bucket)
		.key("err/error.html")
		.body(ByteStream::from_static(BODY_ERR))
		.send()
		.await
		.unwrap();

	let conf = WebsiteConfiguration::builder()
		.index_document(IndexDocument::builder().suffix("home.html").build())
		.error_document(ErrorDocument::builder().key("err/error.html").build())
		.build();

	ctx.client
		.put_bucket_website()
		.bucket(&bucket)
		.website_configuration(conf)
		.send()
		.await
		.unwrap();

	let cors = CorsConfiguration::builder()
		.cors_rules(
			CorsRule::builder()
				.id("main-rule")
				.allowed_headers("*")
				.allowed_methods("GET")
				.allowed_methods("PUT")
				.allowed_origins("*")
				.build(),
		)
		.build();

	ctx.client
		.put_bucket_cors()
		.bucket(&bucket)
		.cors_configuration(cors)
		.send()
		.await
		.unwrap();

	{
		let cors_res = ctx
			.client
			.get_bucket_cors()
			.bucket(&bucket)
			.send()
			.await
			.unwrap();

		let main_rule = cors_res.cors_rules().unwrap().iter().next().unwrap();

		assert_eq!(main_rule.id.as_ref().unwrap(), "main-rule");
		assert_eq!(
			main_rule.allowed_headers.as_ref().unwrap(),
			&vec!["*".to_string()]
		);
		assert_eq!(
			main_rule.allowed_origins.as_ref().unwrap(),
			&vec!["*".to_string()]
		);
		assert_eq!(
			main_rule.allowed_methods.as_ref().unwrap(),
			&vec!["GET".to_string(), "PUT".to_string()]
		);
	}

	let client = Client::new();

	// Test direct requests with CORS
	{
		let req = Request::builder()
			.method("GET")
			.uri(format!("http://127.0.0.1:{}/site/", ctx.garage.web_port))
			.header("Host", format!("{}.web.garage", BCKT_NAME))
			.header("Origin", "https://example.com")
			.body(Body::empty())
			.unwrap();

		let mut resp = client.request(req).await.unwrap();

		assert_eq!(resp.status(), StatusCode::OK);
		assert_eq!(
			resp.headers().get("access-control-allow-origin").unwrap(),
			"*"
		);
		assert_eq!(
			to_bytes(resp.body_mut()).await.unwrap().as_ref(),
			BODY.as_ref()
		);
	}

	// Test ErrorDocument on 404
	{
		let req = Request::builder()
			.method("GET")
			.uri(format!(
				"http://127.0.0.1:{}/wrong.html",
				ctx.garage.web_port
			))
			.header("Host", format!("{}.web.garage", BCKT_NAME))
			.body(Body::empty())
			.unwrap();

		let mut resp = client.request(req).await.unwrap();

		assert_eq!(resp.status(), StatusCode::NOT_FOUND);
		assert_eq!(
			to_bytes(resp.body_mut()).await.unwrap().as_ref(),
			BODY_ERR.as_ref()
		);
	}

	// Test CORS with an allowed preflight request
	{
		let req = Request::builder()
			.method("OPTIONS")
			.uri(format!("http://127.0.0.1:{}/site/", ctx.garage.web_port))
			.header("Host", format!("{}.web.garage", BCKT_NAME))
			.header("Origin", "https://example.com")
			.header("Access-Control-Request-Method", "PUT")
			.body(Body::empty())
			.unwrap();

		let mut resp = client.request(req).await.unwrap();

		assert_eq!(resp.status(), StatusCode::OK);
		assert_eq!(
			resp.headers().get("access-control-allow-origin").unwrap(),
			"*"
		);
		assert_ne!(
			to_bytes(resp.body_mut()).await.unwrap().as_ref(),
			BODY.as_ref()
		);
	}

	// Test CORS with a forbidden preflight request
	{
		let req = Request::builder()
			.method("OPTIONS")
			.uri(format!("http://127.0.0.1:{}/site/", ctx.garage.web_port))
			.header("Host", format!("{}.web.garage", BCKT_NAME))
			.header("Origin", "https://example.com")
			.header("Access-Control-Request-Method", "DELETE")
			.body(Body::empty())
			.unwrap();

		let mut resp = client.request(req).await.unwrap();

		assert_eq!(resp.status(), StatusCode::FORBIDDEN);
		assert_ne!(
			to_bytes(resp.body_mut()).await.unwrap().as_ref(),
			BODY.as_ref()
		);
	}

	//@TODO test CORS on the S3 endpoint. We need to handle auth manually to check it.

	// Delete cors
	ctx.client
		.delete_bucket_cors()
		.bucket(&bucket)
		.send()
		.await
		.unwrap();

	// Check CORS are deleted from the API
	// @FIXME check what is the expected behavior when GetBucketCors is called on a bucket without
	// any CORS.
	assert!(ctx
		.client
		.get_bucket_cors()
		.bucket(&bucket)
		.send()
		.await
		.is_err());

	// Test CORS are not sent anymore on a previously allowed request
	{
		let req = Request::builder()
			.method("OPTIONS")
			.uri(format!("http://127.0.0.1:{}/site/", ctx.garage.web_port))
			.header("Host", format!("{}.web.garage", BCKT_NAME))
			.header("Origin", "https://example.com")
			.header("Access-Control-Request-Method", "PUT")
			.body(Body::empty())
			.unwrap();

		let mut resp = client.request(req).await.unwrap();

		assert_eq!(resp.status(), StatusCode::FORBIDDEN);
		assert_ne!(
			to_bytes(resp.body_mut()).await.unwrap().as_ref(),
			BODY.as_ref()
		);
	}

	// Disallow website from the API
	ctx.client
		.delete_bucket_website()
		.bucket(&bucket)
		.send()
		.await
		.unwrap();

	// Check that the website is not served anymore
	{
		let req = Request::builder()
			.method("GET")
			.uri(format!("http://127.0.0.1:{}/site/", ctx.garage.web_port))
			.header("Host", format!("{}.web.garage", BCKT_NAME))
			.body(Body::empty())
			.unwrap();

		let mut resp = client.request(req).await.unwrap();

		assert_eq!(resp.status(), StatusCode::NOT_FOUND);
		assert_ne!(
			to_bytes(resp.body_mut()).await.unwrap().as_ref(),
			BODY_ERR.as_ref()
		);
		assert_ne!(
			to_bytes(resp.body_mut()).await.unwrap().as_ref(),
			BODY.as_ref()
		);
	}
}

#[tokio::test]
async fn test_website_check_website_enabled() {
	let ctx = common::context();

	let client = Client::new();

	let admin_req = || {
		Request::builder()
			.method("GET")
			.uri(format!("http://127.0.0.1:{}/check", ctx.garage.admin_port))
			.body(Body::empty())
			.unwrap()
	};

	let admin_resp = client.request(admin_req()).await.unwrap();
	assert_eq!(admin_resp.status(), StatusCode::BAD_REQUEST);
	let res_body = json_body(admin_resp).await;
	assert_json_eq!(
		res_body,
		json!({
			"code": "InvalidRequest",
			"message": "Bad request: No domain header found",
			"region": "garage-integ-test",
			"path": "/check",
		})
	);

	let admin_req = || {
		Request::builder()
			.method("GET")
			.uri(format!("http://127.0.0.1:{}/check", ctx.garage.admin_port))
			.header("domain", "foobar")
			.body(Body::empty())
			.unwrap()
	};

	let admin_resp = client.request(admin_req()).await.unwrap();
	assert_eq!(admin_resp.status(), StatusCode::NOT_FOUND);
	let res_body = json_body(admin_resp).await;
	assert_json_eq!(
		res_body,
		json!({
			"code": "NoSuchBucket",
			"message": "Bucket not found: foobar",
			"region": "garage-integ-test",
			"path": "/check",
		})
	);

	let admin_req = || {
		Request::builder()
			.method("GET")
			.uri(format!("http://127.0.0.1:{}/check", ctx.garage.admin_port))
			.header("domain", "☹")
			.body(Body::empty())
			.unwrap()
	};

	let admin_resp = client.request(admin_req()).await.unwrap();
	assert_eq!(admin_resp.status(), StatusCode::BAD_REQUEST);
	let res_body = json_body(admin_resp).await;
	assert_json_eq!(
		res_body,
		json!({
			"code": "InvalidRequest",
			"message": "Bad request: Invalid characters found in domain header: failed to convert header to a str",
			"region": "garage-integ-test",
			"path": "/check",
		})
	);
}
