use crate::common;
use crate::common::ext::*;
use crate::json_body;

use assert_json_diff::assert_json_eq;
use aws_sdk_s3::{
	primitives::ByteStream,
	types::{CorsConfiguration, CorsRule, ErrorDocument, IndexDocument, WebsiteConfiguration},
};
use http::{Request, StatusCode};
use http_body_util::BodyExt;
use http_body_util::Full as FullBody;
use hyper::body::Bytes;
use hyper::header::LOCATION;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use serde_json::json;

const BODY: &[u8; 16] = b"<h1>bonjour</h1>";
const BODY_ERR: &[u8; 6] = b"erreur";

pub type Body = FullBody<Bytes>;

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

	let client = Client::builder(TokioExecutor::new()).build_http();

	let req = || {
		Request::builder()
			.method("GET")
			.uri(format!("http://127.0.0.1:{}/", ctx.garage.web_port))
			.header("Host", format!("{}.web.garage", BCKT_NAME))
			.body(Body::new(Bytes::new()))
			.unwrap()
	};

	let mut resp = client.request(req()).await.unwrap();

	assert_eq!(resp.status(), StatusCode::NOT_FOUND);
	assert_ne!(
		BodyExt::collect(resp.into_body()).await.unwrap().to_bytes(),
		BODY.as_ref()
	); /* check that we do not leak body */

	let admin_req = || {
		Request::builder()
			.method("GET")
			.uri(format!(
				"http://127.0.0.1:{0}/check?domain={1}",
				ctx.garage.admin_port, BCKT_NAME
			))
			.body(Body::new(Bytes::new()))
			.unwrap()
	};

	let admin_resp = client.request(admin_req()).await.unwrap();
	assert_eq!(admin_resp.status(), StatusCode::BAD_REQUEST);
	let res_body = json_body(admin_resp).await;
	assert_json_eq!(
		res_body,
		json!({
			"code": "InvalidRequest",
			"message": "Bad request: Domain 'my-website' is not managed by Garage",
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
		resp.into_body().collect().await.unwrap().to_bytes(),
		BODY.as_ref()
	);

	for bname in [
		BCKT_NAME.to_string(),
		format!("{BCKT_NAME}.web.garage"),
		format!("{BCKT_NAME}.s3.garage"),
	] {
		let admin_req = || {
			Request::builder()
				.method("GET")
				.uri(format!(
					"http://127.0.0.1:{0}/check?domain={1}",
					ctx.garage.admin_port, bname
				))
				.body(Body::new(Bytes::new()))
				.unwrap()
		};

		let admin_resp = client.request(admin_req()).await.unwrap();
		assert_eq!(admin_resp.status(), StatusCode::OK);
		assert_eq!(
			admin_resp.into_body().collect().await.unwrap().to_bytes(),
			format!("Domain '{bname}' is managed by Garage").as_bytes()
		);
	}

	ctx.garage
		.command()
		.args(["bucket", "website", "--deny", BCKT_NAME])
		.quiet()
		.expect_success_status("Could not deny website on bucket");

	resp = client.request(req()).await.unwrap();
	assert_eq!(resp.status(), StatusCode::NOT_FOUND);
	assert_ne!(
		resp.into_body().collect().await.unwrap().to_bytes(),
		BODY.as_ref()
	); /* check that we do not leak body */

	let admin_req = || {
		Request::builder()
			.method("GET")
			.uri(format!(
				"http://127.0.0.1:{0}/check?domain={1}",
				ctx.garage.admin_port, BCKT_NAME
			))
			.body(Body::new(Bytes::new()))
			.unwrap()
	};

	let admin_resp = client.request(admin_req()).await.unwrap();
	assert_eq!(admin_resp.status(), StatusCode::BAD_REQUEST);
	let res_body = json_body(admin_resp).await;
	assert_json_eq!(
		res_body,
		json!({
			"code": "InvalidRequest",
			"message": "Bad request: Domain 'my-website' is not managed by Garage",
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
		.index_document(
			IndexDocument::builder()
				.suffix("home.html")
				.build()
				.unwrap(),
		)
		.error_document(
			ErrorDocument::builder()
				.key("err/error.html")
				.build()
				.unwrap(),
		)
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
				.build()
				.unwrap(),
		)
		.build()
		.unwrap();

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

		let main_rule = cors_res.cors_rules().iter().next().unwrap();

		assert_eq!(main_rule.id.as_ref().unwrap(), "main-rule");
		assert_eq!(
			main_rule.allowed_headers.as_ref().unwrap(),
			&vec!["*".to_string()]
		);
		assert_eq!(&main_rule.allowed_origins, &vec!["*".to_string()]);
		assert_eq!(
			&main_rule.allowed_methods,
			&vec!["GET".to_string(), "PUT".to_string()]
		);
	}

	let client = Client::builder(TokioExecutor::new()).build_http();

	// Test direct requests with CORS
	{
		let req = Request::builder()
			.method("GET")
			.uri(format!("http://127.0.0.1:{}/site/", ctx.garage.web_port))
			.header("Host", format!("{}.web.garage", BCKT_NAME))
			.header("Origin", "https://example.com")
			.body(Body::new(Bytes::new()))
			.unwrap();

		let resp = client.request(req).await.unwrap();

		assert_eq!(resp.status(), StatusCode::OK);
		assert_eq!(
			resp.headers().get("access-control-allow-origin").unwrap(),
			"*"
		);
		assert_eq!(
			resp.into_body().collect().await.unwrap().to_bytes(),
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
			.body(Body::new(Bytes::new()))
			.unwrap();

		let resp = client.request(req).await.unwrap();

		assert_eq!(resp.status(), StatusCode::NOT_FOUND);
		assert_eq!(
			resp.into_body().collect().await.unwrap().to_bytes(),
			BODY_ERR.as_ref()
		);
	}

	// Test x-amz-website-redirect-location
	{
		ctx.client
			.put_object()
			.bucket(&bucket)
			.key("test-redirect.html")
			.website_redirect_location("https://perdu.com")
			.send()
			.await
			.unwrap();

		let req = Request::builder()
			.method("GET")
			.uri(format!(
				"http://127.0.0.1:{}/test-redirect.html",
				ctx.garage.web_port
			))
			.header("Host", format!("{}.web.garage", BCKT_NAME))
			.body(Body::new(Bytes::new()))
			.unwrap();

		let resp = client.request(req).await.unwrap();

		assert_eq!(resp.status(), StatusCode::MOVED_PERMANENTLY);
		assert_eq!(resp.headers().get(LOCATION).unwrap(), "https://perdu.com");
	}

	// Test CORS with an allowed preflight request
	{
		let req = Request::builder()
			.method("OPTIONS")
			.uri(format!("http://127.0.0.1:{}/site/", ctx.garage.web_port))
			.header("Host", format!("{}.web.garage", BCKT_NAME))
			.header("Origin", "https://example.com")
			.header("Access-Control-Request-Method", "PUT")
			.body(Body::new(Bytes::new()))
			.unwrap();

		let resp = client.request(req).await.unwrap();

		assert_eq!(resp.status(), StatusCode::OK);
		assert_eq!(
			resp.headers().get("access-control-allow-origin").unwrap(),
			"*"
		);
		assert_ne!(
			resp.into_body().collect().await.unwrap().to_bytes(),
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
			.body(Body::new(Bytes::new()))
			.unwrap();

		let resp = client.request(req).await.unwrap();

		assert_eq!(resp.status(), StatusCode::FORBIDDEN);
		assert_ne!(
			resp.into_body().collect().await.unwrap().to_bytes(),
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
			.body(Body::new(Bytes::new()))
			.unwrap();

		let resp = client.request(req).await.unwrap();

		assert_eq!(resp.status(), StatusCode::FORBIDDEN);
		assert_ne!(
			resp.into_body().collect().await.unwrap().to_bytes(),
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
			.body(Body::new(Bytes::new()))
			.unwrap();

		let resp = client.request(req).await.unwrap();

		assert_eq!(resp.status(), StatusCode::NOT_FOUND);
		let resp_bytes = resp.into_body().collect().await.unwrap().to_bytes();
		assert_ne!(resp_bytes, BODY_ERR.as_ref());
		assert_ne!(resp_bytes, BODY.as_ref());
	}
}

#[tokio::test]
async fn test_website_check_domain() {
	let ctx = common::context();

	let client = Client::builder(TokioExecutor::new()).build_http();

	let admin_req = || {
		Request::builder()
			.method("GET")
			.uri(format!("http://127.0.0.1:{}/check", ctx.garage.admin_port))
			.body(Body::new(Bytes::new()))
			.unwrap()
	};

	let admin_resp = client.request(admin_req()).await.unwrap();
	assert_eq!(admin_resp.status(), StatusCode::BAD_REQUEST);
	let res_body = json_body(admin_resp).await;
	assert_json_eq!(
		res_body,
		json!({
			"code": "InvalidRequest",
			"message": "Bad request: No domain query string found",
			"region": "garage-integ-test",
			"path": "/check",
		})
	);

	let admin_req = || {
		Request::builder()
			.method("GET")
			.uri(format!(
				"http://127.0.0.1:{}/check?domain=",
				ctx.garage.admin_port
			))
			.body(Body::new(Bytes::new()))
			.unwrap()
	};

	let admin_resp = client.request(admin_req()).await.unwrap();
	assert_eq!(admin_resp.status(), StatusCode::BAD_REQUEST);
	let res_body = json_body(admin_resp).await;
	assert_json_eq!(
		res_body,
		json!({
			"code": "InvalidRequest",
			"message": "Bad request: Domain '' is not managed by Garage",
			"region": "garage-integ-test",
			"path": "/check",
		})
	);

	let admin_req = || {
		Request::builder()
			.method("GET")
			.uri(format!(
				"http://127.0.0.1:{}/check?domain=foobar",
				ctx.garage.admin_port
			))
			.body(Body::new(Bytes::new()))
			.unwrap()
	};

	let admin_resp = client.request(admin_req()).await.unwrap();
	assert_eq!(admin_resp.status(), StatusCode::BAD_REQUEST);
	let res_body = json_body(admin_resp).await;
	assert_json_eq!(
		res_body,
		json!({
			"code": "InvalidRequest",
			"message": "Bad request: Domain 'foobar' is not managed by Garage",
			"region": "garage-integ-test",
			"path": "/check",
		})
	);

	let admin_req = || {
		Request::builder()
			.method("GET")
			.uri(format!(
				"http://127.0.0.1:{}/check?domain=%E2%98%B9",
				ctx.garage.admin_port
			))
			.body(Body::new(Bytes::new()))
			.unwrap()
	};

	let admin_resp = client.request(admin_req()).await.unwrap();
	assert_eq!(admin_resp.status(), StatusCode::BAD_REQUEST);
	let res_body = json_body(admin_resp).await;
	assert_json_eq!(
		res_body,
		json!({
			"code": "InvalidRequest",
			"message": "Bad request: Domain '☹' is not managed by Garage",
			"region": "garage-integ-test",
			"path": "/check",
		})
	);
}

#[tokio::test]
async fn test_website_puny() {
	const BCKT_NAME: &str = "xn--pda.eu";
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

	let client = Client::builder(TokioExecutor::new()).build_http();

	let req = |suffix| {
		Request::builder()
			.method("GET")
			.uri(format!("http://127.0.0.1:{}/", ctx.garage.web_port))
			.header("Host", format!("{}{}", BCKT_NAME, suffix))
			.body(Body::new(Bytes::new()))
			.unwrap()
	};

	ctx.garage
		.command()
		.args(["bucket", "website", "--allow", BCKT_NAME])
		.quiet()
		.expect_success_status("Could not allow website on bucket");

	let mut resp = client.request(req("")).await.unwrap();
	assert_eq!(resp.status(), StatusCode::OK);
	assert_eq!(
		resp.into_body().collect().await.unwrap().to_bytes(),
		BODY.as_ref()
	);

	resp = client.request(req(".web.garage")).await.unwrap();
	assert_eq!(resp.status(), StatusCode::OK);
	assert_eq!(
		resp.into_body().collect().await.unwrap().to_bytes(),
		BODY.as_ref()
	);

	for bname in [
		BCKT_NAME.to_string(),
		format!("{BCKT_NAME}.web.garage"),
		format!("{BCKT_NAME}.s3.garage"),
	] {
		let admin_req = || {
			Request::builder()
				.method("GET")
				.uri(format!(
					"http://127.0.0.1:{0}/check?domain={1}",
					ctx.garage.admin_port, bname
				))
				.body(Body::new(Bytes::new()))
				.unwrap()
		};

		let admin_resp = client.request(admin_req()).await.unwrap();
		assert_eq!(admin_resp.status(), StatusCode::OK);
		assert_eq!(
			admin_resp.into_body().collect().await.unwrap().to_bytes(),
			format!("Domain '{bname}' is managed by Garage").as_bytes()
		);
	}
}

#[tokio::test]
async fn test_website_object_not_found() {
	const BCKT_NAME: &str = "not-found";
	let ctx = common::context();
	let _bucket = ctx.create_bucket(BCKT_NAME);

	let client = Client::builder(TokioExecutor::new()).build_http();

	let req = |suffix| {
		Request::builder()
			.method("GET")
			.uri(format!("http://127.0.0.1:{}/", ctx.garage.web_port))
			.header("Host", format!("{}{}", BCKT_NAME, suffix))
			.body(Body::new(Bytes::new()))
			.unwrap()
	};

	ctx.garage
		.command()
		.args(["bucket", "website", "--allow", BCKT_NAME])
		.quiet()
		.expect_success_status("Could not allow website on bucket");

	let resp = client.request(req("")).await.unwrap();
	assert_eq!(resp.status(), StatusCode::NOT_FOUND);
	// the error we return by default are *not* xml
	assert_eq!(
		resp.headers().get(http::header::CONTENT_TYPE).unwrap(),
		"text/html; charset=utf-8"
	);
	let result = String::from_utf8(
		resp.into_body()
			.collect()
			.await
			.unwrap()
			.to_bytes()
			.to_vec(),
	)
	.unwrap();
	assert!(result.contains("not found"));
}
