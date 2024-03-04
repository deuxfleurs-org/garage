use std::time::{Duration, SystemTime};

use crate::common;
use aws_sdk_s3::presigning::PresigningConfig;
use bytes::Bytes;
use hyper::{Body, Request};

const STD_KEY: &str = "hello world";
const BODY: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

#[tokio::test]
async fn test_presigned_url() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("presigned");

	let etag = "\"46cf18a9b447991b450cad3facf5937e\"";
	let body = Bytes::from(BODY.to_vec());

	let psc = PresigningConfig::builder()
		.start_time(SystemTime::now() - Duration::from_secs(60))
		.expires_in(Duration::from_secs(3600))
		.build()
		.unwrap();

	{
		// PutObject
		let req = ctx
			.client
			.put_object()
			.bucket(&bucket)
			.key(STD_KEY)
			.presigned(psc.clone())
			.await
			.unwrap();

		let client = ctx.custom_request.client();
		let req = Request::builder()
			.method("PUT")
			.uri(req.uri())
			.body(body.clone().into())
			.unwrap();
		let res = client.request(req).await.unwrap();
		assert_eq!(res.status(), 200);
		assert_eq!(res.headers().get("etag").unwrap(), etag);
	}

	{
		// GetObject
		let req = ctx
			.client
			.get_object()
			.bucket(&bucket)
			.key(STD_KEY)
			.presigned(psc)
			.await
			.unwrap();

		let client = ctx.custom_request.client();
		let req = Request::builder()
			.method("GET")
			.uri(req.uri())
			.body(Body::empty())
			.unwrap();
		let res = client.request(req).await.unwrap();
		assert_eq!(res.status(), 200);
		assert_eq!(res.headers().get("etag").unwrap(), etag);

		let body2 = hyper::body::to_bytes(res.into_body()).await.unwrap();
		assert_eq!(body, body2);
	}
}
