use std::collections::HashMap;

use crate::common;
use common::custom_requester::BodySignature;
use hyper::Method;

const STD_KEY: &str = "hello-world";
//const CTRL_KEY: &str = "\x00\x01\x02\x00";
const BODY: &[u8; 62] = b"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

#[tokio::test]
async fn test_putobject_streaming() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("putobject-streaming");

	{
		// Send an empty object (can serve as a directory marker)
		// with a content type
		let etag = "\"d41d8cd98f00b204e9800998ecf8427e\"";
		let content_type = "text/csv";
		let mut headers = HashMap::new();
		headers.insert("content-type".to_owned(), content_type.to_owned());
		let _ = ctx
			.custom_request
			.builder(bucket.clone())
			.method(Method::PUT)
			.path(STD_KEY.to_owned())
			.unsigned_headers(headers)
			.vhost_style(true)
			.body(vec![])
			.body_signature(BodySignature::Streaming(10))
			.send()
			.await
			.unwrap();

		// assert_eq!(r.e_tag.unwrap().as_str(), etag);
		// We return a version ID here
		// We should check if Amazon is returning one when versioning is not enabled
		// assert!(r.version_id.is_some());

		//let _version = r.version_id.unwrap();

		let o = ctx
			.client
			.get_object()
			.bucket(&bucket)
			.key(STD_KEY)
			.send()
			.await
			.unwrap();

		assert_bytes_eq!(o.body, b"");
		assert_eq!(o.e_tag.unwrap(), etag);
		// We do not return version ID
		// We should check if Amazon is returning one when versioning is not enabled
		// assert_eq!(o.version_id.unwrap(), _version);
		assert_eq!(o.content_type.unwrap(), content_type);
		assert!(o.last_modified.is_some());
		assert_eq!(o.content_length, 0);
		assert_eq!(o.parts_count, 0);
		assert_eq!(o.tag_count, 0);
	}

	{
		let etag = "\"46cf18a9b447991b450cad3facf5937e\"";

		let _ = ctx
			.custom_request
			.builder(bucket.clone())
			.method(Method::PUT)
			//.path(CTRL_KEY.to_owned()) at the moment custom_request does not encode url so this
			//fail
			.path("abc".to_owned())
			.vhost_style(true)
			.body(BODY.to_vec())
			.body_signature(BodySignature::Streaming(16))
			.send()
			.await
			.unwrap();

		// assert_eq!(r.e_tag.unwrap().as_str(), etag);
		// assert!(r.version_id.is_some());

		let o = ctx
			.client
			.get_object()
			.bucket(&bucket)
			//.key(CTRL_KEY)
			.key("abc")
			.send()
			.await
			.unwrap();

		assert_bytes_eq!(o.body, BODY);
		assert_eq!(o.e_tag.unwrap(), etag);
		assert!(o.last_modified.is_some());
		assert_eq!(o.content_length, 62);
		assert_eq!(o.parts_count, 0);
		assert_eq!(o.tag_count, 0);
	}
}

#[tokio::test]
async fn test_create_bucket_streaming() {
	let ctx = common::context();
	let bucket = "createbucket-streaming";

	{
		// create bucket
		let _ = ctx
			.custom_request
			.builder(bucket.to_owned())
			.method(Method::PUT)
			.body_signature(BodySignature::Streaming(10))
			.send()
			.await
			.unwrap();

		// test if the bucket exists and works properly
		let etag = "\"d41d8cd98f00b204e9800998ecf8427e\"";
		let content_type = "text/csv";
		let _ = ctx
			.client
			.put_object()
			.bucket(bucket)
			.key(STD_KEY)
			.content_type(content_type)
			.send()
			.await
			.unwrap();

		let o = ctx
			.client
			.get_object()
			.bucket(bucket)
			.key(STD_KEY)
			.send()
			.await
			.unwrap();

		assert_eq!(o.e_tag.unwrap(), etag);
	}
}

#[tokio::test]
async fn test_put_website_streaming() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("putwebsite-streaming");

	{
		let website_config = r#"<?xml version="1.0" encoding="UTF-8"?>
<WebsiteConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <ErrorDocument>
      <Key>err/error.html</Key>
   </ErrorDocument>
   <IndexDocument>
      <Suffix>home.html</Suffix>
   </IndexDocument>
</WebsiteConfiguration>"#;

		let mut query = HashMap::new();
		query.insert("website".to_owned(), None);
		let _ = ctx
			.custom_request
			.builder(bucket.clone())
			.method(Method::PUT)
			.query_params(query)
			.body(website_config.as_bytes().to_vec())
			.body_signature(BodySignature::Streaming(10))
			.send()
			.await
			.unwrap();

		let o = ctx
			.client
			.get_bucket_website()
			.bucket(&bucket)
			.send()
			.await
			.unwrap();

		assert_eq!(o.index_document.unwrap().suffix.unwrap(), "home.html");
		assert_eq!(o.error_document.unwrap().key.unwrap(), "err/error.html");
	}
}
