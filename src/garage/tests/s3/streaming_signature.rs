use std::collections::HashMap;

use base64::prelude::*;
use crc32fast::Hasher as Crc32;

use crate::common;
use crate::common::ext::CommandExt;
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
		let res = ctx
			.custom_request
			.builder(bucket.clone())
			.method(Method::PUT)
			.path(STD_KEY.to_owned())
			.signed_headers(headers)
			.vhost_style(true)
			.body(vec![])
			.body_signature(BodySignature::Streaming { chunk_size: 10 })
			.send()
			.await
			.unwrap();
		assert!(res.status().is_success(), "got response: {:?}", res);

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
		assert_eq!(o.content_length.unwrap(), 0);
		assert_eq!(o.parts_count, None);
		assert_eq!(o.tag_count, None);
	}

	{
		let etag = "\"46cf18a9b447991b450cad3facf5937e\"";

		let mut crc32 = Crc32::new();
		crc32.update(&BODY[..]);
		let crc32 = BASE64_STANDARD.encode(&u32::to_be_bytes(crc32.finalize())[..]);

		let mut headers = HashMap::new();
		headers.insert("x-amz-checksum-crc32".to_owned(), crc32.clone());

		let res = ctx
			.custom_request
			.builder(bucket.clone())
			.method(Method::PUT)
			//.path(CTRL_KEY.to_owned()) at the moment custom_request does not encode url so this
			//fail
			.path("abc".to_owned())
			.vhost_style(true)
			.signed_headers(headers)
			.body(BODY.to_vec())
			.body_signature(BodySignature::Streaming { chunk_size: 16 })
			.send()
			.await
			.unwrap();
		assert!(res.status().is_success(), "got response: {:?}", res);

		// assert_eq!(r.e_tag.unwrap().as_str(), etag);
		// assert!(r.version_id.is_some());

		let o = ctx
			.client
			.get_object()
			.bucket(&bucket)
			//.key(CTRL_KEY)
			.key("abc")
			.checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
			.send()
			.await
			.unwrap();

		assert_bytes_eq!(o.body, BODY);
		assert_eq!(o.e_tag.unwrap(), etag);
		assert!(o.last_modified.is_some());
		assert_eq!(o.content_length.unwrap(), 62);
		assert_eq!(o.parts_count, None);
		assert_eq!(o.tag_count, None);
		assert_eq!(o.checksum_crc32.unwrap(), crc32);
	}
}

#[tokio::test]
async fn test_putobject_streaming_unsigned_trailer() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("putobject-streaming-unsigned-trailer");

	{
		// Send an empty object (can serve as a directory marker)
		// with a content type
		let etag = "\"d41d8cd98f00b204e9800998ecf8427e\"";
		let content_type = "text/csv";
		let mut headers = HashMap::new();
		headers.insert("content-type".to_owned(), content_type.to_owned());

		let empty_crc32 = BASE64_STANDARD.encode(&u32::to_be_bytes(Crc32::new().finalize())[..]);

		let res = ctx
			.custom_request
			.builder(bucket.clone())
			.method(Method::PUT)
			.path(STD_KEY.to_owned())
			.signed_headers(headers)
			.vhost_style(true)
			.body(vec![])
			.body_signature(BodySignature::StreamingUnsignedTrailer {
				chunk_size: 10,
				trailer_algorithm: "x-amz-checksum-crc32".into(),
				trailer_value: empty_crc32,
			})
			.send()
			.await
			.unwrap();
		assert!(res.status().is_success(), "got response: {:?}", res);

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
		assert_eq!(o.content_length.unwrap(), 0);
		assert_eq!(o.parts_count, None);
		assert_eq!(o.tag_count, None);
	}

	{
		let etag = "\"46cf18a9b447991b450cad3facf5937e\"";

		let mut crc32 = Crc32::new();
		crc32.update(&BODY[..]);
		let crc32 = BASE64_STANDARD.encode(&u32::to_be_bytes(crc32.finalize())[..]);

		// try sending with wrong crc32, check that it fails
		let err_res = ctx
			.custom_request
			.builder(bucket.clone())
			.method(Method::PUT)
			//.path(CTRL_KEY.to_owned()) at the moment custom_request does not encode url so this
			//fail
			.path("abc".to_owned())
			.vhost_style(true)
			.body(BODY.to_vec())
			.body_signature(BodySignature::StreamingUnsignedTrailer {
				chunk_size: 16,
				trailer_algorithm: "x-amz-checksum-crc32".into(),
				trailer_value: "2Yp9Yw==".into(),
			})
			.send()
			.await
			.unwrap();
		assert!(
			err_res.status().is_client_error(),
			"got response: {:?}",
			err_res
		);

		let res = ctx
			.custom_request
			.builder(bucket.clone())
			.method(Method::PUT)
			//.path(CTRL_KEY.to_owned()) at the moment custom_request does not encode url so this
			//fail
			.path("abc".to_owned())
			.vhost_style(true)
			.body(BODY.to_vec())
			.body_signature(BodySignature::StreamingUnsignedTrailer {
				chunk_size: 16,
				trailer_algorithm: "x-amz-checksum-crc32".into(),
				trailer_value: crc32.clone(),
			})
			.send()
			.await
			.unwrap();
		assert!(res.status().is_success(), "got response: {:?}", res);

		// assert_eq!(r.e_tag.unwrap().as_str(), etag);
		// assert!(r.version_id.is_some());

		let o = ctx
			.client
			.get_object()
			.bucket(&bucket)
			//.key(CTRL_KEY)
			.key("abc")
			.checksum_mode(aws_sdk_s3::types::ChecksumMode::Enabled)
			.send()
			.await
			.unwrap();

		assert_bytes_eq!(o.body, BODY);
		assert_eq!(o.e_tag.unwrap(), etag);
		assert!(o.last_modified.is_some());
		assert_eq!(o.content_length.unwrap(), 62);
		assert_eq!(o.parts_count, None);
		assert_eq!(o.tag_count, None);
		assert_eq!(o.checksum_crc32.unwrap(), crc32);
	}
}

#[tokio::test]
async fn test_create_bucket_streaming() {
	let ctx = common::context();
	let bucket = "createbucket-streaming";

	ctx.garage
		.command()
		.args(["key", "allow"])
		.args(["--create-bucket", &ctx.key.id])
		.quiet()
		.expect_success_output("Could not allow key to create buckets");

	{
		// create bucket
		let _ = ctx
			.custom_request
			.builder(bucket.to_owned())
			.method(Method::PUT)
			.body_signature(BodySignature::Streaming { chunk_size: 10 })
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
			.body_signature(BodySignature::Streaming { chunk_size: 10 })
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

		assert_eq!(o.index_document.unwrap().suffix, "home.html");
		assert_eq!(o.error_document.unwrap().key, "err/error.html");
	}
}
