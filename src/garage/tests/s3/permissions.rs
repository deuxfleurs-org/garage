use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::*;

use crate::common;

#[tokio::test]
async fn test_read_only() {
	let ctx = common::context();
	let bucket = ctx.create_bucket_with_permissions(
		"readonly",
		common::BucketKeyPerm {
			read: true,
			write: false,
			owner: false,
		},
	);

	let data = ByteStream::from_static(b"Hello world!");
	assert!(ctx
		.client
		.put_object()
		.bucket(&bucket)
		.key("test")
		.body(data)
		.send()
		.await
		.is_err());

	let r = ctx
		.client
		.list_objects_v2()
		.bucket(&bucket)
		.send()
		.await
		.unwrap();

	assert!(r.contents.is_none());
	assert!(r.common_prefixes.is_none());
}

#[tokio::test]
async fn test_write_only() {
	let ctx = common::context();
	let bucket = ctx.create_bucket_with_permissions(
		"readonly",
		common::BucketKeyPerm {
			read: false,
			write: true,
			owner: false,
		},
	);

	let data = ByteStream::from_static(b"Hello world!");

	ctx.client
		.put_object()
		.bucket(&bucket)
		.key("test")
		.body(data)
		.send()
		.await
		.unwrap();

	assert!(ctx
		.client
		.put_bucket_lifecycle_configuration()
		.bucket(&bucket)
		.lifecycle_configuration(
			BucketLifecycleConfiguration::builder()
				.set_rules(Some(vec![]))
				.build()
				.unwrap()
		)
		.send()
		.await
		.is_err());

	assert!(ctx
		.client
		.delete_bucket_lifecycle()
		.bucket(&bucket)
		.send()
		.await
		.is_err());
}

#[tokio::test]
async fn test_owner_only() {
	let ctx = common::context();
	let bucket = ctx.create_bucket_with_permissions(
		"readonly",
		common::BucketKeyPerm {
			read: false,
			write: false,
			owner: true,
		},
	);

	ctx.client
		.put_bucket_lifecycle_configuration()
		.bucket(&bucket)
		.lifecycle_configuration(
			BucketLifecycleConfiguration::builder()
				.set_rules(Some(vec![]))
				.build()
				.unwrap(),
		)
		.send()
		.await
		.unwrap();

	ctx.client
		.delete_bucket_lifecycle()
		.bucket(&bucket)
		.send()
		.await
		.unwrap();
}
