use crate::common;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart, Tag, Tagging, TaggingDirective};

const KEY: &str = "test";
const KEY2: &str = "test";

#[tokio::test]
async fn test_bucket_tagging() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("bucket-tags");
	ctx.bucket_allow_owner(&bucket);

	{
		let res = ctx.client.get_bucket_tagging().bucket(&bucket).send().await;
		assert!(res.is_err());
	}

	ctx.client
		.put_bucket_tagging()
		.bucket(&bucket)
		.tagging(
			Tagging::builder()
				.tag_set(Tag::builder().key("key1").value("val1").build().unwrap())
				.tag_set(Tag::builder().key("key2").value("val2").build().unwrap())
				.build()
				.unwrap(),
		)
		.send()
		.await
		.unwrap();

	{
		let o = ctx
			.client
			.get_bucket_tagging()
			.bucket(&bucket)
			.send()
			.await
			.unwrap();
		assert_eq!(o.tag_set.len(), 2);
		assert_eq!(o.tag_set[0].key, "key1");
		assert_eq!(o.tag_set[1].key, "key2");
		assert_eq!(o.tag_set[0].value, "val1");
		assert_eq!(o.tag_set[1].value, "val2");
	}

	ctx.client
		.delete_bucket_tagging()
		.bucket(&bucket)
		.send()
		.await
		.unwrap();

	{
		let res = ctx.client.get_bucket_tagging().bucket(&bucket).send().await;
		assert!(res.is_err());
	}
}

#[tokio::test]
async fn test_object_tagging() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("object-tags");

	let data = ByteStream::from_static(b"Hello world!");
	ctx.client
		.put_object()
		.bucket(&bucket)
		.key(KEY)
		.body(data)
		.send()
		.await
		.unwrap();

	{
		let res = ctx
			.client
			.get_object_tagging()
			.key(KEY)
			.bucket(&bucket)
			.send()
			.await
			.unwrap();
		assert!(res.tag_set.is_empty());
	}

	ctx.client
		.put_object_tagging()
		.bucket(&bucket)
		.key(KEY)
		.tagging(
			Tagging::builder()
				.tag_set(Tag::builder().key("key1").value("val1").build().unwrap())
				.tag_set(Tag::builder().key("key2").value("val2").build().unwrap())
				.build()
				.unwrap(),
		)
		.send()
		.await
		.unwrap();

	check_has_tags(&ctx, &bucket, KEY).await;

	ctx.client
		.delete_object_tagging()
		.bucket(&bucket)
		.key(KEY)
		.send()
		.await
		.unwrap();

	check_has_no_tags(&ctx, &bucket, KEY).await;
}

#[tokio::test]
async fn test_putobject_tagging() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("object-tags");

	let data = ByteStream::from_static(b"Hello world!");
	ctx.client
		.put_object()
		.bucket(&bucket)
		.key(KEY)
		.tagging("key1=val1&key2=val2")
		.body(data)
		.send()
		.await
		.unwrap();

	check_has_tags(&ctx, &bucket, KEY).await;

	ctx.client
		.delete_object_tagging()
		.bucket(&bucket)
		.key(KEY)
		.send()
		.await
		.unwrap();

	check_has_no_tags(&ctx, &bucket, KEY).await;
}

#[tokio::test]
async fn test_copyobject_tagging() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("object-tags");

	let data = ByteStream::from_static(b"Hello world!");
	ctx.client
		.put_object()
		.bucket(&bucket)
		.key(KEY)
		.tagging("key1=val1&key2=val2")
		.body(data)
		.send()
		.await
		.unwrap();

	ctx.client
		.copy_object()
		.bucket(&bucket)
		.key(KEY2)
		.copy_source(format!("{}/{}", bucket, KEY))
		.send()
		.await
		.unwrap();

	check_has_tags(&ctx, &bucket, KEY2).await;

	ctx.client
		.copy_object()
		.bucket(&bucket)
		.key(KEY2)
		.copy_source(format!("{}/{}", bucket, KEY))
		.tagging_directive(TaggingDirective::Replace)
		.send()
		.await
		.unwrap();

	check_has_no_tags(&ctx, &bucket, KEY2).await;

	ctx.client
		.copy_object()
		.bucket(&bucket)
		.key(KEY2)
		.copy_source(format!("{}/{}", bucket, KEY))
		.tagging_directive(TaggingDirective::Replace)
		.tagging("key3=val3")
		.send()
		.await
		.unwrap();

	{
		let o = ctx
			.client
			.get_object_tagging()
			.bucket(bucket)
			.key(KEY2)
			.send()
			.await
			.unwrap();
		assert_eq!(o.tag_set.len(), 1);
		assert_eq!(o.tag_set[0].key, "key3");
		assert_eq!(o.tag_set[0].value, "val3");
	}
}

#[tokio::test]
async fn test_multipart_tagging() {
	let ctx = common::context();
	let bucket = ctx.create_bucket("object-tags");

	let u1 = vec![0x11; 128 * 1024];
	let u2 = vec![0x22; 128 * 1024];

	let up = ctx
		.client
		.create_multipart_upload()
		.bucket(&bucket)
		.key(KEY)
		.tagging("key1=val1&key2=val2")
		.send()
		.await
		.unwrap();
	assert!(up.upload_id.is_some());

	let uid = up.upload_id.as_ref().unwrap();

	let p1 = ctx
		.client
		.upload_part()
		.bucket(&bucket)
		.key(KEY)
		.upload_id(uid)
		.part_number(1)
		.body(ByteStream::from(u1))
		.send()
		.await
		.unwrap();

	let p2 = ctx
		.client
		.upload_part()
		.bucket(&bucket)
		.key(KEY)
		.upload_id(uid)
		.part_number(2)
		.body(ByteStream::from(u2))
		.send()
		.await
		.unwrap();

	let cmp = CompletedMultipartUpload::builder()
		.parts(
			CompletedPart::builder()
				.part_number(1)
				.e_tag(p1.e_tag.unwrap())
				.build(),
		)
		.parts(
			CompletedPart::builder()
				.part_number(2)
				.e_tag(p2.e_tag.unwrap())
				.build(),
		)
		.build();

	ctx.client
		.complete_multipart_upload()
		.bucket(&bucket)
		.key(KEY)
		.upload_id(uid)
		.multipart_upload(cmp)
		.send()
		.await
		.unwrap();

	check_has_tags(&ctx, &bucket, KEY).await;
}

async fn check_has_tags(ctx: &common::Context, bucket: &str, key: &str) {
	let o = ctx
		.client
		.get_object_tagging()
		.bucket(bucket)
		.key(key)
		.send()
		.await
		.unwrap();
	assert_eq!(o.tag_set.len(), 2);
	assert_eq!(o.tag_set[0].key, "key1");
	assert_eq!(o.tag_set[1].key, "key2");
	assert_eq!(o.tag_set[0].value, "val1");
	assert_eq!(o.tag_set[1].value, "val2");
}

async fn check_has_no_tags(ctx: &common::Context, bucket: &str, key: &str) {
	let o = ctx
		.client
		.get_object_tagging()
		.bucket(bucket)
		.key(key)
		.send()
		.await
		.unwrap();
	assert!(o.tag_set.is_empty());
}
