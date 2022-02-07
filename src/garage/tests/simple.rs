use crate::common;

#[tokio::test]
async fn test_simple() {
	use aws_sdk_s3::ByteStream;

	let ctx = common::context();
	let bucket = ctx.create_bucket("test-simple");

	let data = ByteStream::from_static(b"Hello world!");

	ctx.client
		.put_object()
		.bucket(&bucket)
		.key("test")
		.body(data)
		.send()
		.await
		.unwrap();

	let res = ctx
		.client
		.get_object()
		.bucket(&bucket)
		.key("test")
		.send()
		.await
		.unwrap();

	assert_bytes_eq!(res.body, b"Hello world!");
}
