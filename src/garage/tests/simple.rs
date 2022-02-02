use crate::common;

#[tokio::test]
async fn test_simple() {
	use aws_sdk_s3::ByteStream;

	let ctx = common::context();
	ctx.create_bucket("test-simple");

	let data = ByteStream::from_static(b"Hello world!");

	ctx.client
		.put_object()
		.bucket("test-simple")
		.key("test")
		.body(data)
		.send()
		.await
		.unwrap();

	let res = ctx
		.client
		.get_object()
		.bucket("test-simple")
		.key("test")
		.send()
		.await
		.unwrap();

	assert_bytes_eq!(res.body, b"Hello world!");
}

#[tokio::test]
async fn test_simple_2() {
	use aws_sdk_s3::ByteStream;

	let ctx = common::context();
	ctx.create_bucket("test-simple-2");

	let data = ByteStream::from_static(b"Hello world!");

	ctx.client
		.put_object()
		.bucket("test-simple-2")
		.key("test")
		.body(data)
		.send()
		.await
		.unwrap();

	let res = ctx
		.client
		.get_object()
		.bucket("test-simple-2")
		.key("test")
		.send()
		.await
		.unwrap();

	assert_bytes_eq!(res.body, b"Hello world!");
}
