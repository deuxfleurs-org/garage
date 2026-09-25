use crate::common;
use crate::common::ext::CommandExt;
use aws_sdk_s3::operation::delete_bucket::DeleteBucketOutput;

#[tokio::test]
async fn test_bucket() {
	let ctx = common::context();
	let bucket_name = "hello";

	{
		// Check bucket cannot be created if not authorized
		ctx.garage
			.command()
			.args(["key", "deny"])
			.args(["--create-bucket", &ctx.key.id])
			.quiet()
			.expect_success_output("Could not deny key to create buckets");

		// Try create bucket, should fail
		let r = ctx.client.create_bucket().bucket(bucket_name).send().await;
		assert!(r.is_err());
	}
	{
		// Now allow key to create bucket
		ctx.garage
			.command()
			.args(["key", "allow"])
			.args(["--create-bucket", &ctx.key.id])
			.quiet()
			.expect_success_output("Could not deny key to create buckets");

		// Create bucket
		//@TODO check with an invalid bucket name + with an already existing bucket
		let r = ctx
			.client
			.create_bucket()
			.bucket(bucket_name)
			.send()
			.await
			.unwrap();

		assert_eq!(r.location.unwrap(), "/hello");
	}
	{
		// List buckets
		let r = ctx.client.list_buckets().send().await.unwrap();
		assert!(r
			.buckets
			.as_ref()
			.unwrap()
			.iter()
			.filter(|x| x.name.as_ref().is_some())
			.any(|x| x.name.as_ref().unwrap() == "hello"));
	}
	{
		// Get its location
		let r = ctx
			.client
			.get_bucket_location()
			.bucket(bucket_name)
			.send()
			.await
			.unwrap();

		assert_eq!(r.location_constraint.unwrap().as_str(), "garage-integ-test");
	}
	{
		// (Stub) check GetVersioning
		let r = ctx
			.client
			.get_bucket_versioning()
			.bucket(bucket_name)
			.send()
			.await
			.unwrap();

		assert!(r.status.is_none());
	}
	{
		// Delete bucket
		// @TODO add a check with a non-empty bucket and check failure
		let r = ctx
			.client
			.delete_bucket()
			.bucket(bucket_name)
			.send()
			.await
			.unwrap();

		assert_eq!(r, DeleteBucketOutput::builder().build());
	}
	{
		// Check bucket is deleted with List buckets
		let r = ctx.client.list_buckets().send().await.unwrap();
		assert!(!r
			.buckets
			.as_ref()
			.unwrap()
			.iter()
			.filter(|x| x.name.as_ref().is_some())
			.any(|x| x.name.as_ref().unwrap() == "hello"));
	}
}

#[tokio::test]
async fn test_bucket_perms() {
	const BCKT_NAME: &str = "seau";

	let ctx = common::context();

	let hb = || ctx.client.head_bucket().bucket(BCKT_NAME).send();

	assert!(hb().await.is_err());

	ctx.garage
		.command()
		.args(["bucket", "create", BCKT_NAME])
		.quiet()
		.expect_success_status("Could not create bucket");

	assert!(hb().await.is_err());

	ctx.garage
		.command()
		.args(["bucket", "allow", "--read", "--key", &ctx.key.id, BCKT_NAME])
		.quiet()
		.expect_success_status("Could not create bucket");

	assert!(hb().await.is_ok());

	ctx.garage
		.command()
		.args(["bucket", "deny", "--read", "--key", &ctx.key.id, BCKT_NAME])
		.quiet()
		.expect_success_status("Could not create bucket");

	assert!(hb().await.is_err());

	ctx.garage
		.command()
		.args(["bucket", "allow", "--read", "--key", &ctx.key.id, BCKT_NAME])
		.quiet()
		.expect_success_status("Could not create bucket");

	assert!(hb().await.is_ok());

	ctx.garage
		.command()
		.args(["bucket", "delete", "--yes", BCKT_NAME])
		.quiet()
		.expect_success_status("Could not delete bucket");

	assert!(hb().await.is_err());
}
