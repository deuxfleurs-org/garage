use crate::common;
use crate::common::ext::*;

const BCKT_NAME: &str = "seau";

#[tokio::test]
async fn test_admin_bucket_perms() {
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
