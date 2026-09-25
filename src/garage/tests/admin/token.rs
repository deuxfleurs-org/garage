use crate::common;

#[tokio::test]
async fn test_get_current_admin_token() {
	let ctx = common::context();

	// Test with the correct admin token in Authorization header
	// should work
	let rep = ctx
		.admin_get("GetCurrentAdminTokenInfo")
		.send()
		.await
		.unwrap();
	eprintln!("{:?}", rep);
	assert!(rep.status().is_success());

	// Test with an invalid admin token in Authorization header
	// should return HTTP 403
	let rep = ctx
		.admin_client
		.get(format!(
			"{}/v2/GetCurrentAdminTokenInfo",
			ctx.garage.admin_uri()
		))
		.bearer_auth("notvalid")
		.send()
		.await
		.unwrap();
	eprintln!("{:?}", rep);
	assert!(rep.status().is_client_error());

	// Test with the correct admin token in Authorization header,
	// but preceded by an extraneous space character: should work as well
	// This causes a server crash in Garage <= v2.4.1
	let rep = ctx
		.admin_client
		.get(format!(
			"{}/v2/GetCurrentAdminTokenInfo",
			ctx.garage.admin_uri()
		))
		.bearer_auth(format!(" {}", ctx.garage.admin_token))
		.send()
		.await
		.unwrap();
	eprintln!("{:?}", rep);
	assert!(rep.status().is_client_error());
}

#[tokio::test]
async fn test_admin_post() {
	let ctx = common::context();

	let rep = ctx
		.admin_post("PreviewClusterLayoutChanges")
		.send()
		.await
		.unwrap();
	assert!(rep.status().is_success());
}
