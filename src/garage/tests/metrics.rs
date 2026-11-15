use bytes::Bytes;
use http::{Request, StatusCode};
use http_body_util::{BodyExt, Full};

use crate::common;

#[tokio::test]
async fn check_metrics_name() {
	let ctx = common::context();

	let req_url = format!("{}/metrics", ctx.garage.admin_uri());
	let client = ctx.custom_request.client();
	let get_metrics_req = Request::builder()
		.method("GET")
		.uri(req_url)
		.body(Full::new(Bytes::new()))
		.unwrap();

	let response = client
		.request(get_metrics_req)
		.await
		.expect("failed to build 'get metrics' request");

	assert_eq!(response.status(), StatusCode::OK);
	let body = BodyExt::collect(response.into_body())
		.await
		.expect("failed to collect bytes from body stream")
		.to_bytes();
	let body = String::from_utf8_lossy(&body);

	//dbg!(&body);
	let invalid_metrics_name = body
		.lines()
		.filter(is_not_comment_line) // skip the comment lines
		.filter(is_not_target_info)
		.filter(hasnt_prefix_garage)
		.collect::<Vec<_>>();

	if !invalid_metrics_name.is_empty() {
		panic!("metrics name should all start with 'garage_' prefix.\nDoc: https://prometheus.io/docs/practices/naming/#metric-names\n\nInvalid:\n{:#?}", invalid_metrics_name);
	}
}

fn is_not_comment_line(line: &&str) -> bool {
	!line.starts_with("#")
}

fn is_not_target_info(line: &&str) -> bool {
	!line.starts_with("target_info")
}

fn hasnt_prefix_garage(line: &&str) -> bool {
	!line.starts_with("garage_") || line.starts_with("garage_garage")
}
