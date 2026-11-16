use quick_xml::de::from_reader;

use hyper::{header::HeaderName, Request, Response, StatusCode};

use garage_model::bucket_table::{Bucket, WebsiteConfig};

use garage_api_common::helpers::*;
use garage_api_common::xml::website::*;

use crate::api_server::{ReqBody, ResBody};
use crate::error::*;
use crate::xml::{to_xml_with_header, Value};

pub const X_AMZ_WEBSITE_REDIRECT_LOCATION: HeaderName =
	HeaderName::from_static("x-amz-website-redirect-location");

pub async fn handle_get_website(ctx: ReqCtx) -> Result<Response<ResBody>, Error> {
	let ReqCtx { bucket_params, .. } = ctx;
	let website = bucket_params.website_config.get();
	if website.enabled {
		let wc = WebsiteConfiguration {
			xmlns: (),
			error_document: website.error_document.as_ref().map(|v| Key {
				key: Value(v.to_string()),
			}),
			index_document: website.index_document.as_ref().map(|doc| Suffix {
				suffix: Value(doc.to_string()),
			}),
			redirect_all_requests_to: None,
			routing_rules: RoutingRules {
				rules: website
					.routing_rules
					.clone()
					.into_iter()
					.map(RoutingRule::from_garage_routing_rule)
					.collect(),
			},
		};
		let xml = to_xml_with_header(&wc)?;
		Ok(Response::builder()
			.status(StatusCode::OK)
			.header(http::header::CONTENT_TYPE, "application/xml")
			.body(string_body(xml))?)
	} else {
		Ok(Response::builder()
			.status(StatusCode::NO_CONTENT)
			.body(empty_body())?)
	}
}

pub async fn handle_delete_website(ctx: ReqCtx) -> Result<Response<ResBody>, Error> {
	let ReqCtx {
		garage,
		bucket_id,
		mut bucket_params,
		..
	} = ctx;
	// In S3 API, disabling website access also deletes all configuration options,
	// such as routing rules, error document, etc.
	// This is OK because when the client enables website access again using S3 API,
	// they will send the entire website configuration again.
	// When using the admin api, we have more precise control, we can disable
	// website access without deleting all configuration options.
	bucket_params
		.website_config
		.update(WebsiteConfig::disabled());
	garage
		.bucket_table
		.insert(&Bucket::present(bucket_id, bucket_params))
		.await?;

	Ok(Response::builder()
		.status(StatusCode::NO_CONTENT)
		.body(empty_body())?)
}

pub async fn handle_put_website(
	ctx: ReqCtx,
	req: Request<ReqBody>,
) -> Result<Response<ResBody>, Error> {
	let ReqCtx {
		garage,
		bucket_id,
		mut bucket_params,
		..
	} = ctx;

	let body = req.into_body().collect().await?;

	let conf: WebsiteConfiguration = from_reader(&body as &[u8])?;
	conf.validate()?;

	bucket_params
		.website_config
		.update(conf.into_garage_website_config()?);
	garage
		.bucket_table
		.insert(&Bucket::present(bucket_id, bucket_params))
		.await?;

	Ok(Response::builder()
		.status(StatusCode::OK)
		.body(empty_body())?)
}
