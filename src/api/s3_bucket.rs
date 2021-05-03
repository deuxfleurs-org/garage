use std::sync::Arc;

use hyper::{Body, Response};

use garage_model::garage::Garage;
use garage_model::key_table::Key;
use garage_util::time::*;

use crate::error::*;
use crate::s3_xml;

pub fn handle_get_bucket_location(garage: Arc<Garage>) -> Result<Response<Body>, Error> {
	let loc = s3_xml::LocationConstraint {
		xmlns: (),
		region: garage.config.s3_api.s3_region.to_string(),
	};
	let xml = s3_xml::to_xml_with_header(&loc)?;

	Ok(Response::builder()
		.header("Content-Type", "application/xml")
		.body(Body::from(xml.into_bytes()))?)
}

pub fn handle_list_buckets(api_key: &Key) -> Result<Response<Body>, Error> {
	let list_buckets = s3_xml::ListAllMyBucketsResult {
		owner: s3_xml::Owner {
			display_name: s3_xml::Value(api_key.name.get().to_string()),
			id: s3_xml::Value(api_key.key_id.to_string()),
		},
		buckets: s3_xml::BucketList {
			entries: api_key
				.authorized_buckets
				.items()
				.iter()
				.map(|(name, ts, _)| s3_xml::Bucket {
					creation_date: s3_xml::Value(msec_to_rfc3339(*ts)),
					name: s3_xml::Value(name.to_string()),
				})
				.collect(),
		},
	};

	let xml = s3_xml::to_xml_with_header(&list_buckets)?;
	trace!("xml: {}", xml);

	Ok(Response::builder()
		.header("Content-Type", "application/xml")
		.body(Body::from(xml))?)
}
