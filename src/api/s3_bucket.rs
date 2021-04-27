use std::fmt::Write;
use std::sync::Arc;

use hyper::{Body, Response};

use garage_model::garage::Garage;

use crate::error::*;

pub fn handle_get_bucket_location(garage: Arc<Garage>) -> Result<Response<Body>, Error> {
	let mut xml = String::new();

	writeln!(&mut xml, r#"<?xml version="1.0" encoding="UTF-8"?>"#).unwrap();
	writeln!(
		&mut xml,
		r#"<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">{}</LocationConstraint>"#,
		garage.config.s3_api.s3_region
	)
	.unwrap();

	Ok(Response::builder()
		.header("Content-Type", "application/xml")
		.body(Body::from(xml.into_bytes()))?)
}
