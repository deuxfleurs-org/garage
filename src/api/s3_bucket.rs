use std::fmt::Write;
use std::sync::Arc;

use hyper::{Body, Response};
use quick_xml::se::to_string;
use serde::Serialize;

use garage_model::garage::Garage;
use garage_model::key_table::Key;
use garage_util::time::*;

use crate::error::*;

#[derive(Debug, Serialize, PartialEq)]
struct CreationDate {
	#[serde(rename = "$value")]
	pub body: String,
}
#[derive(Debug, Serialize, PartialEq)]
struct Name {
	#[serde(rename = "$value")]
	pub body: String,
}
#[derive(Debug, Serialize, PartialEq)]
struct Bucket {
	#[serde(rename = "CreationDate")]
	pub creation_date: CreationDate,
	#[serde(rename = "Name")]
	pub name: Name,
}
#[derive(Debug, Serialize, PartialEq)]
struct DisplayName {
	#[serde(rename = "$value")]
	pub body: String,
}
#[derive(Debug, Serialize, PartialEq)]
struct ID {
	#[serde(rename = "$value")]
	pub body: String,
}
#[derive(Debug, Serialize, PartialEq)]
struct Owner {
	#[serde(rename = "DisplayName")]
	display_name: DisplayName,
	#[serde(rename = "ID")]
	id: ID,
}
#[derive(Debug, Serialize, PartialEq)]
struct BucketList {
	#[serde(rename = "Bucket")]
	pub entries: Vec<Bucket>,
}
#[derive(Debug, Serialize, PartialEq)]
struct ListAllMyBucketsResult {
	#[serde(rename = "Buckets")]
	buckets: BucketList,
	#[serde(rename = "Owner")]
	owner: Owner,
}

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

pub fn handle_list_buckets(api_key: &Key) -> Result<Response<Body>, Error> {
	let list_buckets = ListAllMyBucketsResult {
		owner: Owner {
			display_name: DisplayName {
				body: api_key.name.get().to_string(),
			},
			id: ID {
				body: api_key.key_id.to_string(),
			},
		},
		buckets: BucketList {
			entries: api_key
				.authorized_buckets
				.items()
				.iter()
				.map(|(name, ts, _)| Bucket {
					creation_date: CreationDate {
						body: msec_to_rfc3339(*ts),
					},
					name: Name {
						body: name.to_string(),
					},
				})
				.collect(),
		},
	};

	let mut xml = r#"<?xml version="1.0" encoding="UTF-8"?>"#.to_string();
	xml.push_str(&to_string(&list_buckets)?);
	trace!("xml: {}", xml);

	Ok(Response::builder()
		.header("Content-Type", "application/xml")
		.body(Body::from(xml))?)
}
