use quick_xml::de::from_reader;
use std::sync::Arc;

use hyper::{Body, Request, Response, StatusCode};

use serde::{Deserialize, Serialize};

use crate::s3::error::*;
use crate::s3::xml::{to_xml_with_header, xmlns_tag, IntValue, Value};
use crate::signature::verify_signed_content;

use garage_model::bucket_table::{
	Bucket, LifecycleExpiration as GarageLifecycleExpiration,
	LifecycleFilter as GarageLifecycleFilter, LifecycleRule as GarageLifecycleRule,
};
use garage_model::garage::Garage;
use garage_util::data::*;

pub async fn handle_get_lifecycle(bucket: &Bucket) -> Result<Response<Body>, Error> {
	let param = bucket
		.params()
		.ok_or_internal_error("Bucket should not be deleted at this point")?;

	if let Some(lifecycle) = param.lifecycle_config.get() {
		let wc = LifecycleConfiguration {
			xmlns: (),
			lifecycle_rules: lifecycle
				.iter()
				.map(LifecycleRule::from_garage_lifecycle_rule)
				.collect::<Vec<_>>(),
		};
		let xml = to_xml_with_header(&wc)?;
		Ok(Response::builder()
			.status(StatusCode::OK)
			.header(http::header::CONTENT_TYPE, "application/xml")
			.body(Body::from(xml))?)
	} else {
		Ok(Response::builder()
			.status(StatusCode::NO_CONTENT)
			.body(Body::empty())?)
	}
}

pub async fn handle_delete_lifecycle(
	garage: Arc<Garage>,
	bucket_id: Uuid,
) -> Result<Response<Body>, Error> {
	let mut bucket = garage
		.bucket_helper()
		.get_existing_bucket(bucket_id)
		.await?;

	let param = bucket.params_mut().unwrap();

	param.lifecycle_config.update(None);
	garage.bucket_table.insert(&bucket).await?;

	Ok(Response::builder()
		.status(StatusCode::NO_CONTENT)
		.body(Body::empty())?)
}

pub async fn handle_put_lifecycle(
	garage: Arc<Garage>,
	bucket_id: Uuid,
	req: Request<Body>,
	content_sha256: Option<Hash>,
) -> Result<Response<Body>, Error> {
	let body = hyper::body::to_bytes(req.into_body()).await?;

	if let Some(content_sha256) = content_sha256 {
		verify_signed_content(content_sha256, &body[..])?;
	}

	let mut bucket = garage
		.bucket_helper()
		.get_existing_bucket(bucket_id)
		.await?;

	let param = bucket.params_mut().unwrap();

	let conf: LifecycleConfiguration = from_reader(&body as &[u8])?;

	param
		.lifecycle_config
		.update(Some(conf.validate_into_garage_lifecycle_config()?));
	garage.bucket_table.insert(&bucket).await?;

	Ok(Response::builder()
		.status(StatusCode::OK)
		.body(Body::empty())?)
}

// ---- SERIALIZATION AND DESERIALIZATION TO/FROM S3 XML ----

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename = "LifecycleConfiguration")]
pub struct LifecycleConfiguration {
	#[serde(serialize_with = "xmlns_tag", skip_deserializing)]
	pub xmlns: (),
	#[serde(rename = "Rule")]
	pub lifecycle_rules: Vec<LifecycleRule>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct LifecycleRule {
	#[serde(rename = "ID")]
	pub id: Option<Value>,
	#[serde(rename = "Status")]
	pub status: Value,
	#[serde(rename = "Filter", default)]
	pub filter: Filter,
	#[serde(rename = "Expiration", default)]
	pub expiration: Option<Expiration>,
	#[serde(rename = "AbortIncompleteMultipartUpload", default)]
	pub abort_incomplete_mpu: Option<AbortIncompleteMpu>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct Filter {
	#[serde(rename = "And")]
	pub and: Option<Box<Filter>>,
	#[serde(rename = "Prefix")]
	pub prefix: Option<Value>,
	#[serde(rename = "ObjectSizeGreaterThan")]
	pub size_gt: Option<IntValue>,
	#[serde(rename = "ObjectSizeLessThan")]
	pub size_lt: Option<IntValue>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Expiration {
	#[serde(rename = "Days")]
	pub days: Option<IntValue>,
	#[serde(rename = "Date")]
	pub at_date: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct AbortIncompleteMpu {
	#[serde(rename = "DaysAfterInitiation")]
	pub days: Option<IntValue>,
}

impl LifecycleConfiguration {
	pub fn validate_into_garage_lifecycle_config(self) -> Result<Vec<GarageLifecycleRule>, Error> {
		let mut ret = vec![];
		for rule in self.lifecycle_rules {
			ret.push(rule.validate_into_garage_lifecycle_rule()?);
		}
		Ok(ret)
	}

	pub fn from_garage_lifecycle_config(config: &[GarageLifecycleRule]) -> Self {
		Self {
			xmlns: (),
			lifecycle_rules: config
				.iter()
				.map(LifecycleRule::from_garage_lifecycle_rule)
				.collect(),
		}
	}
}

impl LifecycleRule {
	pub fn validate_into_garage_lifecycle_rule(self) -> Result<GarageLifecycleRule, Error> {
		todo!()
	}

	pub fn from_garage_lifecycle_rule(rule: &GarageLifecycleRule) -> Self {
		todo!()
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	use quick_xml::de::from_str;

	#[test]
	fn test_deserialize_lifecycle_config() -> Result<(), Error> {
		let message = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Rule>
    <ID>id1</ID>
    <Status>Enabled</Status>
    <Filter>
       <Prefix>documents/</Prefix>
    </Filter>
    <AbortIncompleteMultipartUpload>
       <DaysAfterInitiation>7</DaysAfterInitiation>
    </AbortIncompleteMultipartUpload>
  </Rule>
  <Rule>
    <ID>id2</ID>
    <Status>Enabled</Status>
    <Filter>
       <And>
          <Prefix>logs/</Prefix>
          <ObjectSizeGreaterThan>1000000</ObjectSizeGreaterThan>
       </And>
    </Filter>
    <Expiration>
      <Days>365</Days>
    </Expiration>
  </Rule>
</LifecycleConfiguration>"#;
		let conf: LifecycleConfiguration = from_str(message).unwrap();
		let ref_value = LifecycleConfiguration {
			xmlns: (),
			lifecycle_rules: vec![
				LifecycleRule {
					id: Some("id1".into()),
					status: "Enabled".into(),
					filter: Filter {
						prefix: Some("documents/".into()),
						..Default::default()
					},
					expiration: None,
					abort_incomplete_mpu: Some(AbortIncompleteMpu {
						days: Some(IntValue(7)),
					}),
				},
				LifecycleRule {
					id: Some("id2".into()),
					status: "Enabled".into(),
					filter: Filter {
						and: Some(Box::new(Filter {
							prefix: Some("logs/".into()),
							size_gt: Some(IntValue(1000000)),
							..Default::default()
						})),
						..Default::default()
					},
					expiration: Some(Expiration {
						days: Some(IntValue(365)),
						at_date: None,
					}),
					abort_incomplete_mpu: None,
				},
			],
		};
		assert_eq! {
			ref_value,
			conf
		};

		let message2 = to_xml_with_header(&ref_value)?;

		let cleanup = |c: &str| c.replace(char::is_whitespace, "");
		assert_eq!(cleanup(message), cleanup(&message2));

		Ok(())
	}
}
