use quick_xml::de::from_reader;

use hyper::{header::HeaderName, Method, Request, Response, StatusCode};

use serde::{Deserialize, Serialize};

use garage_model::bucket_table::{Bucket, CorsRule as GarageCorsRule};

use garage_api_common::helpers::*;

use crate::api_server::{ReqBody, ResBody};
use crate::error::*;
use crate::xml::{to_xml_with_header, xmlns_tag, IntValue, Value};

pub async fn handle_get_cors(ctx: ReqCtx) -> Result<Response<ResBody>, Error> {
	let ReqCtx { bucket_params, .. } = ctx;
	if let Some(cors) = bucket_params.cors_config.get() {
		let wc = CorsConfiguration {
			xmlns: (),
			cors_rules: cors
				.iter()
				.map(CorsRule::from_garage_cors_rule)
				.collect::<Vec<_>>(),
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

pub async fn handle_delete_cors(ctx: ReqCtx) -> Result<Response<ResBody>, Error> {
	let ReqCtx {
		garage,
		bucket_id,
		mut bucket_params,
		..
	} = ctx;
	bucket_params.cors_config.update(None);
	garage
		.bucket_table
		.insert(&Bucket::present(bucket_id, bucket_params))
		.await?;

	Ok(Response::builder()
		.status(StatusCode::NO_CONTENT)
		.body(empty_body())?)
}

pub async fn handle_put_cors(
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

	let conf: CorsConfiguration = from_reader(&body as &[u8])?;
	conf.validate()?;

	bucket_params
		.cors_config
		.update(Some(conf.into_garage_cors_config()?));
	garage
		.bucket_table
		.insert(&Bucket::present(bucket_id, bucket_params))
		.await?;

	Ok(Response::builder()
		.status(StatusCode::OK)
		.body(empty_body())?)
}

// ---- SERIALIZATION AND DESERIALIZATION TO/FROM S3 XML ----

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename = "CORSConfiguration")]
pub struct CorsConfiguration {
	#[serde(serialize_with = "xmlns_tag", skip_deserializing)]
	pub xmlns: (),
	#[serde(rename = "CORSRule")]
	pub cors_rules: Vec<CorsRule>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct CorsRule {
	#[serde(rename = "ID")]
	pub id: Option<Value>,
	#[serde(rename = "MaxAgeSeconds")]
	pub max_age_seconds: Option<IntValue>,
	#[serde(rename = "AllowedOrigin")]
	pub allowed_origins: Vec<Value>,
	#[serde(rename = "AllowedMethod")]
	pub allowed_methods: Vec<Value>,
	#[serde(rename = "AllowedHeader", default)]
	pub allowed_headers: Vec<Value>,
	#[serde(rename = "ExposeHeader", default)]
	pub expose_headers: Vec<Value>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct AllowedMethod {
	#[serde(rename = "AllowedMethod")]
	pub allowed_method: Value,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct AllowedHeader {
	#[serde(rename = "AllowedHeader")]
	pub allowed_header: Value,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct ExposeHeader {
	#[serde(rename = "ExposeHeader")]
	pub expose_header: Value,
}

impl CorsConfiguration {
	pub fn validate(&self) -> Result<(), Error> {
		for r in self.cors_rules.iter() {
			r.validate()?;
		}
		Ok(())
	}

	pub fn into_garage_cors_config(self) -> Result<Vec<GarageCorsRule>, Error> {
		Ok(self
			.cors_rules
			.iter()
			.map(CorsRule::to_garage_cors_rule)
			.collect())
	}
}

impl CorsRule {
	pub fn validate(&self) -> Result<(), Error> {
		for method in self.allowed_methods.iter() {
			method
				.0
				.parse::<Method>()
				.ok_or_bad_request("Invalid CORSRule method")?;
		}
		for header in self
			.allowed_headers
			.iter()
			.chain(self.expose_headers.iter())
		{
			header
				.0
				.parse::<HeaderName>()
				.ok_or_bad_request("Invalid HTTP header name")?;
		}
		Ok(())
	}

	pub fn to_garage_cors_rule(&self) -> GarageCorsRule {
		let convert_vec =
			|vval: &[Value]| vval.iter().map(|x| x.0.to_owned()).collect::<Vec<String>>();
		GarageCorsRule {
			id: self.id.as_ref().map(|x| x.0.to_owned()),
			max_age_seconds: self.max_age_seconds.as_ref().map(|x| x.0 as u64),
			allow_origins: convert_vec(&self.allowed_origins),
			allow_methods: convert_vec(&self.allowed_methods),
			allow_headers: convert_vec(&self.allowed_headers),
			expose_headers: convert_vec(&self.expose_headers),
		}
	}

	pub fn from_garage_cors_rule(rule: &GarageCorsRule) -> Self {
		let convert_vec = |vval: &[String]| {
			vval.iter()
				.map(|x| Value(x.clone()))
				.collect::<Vec<Value>>()
		};
		Self {
			id: rule.id.as_ref().map(|x| Value(x.clone())),
			max_age_seconds: rule.max_age_seconds.map(|x| IntValue(x as i64)),
			allowed_origins: convert_vec(&rule.allow_origins),
			allowed_methods: convert_vec(&rule.allow_methods),
			allowed_headers: convert_vec(&rule.allow_headers),
			expose_headers: convert_vec(&rule.expose_headers),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	use quick_xml::de::from_str;

	#[test]
	fn test_deserialize() -> Result<(), Error> {
		let message = r#"<?xml version="1.0" encoding="UTF-8"?>
<CORSConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
 <CORSRule>
   <AllowedOrigin>http://www.example.com</AllowedOrigin>

   <AllowedMethod>PUT</AllowedMethod>
   <AllowedMethod>POST</AllowedMethod>
   <AllowedMethod>DELETE</AllowedMethod>

   <AllowedHeader>*</AllowedHeader>
 </CORSRule>
 <CORSRule>
   <AllowedOrigin>*</AllowedOrigin>
   <AllowedMethod>GET</AllowedMethod>
 </CORSRule>
 <CORSRule>
   <ID>qsdfjklm</ID>
   <MaxAgeSeconds>12345</MaxAgeSeconds>
   <AllowedOrigin>https://perdu.com</AllowedOrigin>

   <AllowedMethod>GET</AllowedMethod>
   <AllowedMethod>DELETE</AllowedMethod>
   <AllowedHeader>*</AllowedHeader>
   <ExposeHeader>*</ExposeHeader>
 </CORSRule>
</CORSConfiguration>"#;
		let conf: CorsConfiguration = from_str(message).unwrap();
		let ref_value = CorsConfiguration {
			xmlns: (),
			cors_rules: vec![
				CorsRule {
					id: None,
					max_age_seconds: None,
					allowed_origins: vec!["http://www.example.com".into()],
					allowed_methods: vec!["PUT".into(), "POST".into(), "DELETE".into()],
					allowed_headers: vec!["*".into()],
					expose_headers: vec![],
				},
				CorsRule {
					id: None,
					max_age_seconds: None,
					allowed_origins: vec!["*".into()],
					allowed_methods: vec!["GET".into()],
					allowed_headers: vec![],
					expose_headers: vec![],
				},
				CorsRule {
					id: Some("qsdfjklm".into()),
					max_age_seconds: Some(IntValue(12345)),
					allowed_origins: vec!["https://perdu.com".into()],
					allowed_methods: vec!["GET".into(), "DELETE".into()],
					allowed_headers: vec!["*".into()],
					expose_headers: vec!["*".into()],
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
