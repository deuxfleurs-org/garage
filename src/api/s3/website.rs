use quick_xml::de::from_reader;

use hyper::{header::HeaderName, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};

use garage_model::bucket_table::*;

use garage_api_common::helpers::*;

use crate::api_server::{ReqBody, ResBody};
use crate::error::*;
use crate::xml::{to_xml_with_header, xmlns_tag, IntValue, Value};

pub const X_AMZ_WEBSITE_REDIRECT_LOCATION: HeaderName =
	HeaderName::from_static("x-amz-website-redirect-location");

pub async fn handle_get_website(ctx: ReqCtx) -> Result<Response<ResBody>, Error> {
	let ReqCtx { bucket_params, .. } = ctx;
	if let Some(website) = bucket_params.website_config.get() {
		let wc = WebsiteConfiguration {
			xmlns: (),
			error_document: website.error_document.as_ref().map(|v| Key {
				key: Value(v.to_string()),
			}),
			index_document: Some(Suffix {
				suffix: Value(website.index_document.to_string()),
			}),
			redirect_all_requests_to: None,
			routing_rules: None,
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
	bucket_params.website_config.update(None);
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
		.update(Some(conf.into_garage_website_config()?));
	garage
		.bucket_table
		.insert(&Bucket::present(bucket_id, bucket_params))
		.await?;

	Ok(Response::builder()
		.status(StatusCode::OK)
		.body(empty_body())?)
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct WebsiteConfiguration {
	#[serde(serialize_with = "xmlns_tag", skip_deserializing)]
	pub xmlns: (),
	#[serde(rename = "ErrorDocument")]
	pub error_document: Option<Key>,
	#[serde(rename = "IndexDocument")]
	pub index_document: Option<Suffix>,
	#[serde(rename = "RedirectAllRequestsTo")]
	pub redirect_all_requests_to: Option<Target>,
	#[serde(rename = "RoutingRules")]
	pub routing_rules: Option<Vec<RoutingRule>>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct RoutingRule {
	#[serde(rename = "RoutingRule")]
	pub inner: RoutingRuleInner,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct RoutingRuleInner {
	#[serde(rename = "Condition")]
	pub condition: Option<Condition>,
	#[serde(rename = "Redirect")]
	pub redirect: Redirect,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Key {
	#[serde(rename = "Key")]
	pub key: Value,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Suffix {
	#[serde(rename = "Suffix")]
	pub suffix: Value,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Target {
	#[serde(rename = "HostName")]
	pub hostname: Value,
	#[serde(rename = "Protocol")]
	pub protocol: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Condition {
	#[serde(rename = "HttpErrorCodeReturnedEquals")]
	pub http_error_code: Option<IntValue>,
	#[serde(rename = "KeyPrefixEquals")]
	pub prefix: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct Redirect {
	#[serde(rename = "HostName")]
	pub hostname: Option<Value>,
	#[serde(rename = "Protocol")]
	pub protocol: Option<Value>,
	#[serde(rename = "HttpRedirectCode")]
	pub http_redirect_code: Option<IntValue>,
	#[serde(rename = "ReplaceKeyPrefixWith")]
	pub replace_prefix: Option<Value>,
	#[serde(rename = "ReplaceKeyWith")]
	pub replace_full: Option<Value>,
}

impl WebsiteConfiguration {
	pub fn validate(&self) -> Result<(), Error> {
		if self.redirect_all_requests_to.is_some()
			&& (self.error_document.is_some()
				|| self.index_document.is_some()
				|| self.routing_rules.is_some())
		{
			return Err(Error::bad_request(
				"Bad XML: can't have RedirectAllRequestsTo and other fields",
			));
		}
		if let Some(ref ed) = self.error_document {
			ed.validate()?;
		}
		if let Some(ref id) = self.index_document {
			id.validate()?;
		}
		if let Some(ref rart) = self.redirect_all_requests_to {
			rart.validate()?;
		}
		if let Some(ref rrs) = self.routing_rules {
			for rr in rrs {
				rr.inner.validate()?;
			}
		}

		Ok(())
	}

	pub fn into_garage_website_config(self) -> Result<WebsiteConfig, Error> {
		if self.redirect_all_requests_to.is_some() {
			Err(Error::NotImplemented(
				"S3 website redirects are not currently implemented in Garage.".into(),
			))
		} else if self.routing_rules.map(|x| !x.is_empty()).unwrap_or(false) {
			Err(Error::NotImplemented(
				"S3 routing rules are not currently implemented in Garage.".into(),
			))
		} else {
			Ok(WebsiteConfig {
				index_document: self
					.index_document
					.map(|x| x.suffix.0)
					.unwrap_or_else(|| "index.html".to_string()),
				error_document: self.error_document.map(|x| x.key.0),
			})
		}
	}
}

impl Key {
	pub fn validate(&self) -> Result<(), Error> {
		if self.key.0.is_empty() {
			Err(Error::bad_request(
				"Bad XML: error document specified but empty",
			))
		} else {
			Ok(())
		}
	}
}

impl Suffix {
	pub fn validate(&self) -> Result<(), Error> {
		if self.suffix.0.is_empty() | self.suffix.0.contains('/') {
			Err(Error::bad_request(
				"Bad XML: index document is empty or contains /",
			))
		} else {
			Ok(())
		}
	}
}

impl Target {
	pub fn validate(&self) -> Result<(), Error> {
		if let Some(ref protocol) = self.protocol {
			if protocol.0 != "http" && protocol.0 != "https" {
				return Err(Error::bad_request("Bad XML: invalid protocol"));
			}
		}
		Ok(())
	}
}

impl RoutingRuleInner {
	pub fn validate(&self) -> Result<(), Error> {
		let has_prefix = self
			.condition
			.as_ref()
			.and_then(|c| c.prefix.as_ref())
			.is_some();
		self.redirect.validate(has_prefix)
	}
}

impl Redirect {
	pub fn validate(&self, has_prefix: bool) -> Result<(), Error> {
		if self.replace_prefix.is_some() {
			if self.replace_full.is_some() {
				return Err(Error::bad_request(
					"Bad XML: both ReplaceKeyPrefixWith and ReplaceKeyWith are set",
				));
			}
			if !has_prefix {
				return Err(Error::bad_request(
					"Bad XML: ReplaceKeyPrefixWith is set, but  KeyPrefixEquals isn't",
				));
			}
		}
		if let Some(ref protocol) = self.protocol {
			if protocol.0 != "http" && protocol.0 != "https" {
				return Err(Error::bad_request("Bad XML: invalid protocol"));
			}
		}
		// TODO there are probably more invalid cases, but which ones?
		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	use quick_xml::de::from_str;

	#[test]
	fn test_deserialize() -> Result<(), Error> {
		let message = r#"<?xml version="1.0" encoding="UTF-8"?>
<WebsiteConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <ErrorDocument>
         <Key>my-error-doc</Key>
   </ErrorDocument>
   <IndexDocument>
      <Suffix>my-index</Suffix>
   </IndexDocument>
   <RedirectAllRequestsTo>
      <HostName>garage.tld</HostName>
      <Protocol>https</Protocol>
   </RedirectAllRequestsTo>
   <RoutingRules>
      <RoutingRule>
         <Condition>
            <HttpErrorCodeReturnedEquals>404</HttpErrorCodeReturnedEquals>
            <KeyPrefixEquals>prefix1</KeyPrefixEquals>
         </Condition>
         <Redirect>
            <HostName>gara.ge</HostName>
            <Protocol>http</Protocol>
            <HttpRedirectCode>303</HttpRedirectCode>
            <ReplaceKeyPrefixWith>prefix2</ReplaceKeyPrefixWith>
            <ReplaceKeyWith>fullkey</ReplaceKeyWith>
         </Redirect>
      </RoutingRule>
   </RoutingRules>
</WebsiteConfiguration>"#;
		let conf: WebsiteConfiguration = from_str(message).unwrap();
		let ref_value = WebsiteConfiguration {
			xmlns: (),
			error_document: Some(Key {
				key: Value("my-error-doc".to_owned()),
			}),
			index_document: Some(Suffix {
				suffix: Value("my-index".to_owned()),
			}),
			redirect_all_requests_to: Some(Target {
				hostname: Value("garage.tld".to_owned()),
				protocol: Some(Value("https".to_owned())),
			}),
			routing_rules: Some(vec![RoutingRule {
				inner: RoutingRuleInner {
					condition: Some(Condition {
						http_error_code: Some(IntValue(404)),
						prefix: Some(Value("prefix1".to_owned())),
					}),
					redirect: Redirect {
						hostname: Some(Value("gara.ge".to_owned())),
						protocol: Some(Value("http".to_owned())),
						http_redirect_code: Some(IntValue(303)),
						replace_prefix: Some(Value("prefix2".to_owned())),
						replace_full: Some(Value("fullkey".to_owned())),
					},
				},
			}]),
		};
		assert_eq! {
			ref_value,
			conf
		}

		let message2 = to_xml_with_header(&ref_value)?;

		let cleanup = |c: &str| c.replace(char::is_whitespace, "");
		assert_eq!(cleanup(message), cleanup(&message2));

		Ok(())
	}
}
