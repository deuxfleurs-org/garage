use quick_xml::de::from_reader;

use hyper::{header::HeaderName, Request, Response, StatusCode};
use serde::{Deserialize, Serialize};

use garage_model::bucket_table::{self, *};

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
			routing_rules: RoutingRules {
				rules: website
					.routing_rules
					.clone()
					.into_iter()
					.map(|rule| RoutingRule {
						condition: rule.condition.map(|cond| Condition {
							http_error_code: cond.http_error_code.map(|c| IntValue(c as i64)),
							prefix: cond.prefix.map(Value),
						}),
						redirect: Redirect {
							hostname: rule.redirect.hostname.map(Value),
							http_redirect_code: Some(IntValue(
								rule.redirect.http_redirect_code as i64,
							)),
							protocol: rule.redirect.protocol.map(Value),
							replace_full: rule.redirect.replace_key.map(Value),
							replace_prefix: rule.redirect.replace_key_prefix.map(Value),
						},
					})
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
	#[serde(
		rename = "RoutingRules",
		default,
		skip_serializing_if = "RoutingRules::is_empty"
	)]
	pub routing_rules: RoutingRules,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Default)]
pub struct RoutingRules {
	#[serde(rename = "RoutingRule")]
	pub rules: Vec<RoutingRule>,
}

impl RoutingRules {
	fn is_empty(&self) -> bool {
		self.rules.is_empty()
	}
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct RoutingRule {
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
				|| !self.routing_rules.is_empty())
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
		for rr in &self.routing_rules.rules {
			rr.validate()?;
		}
		if self.routing_rules.rules.len() > 1000 {
			// we will do linear scans, best to avoid overly long configuration. The
			// limit was choosen arbitrarily
			return Err(Error::bad_request(
				"Bad XML: RoutingRules can't have more than 1000 child elements",
			));
		}

		Ok(())
	}

	pub fn into_garage_website_config(self) -> Result<WebsiteConfig, Error> {
		if self.redirect_all_requests_to.is_some() {
			Err(Error::NotImplemented(
				"RedirectAllRequestsTo is not currently implemented in Garage, however its effect can be emulated using a single inconditional RoutingRule.".into(),
			))
		} else {
			Ok(WebsiteConfig {
				index_document: self
					.index_document
					.map(|x| x.suffix.0)
					.unwrap_or_else(|| "index.html".to_string()),
				error_document: self.error_document.map(|x| x.key.0),
				redirect_all: None,
				routing_rules: self
					.routing_rules
					.rules
					.into_iter()
					.map(|rule| {
						bucket_table::RoutingRule {
							condition: rule.condition.map(|condition| {
								bucket_table::RedirectCondition {
									http_error_code: condition.http_error_code.map(|c| c.0 as u16),
									prefix: condition.prefix.map(|p| p.0),
								}
							}),
							redirect: bucket_table::Redirect {
								hostname: rule.redirect.hostname.map(|h| h.0),
								protocol: rule.redirect.protocol.map(|p| p.0),
								// aws default to 301, which i find punitive in case of
								// missconfiguration (can be permanently cached on the
								// user agent)
								http_redirect_code: rule
									.redirect
									.http_redirect_code
									.map(|c| c.0 as u16)
									.unwrap_or(302),
								replace_key_prefix: rule.redirect.replace_prefix.map(|k| k.0),
								replace_key: rule.redirect.replace_full.map(|k| k.0),
							},
						}
					})
					.collect(),
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

impl RoutingRule {
	pub fn validate(&self) -> Result<(), Error> {
		if let Some(condition) = &self.condition {
			condition.validate()?;
		}
		self.redirect.validate()
	}
}

impl Condition {
	pub fn validate(&self) -> Result<bool, Error> {
		if let Some(ref error_code) = self.http_error_code {
			// TODO do other error codes make sense? Aws only allows 4xx and 5xx
			if error_code.0 != 404 {
				return Err(Error::bad_request(
					"Bad XML: HttpErrorCodeReturnedEquals must be 404 or absent",
				));
			}
		}
		Ok(self.prefix.is_some())
	}
}

impl Redirect {
	pub fn validate(&self) -> Result<(), Error> {
		if self.replace_prefix.is_some() && self.replace_full.is_some() {
			return Err(Error::bad_request(
				"Bad XML: both ReplaceKeyPrefixWith and ReplaceKeyWith are set",
			));
		}
		if let Some(ref protocol) = self.protocol {
			if protocol.0 != "http" && protocol.0 != "https" {
				return Err(Error::bad_request("Bad XML: invalid protocol"));
			}
		}
		if let Some(ref http_redirect_code) = self.http_redirect_code {
			match http_redirect_code.0 {
				// aws allows all 3xx except 300, but some are non-sensical (not modified,
				// use proxy...)
				301 | 302 | 303 | 307 | 308 => {
					if self.hostname.is_none() && self.protocol.is_some() {
						return Err(Error::bad_request(
							"Bad XML: HostName must be set if Protocol is set",
						));
					}
				}
				// aws doesn't allow these codes, but netlify does, and it seems like a
				// cool feature (change the page seen without changing the url shown by the
				// user agent)
				200 | 404 => {
					if self.hostname.is_some() || self.protocol.is_some() {
						// hostname would mean different bucket, protocol doesn't make
						// sense
						return Err(Error::bad_request(
                                        "Bad XML: an HttpRedirectCode of 200 is not acceptable alongside HostName or Protocol",
                                ));
					}
				}
				_ => {
					return Err(Error::bad_request("Bad XML: invalid HttpRedirectCode"));
				}
			}
		}
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
      <RoutingRule>
         <Condition>
            <KeyPrefixEquals></KeyPrefixEquals>
         </Condition>
         <Redirect>
            <HttpRedirectCode>404</HttpRedirectCode>
            <ReplaceKeyWith>missing</ReplaceKeyWith>
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
			routing_rules: RoutingRules {
				rules: vec![
					RoutingRule {
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
					RoutingRule {
						condition: Some(Condition {
							http_error_code: None,
							prefix: Some(Value("".to_owned())),
						}),
						redirect: Redirect {
							hostname: None,
							protocol: None,
							http_redirect_code: Some(IntValue(404)),
							replace_prefix: None,
							replace_full: Some(Value("missing".to_owned())),
						},
					},
				],
			},
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
