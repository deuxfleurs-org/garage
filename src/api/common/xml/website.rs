use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use garage_model::bucket_table::{self, RoutingRule as GarageRoutingRule, WebsiteConfig};

use crate::common_error::CommonError as Error;
use crate::xml::{xmlns_tag, IntValue, Value};

pub const DEFAULT_INDEX_DOCUMENT: &str = "index.html";

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct WebsiteConfiguration {
	#[serde(rename = "@xmlns", serialize_with = "xmlns_tag", skip_deserializing)]
	pub xmlns: (),
	#[serde(rename = "ErrorDocument", skip_serializing_if = "Option::is_none")]
	pub error_document: Option<Key>,
	#[serde(rename = "IndexDocument", skip_serializing_if = "Option::is_none")]
	pub index_document: Option<Suffix>,
	#[serde(
		rename = "RedirectAllRequestsTo",
		skip_serializing_if = "Option::is_none"
	)]
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

#[derive(Debug, ToSchema, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone)]
#[schema(as = website::RoutingRule)]
pub struct RoutingRule {
	#[serde(rename = "Condition")]
	pub condition: Option<Condition>,
	#[serde(rename = "Redirect")]
	pub redirect: Redirect,
}

#[derive(Debug, ToSchema, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone)]
#[schema(as = website::Key)]
pub struct Key {
	#[serde(rename = "Key")]
	pub key: Value,
}

#[derive(Debug, ToSchema, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone)]
#[schema(as = website::Suffix)]
pub struct Suffix {
	#[serde(rename = "Suffix")]
	pub suffix: Value,
}

#[derive(Debug, ToSchema, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone)]
#[schema(as = website::Target)]
pub struct Target {
	#[serde(rename = "HostName")]
	pub hostname: Value,
	#[serde(rename = "Protocol")]
	pub protocol: Option<Value>,
}

#[derive(Debug, ToSchema, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone)]
#[schema(as = website::Condition)]
pub struct Condition {
	#[serde(
		rename = "HttpErrorCodeReturnedEquals",
		skip_serializing_if = "Option::is_none"
	)]
	pub http_error_code: Option<IntValue>,
	#[serde(rename = "KeyPrefixEquals", skip_serializing_if = "Option::is_none")]
	pub prefix: Option<Value>,
}

#[derive(Debug, ToSchema, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone)]
#[schema(as = website::Redirect)]
pub struct Redirect {
	#[serde(rename = "HostName", skip_serializing_if = "Option::is_none")]
	pub hostname: Option<Value>,
	#[serde(rename = "Protocol", skip_serializing_if = "Option::is_none")]
	pub protocol: Option<Value>,
	#[serde(rename = "HttpRedirectCode", skip_serializing_if = "Option::is_none")]
	pub http_redirect_code: Option<IntValue>,
	#[serde(
		rename = "ReplaceKeyPrefixWith",
		skip_serializing_if = "Option::is_none"
	)]
	pub replace_prefix: Option<Value>,
	#[serde(rename = "ReplaceKeyWith", skip_serializing_if = "Option::is_none")]
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
			// limit was chosen arbitrarily
			return Err(Error::bad_request(
				"Bad XML: RoutingRules can't have more than 1000 child elements",
			));
		}

		Ok(())
	}

	pub fn into_garage_website_config(self) -> Result<WebsiteConfig, Error> {
		if self.redirect_all_requests_to.is_some() {
			Err(Error::NotImplemented(
				"RedirectAllRequestsTo is not currently implemented in Garage, however its effect can be emulated using a single unconditional RoutingRule.".into(),
			))
		} else {
			Ok(WebsiteConfig {
				enabled: true,
				index_document: Some(
					self.index_document
						.map(|x| x.suffix.0)
						.unwrap_or_else(|| DEFAULT_INDEX_DOCUMENT.to_string()),
				),
				error_document: self.error_document.map(|x| x.key.0),
				redirect_all: None,
				routing_rules: self
					.routing_rules
					.rules
					.into_iter()
					.map(RoutingRule::into_garage_routing_rule)
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

	pub fn from_garage_routing_rule(rule: GarageRoutingRule) -> Self {
		RoutingRule {
			condition: rule.condition.map(|cond| Condition {
				http_error_code: cond.http_error_code.map(|c| IntValue(c as i64)),
				prefix: cond.prefix.map(Value),
			}),
			redirect: Redirect {
				hostname: rule.redirect.hostname.map(Value),
				http_redirect_code: Some(IntValue(rule.redirect.http_redirect_code as i64)),
				protocol: rule.redirect.protocol.map(Value),
				replace_full: rule.redirect.replace_key.map(Value),
				replace_prefix: rule.redirect.replace_key_prefix.map(Value),
			},
		}
	}

	pub fn into_garage_routing_rule(self) -> bucket_table::RoutingRule {
		bucket_table::RoutingRule {
			condition: self
				.condition
				.map(|condition| bucket_table::RedirectCondition {
					http_error_code: condition.http_error_code.map(|c| c.0 as u16),
					prefix: condition.prefix.map(|p| p.0),
				}),
			redirect: bucket_table::Redirect {
				hostname: self.redirect.hostname.map(|h| h.0),
				protocol: self.redirect.protocol.map(|p| p.0),
				// aws default to 301, which i find punitive in case of
				// misconfiguration (can be permanently cached on the
				// user agent)
				http_redirect_code: self
					.redirect
					.http_redirect_code
					.map(|c| c.0 as u16)
					.unwrap_or(302),
				replace_key_prefix: self.redirect.replace_prefix.map(|k| k.0),
				replace_key: self.redirect.replace_full.map(|k| k.0),
			},
		}
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
	use crate::xml::{to_xml_with_header, unprettify_xml};

	use super::*;

	use quick_xml::de::from_str;

	#[test]
	fn test_deserialize() {
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
		let conf: WebsiteConfiguration =
			from_str(message).expect("failed to deserialize xml in `WebsiteConfiguration`");
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

		let message2 = to_xml_with_header(&ref_value).expect("xml serialization");

		assert_eq!(unprettify_xml(message), unprettify_xml(&message2));
	}

	#[test]
	fn test_serialize_empty() {
		let conf = WebsiteConfiguration {
			xmlns: (),
			error_document: None,
			index_document: None,
			redirect_all_requests_to: None,
			routing_rules: RoutingRules { rules: vec![] },
		};
		let serialized_ref = r#"<?xml version="1.0" encoding="UTF-8"?>
<WebsiteConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
</WebsiteConfiguration>"#;

		let serialized = to_xml_with_header(&conf).expect("xml serialization");
		assert_eq!(unprettify_xml(&serialized), unprettify_xml(&serialized_ref));
	}
}
