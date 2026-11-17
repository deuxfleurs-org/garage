pub mod cors;
pub mod lifecycle;
pub mod tagging;
pub mod website;

use serde::{Deserialize, Serialize, Serializer};
use utoipa::ToSchema;

pub fn to_xml_with_header<T: Serialize>(x: &T) -> Result<String, quick_xml::se::SeError> {
	use quick_xml::se::{self, EmptyElementHandling, QuoteLevel};

	let mut xml = r#"<?xml version="1.0" encoding="UTF-8"?>"#.to_string();

	let mut ser = se::Serializer::new(&mut xml);
	ser.set_quote_level(QuoteLevel::Full)
		.empty_element_handling(EmptyElementHandling::Expanded);
	let _serialized = x.serialize(ser)?;
	Ok(xml)
}

#[cfg(test)]
pub fn unprettify_xml(xml_in: &str) -> String {
	xml_in.trim().lines().fold(String::new(), |mut val, line| {
		val.push_str(line.trim());
		val
	})
}

pub fn xmlns_tag<S: Serializer>(_v: &(), s: S) -> Result<S::Ok, S::Error> {
	s.serialize_str("http://s3.amazonaws.com/doc/2006-03-01/")
}

pub fn xmlns_xsi_tag<S: Serializer>(_v: &(), s: S) -> Result<S::Ok, S::Error> {
	s.serialize_str("http://www.w3.org/2001/XMLSchema-instance")
}

#[derive(Debug, ToSchema, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone)]
#[schema(as = xml::Value)]
pub struct Value(#[serde(rename = "$value")] pub String);

impl From<&str> for Value {
	fn from(s: &str) -> Value {
		Value(s.to_string())
	}
}

#[derive(Debug, ToSchema, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Clone)]
#[schema(as = xml::IntValue)]
pub struct IntValue(#[serde(rename = "$value")] pub i64);
