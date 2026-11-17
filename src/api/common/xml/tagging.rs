use serde::{Deserialize, Serialize};

use super::{xmlns_tag, Value};
use crate::common_error::CommonError as Error;

use garage_model::bucket_table::TagMap;

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename = "Tagging")]
pub struct Tagging {
	#[serde(rename = "@xmlns", serialize_with = "xmlns_tag", skip_deserializing)]
	pub xmlns: (),
	// "default" is required to be able to parse an empty list of rules,
	// cf https://docs.rs/quick-xml/latest/quick_xml/de/#sequences-xsall-and-xssequence-xml-schema-types
	#[serde(rename = "TagSet")]
	pub tag_set: TagSet,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct TagSet {
	#[serde(rename = "Tag")]
	pub tags: Vec<Tag>,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Tag {
	#[serde(rename = "Key")]
	pub key: Value,
	#[serde(rename = "Value")]
	pub value: Value,
}

impl Tagging {
	pub fn to_map(self) -> Result<TagMap, Error> {
		let mut ret = TagMap::new();
		for tag in self.tag_set.tags.iter() {
			if ret
				.insert(tag.key.0.to_string(), tag.value.0.to_string())
				.is_some()
			{
				return Err(Error::bad_request(format!("Duplicate tag: {}", tag.key.0)));
			}
		}
		Ok(ret)
	}

	pub fn from_map(map: TagMap) -> Self {
		Self {
			xmlns: (),
			tag_set: TagSet {
				tags: map
					.into_iter()
					.map(|(key, value)| Tag {
						key: Value(key),
						value: Value(value),
					})
					.collect(),
			},
		}
	}
}
