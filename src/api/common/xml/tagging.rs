use serde::{Deserialize, Serialize};

use super::{xmlns_tag, Value};
use crate::common_error::CommonError as Error;

use garage_model::bucket_table::TagMap;

pub const KEY_MAX_CHARS: usize = 128;
pub const VAL_MAX_CHARS: usize = 256;

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
	pub fn into_map(self, max_cnt: usize) -> Result<TagMap, Error> {
		let mut ret = TagMap::new();
		for tag in self.tag_set.tags.iter() {
			if tag.key.0.chars().count() > KEY_MAX_CHARS {
				return Err(Error::InvalidTag(format!(
					"Tag name must not exceed {} unicode characters",
					KEY_MAX_CHARS
				)));
			}
			if tag.value.0.chars().count() > VAL_MAX_CHARS {
				return Err(Error::InvalidTag(format!(
					"Tag value must not exceed {} unicode characters",
					VAL_MAX_CHARS
				)));
			}
			if ret
				.insert(tag.key.0.to_string(), tag.value.0.to_string())
				.is_some()
			{
				return Err(Error::InvalidTag(format!("Duplicate tag: {}", tag.key.0)));
			}
			if ret.len() > max_cnt {
				return Err(Error::InvalidTag(format!(
					"Too many tags, maximum is {}",
					max_cnt
				)));
			}
		}
		Ok(ret)
	}

	pub fn from_map(map: &TagMap) -> Self {
		Self {
			xmlns: (),
			tag_set: TagSet {
				tags: map
					.iter()
					.map(|(key, value)| Tag {
						key: Value(key.to_string()),
						value: Value(value.to_string()),
					})
					.collect(),
			},
		}
	}
}
