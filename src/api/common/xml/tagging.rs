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

#[cfg(test)]
mod tests {
	use crate::xml::{to_xml_with_header, unprettify_xml, Value};

	use super::*;

	use quick_xml::de::from_str;

	#[test]
	fn test_deserialize() {
		let message = r#"<?xml version="1.0" encoding="UTF-8"?>
<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
   <TagSet>
      <Tag>
         <Key>kkey</Key>
         <Value>vvalue</Value>
      </Tag>
   </TagSet>
</Tagging>"#;
		let conf: Tagging =
			from_str(message).expect("failed to deserialize xml in `WebsiteConfiguration`");
		let ref_value = Tagging {
			xmlns: (),
			tag_set: TagSet {
				tags: vec![Tag {
					key: Value("kkey".to_string()),
					value: Value("vvalue".to_string()),
				}],
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
		let conf = Tagging {
			xmlns: (),
			tag_set: TagSet { tags: vec![] },
		};
		let serialized_ref = r#"<?xml version="1.0" encoding="UTF-8"?>
<Tagging xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
    <TagSet>
    </TagSet>
</Tagging>"#;

		let serialized = to_xml_with_header(&conf).expect("xml serialization");
		assert_eq!(unprettify_xml(&serialized), unprettify_xml(&serialized_ref));
	}
}
