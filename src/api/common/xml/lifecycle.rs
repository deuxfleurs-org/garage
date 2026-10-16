use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use garage_model::bucket_table::{
	parse_lifecycle_date, LifecycleExpiration as GarageLifecycleExpiration,
	LifecycleFilter as GarageLifecycleFilter, LifecycleRule as GarageLifecycleRule,
};

use super::{xmlns_tag, IntValue, Value};

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct LifecycleConfiguration {
	#[serde(rename = "@xmlns", serialize_with = "xmlns_tag", skip_deserializing)]
	pub xmlns: (),
	#[serde(rename = "Rule", default)]
	pub lifecycle_rules: Vec<LifecycleRule>,
}

#[derive(Debug, ToSchema, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[schema(as = lifecycle::Rule)]
pub struct LifecycleRule {
	#[serde(rename = "ID", skip_serializing_if = "Option::is_none")]
	pub id: Option<Value>,
	#[serde(rename = "Status")]
	pub status: Value,
	#[serde(rename = "Filter", default, skip_serializing_if = "Option::is_none")]
	pub filter: Option<Filter>,
	#[serde(
		rename = "Expiration",
		default,
		skip_serializing_if = "Option::is_none"
	)]
	pub expiration: Option<Expiration>,
	#[serde(
		rename = "AbortIncompleteMultipartUpload",
		default,
		skip_serializing_if = "Option::is_none"
	)]
	pub abort_incomplete_mpu: Option<AbortIncompleteMpu>,
}

#[derive(
	Debug, ToSchema, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord, Default,
)]
#[schema(as = lifecycle::Filter)]
pub struct Filter {
	#[serde(rename = "And", skip_serializing_if = "Option::is_none")]
	#[schema(no_recursion)]
	pub and: Option<Box<Filter>>,
	#[serde(rename = "Prefix", skip_serializing_if = "Option::is_none")]
	pub prefix: Option<Value>,
	#[serde(
		rename = "ObjectSizeGreaterThan",
		skip_serializing_if = "Option::is_none"
	)]
	pub size_gt: Option<IntValue>,
	#[serde(rename = "ObjectSizeLessThan", skip_serializing_if = "Option::is_none")]
	pub size_lt: Option<IntValue>,
}

#[derive(Debug, ToSchema, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[schema(as = lifecycle::Expiration)]
pub struct Expiration {
	#[serde(rename = "Days", skip_serializing_if = "Option::is_none")]
	pub days: Option<IntValue>,
	#[serde(rename = "Date", skip_serializing_if = "Option::is_none")]
	pub at_date: Option<Value>,
}

#[derive(Debug, ToSchema, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
#[schema(as = lifecycle::AbortIncompleteMpu)]
pub struct AbortIncompleteMpu {
	#[serde(rename = "DaysAfterInitiation")]
	pub days: IntValue,
}

impl LifecycleConfiguration {
	pub fn validate_into_garage_lifecycle_config(
		self,
	) -> Result<Vec<GarageLifecycleRule>, &'static str> {
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
	pub fn validate_into_garage_lifecycle_rule(self) -> Result<GarageLifecycleRule, &'static str> {
		let enabled = match self.status.0.as_str() {
			"Enabled" => true,
			"Disabled" => false,
			_ => return Err("invalid value for <Status>"),
		};

		let filter = self
			.filter
			.map(Filter::validate_into_garage_lifecycle_filter)
			.transpose()?
			.unwrap_or_default();

		let abort_incomplete_mpu_days = self.abort_incomplete_mpu.map(|x| x.days.0 as usize);

		let expiration = self
			.expiration
			.map(Expiration::validate_into_garage_lifecycle_expiration)
			.transpose()?;

		Ok(GarageLifecycleRule {
			id: self.id.map(|x| x.0),
			enabled,
			filter,
			abort_incomplete_mpu_days,
			expiration,
		})
	}

	pub fn from_garage_lifecycle_rule(rule: &GarageLifecycleRule) -> Self {
		Self {
			id: rule.id.as_deref().map(Value::from),
			status: if rule.enabled {
				Value::from("Enabled")
			} else {
				Value::from("Disabled")
			},
			filter: Filter::from_garage_lifecycle_filter(&rule.filter),
			abort_incomplete_mpu: rule
				.abort_incomplete_mpu_days
				.map(|days| AbortIncompleteMpu {
					days: IntValue(days as i64),
				}),
			expiration: rule
				.expiration
				.as_ref()
				.map(Expiration::from_garage_lifecycle_expiration),
		}
	}
}

impl Filter {
	pub fn count(&self) -> i32 {
		fn count<T>(x: &Option<T>) -> i32 {
			x.as_ref().map(|_| 1).unwrap_or(0)
		}
		count(&self.prefix) + count(&self.size_gt) + count(&self.size_lt)
	}

	pub fn validate_into_garage_lifecycle_filter(
		self,
	) -> Result<GarageLifecycleFilter, &'static str> {
		if self.count() > 0 && self.and.is_some() {
			Err("Filter tag cannot contain both <And> and another condition")
		} else if let Some(and) = self.and {
			if and.and.is_some() {
				return Err("Nested <And> tags");
			}
			Ok(and.internal_into_garage_lifecycle_filter())
		} else if self.count() > 1 {
			Err("Multiple Filter conditions must be wrapped in an <And> tag")
		} else {
			Ok(self.internal_into_garage_lifecycle_filter())
		}
	}

	fn internal_into_garage_lifecycle_filter(self) -> GarageLifecycleFilter {
		GarageLifecycleFilter {
			prefix: self.prefix.map(|x| x.0),
			size_gt: self.size_gt.map(|x| x.0 as u64),
			size_lt: self.size_lt.map(|x| x.0 as u64),
		}
	}

	pub fn from_garage_lifecycle_filter(rule: &GarageLifecycleFilter) -> Option<Self> {
		let filter = Filter {
			and: None,
			prefix: rule.prefix.as_deref().map(Value::from),
			size_gt: rule.size_gt.map(|x| IntValue(x as i64)),
			size_lt: rule.size_lt.map(|x| IntValue(x as i64)),
		};
		match filter.count() {
			0 => None,
			1 => Some(filter),
			_ => Some(Filter {
				and: Some(Box::new(filter)),
				..Default::default()
			}),
		}
	}
}

impl Expiration {
	pub fn validate_into_garage_lifecycle_expiration(
		self,
	) -> Result<GarageLifecycleExpiration, &'static str> {
		match (self.days, self.at_date) {
			(Some(_), Some(_)) => Err("cannot have both <Days> and <Date> in <Expiration>"),
			(None, None) => Err("<Expiration> must contain either <Days> or <Date>"),
			(Some(days), None) => Ok(GarageLifecycleExpiration::AfterDays(days.0 as usize)),
			(None, Some(date)) => {
				parse_lifecycle_date(&date.0)?;
				Ok(GarageLifecycleExpiration::AtDate(date.0))
			}
		}
	}

	pub fn from_garage_lifecycle_expiration(exp: &GarageLifecycleExpiration) -> Self {
		match exp {
			GarageLifecycleExpiration::AfterDays(days) => Expiration {
				days: Some(IntValue(*days as i64)),
				at_date: None,
			},
			GarageLifecycleExpiration::AtDate(date) => Expiration {
				days: None,
				at_date: Some(Value(date.to_string())),
			},
		}
	}
}

#[cfg(test)]
mod tests {
	use crate::xml::{to_xml_with_header, unprettify_xml};

	use super::*;

	use quick_xml::de::from_str;

	#[test]
	fn test_empty() {
		let message = r#"<?xml version="1.0" encoding="UTF-8"?>
<LifecycleConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
</LifecycleConfiguration>"#;

		let conf: LifecycleConfiguration = from_str(message).unwrap();
		let ref_value = LifecycleConfiguration {
			xmlns: (),
			lifecycle_rules: vec![],
		};
		assert_eq! {
			ref_value,
			conf
		};
	}

	#[test]
	fn test_deserialize_lifecycle_config() {
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
					filter: Some(Filter {
						prefix: Some("documents/".into()),
						..Default::default()
					}),
					expiration: None,
					abort_incomplete_mpu: Some(AbortIncompleteMpu { days: IntValue(7) }),
				},
				LifecycleRule {
					id: Some("id2".into()),
					status: "Enabled".into(),
					filter: Some(Filter {
						and: Some(Box::new(Filter {
							prefix: Some("logs/".into()),
							size_gt: Some(IntValue(1000000)),
							..Default::default()
						})),
						..Default::default()
					}),
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

		let message2 = to_xml_with_header(&ref_value).expect("serialize xml");

		assert_eq!(unprettify_xml(message), unprettify_xml(&message2));

		// Check validation
		let validated = ref_value
			.validate_into_garage_lifecycle_config()
			.expect("invalid xml config");

		let ref_config = vec![
			GarageLifecycleRule {
				id: Some("id1".into()),
				enabled: true,
				filter: GarageLifecycleFilter {
					prefix: Some("documents/".into()),
					..Default::default()
				},
				expiration: None,
				abort_incomplete_mpu_days: Some(7),
			},
			GarageLifecycleRule {
				id: Some("id2".into()),
				enabled: true,
				filter: GarageLifecycleFilter {
					prefix: Some("logs/".into()),
					size_gt: Some(1000000),
					..Default::default()
				},
				expiration: Some(GarageLifecycleExpiration::AfterDays(365)),
				abort_incomplete_mpu_days: None,
			},
		];
		assert_eq!(validated, ref_config);

		let message3 = to_xml_with_header(&LifecycleConfiguration::from_garage_lifecycle_config(
			&validated,
		))
		.expect("serialize xml");
		assert_eq!(unprettify_xml(message), unprettify_xml(&message3));
	}
}
