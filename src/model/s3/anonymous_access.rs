use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
pub enum AnonymousMethod {
	HeadObject,
	GetObject,
}

impl AsRef<str> for AnonymousMethod {
	fn as_ref(&self) -> &'static str {
		use AnonymousMethod::*;

		match self {
			HeadObject => "s3:HeadObject",
			GetObject => "s3:GetObject",
		}
	}
}

impl FromStr for AnonymousMethod {
	type Err = &'static str;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		use AnonymousMethod::*;

		match s {
			"s3:HeadObject" => Ok(HeadObject),
			"s3:GetObject" => Ok(GetObject),
			_ => Err("invalid anonymous method"),
		}
	}
}
