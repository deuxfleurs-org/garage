use std::convert::{TryFrom, TryInto};
use std::hash::Hasher;

use base64::prelude::*;
use crc32c::Crc32cHasher as Crc32c;
use crc32fast::Hasher as Crc32;
use md5::{Digest, Md5};
use sha1::Sha1;
use sha2::Sha256;

use http::{HeaderMap, HeaderValue};

use garage_util::error::OkOrMessage;

use garage_model::s3::object_table::*;

use garage_api_common::signature::checksum::*;

use crate::error::*;

#[derive(Default)]
pub(crate) struct MultipartChecksummer {
	pub md5: Md5,
	pub extra: Option<MultipartExtraChecksummer>,
}

pub(crate) enum MultipartExtraChecksummer {
	Crc32(Crc32),
	Crc32c(Crc32c),
	Sha1(Sha1),
	Sha256(Sha256),
}

impl MultipartChecksummer {
	pub(crate) fn init(algo: Option<ChecksumAlgorithm>) -> Self {
		Self {
			md5: Md5::new(),
			extra: match algo {
				None => None,
				Some(ChecksumAlgorithm::Crc32) => {
					Some(MultipartExtraChecksummer::Crc32(Crc32::new()))
				}
				Some(ChecksumAlgorithm::Crc32c) => {
					Some(MultipartExtraChecksummer::Crc32c(Crc32c::default()))
				}
				Some(ChecksumAlgorithm::Sha1) => Some(MultipartExtraChecksummer::Sha1(Sha1::new())),
				Some(ChecksumAlgorithm::Sha256) => {
					Some(MultipartExtraChecksummer::Sha256(Sha256::new()))
				}
			},
		}
	}

	pub(crate) fn update(
		&mut self,
		etag: &str,
		checksum: Option<ChecksumValue>,
	) -> Result<(), Error> {
		self.md5
			.update(&hex::decode(&etag).ok_or_message("invalid etag hex")?);
		match (&mut self.extra, checksum) {
			(None, _) => (),
			(
				Some(MultipartExtraChecksummer::Crc32(ref mut crc32)),
				Some(ChecksumValue::Crc32(x)),
			) => {
				crc32.update(&x);
			}
			(
				Some(MultipartExtraChecksummer::Crc32c(ref mut crc32c)),
				Some(ChecksumValue::Crc32c(x)),
			) => {
				crc32c.write(&x);
			}
			(Some(MultipartExtraChecksummer::Sha1(ref mut sha1)), Some(ChecksumValue::Sha1(x))) => {
				sha1.update(&x);
			}
			(
				Some(MultipartExtraChecksummer::Sha256(ref mut sha256)),
				Some(ChecksumValue::Sha256(x)),
			) => {
				sha256.update(&x);
			}
			(Some(_), b) => {
				return Err(Error::internal_error(format!(
					"part checksum was not computed correctly, got: {:?}",
					b
				)))
			}
		}
		Ok(())
	}

	pub(crate) fn finalize(self) -> (Md5Checksum, Option<ChecksumValue>) {
		let md5 = self.md5.finalize()[..].try_into().unwrap();
		let extra = match self.extra {
			None => None,
			Some(MultipartExtraChecksummer::Crc32(crc32)) => {
				Some(ChecksumValue::Crc32(u32::to_be_bytes(crc32.finalize())))
			}
			Some(MultipartExtraChecksummer::Crc32c(crc32c)) => Some(ChecksumValue::Crc32c(
				u32::to_be_bytes(u32::try_from(crc32c.finish()).unwrap()),
			)),
			Some(MultipartExtraChecksummer::Sha1(sha1)) => {
				Some(ChecksumValue::Sha1(sha1.finalize()[..].try_into().unwrap()))
			}
			Some(MultipartExtraChecksummer::Sha256(sha256)) => Some(ChecksumValue::Sha256(
				sha256.finalize()[..].try_into().unwrap(),
			)),
		};
		(md5, extra)
	}
}

// ----

/// Extract the value of the x-amz-checksum-algorithm header
pub(crate) fn request_checksum_algorithm(
	headers: &HeaderMap<HeaderValue>,
) -> Result<Option<ChecksumAlgorithm>, Error> {
	match headers.get(X_AMZ_CHECKSUM_ALGORITHM) {
		None => Ok(None),
		Some(x) if x == "CRC32" => Ok(Some(ChecksumAlgorithm::Crc32)),
		Some(x) if x == "CRC32C" => Ok(Some(ChecksumAlgorithm::Crc32c)),
		Some(x) if x == "SHA1" => Ok(Some(ChecksumAlgorithm::Sha1)),
		Some(x) if x == "SHA256" => Ok(Some(ChecksumAlgorithm::Sha256)),
		_ => Err(Error::bad_request("invalid checksum algorithm")),
	}
}

/// Extract the value of any of the x-amz-checksum-* headers
pub(crate) fn request_checksum_value(
	headers: &HeaderMap<HeaderValue>,
) -> Result<Option<ChecksumValue>, Error> {
	let mut ret = vec![];

	if let Some(crc32_str) = headers.get(X_AMZ_CHECKSUM_CRC32) {
		let crc32 = BASE64_STANDARD
			.decode(&crc32_str)
			.ok()
			.and_then(|x| x.try_into().ok())
			.ok_or_bad_request("invalid x-amz-checksum-crc32 header")?;
		ret.push(ChecksumValue::Crc32(crc32))
	}
	if let Some(crc32c_str) = headers.get(X_AMZ_CHECKSUM_CRC32C) {
		let crc32c = BASE64_STANDARD
			.decode(&crc32c_str)
			.ok()
			.and_then(|x| x.try_into().ok())
			.ok_or_bad_request("invalid x-amz-checksum-crc32c header")?;
		ret.push(ChecksumValue::Crc32c(crc32c))
	}
	if let Some(sha1_str) = headers.get(X_AMZ_CHECKSUM_SHA1) {
		let sha1 = BASE64_STANDARD
			.decode(&sha1_str)
			.ok()
			.and_then(|x| x.try_into().ok())
			.ok_or_bad_request("invalid x-amz-checksum-sha1 header")?;
		ret.push(ChecksumValue::Sha1(sha1))
	}
	if let Some(sha256_str) = headers.get(X_AMZ_CHECKSUM_SHA256) {
		let sha256 = BASE64_STANDARD
			.decode(&sha256_str)
			.ok()
			.and_then(|x| x.try_into().ok())
			.ok_or_bad_request("invalid x-amz-checksum-sha256 header")?;
		ret.push(ChecksumValue::Sha256(sha256))
	}

	if ret.len() > 1 {
		return Err(Error::bad_request(
			"multiple x-amz-checksum-* headers given",
		));
	}
	Ok(ret.pop())
}

/// Checks for the presence of x-amz-checksum-algorithm
/// if so extract the corresponding x-amz-checksum-* value
pub(crate) fn request_checksum_algorithm_value(
	headers: &HeaderMap<HeaderValue>,
) -> Result<Option<ChecksumValue>, Error> {
	match headers.get(X_AMZ_CHECKSUM_ALGORITHM) {
		Some(x) if x == "CRC32" => {
			let crc32 = headers
				.get(X_AMZ_CHECKSUM_CRC32)
				.and_then(|x| BASE64_STANDARD.decode(&x).ok())
				.and_then(|x| x.try_into().ok())
				.ok_or_bad_request("invalid x-amz-checksum-crc32 header")?;
			Ok(Some(ChecksumValue::Crc32(crc32)))
		}
		Some(x) if x == "CRC32C" => {
			let crc32c = headers
				.get(X_AMZ_CHECKSUM_CRC32C)
				.and_then(|x| BASE64_STANDARD.decode(&x).ok())
				.and_then(|x| x.try_into().ok())
				.ok_or_bad_request("invalid x-amz-checksum-crc32c header")?;
			Ok(Some(ChecksumValue::Crc32c(crc32c)))
		}
		Some(x) if x == "SHA1" => {
			let sha1 = headers
				.get(X_AMZ_CHECKSUM_SHA1)
				.and_then(|x| BASE64_STANDARD.decode(&x).ok())
				.and_then(|x| x.try_into().ok())
				.ok_or_bad_request("invalid x-amz-checksum-sha1 header")?;
			Ok(Some(ChecksumValue::Sha1(sha1)))
		}
		Some(x) if x == "SHA256" => {
			let sha256 = headers
				.get(X_AMZ_CHECKSUM_SHA256)
				.and_then(|x| BASE64_STANDARD.decode(&x).ok())
				.and_then(|x| x.try_into().ok())
				.ok_or_bad_request("invalid x-amz-checksum-sha256 header")?;
			Ok(Some(ChecksumValue::Sha256(sha256)))
		}
		Some(_) => Err(Error::bad_request("invalid x-amz-checksum-algorithm")),
		None => Ok(None),
	}
}

pub(crate) fn add_checksum_response_headers(
	checksum: &Option<ChecksumValue>,
	mut resp: http::response::Builder,
) -> http::response::Builder {
	match checksum {
		Some(ChecksumValue::Crc32(crc32)) => {
			resp = resp.header(X_AMZ_CHECKSUM_CRC32, BASE64_STANDARD.encode(&crc32));
		}
		Some(ChecksumValue::Crc32c(crc32c)) => {
			resp = resp.header(X_AMZ_CHECKSUM_CRC32C, BASE64_STANDARD.encode(&crc32c));
		}
		Some(ChecksumValue::Sha1(sha1)) => {
			resp = resp.header(X_AMZ_CHECKSUM_SHA1, BASE64_STANDARD.encode(&sha1));
		}
		Some(ChecksumValue::Sha256(sha256)) => {
			resp = resp.header(X_AMZ_CHECKSUM_SHA256, BASE64_STANDARD.encode(&sha256));
		}
		None => (),
	}
	resp
}
