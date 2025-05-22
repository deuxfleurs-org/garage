use std::convert::TryInto;

use base64::prelude::*;
use crc_fast::{CrcAlgorithm, Digest as CrcDigest};
use md5::{Digest, Md5};
use sha1::Sha1;
use sha2::Sha256;

use http::{HeaderMap, HeaderName, HeaderValue};

use garage_util::data::*;

use super::*;

pub use garage_model::s3::object_table::{ChecksumAlgorithm, ChecksumValue};

pub const CONTENT_MD5: HeaderName = HeaderName::from_static("content-md5");

pub const X_AMZ_CHECKSUM_ALGORITHM: HeaderName =
	HeaderName::from_static("x-amz-checksum-algorithm");
pub const X_AMZ_CHECKSUM_MODE: HeaderName = HeaderName::from_static("x-amz-checksum-mode");
pub const X_AMZ_CHECKSUM_TYPE: HeaderName = HeaderName::from_static("x-amz-checksum-type");
pub const X_AMZ_CHECKSUM_CRC32: HeaderName = HeaderName::from_static("x-amz-checksum-crc32");
pub const X_AMZ_CHECKSUM_CRC32C: HeaderName = HeaderName::from_static("x-amz-checksum-crc32c");
pub const X_AMZ_CHECKSUM_CRC64NVME: HeaderName =
	HeaderName::from_static("x-amz-checksum-crc64nvme");
pub const X_AMZ_CHECKSUM_SHA1: HeaderName = HeaderName::from_static("x-amz-checksum-sha1");
pub const X_AMZ_CHECKSUM_SHA256: HeaderName = HeaderName::from_static("x-amz-checksum-sha256");

// Values for x-amz-checksum-type
pub const COMPOSITE: &str = "COMPOSITE";
pub const FULL_OBJECT: &str = "FULL_OBJECT";

pub type Crc32Checksum = [u8; 4];
pub type Crc32cChecksum = [u8; 4];
pub type Crc64NvmeChecksum = [u8; 8];
pub type Md5Checksum = [u8; 16];
pub type Sha1Checksum = [u8; 20];
pub type Sha256Checksum = [u8; 32];

// -- MAP OF CRC ALGORITHMS :
// CRC32        -> CrcAlgorithm::Crc32IsoHdlc
// CRC32C       -> CrcAlgorithm::Crc32Iscsi
// CRC64NVME    -> CrcAlgorithm::Crc64Nvme

pub fn new_crc32() -> CrcDigest {
	CrcDigest::new(CrcAlgorithm::Crc32IsoHdlc)
}
pub fn new_crc32c() -> CrcDigest {
	CrcDigest::new(CrcAlgorithm::Crc32Iscsi)
}
pub fn new_crc64nvme() -> CrcDigest {
	CrcDigest::new(CrcAlgorithm::Crc64Nvme)
}

#[derive(Debug, Default, Clone)]
pub struct ExpectedChecksums {
	// base64-encoded md5 (content-md5 header)
	pub md5: Option<String>,
	// content_sha256 (as a Hash / FixedBytes32)
	pub sha256: Option<Hash>,
	// extra x-amz-checksum-* header
	pub extra: Option<ChecksumValue>,
}

pub struct Checksummer {
	pub crc32: Option<CrcDigest>,
	pub crc32c: Option<CrcDigest>,
	pub crc64nvme: Option<CrcDigest>,
	pub md5: Option<Md5>,
	pub sha1: Option<Sha1>,
	pub sha256: Option<Sha256>,
}

#[derive(Default)]
pub struct Checksums {
	pub crc32: Option<Crc32Checksum>,
	pub crc32c: Option<Crc32cChecksum>,
	pub crc64nvme: Option<Crc64NvmeChecksum>,
	pub md5: Option<Md5Checksum>,
	pub sha1: Option<Sha1Checksum>,
	pub sha256: Option<Sha256Checksum>,
}

impl Checksummer {
	pub fn new() -> Self {
		Self {
			crc32: None,
			crc32c: None,
			crc64nvme: None,
			md5: None,
			sha1: None,
			sha256: None,
		}
	}

	pub fn init(expected: &ExpectedChecksums, add_md5: bool) -> Self {
		let mut ret = Self::new();
		ret.add_expected(expected);
		if add_md5 {
			ret.add_md5();
		}
		ret
	}

	pub fn add_md5(&mut self) {
		self.md5 = Some(Md5::new());
	}

	pub fn add_expected(&mut self, expected: &ExpectedChecksums) {
		if expected.md5.is_some() {
			self.md5 = Some(Md5::new());
		}
		if expected.sha256.is_some() || matches!(&expected.extra, Some(ChecksumValue::Sha256(_))) {
			self.sha256 = Some(Sha256::new());
		}
		if matches!(&expected.extra, Some(ChecksumValue::Crc32(_))) {
			self.crc32 = Some(new_crc32());
		}
		if matches!(&expected.extra, Some(ChecksumValue::Crc32c(_))) {
			self.crc32c = Some(new_crc32c());
		}
		if matches!(&expected.extra, Some(ChecksumValue::Crc64Nvme(_))) {
			self.crc64nvme = Some(new_crc64nvme());
		}
		if matches!(&expected.extra, Some(ChecksumValue::Sha1(_))) {
			self.sha1 = Some(Sha1::new());
		}
	}

	pub fn add(mut self, algo: Option<ChecksumAlgorithm>) -> Self {
		match algo {
			Some(ChecksumAlgorithm::Crc32) => {
				self.crc32 = Some(new_crc32());
			}
			Some(ChecksumAlgorithm::Crc32c) => {
				self.crc32c = Some(new_crc32c());
			}
			Some(ChecksumAlgorithm::Crc64Nvme) => {
				self.crc64nvme = Some(new_crc64nvme());
			}
			Some(ChecksumAlgorithm::Sha1) => {
				self.sha1 = Some(Sha1::new());
			}
			Some(ChecksumAlgorithm::Sha256) => {
				self.sha256 = Some(Sha256::new());
			}
			None => (),
		}
		self
	}

	pub fn update(&mut self, bytes: &[u8]) {
		if let Some(crc32) = &mut self.crc32 {
			crc32.update(bytes);
		}
		if let Some(crc32c) = &mut self.crc32c {
			crc32c.update(bytes);
		}
		if let Some(crc64nvme) = &mut self.crc64nvme {
			crc64nvme.update(bytes);
		}
		if let Some(md5) = &mut self.md5 {
			md5.update(bytes);
		}
		if let Some(sha1) = &mut self.sha1 {
			sha1.update(bytes);
		}
		if let Some(sha256) = &mut self.sha256 {
			sha256.update(bytes);
		}
	}

	pub fn finalize(self) -> Checksums {
		Checksums {
			crc32: self.crc32.map(|x| u32::to_be_bytes(x.finalize() as u32)),
			crc32c: self.crc32c.map(|x| u32::to_be_bytes(x.finalize() as u32)),
			crc64nvme: self.crc64nvme.map(|x| u64::to_be_bytes(x.finalize())),
			md5: self.md5.map(|x| x.finalize()[..].try_into().unwrap()),
			sha1: self.sha1.map(|x| x.finalize()[..].try_into().unwrap()),
			sha256: self.sha256.map(|x| x.finalize()[..].try_into().unwrap()),
		}
	}
}

impl Checksums {
	pub fn verify(&self, expected: &ExpectedChecksums) -> Result<(), Error> {
		if let Some(expected_md5) = &expected.md5 {
			match self.md5 {
				Some(md5) if BASE64_STANDARD.encode(&md5) == expected_md5.trim_matches('"') => (),
				_ => {
					return Err(Error::InvalidDigest(
						"MD5 checksum verification failed (from content-md5)".into(),
					))
				}
			}
		}
		if let Some(expected_sha256) = &expected.sha256 {
			match self.sha256 {
				Some(sha256) if &sha256[..] == expected_sha256.as_slice() => (),
				_ => {
					return Err(Error::InvalidDigest(
						"SHA256 checksum verification failed (from x-amz-content-sha256)".into(),
					))
				}
			}
		}
		if let Some(extra) = expected.extra {
			let algo = extra.algorithm();
			let calculated = self.extract(Some(algo));
			if calculated != Some(extra) {
				return Err(Error::InvalidDigest(format!(
					"Failed to validate checksum for algorithm {:?}: calculated {:?}, expected {:?}",
					algo, calculated, extra
				)));
			}
		}
		Ok(())
	}

	pub fn extract(&self, algo: Option<ChecksumAlgorithm>) -> Option<ChecksumValue> {
		match algo {
			None => None,
			Some(ChecksumAlgorithm::Crc32) => Some(ChecksumValue::Crc32(self.crc32.unwrap())),
			Some(ChecksumAlgorithm::Crc32c) => Some(ChecksumValue::Crc32c(self.crc32c.unwrap())),
			Some(ChecksumAlgorithm::Crc64Nvme) => {
				Some(ChecksumValue::Crc64Nvme(self.crc64nvme.unwrap()))
			}
			Some(ChecksumAlgorithm::Sha1) => Some(ChecksumValue::Sha1(self.sha1.unwrap())),
			Some(ChecksumAlgorithm::Sha256) => Some(ChecksumValue::Sha256(self.sha256.unwrap())),
		}
	}
}

// ----

pub fn parse_checksum_algorithm(algo: &str) -> Result<ChecksumAlgorithm, Error> {
	match algo {
		"CRC32" => Ok(ChecksumAlgorithm::Crc32),
		"CRC32C" => Ok(ChecksumAlgorithm::Crc32c),
		"CRC64NVME" => Ok(ChecksumAlgorithm::Crc64Nvme),
		"SHA1" => Ok(ChecksumAlgorithm::Sha1),
		"SHA256" => Ok(ChecksumAlgorithm::Sha256),
		_ => Err(Error::bad_request("invalid checksum algorithm")),
	}
}

/// Extract the value of the x-amz-checksum-algorithm header
pub fn request_checksum_algorithm(
	headers: &HeaderMap<HeaderValue>,
) -> Result<Option<ChecksumAlgorithm>, Error> {
	match headers.get(X_AMZ_CHECKSUM_ALGORITHM) {
		None => Ok(None),
		Some(x) => parse_checksum_algorithm(x.to_str()?).map(Some),
	}
}

pub fn request_trailer_checksum_algorithm(
	headers: &HeaderMap<HeaderValue>,
) -> Result<Option<ChecksumAlgorithm>, Error> {
	match headers.get(X_AMZ_TRAILER).map(|x| x.to_str()).transpose()? {
		None => Ok(None),
		Some(x) if x == X_AMZ_CHECKSUM_CRC32 => Ok(Some(ChecksumAlgorithm::Crc32)),
		Some(x) if x == X_AMZ_CHECKSUM_CRC32C => Ok(Some(ChecksumAlgorithm::Crc32c)),
		Some(x) if x == X_AMZ_CHECKSUM_CRC64NVME => Ok(Some(ChecksumAlgorithm::Crc64Nvme)),
		Some(x) if x == X_AMZ_CHECKSUM_SHA1 => Ok(Some(ChecksumAlgorithm::Sha1)),
		Some(x) if x == X_AMZ_CHECKSUM_SHA256 => Ok(Some(ChecksumAlgorithm::Sha256)),
		_ => Err(Error::bad_request("invalid checksum algorithm")),
	}
}

/// Extract the value of any of the x-amz-checksum-* headers
pub fn request_checksum_value(
	headers: &HeaderMap<HeaderValue>,
) -> Result<Option<ChecksumValue>, Error> {
	let mut ret = vec![];

	if headers.contains_key(X_AMZ_CHECKSUM_CRC32) {
		ret.push(extract_checksum_value(headers, ChecksumAlgorithm::Crc32)?);
	}
	if headers.contains_key(X_AMZ_CHECKSUM_CRC32C) {
		ret.push(extract_checksum_value(headers, ChecksumAlgorithm::Crc32c)?);
	}
	if headers.contains_key(X_AMZ_CHECKSUM_CRC64NVME) {
		ret.push(extract_checksum_value(
			headers,
			ChecksumAlgorithm::Crc64Nvme,
		)?);
	}
	if headers.contains_key(X_AMZ_CHECKSUM_SHA1) {
		ret.push(extract_checksum_value(headers, ChecksumAlgorithm::Sha1)?);
	}
	if headers.contains_key(X_AMZ_CHECKSUM_SHA256) {
		ret.push(extract_checksum_value(headers, ChecksumAlgorithm::Sha256)?);
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
pub fn extract_checksum_value(
	headers: &HeaderMap<HeaderValue>,
	algo: ChecksumAlgorithm,
) -> Result<ChecksumValue, Error> {
	match algo {
		ChecksumAlgorithm::Crc32 => {
			let crc32 = headers
				.get(X_AMZ_CHECKSUM_CRC32)
				.and_then(|x| BASE64_STANDARD.decode(&x).ok())
				.and_then(|x| x.try_into().ok())
				.ok_or_bad_request("invalid x-amz-checksum-crc32 header")?;
			Ok(ChecksumValue::Crc32(crc32))
		}
		ChecksumAlgorithm::Crc32c => {
			let crc32c = headers
				.get(X_AMZ_CHECKSUM_CRC32C)
				.and_then(|x| BASE64_STANDARD.decode(&x).ok())
				.and_then(|x| x.try_into().ok())
				.ok_or_bad_request("invalid x-amz-checksum-crc32c header")?;
			Ok(ChecksumValue::Crc32c(crc32c))
		}
		ChecksumAlgorithm::Crc64Nvme => {
			let crc64nvme = headers
				.get(X_AMZ_CHECKSUM_CRC64NVME)
				.and_then(|x| BASE64_STANDARD.decode(&x).ok())
				.and_then(|x| x.try_into().ok())
				.ok_or_bad_request("invalid x-amz-checksum-crc64nvme header")?;
			Ok(ChecksumValue::Crc64Nvme(crc64nvme))
		}
		ChecksumAlgorithm::Sha1 => {
			let sha1 = headers
				.get(X_AMZ_CHECKSUM_SHA1)
				.and_then(|x| BASE64_STANDARD.decode(&x).ok())
				.and_then(|x| x.try_into().ok())
				.ok_or_bad_request("invalid x-amz-checksum-sha1 header")?;
			Ok(ChecksumValue::Sha1(sha1))
		}
		ChecksumAlgorithm::Sha256 => {
			let sha256 = headers
				.get(X_AMZ_CHECKSUM_SHA256)
				.and_then(|x| BASE64_STANDARD.decode(&x).ok())
				.and_then(|x| x.try_into().ok())
				.ok_or_bad_request("invalid x-amz-checksum-sha256 header")?;
			Ok(ChecksumValue::Sha256(sha256))
		}
	}
}

pub fn add_checksum_response_headers(
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
		Some(ChecksumValue::Crc64Nvme(crc64nvme)) => {
			resp = resp.header(X_AMZ_CHECKSUM_CRC64NVME, BASE64_STANDARD.encode(&crc64nvme));
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
