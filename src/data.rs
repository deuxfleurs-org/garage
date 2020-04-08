use std::time::{SystemTime, UNIX_EPOCH};
use std::fmt;
use std::collections::HashMap;
use serde::{Serializer, Deserializer, Serialize, Deserialize};
use serde::de::{self, Visitor};
use rand::Rng;
use sha2::{Sha256, Digest};

#[derive(Default, PartialOrd, Ord, Clone, Hash, PartialEq)]
pub struct FixedBytes32([u8; 32]);

impl From<[u8; 32]> for FixedBytes32 {
	fn from(x: [u8; 32]) -> FixedBytes32 {
		FixedBytes32(x)
	}
}

impl std::convert::AsRef<[u8]> for FixedBytes32 {
	fn as_ref(&self) -> &[u8] {
		&self.0[..]
	}
}

impl Eq for FixedBytes32 {}

impl fmt::Debug for FixedBytes32 {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", hex::encode(self.0))
	}
}

struct FixedBytes32Visitor;
impl<'de> Visitor<'de> for FixedBytes32Visitor {
	type Value = FixedBytes32;

	fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
		formatter.write_str("a byte slice of size 32")
	}

	fn visit_bytes<E: de::Error>(self, value: &[u8]) -> Result<Self::Value, E> {
		if value.len() == 32 {
			let mut res = [0u8; 32];
			res.copy_from_slice(value);
			Ok(res.into())
		} else {
			Err(E::custom(format!("Invalid byte string length {}, expected 32", value.len())))
		}
	}
}

impl<'de> Deserialize<'de> for FixedBytes32 {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<FixedBytes32, D::Error> {
		deserializer.deserialize_bytes(FixedBytes32Visitor)
	}
}

impl Serialize for FixedBytes32 {
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		serializer.serialize_bytes(&self.0[..])
	}
}

impl FixedBytes32 {
	pub fn as_slice(&self) -> &[u8] {
		&self.0[..]
	}
	pub fn as_slice_mut(&mut self) -> &mut [u8] {
		&mut self.0[..]
	}
	pub fn to_vec(&self) -> Vec<u8> {
		self.0.to_vec()
	}
}

pub type UUID = FixedBytes32;
pub type Hash = FixedBytes32;

pub fn hash(data: &[u8]) -> Hash {
	let mut hasher = Sha256::new();
	hasher.input(data);
	let mut hash = [0u8; 32];
	hash.copy_from_slice(&hasher.result()[..]);
	hash.into()
}

pub fn gen_uuid() -> UUID {
	rand::thread_rng().gen::<[u8; 32]>().into()
}

pub fn now_msec() -> u64 {
	SystemTime::now().duration_since(UNIX_EPOCH)
		.expect("Fix your clock :o")
		.as_millis() as u64
}

// Network management

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkConfig {
	pub members: HashMap<UUID, NetworkConfigEntry>,
	pub version: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NetworkConfigEntry {
	pub datacenter: String,
	pub n_tokens: u32,
}

// Data management

pub const INLINE_THRESHOLD: usize = 3072;

#[derive(Debug, Serialize, Deserialize)]
pub struct SplitpointMeta {
	pub bucket: String,
	pub key: String,

	pub timestamp: u64,
	pub uuid: UUID,
	pub deleted: bool,
}

pub use crate::version_table::*;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BlockMeta {
	pub version_uuid: UUID,
	pub offset: u64,
	pub hash: Hash,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BlockReverseMeta {
	pub versions: Vec<UUID>,
	pub deleted_versions: Vec<UUID>,
}
