use rand::Rng;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Default, PartialOrd, Ord, Clone, Hash, PartialEq, Copy)]
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
		write!(f, "{}…", hex::encode(&self.0[..8]))
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
			Err(E::custom(format!(
				"Invalid byte string length {}, expected 32",
				value.len()
			)))
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
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.expect("Fix your clock :o")
		.as_millis() as u64
}

// RMP serialization with names of fields and variants

pub fn rmp_to_vec_all_named<T>(val: &T) -> Result<Vec<u8>, rmp_serde::encode::Error>
where
	T: Serialize + ?Sized,
{
	let mut wr = Vec::with_capacity(128);
	let mut se = rmp_serde::Serializer::new(&mut wr)
		.with_struct_map()
		.with_string_variants();
	val.serialize(&mut se)?;
	Ok(wr)
}

pub fn debug_serialize<T: Serialize>(x: T) -> String {
	match serde_json::to_string(&x) {
		Ok(ss) => {
			if ss.len() > 100 {
				ss[..100].to_string()
			} else {
				ss
			}
		}
		Err(e) => format!("<JSON serialization error: {}>", e),
	}
}
