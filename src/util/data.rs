//! Contains common types and functions related to serialization and integrity
use rand::Rng;
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt;

/// An array of 32 bytes
#[derive(Default, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Copy)]
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

impl fmt::Debug for FixedBytes32 {
	fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
		write!(f, "{}", hex::encode(&self.0[..8]))
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
	/// Access the content as a slice
	pub fn as_slice(&self) -> &[u8] {
		&self.0[..]
	}
	/// Access the content as a mutable slice
	pub fn as_slice_mut(&mut self) -> &mut [u8] {
		&mut self.0[..]
	}
	/// Copy to a slice
	pub fn to_vec(self) -> Vec<u8> {
		self.0.to_vec()
	}
	/// Try building a FixedBytes32 from a slice
	/// Return None if the slice is not 32 bytes long
	pub fn try_from(by: &[u8]) -> Option<Self> {
		if by.len() != 32 {
			return None;
		}
		let mut ret = [0u8; 32];
		ret.copy_from_slice(by);
		Some(Self(ret))
	}
	/// Return the next hash
	pub fn increment(&self) -> Option<Self> {
		let mut ret = *self;
		for byte in ret.0.iter_mut().rev() {
			if *byte == u8::MAX {
				*byte = 0;
			} else {
				*byte = *byte + 1;
				return Some(ret);
			}
		}
		return None;
	}
}

impl From<garage_net::NodeID> for FixedBytes32 {
	fn from(node_id: garage_net::NodeID) -> FixedBytes32 {
		FixedBytes32::try_from(node_id.as_ref()).unwrap()
	}
}

impl From<FixedBytes32> for garage_net::NodeID {
	fn from(bytes: FixedBytes32) -> garage_net::NodeID {
		garage_net::NodeID::from_slice(bytes.as_slice()).unwrap()
	}
}

/// A 32 bytes UUID
pub type Uuid = FixedBytes32;
/// A 256 bit cryptographic hash, can be sha256 or blake2 depending on provenance
pub type Hash = FixedBytes32;

/// Compute the sha256 of a slice
pub fn sha256sum(data: &[u8]) -> Hash {
	use sha2::{Digest, Sha256};

	let mut hasher = Sha256::new();
	hasher.update(data);
	let mut hash = [0u8; 32];
	hash.copy_from_slice(&hasher.finalize()[..]);
	hash.into()
}

/// Compute the blake2 of a slice
pub fn blake2sum(data: &[u8]) -> Hash {
	use blake2::{Blake2b512, Digest};

	let mut hasher = Blake2b512::new();
	hasher.update(data);
	let mut hash = [0u8; 32];
	hash.copy_from_slice(&hasher.finalize()[..32]);
	hash.into()
}

/// A 64 bit non cryptographic hash
pub type FastHash = u64;

/// Compute a (non cryptographic) of a slice
pub fn fasthash(data: &[u8]) -> FastHash {
	use xxhash_rust::xxh3::Xxh3;

	let mut h = Xxh3::new();
	h.update(data);
	h.digest()
}

/// Generate a random 32 bytes UUID
pub fn gen_uuid() -> Uuid {
	rand::thread_rng().gen::<[u8; 32]>().into()
}

#[cfg(test)]
mod test {
	use super::*;

	#[test]
	fn test_increment() {
		let zero: FixedBytes32 = [0u8; 32].into();
		let mut one: FixedBytes32 = [0u8; 32].into();
		one.0[31] = 1;
		let max: FixedBytes32 = [0xFFu8; 32].into();
		assert_eq!(zero.increment(), Some(one));
		assert_eq!(max.increment(), None);

		let mut test: FixedBytes32 = [0u8; 32].into();
		let i = 0x198DF97209F8FFFFu64;
		test.0[24..32].copy_from_slice(&u64::to_be_bytes(i));
		let mut test2: FixedBytes32 = [0u8; 32].into();
		test2.0[24..32].copy_from_slice(&u64::to_be_bytes(i + 1));
		assert_eq!(test.increment(), Some(test2));
	}
}
