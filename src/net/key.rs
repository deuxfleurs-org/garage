use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::netapp::NOISE_PROTOCOL_PATTERN;
use crate::util::checked_length;

/// A node's identifier, which is also its public cryptographic key
#[derive(Copy, Clone, Ord, PartialOrd, Eq, PartialEq, Hash)]
pub struct NodeID(pub [u8; 32]);

/// A node's secret key
#[derive(Copy, Clone)]
pub struct NodeSecretKey(pub [u8; 64]);

/// A network pre-shared secret key
#[derive(Copy, Clone)]
pub struct NetworkKey(pub [u8; 32]);

impl NodeID {
	pub fn hex(&self) -> String {
		hex::encode(self.0)
	}
	pub fn hex_short(&self) -> String {
		hex::encode(&self.0[..8])
	}
}

impl std::fmt::Debug for NodeID {
	fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
		write!(fmt, "{}", self.hex())
	}
}

impl std::ops::Deref for NodeID {
	type Target = [u8];
	fn deref(&self) -> &[u8] {
		&self.0[..]
	}
}

impl AsRef<[u8]> for NodeSecretKey {
	fn as_ref(&self) -> &[u8] {
		&self.0[..]
	}
}

impl AsRef<[u8]> for NetworkKey {
	fn as_ref(&self) -> &[u8] {
		&self.0[..]
	}
}

struct NodeIDVisitor;
impl<'de> Visitor<'de> for NodeIDVisitor {
	type Value = NodeID;

	fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
		write!(formatter, "a byte slice of size 32")
	}

	fn visit_bytes<E: de::Error>(self, value: &[u8]) -> Result<Self::Value, E> {
		checked_length(value).map(NodeID).ok_or_else(|| {
			E::custom(format!(
				"Invalid byte string length {}, expected 32",
				value.len()
			))
		})
	}
}

impl<'de> Deserialize<'de> for NodeID {
	fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<NodeID, D::Error> {
		deserializer.deserialize_bytes(NodeIDVisitor)
	}
}

impl Serialize for NodeID {
	fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
		serializer.serialize_bytes(&self.0[..])
	}
}

impl NodeSecretKey {
	pub fn generate() -> Self {
		let noise = snow::Builder::new(
			NOISE_PROTOCOL_PATTERN
				.parse()
				.expect("noise protocol pattern is invalid"),
		);
		let keypair = noise
			.generate_keypair()
			.expect("could not generate keypair");
		Self(
			checked_length(&[&keypair.private[..], &keypair.public[..]].concat())
				.expect("private + public key length is invalid"),
		)
	}
	pub fn public_key(&self) -> NodeID {
		NodeID(checked_length(&self.0[32..]).unwrap())
	}
}
