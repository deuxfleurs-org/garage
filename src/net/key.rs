use ed25519_compact::{PublicKey, SecretKey};

/// A node's identifier, which is also its public cryptographic key
pub type NodeID = PublicKey;

/// A node's secret key
pub type NodeSecretKey = SecretKey;

/// A network pre-shared secret key
#[derive(Copy, Clone)]
pub struct NetworkKey(pub [u8; 32]);

impl AsRef<[u8]> for NetworkKey {
	fn as_ref(&self) -> &[u8] {
		&self.0[..]
	}
}

pub fn generate_node_secret_key() -> NodeSecretKey {
	ed25519_compact::KeyPair::generate().sk
}

pub(crate) mod nodeid_serde {
	use super::NodeID;
	use serde::{Deserialize, Deserializer, Serialize, Serializer};

	pub fn serialize<S: Serializer>(key: &NodeID, ser: S) -> Result<S::Ok, S::Error> {
		serde_bytes::ByteArray::new(**key).serialize(ser)
	}

	pub fn deserialize<'de, D: Deserializer<'de>>(de: D) -> Result<NodeID, D::Error> {
		let bytes: serde_bytes::ByteArray<32> = serde_bytes::ByteArray::<32>::deserialize(de)?;
		Ok(NodeID::new(bytes.into_array()))
	}
}
