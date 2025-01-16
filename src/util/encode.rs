use serde::{Deserialize, Serialize};

/// Serialize to MessagePack, without versioning
/// (see garage_util::migrate for functions that manage versioned
/// data formats)
pub fn nonversioned_encode<T>(val: &T) -> Result<Vec<u8>, rmp_serde::encode::Error>
where
	T: Serialize + ?Sized,
{
	let mut wr = Vec::with_capacity(128);
	let mut se = rmp_serde::Serializer::new(&mut wr).with_struct_map();
	val.serialize(&mut se)?;
	Ok(wr)
}

/// Deserialize from MessagePack, without versioning
/// (see garage_util::migrate for functions that manage versioned
/// data formats)
pub fn nonversioned_decode<T>(bytes: &[u8]) -> Result<T, rmp_serde::decode::Error>
where
	T: for<'de> Deserialize<'de> + ?Sized,
{
	rmp_serde::decode::from_slice::<_>(bytes)
}

/// Serialize to JSON, truncating long result
pub fn debug_serialize<T: Serialize>(x: T) -> String {
	match serde_json::to_string(&x) {
		Ok(ss) => {
			if ss.len() > 100 {
				// TODO this can panic if 100 is not a codepoint boundary, but inside a 2 Bytes
				// (or more) codepoint
				ss[..100].to_string()
			} else {
				ss
			}
		}
		Err(e) => format!("<JSON serialization error: {}>", e),
	}
}
