use serde::{Deserialize, Serialize};

/// Serialize to MessagePacki, without versionning
/// (see garage_util::migrate for functions that manage versionned
/// data formats)
pub fn nonversioned_encode<T>(val: &T) -> Result<Vec<u8>, rmp_serde::encode::Error>
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

/// Deserialize from MessagePacki, without versionning
/// (see garage_util::migrate for functions that manage versionned
/// data formats)
pub fn nonversioned_decode<T>(bytes: &[u8]) -> Result<T, rmp_serde::decode::Error>
where
	T: for<'de> Deserialize<'de> + ?Sized,
{
	rmp_serde::decode::from_read_ref::<_, T>(bytes)
}
