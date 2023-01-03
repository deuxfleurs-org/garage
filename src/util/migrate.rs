use serde::{Deserialize, Serialize};

pub trait Migrate: Serialize + for<'de> Deserialize<'de> + 'static {
	/// A sequence of bytes to add at the beginning of the serialized
	/// string, to identify that the data is of this version.
	const MARKER: &'static [u8] = b"";

	/// The previous version of this data type, from which items of this version
	/// can be migrated. Set `type Previous = NoPrevious` to indicate that this datatype
	/// is the initial schema and cannot be migrated.
	type Previous: Migrate;

	/// This function must be filled in by implementors to migrate from a previons iteration
	/// of the data format.
	fn migrate(previous: Self::Previous) -> Self;

	fn decode(bytes: &[u8]) -> Option<Self> {
		if bytes.len() >= Self::MARKER.len() && &bytes[..Self::MARKER.len()] == Self::MARKER {
			if let Ok(value) =
				rmp_serde::decode::from_read_ref::<_, Self>(&bytes[Self::MARKER.len()..])
			{
				return Some(value);
			}
		}

		Self::Previous::decode(bytes).map(Self::migrate)
	}

	fn encode(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
		let mut wr = Vec::with_capacity(128);
		wr.extend_from_slice(Self::MARKER);
		let mut se = rmp_serde::Serializer::new(&mut wr)
			.with_struct_map()
			.with_string_variants();
		self.serialize(&mut se)?;
		Ok(wr)
	}
}

pub trait InitialFormat: Serialize + for<'de> Deserialize<'de> + 'static {
	/// A sequence of bytes to add at the beginning of the serialized
	/// string, to identify that the data is of this version.
	const MARKER: &'static [u8] = b"";
}

// ----

impl<T: InitialFormat> Migrate for T {
	const MARKER: &'static [u8] = <T as InitialFormat>::MARKER;

	type Previous = NoPrevious;

	fn migrate(_previous: Self::Previous) -> Self {
		unreachable!();
	}
}

#[derive(Serialize, Deserialize)]
pub struct NoPrevious;

impl Migrate for NoPrevious {
	type Previous = NoPrevious;

	fn migrate(_previous: Self::Previous) -> Self {
		unreachable!();
	}

	fn decode(_bytes: &[u8]) -> Option<Self> {
		None
	}

	fn encode(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
		unreachable!()
	}
}
