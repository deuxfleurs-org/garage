use serde::{Deserialize, Serialize};

/// Indicates that this type has an encoding that can be migrated from
/// a previous version upon upgrades of Garage.
pub trait Migrate: Serialize + for<'de> Deserialize<'de> + 'static {
	/// A sequence of bytes to add at the beginning of the serialized
	/// string, to identify that the data is of this version.
	const VERSION_MARKER: &'static [u8] = b"";

	/// The previous version of this data type, from which items of this version
	/// can be migrated.
	type Previous: Migrate;

	/// The migration function that transforms a value decoded in the old format
	/// to an up-to-date value.
	fn migrate(previous: Self::Previous) -> Self;

	/// Decode an encoded version of this type, going through a migration if necessary.
	fn decode(bytes: &[u8]) -> Option<Self> {
		let marker_len = Self::VERSION_MARKER.len();
		if bytes.get(..marker_len) == Some(Self::VERSION_MARKER) {
			if let Ok(value) = rmp_serde::decode::from_slice::<_>(&bytes[marker_len..]) {
				return Some(value);
			}
		}

		Self::Previous::decode(bytes).map(Self::migrate)
	}

	/// Encode this type with optional version marker
	fn encode(&self) -> Result<Vec<u8>, rmp_serde::encode::Error> {
		let mut wr = Vec::with_capacity(128);
		wr.extend_from_slice(Self::VERSION_MARKER);
		let mut se = rmp_serde::Serializer::new(&mut wr).with_struct_map();
		self.serialize(&mut se)?;
		Ok(wr)
	}
}

/// Indicates that this type has no previous encoding version to be migrated from.
pub trait InitialFormat: Serialize + for<'de> Deserialize<'de> + 'static {
	/// A sequence of bytes to add at the beginning of the serialized
	/// string, to identify that the data is of this version.
	const VERSION_MARKER: &'static [u8] = b"";
}

impl<T: InitialFormat> Migrate for T {
	const VERSION_MARKER: &'static [u8] = <T as InitialFormat>::VERSION_MARKER;

	type Previous = NoPrevious;

	fn migrate(_previous: Self::Previous) -> Self {
		unreachable!();
	}
}

/// Internal type used by InitialFormat, not meant for general use.
#[derive(Serialize, Deserialize)]
pub enum NoPrevious {}

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

#[cfg(test)]
mod test {
	use super::*;

	#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
	struct V1 {
		a: usize,
		b: String,
	}
	impl InitialFormat for V1 {}

	#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
	struct V2 {
		a: usize,
		b: Vec<String>,
		c: String,
	}
	impl Migrate for V2 {
		const VERSION_MARKER: &'static [u8] = b"GtestV2";
		type Previous = V1;
		fn migrate(prev: V1) -> V2 {
			V2 {
				a: prev.a,
				b: vec![prev.b],
				c: String::new(),
			}
		}
	}

	#[test]
	fn test_v1() {
		let x = V1 {
			a: 12,
			b: "hello".into(),
		};
		let x_enc = x.encode().unwrap();
		let y = V1::decode(&x_enc).unwrap();
		assert_eq!(x, y);
	}

	#[test]
	fn test_v2() {
		let x = V2 {
			a: 12,
			b: vec!["hello".into(), "world".into()],
			c: "plop".into(),
		};
		let x_enc = x.encode().unwrap();
		assert_eq!(&x_enc[..V2::VERSION_MARKER.len()], V2::VERSION_MARKER);
		let y = V2::decode(&x_enc).unwrap();
		assert_eq!(x, y);
	}

	#[test]
	fn test_migrate() {
		let x = V1 {
			a: 12,
			b: "hello".into(),
		};
		let x_enc = x.encode().unwrap();

		let xx = V1::decode(&x_enc).unwrap();
		assert_eq!(x, xx);

		let y = V2::decode(&x_enc).unwrap();
		assert_eq!(
			y,
			V2 {
				a: 12,
				b: vec!["hello".into()],
				c: "".into(),
			}
		);

		let y_enc = y.encode().unwrap();
		assert_eq!(&y_enc[..V2::VERSION_MARKER.len()], V2::VERSION_MARKER);

		let z = V2::decode(&y_enc).unwrap();
		assert_eq!(y, z);
	}
}
