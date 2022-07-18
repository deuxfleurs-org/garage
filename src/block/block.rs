use bytes::Bytes;
use serde::{Deserialize, Serialize};
use zstd::stream::{decode_all as zstd_decode, Encoder};

use garage_util::data::*;
use garage_util::error::*;

/// A possibly compressed block of data
#[derive(Debug, Serialize, Deserialize)]
pub enum DataBlock {
	/// Uncompressed data
	Plain(#[serde(with = "serde_bytes")] Vec<u8>),
	/// Data compressed with zstd
	Compressed(#[serde(with = "serde_bytes")] Vec<u8>),
}

impl DataBlock {
	/// Query whether this block is compressed
	pub fn is_compressed(&self) -> bool {
		matches!(self, DataBlock::Compressed(_))
	}

	/// Get the inner, possibly compressed buffer. You should probably use [`DataBlock::verify_get`]
	/// instead
	pub fn inner_buffer(&self) -> &[u8] {
		use DataBlock::*;
		let (Plain(ref res) | Compressed(ref res)) = self;
		res
	}

	/// Get the buffer, possibly decompressing it, and verify it's integrity.
	/// For Plain block, data is compared to hash, for Compressed block, zstd checksumming system
	/// is used instead.
	pub fn verify_get(self, hash: Hash) -> Result<Vec<u8>, Error> {
		match self {
			DataBlock::Plain(data) => {
				if blake2sum(&data) == hash {
					Ok(data)
				} else {
					Err(Error::CorruptData(hash))
				}
			}
			DataBlock::Compressed(data) => {
				zstd_decode(&data[..]).map_err(|_| Error::CorruptData(hash))
			}
		}
	}

	/// Verify data integrity. Allocate less than [`DataBlock::verify_get`] and don't consume self, but
	/// does not return the buffer content.
	pub fn verify(&self, hash: Hash) -> Result<(), Error> {
		match self {
			DataBlock::Plain(data) => {
				if blake2sum(data) == hash {
					Ok(())
				} else {
					Err(Error::CorruptData(hash))
				}
			}
			DataBlock::Compressed(data) => zstd::stream::copy_decode(&data[..], std::io::sink())
				.map_err(|_| Error::CorruptData(hash)),
		}
	}

	pub async fn from_buffer(data: Bytes, level: Option<i32>) -> DataBlock {
		tokio::task::spawn_blocking(move || {
			if let Some(level) = level {
				if let Ok(data) = zstd_encode(&data[..], level) {
					return DataBlock::Compressed(data);
				}
			}
			DataBlock::Plain(data.to_vec()) // TODO: remove to_vec here
		})
		.await
		.unwrap()
	}
}

fn zstd_encode<R: std::io::Read>(mut source: R, level: i32) -> std::io::Result<Vec<u8>> {
	let mut result = Vec::<u8>::new();
	let mut encoder = Encoder::new(&mut result, level)?;
	encoder.include_checksum(true)?;
	std::io::copy(&mut source, &mut encoder)?;
	encoder.finish()?;
	Ok(result)
}
