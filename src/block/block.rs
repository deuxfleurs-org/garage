use std::path::PathBuf;

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use zstd::stream::Encoder;

use garage_util::data::*;
use garage_util::error::*;

use garage_net::stream::ByteStream;

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
pub enum DataBlockHeader {
	Plain,
	Compressed,
}

#[derive(Debug)]
pub struct DataBlockElem<T> {
	header: DataBlockHeader,
	elem: T,
}

/// A possibly compressed block of data
pub type DataBlock = DataBlockElem<Bytes>;

/// A path to a possibly compressed block of data
pub type DataBlockPath = DataBlockElem<PathBuf>;

/// A stream of possibly compressed block data
pub type DataBlockStream = DataBlockElem<ByteStream>;

impl DataBlockHeader {
	pub fn is_compressed(&self) -> bool {
		matches!(self, DataBlockHeader::Compressed)
	}
}

impl<T> DataBlockElem<T> {
	pub fn from_parts(header: DataBlockHeader, elem: T) -> Self {
		Self { header, elem }
	}

	pub fn plain(elem: T) -> Self {
		Self {
			header: DataBlockHeader::Plain,
			elem,
		}
	}

	pub fn compressed(elem: T) -> Self {
		Self {
			header: DataBlockHeader::Compressed,
			elem,
		}
	}

	pub fn into_parts(self) -> (DataBlockHeader, T) {
		(self.header, self.elem)
	}

	pub fn as_parts_ref(&self) -> (DataBlockHeader, &T) {
		(self.header, &self.elem)
	}
}

impl DataBlock {
	/// Verify data integrity. Does not return the buffer content.
	pub fn verify(&self, hash: Hash) -> Result<(), Error> {
		match self.header {
			DataBlockHeader::Plain => {
				if blake2sum(&self.elem) == hash {
					Ok(())
				} else {
					Err(Error::CorruptData(hash))
				}
			}
			DataBlockHeader::Compressed => {
				zstd::stream::copy_decode(&self.elem[..], std::io::sink())
					.map_err(|_| Error::CorruptData(hash))
			}
		}
	}

	pub async fn from_buffer(data: Bytes, level: Option<i32>) -> DataBlock {
		tokio::task::spawn_blocking(move || {
			if let Some(level) = level {
				if let Ok(data_compressed) = zstd_encode(&data[..], level) {
					return DataBlock::compressed(data_compressed.into());
				}
			}
			DataBlock::plain(data.into())
		})
		.await
		.unwrap()
	}
}

pub fn zstd_encode<R: std::io::Read>(mut source: R, level: i32) -> std::io::Result<Vec<u8>> {
	let mut result = Vec::<u8>::new();
	let mut encoder = Encoder::new(&mut result, level)?;
	encoder.include_checksum(true)?;
	std::io::copy(&mut source, &mut encoder)?;
	encoder.finish()?;
	Ok(result)
}
