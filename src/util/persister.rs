use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use serde::{Deserialize, Serialize};

use crate::data::*;
use crate::error::Error;

pub struct Persister<T: Serialize + for<'de> Deserialize<'de>> {
	path: PathBuf,

	_marker: std::marker::PhantomData<T>,
}

impl<T> Persister<T>
where
	T: Serialize + for<'de> Deserialize<'de>,
{
	pub fn new(base_dir: &Path, file_name: &str) -> Self {
		let mut path = base_dir.to_path_buf();
		path.push(file_name);
		Self {
			path,
			_marker: Default::default(),
		}
	}

	pub fn load(&self) -> Result<T, Error> {
		let mut file = std::fs::OpenOptions::new().read(true).open(&self.path)?;

		let mut bytes = vec![];
		file.read_to_end(&mut bytes)?;

		let value = rmp_serde::decode::from_read_ref(&bytes[..])?;
		Ok(value)
	}

	pub fn save(&self, t: &T) -> Result<(), Error> {
		let bytes = rmp_to_vec_all_named(t)?;

		let mut file = std::fs::OpenOptions::new()
			.write(true)
			.create(true)
			.truncate(true)
			.open(&self.path)?;

		file.write_all(&bytes[..])?;

		Ok(())
	}

	pub async fn load_async(&self) -> Result<T, Error> {
		let mut file = tokio::fs::File::open(&self.path).await?;

		let mut bytes = vec![];
		file.read_to_end(&mut bytes).await?;

		let value = rmp_serde::decode::from_read_ref(&bytes[..])?;
		Ok(value)
	}

	pub async fn save_async(&self, t: &T) -> Result<(), Error> {
		let bytes = rmp_to_vec_all_named(t)?;

		let mut file = tokio::fs::File::create(&self.path).await?;
		file.write_all(&bytes[..]).await?;

		Ok(())
	}
}
