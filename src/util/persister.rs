use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::error::Error;
use crate::migrate::Migrate;

pub struct Persister<T: Migrate> {
	path: PathBuf,

	_marker: std::marker::PhantomData<T>,
}

impl<T: Migrate> Persister<T> {
	pub fn new(base_dir: &Path, file_name: &str) -> Self {
		let mut path = base_dir.to_path_buf();
		path.push(file_name);
		Self {
			path,
			_marker: Default::default(),
		}
	}

	fn decode(&self, bytes: &[u8]) -> Result<T, Error> {
		match T::decode(bytes) {
			Some(v) => Ok(v),
			None => {
				error!(
					"Unable to decode persisted data file {}",
					self.path.display()
				);
				for line in hexdump::hexdump_iter(bytes) {
					debug!("{}", line);
				}
				Err(Error::Message(format!(
					"Unable to decode persisted data file {}",
					self.path.display()
				)))
			}
		}
	}

	pub fn load(&self) -> Result<T, Error> {
		let mut file = std::fs::OpenOptions::new().read(true).open(&self.path)?;

		let mut bytes = vec![];
		file.read_to_end(&mut bytes)?;

		let value = self.decode(&bytes[..])?;
		Ok(value)
	}

	pub fn save(&self, t: &T) -> Result<(), Error> {
		let bytes = t.encode()?;

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

		let value = self.decode(&bytes[..])?;
		Ok(value)
	}

	pub async fn save_async(&self, t: &T) -> Result<(), Error> {
		let bytes = t.encode()?;

		let mut file = tokio::fs::File::create(&self.path).await?;
		file.write_all(&bytes[..]).await?;

		Ok(())
	}
}

pub struct PersisterShared<V: Migrate + Default>(Arc<(Persister<V>, RwLock<V>)>);

impl<V: Migrate + Default> Clone for PersisterShared<V> {
	fn clone(&self) -> PersisterShared<V> {
		PersisterShared(self.0.clone())
	}
}

impl<V: Migrate + Default> PersisterShared<V> {
	pub fn new(base_dir: &Path, file_name: &str) -> Self {
		let persister = Persister::new(base_dir, file_name);
		let value = persister.load().unwrap_or_default();
		Self(Arc::new((persister, RwLock::new(value))))
	}

	pub fn get_with<F, R>(&self, f: F) -> R
	where
		F: FnOnce(&V) -> R,
	{
		let value = self.0 .1.read().unwrap();
		f(&value)
	}

	pub fn set_with<F>(&self, f: F) -> Result<(), Error>
	where
		F: FnOnce(&mut V),
	{
		let mut value = self.0 .1.write().unwrap();
		f(&mut value);
		self.0 .0.save(&value)
	}
}
