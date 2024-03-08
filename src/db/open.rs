use std::path::PathBuf;

use crate::{Db, Error, Result};

/// List of supported database engine types
///
/// The `enum` holds list of *all* database engines that are are be supported by crate, no matter
/// if relevant feature is enabled or not. It allows us to distinguish between invalid engine
/// and valid engine, whose support is not enabled via feature flag.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Engine {
	Lmdb,
	Sqlite,
	Sled,
}

impl Engine {
	/// Return variant name as static `&str`
	pub fn as_str(&self) -> &'static str {
		match self {
			Self::Lmdb => "lmdb",
			Self::Sqlite => "sqlite",
			Self::Sled => "sled",
		}
	}
}

impl std::fmt::Display for Engine {
	fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
		self.as_str().fmt(fmt)
	}
}

impl std::str::FromStr for Engine {
	type Err = Error;

	fn from_str(text: &str) -> Result<Engine> {
		match text {
			"lmdb" | "heed" => Ok(Self::Lmdb),
			"sqlite" | "sqlite3" | "rusqlite" => Ok(Self::Sqlite),
			"sled" => Ok(Self::Sled),
			kind => Err(Error(
				format!(
					"Invalid DB engine: {} (options are: lmdb, sled, sqlite)",
					kind
				)
				.into(),
			)),
		}
	}
}

pub struct OpenOpt {
	pub fsync: bool,
	pub lmdb_map_size: Option<usize>,
	pub sled_cache_capacity: usize,
	pub sled_flush_every_ms: u64,
}

impl Default for OpenOpt {
	fn default() -> Self {
		Self {
			fsync: false,
			lmdb_map_size: None,
			sled_cache_capacity: 1024 * 1024 * 1024,
			sled_flush_every_ms: 2000,
		}
	}
}

pub fn open_db(path: &PathBuf, engine: Engine, opt: &OpenOpt) -> Result<Db> {
	match engine {
		// ---- Sled DB ----
		#[cfg(feature = "sled")]
		Engine::Sled => {
			if opt.fsync {
				return Err(Error(
					"`metadata_fsync = true` is not supported with the Sled database engine".into(),
				));
			}
			info!("Opening Sled database at: {}", path.display());
			let db = crate::sled_adapter::sled::Config::default()
				.path(&path)
				.cache_capacity(opt.sled_cache_capacity as u64)
				.flush_every_ms(Some(opt.sled_flush_every_ms))
				.open()?;
			Ok(crate::sled_adapter::SledDb::init(db))
		}

		// ---- Sqlite DB ----
		#[cfg(feature = "sqlite")]
		Engine::Sqlite => {
			info!("Opening Sqlite database at: {}", path.display());
			let db = crate::sqlite_adapter::rusqlite::Connection::open(&path)?;
			db.pragma_update(None, "journal_mode", "WAL")?;
			if opt.fsync {
				db.pragma_update(None, "synchronous", "NORMAL")?;
			} else {
				db.pragma_update(None, "synchronous", "OFF")?;
			}
			Ok(crate::sqlite_adapter::SqliteDb::init(db))
		}

		// ---- LMDB DB ----
		#[cfg(feature = "lmdb")]
		Engine::Lmdb => {
			info!("Opening LMDB database at: {}", path.display());
			if let Err(e) = std::fs::create_dir_all(&path) {
				return Err(Error(
					format!("Unable to create LMDB data directory: {}", e).into(),
				));
			}

			let map_size = match opt.lmdb_map_size {
				None => crate::lmdb_adapter::recommended_map_size(),
				Some(v) => v - (v % 4096),
			};

			let mut env_builder = heed::EnvOpenOptions::new();
			env_builder.max_dbs(100);
			env_builder.map_size(map_size);
			env_builder.max_readers(2048);
			unsafe {
				env_builder.flag(crate::lmdb_adapter::heed::flags::Flags::MdbNoMetaSync);
				if !opt.fsync {
					env_builder.flag(heed::flags::Flags::MdbNoSync);
				}
			}
			match env_builder.open(&path) {
				Err(heed::Error::Io(e)) if e.kind() == std::io::ErrorKind::OutOfMemory => {
					return Err(Error(
						"OutOfMemory error while trying to open LMDB database. This can happen \
                        if your operating system is not allowing you to use sufficient virtual \
                        memory address space. Please check that no limit is set (ulimit -v). \
                        You may also try to set a smaller `lmdb_map_size` configuration parameter. \
                        On 32-bit machines, you should probably switch to another database engine."
							.into(),
					))
				}
				Err(e) => Err(Error(format!("Cannot open LMDB database: {}", e).into())),
				Ok(db) => Ok(crate::lmdb_adapter::LmdbDb::init(db)),
			}
		}

		// Pattern is unreachable when all supported DB engines are compiled into binary. The allow
		// attribute is added so that we won't have to change this match in case stop building
		// support for one or more engines by default.
		#[allow(unreachable_patterns)]
		engine => Err(Error(
			format!("DB engine support not available in this build: {}", engine).into(),
		)),
	}
}
