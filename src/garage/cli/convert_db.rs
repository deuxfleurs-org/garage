use std::path::PathBuf;

use structopt::StructOpt;

use garage_db::*;

/// K2V command line interface
#[derive(StructOpt, Debug)]
pub struct ConvertDbOpt {
	/// Input database path (not the same as metadata_dir, see
	/// https://garagehq.deuxfleurs.fr/documentation/reference-manual/configuration/#db-engine-since-v0-8-0)
	#[structopt(short = "i")]
	input_path: PathBuf,
	/// Input database engine (sled, lmdb or sqlite; limited by db engines
	/// enabled in this build)
	#[structopt(short = "a")]
	input_engine: String,

	/// Output database path
	#[structopt(short = "o")]
	output_path: PathBuf,
	/// Output database engine
	#[structopt(short = "b")]
	output_engine: String,
}

pub(crate) fn do_conversion(args: ConvertDbOpt) -> Result<()> {
	let input = open_db(args.input_path, args.input_engine)?;
	let output = open_db(args.output_path, args.output_engine)?;
	output.import(&input)?;
	Ok(())
}

fn open_db(path: PathBuf, engine: String) -> Result<Db> {
	match engine.as_str() {
		#[cfg(feature = "sled")]
		"sled" => {
			let db = sled_adapter::sled::Config::default().path(&path).open()?;
			Ok(sled_adapter::SledDb::init(db))
		}
		#[cfg(feature = "sqlite")]
		"sqlite" | "sqlite3" | "rusqlite" => {
			let db = sqlite_adapter::rusqlite::Connection::open(&path)?;
			Ok(sqlite_adapter::SqliteDb::init(db))
		}
		#[cfg(feature = "lmdb")]
		"lmdb" | "heed" => {
			std::fs::create_dir_all(&path).map_err(|e| {
				Error(format!("Unable to create LMDB data directory: {}", e).into())
			})?;

			let map_size = lmdb_adapter::recommended_map_size();

			let db = lmdb_adapter::heed::EnvOpenOptions::new()
				.max_dbs(100)
				.map_size(map_size)
				.open(&path)
				.unwrap();
			Ok(lmdb_adapter::LmdbDb::init(db))
		}
		e => Err(Error(
			format!("Invalid or unsupported DB engine: {}", e).into(),
		)),
	}
}
