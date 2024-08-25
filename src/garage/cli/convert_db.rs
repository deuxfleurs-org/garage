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
	/// Input database engine (lmdb or sqlite; limited by db engines
	/// enabled in this build)
	#[structopt(short = "a")]
	input_engine: Engine,

	/// Output database path
	#[structopt(short = "o")]
	output_path: PathBuf,
	/// Output database engine
	#[structopt(short = "b")]
	output_engine: Engine,

	#[structopt(flatten)]
	#[allow(dead_code)]
	db_open: OpenDbOpt,
}

/// Overrides for database open operation
#[derive(StructOpt, Debug, Default)]
pub struct OpenDbOpt {
	#[cfg(feature = "lmdb")]
	#[structopt(flatten)]
	lmdb: OpenLmdbOpt,
}

/// Overrides for LMDB database open operation
#[cfg(feature = "lmdb")]
#[derive(StructOpt, Debug, Default)]
pub struct OpenLmdbOpt {
	/// LMDB map size override
	/// (supported suffixes: B, KiB, MiB, GiB, TiB, PiB)
	#[cfg(feature = "lmdb")]
	#[structopt(long = "lmdb-map-size", name = "bytes", display_order = 1_000)]
	map_size: Option<bytesize::ByteSize>,
}

pub(crate) fn do_conversion(args: ConvertDbOpt) -> Result<()> {
	if args.input_engine == args.output_engine {
		return Err(Error("input and output database engine must differ".into()));
	}

	let opt = OpenOpt {
		#[cfg(feature = "lmdb")]
		lmdb_map_size: args.db_open.lmdb.map_size.map(|x| x.as_u64() as usize),
		..Default::default()
	};

	let input = open_db(&args.input_path, args.input_engine, &opt)?;
	let output = open_db(&args.output_path, args.output_engine, &opt)?;
	output.import(&input)?;
	Ok(())
}
