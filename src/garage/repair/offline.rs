use std::io::Write;
use std::path::PathBuf;

use serde::Serialize;

use garage_util::config::*;
use garage_util::error::*;

use garage_model::garage::Garage;
use garage_table::{replication::TableReplication, *};

use crate::cli::structs::*;
use crate::secrets::{fill_secrets, Secrets};

pub fn offline_repair(
	config_file: PathBuf,
	secrets: Secrets,
	opt: OfflineRepairOpt,
) -> Result<(), Error> {
	if !opt.yes {
		return Err(Error::Message(
			"Please add the --yes flag to launch repair operation".into(),
		));
	}

	info!("Loading configuration...");
	let config = fill_secrets(read_config(config_file)?, secrets)?;

	info!("Initializing Garage main data store...");
	let garage = Garage::new(config)?;

	info!("Launching repair operation...");
	match opt.what {
		#[cfg(feature = "k2v")]
		OfflineRepairWhat::K2VItemCounters => {
			garage
				.k2v
				.counter_table
				.offline_recount_all(&garage.k2v.item_table)?;
		}
		OfflineRepairWhat::ObjectCounters => {
			garage
				.object_counter_table
				.offline_recount_all(&garage.object_table)?;
		}
	}

	info!("Repair operation finished, shutting down...");

	Ok(())
}

pub fn dump(config_file: PathBuf, secrets: Secrets, opt: DumpNodeOpt) -> Result<(), Error> {
	let what = opt.what.as_str();

	info!("Loading configuration...");
	let config = fill_secrets(read_config(config_file)?, secrets)?;

	info!("Initializing Garage main data store...");
	let garage = Garage::new(config)?;

	match what {
		"bucket" | "buckets" => dump_table_inner(&garage.bucket_table),
		"bucket_alias" | "bucket_aliases" => dump_table_inner(&garage.bucket_alias_table),
		"key" | "keys" => dump_table_inner(&garage.key_table),
		"object" | "objects" => dump_table_inner(&garage.object_table),
		"object_counter" | "object_counters" => Err(Error::Message(
			"object_counters cannot be JSON-serialized".into(),
		)),
		"mpu" => dump_table_inner(&garage.mpu_table),
		"mpu_counter" | "mpu_counters" => Err(Error::Message(
			"mpu_counters cannot be JSON-serialized".into(),
		)),
		"version" | "versions" => dump_table_inner(&garage.version_table),
		"block_ref" | "block_refs" => dump_table_inner(&garage.block_ref_table),
		#[cfg(feature = "k2v")]
		"k2v_item" | "k2v_items" => dump_table_inner(&garage.k2v.item_table),
		//#[cfg(feature = "k2v")]
		"k2v_counter" | "k2v_counters" => Err(Error::Message(
			"k2v_counters cannot be JSON-serialized".into(),
		)),
		other => {
			let mut stdout = std::io::stdout().lock();
			match other {
				"cluster_layout" => Err(Error::Message(
					"cluster_layout cannot be JSON-serialized".into(),
				)),
				_ => Err(Error::Message(format!("invalid thing to dump: {}", what))),
			}
		}
	}
}

#[derive(Serialize)]
struct DumpEntry<'a, T: Serialize> {
	#[serde(with = "serde_bytes")]
	partition_key: &'a [u8],
	#[serde(with = "serde_bytes")]
	sort_key: &'a [u8],
	entry: &'a T,
}

fn dump_table_inner<F, R>(table: &Table<F, R>) -> Result<(), Error>
where
	F: TableSchema,
	R: TableReplication,
{
	eprintln!("Dumping table {}...", F::TABLE_NAME);

	let mut stdout = std::io::stdout().lock();

	for line in table.data.store.iter()? {
		let (_k, v) = line?;
		let v_dec = table.data.decode_entry(&v)?;
		let pkh = v_dec.partition_key().hash();
		let dump_entry = DumpEntry {
			partition_key: pkh.as_slice(),
			sort_key: v_dec.sort_key().sort_key(),
			entry: &v_dec,
		};
		dump_line(&mut stdout, dump_entry)?;
	}
	stdout.flush()?;

	Ok(())
}

fn dump_line<T: Serialize>(
	mut stdout: &mut std::io::StdoutLock<'static>,
	dump_entry: T,
) -> Result<(), Error> {
	let mut ser = serde_json::ser::Serializer::with_formatter(&mut stdout, DumpFormatter);
	dump_entry.serialize(&mut ser)?;
	stdout.write_all(b"\n")?;
	Ok(())
}

struct DumpFormatter;
impl serde_json::ser::Formatter for DumpFormatter {
	fn write_byte_array<W>(&mut self, writer: &mut W, value: &[u8]) -> std::io::Result<()>
	where
		W: ?Sized + std::io::Write,
	{
		writer.write_all(b"\"")?;
		writer.write_all(hex::encode(&value).as_bytes())?;
		writer.write_all(b"\"")
	}
}
