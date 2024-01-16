use std::path::PathBuf;

use garage_util::config::*;
use garage_util::error::*;

use garage_model::garage::Garage;

use crate::cli::structs::*;
use crate::secrets::{fill_secrets, Secrets};

pub async fn offline_repair(
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
