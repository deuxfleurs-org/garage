use std::path::PathBuf;

use tokio::sync::watch;

use garage_util::background::*;
use garage_util::config::*;
use garage_util::error::*;

use garage_model::garage::Garage;

use crate::cli::structs::*;

pub async fn offline_repair(config_file: PathBuf, opt: OfflineRepairOpt) -> Result<(), Error> {
	if !opt.yes {
		return Err(Error::Message(
			"Please add the --yes flag to launch repair operation".into(),
		));
	}

	info!("Loading configuration...");
	let config = read_config(config_file)?;

	info!("Initializing background runner...");
	let (done_tx, done_rx) = watch::channel(false);
	let (background, await_background_done) = BackgroundRunner::new(16, done_rx);

	info!("Initializing Garage main data store...");
	let garage = Garage::new(config.clone(), background)?;

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

	info!("Repair operation finished, shutting down Garage internals...");
	done_tx.send(true).unwrap();
	drop(garage);

	await_background_done.await?;

	info!("Cleaning up...");

	Ok(())
}
