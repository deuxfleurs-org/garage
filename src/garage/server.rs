use std::path::PathBuf;

use tokio::sync::watch;

use garage_util::background::*;
use garage_util::config::*;
use garage_util::error::Error;

use garage_api::run_api_server;
use garage_model::garage::Garage;
use garage_web::run_web_server;

use crate::admin_rpc::*;

async fn wait_from(mut chan: watch::Receiver<bool>) {
	while !*chan.borrow() {
		if chan.changed().await.is_err() {
			return;
		}
	}
}

pub async fn run_server(config_file: PathBuf) -> Result<(), Error> {
	info!("Loading configuration...");
	let config = read_config(config_file).expect("Unable to read config file");

	info!("Opening database...");
	let mut db_path = config.metadata_dir.clone();
	db_path.push("db");
	let db = sled::Config::default()
		.path(&db_path)
		.cache_capacity(config.sled_cache_capacity)
		.flush_every_ms(Some(config.sled_flush_every_ms))
		.open()
		.expect("Unable to open sled DB");

	info!("Initializing background runner...");
	let watch_cancel = netapp::util::watch_ctrl_c();
	let (background, await_background_done) = BackgroundRunner::new(16, watch_cancel.clone());

	info!("Initializing Garage main data store...");
	let garage = Garage::new(config.clone(), db, background);

	let run_system = tokio::spawn(garage.system.clone().run(watch_cancel.clone()));

	info!("Crate admin RPC handler...");
	AdminRpcHandler::new(garage.clone());

	info!("Initializing API server...");
	let api_server = tokio::spawn(run_api_server(
		garage.clone(),
		wait_from(watch_cancel.clone()),
	));

	info!("Initializing web server...");
	let web_server = tokio::spawn(run_web_server(
		garage.clone(),
		wait_from(watch_cancel.clone()),
	));

	// Stuff runs

	// When a cancel signal is sent, stuff stops
	if let Err(e) = api_server.await? {
		warn!("API server exited with error: {}", e);
	}
	if let Err(e) = web_server.await? {
		warn!("Web server exited with error: {}", e);
	}

	// Remove RPC handlers for system to break reference cycles
	garage.system.netapp.drop_all_handlers();

	// Await for last parts to end
	run_system.await?;
	await_background_done.await?;

	info!("Cleaning up...");

	Ok(())
}
