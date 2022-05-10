use std::path::PathBuf;

use tokio::sync::watch;

use garage_util::background::*;
use garage_util::config::*;
use garage_util::error::Error;

use garage_admin::metrics::*;
use garage_admin::tracing_setup::*;
use garage_api::s3::api_server::S3ApiServer;
use garage_model::garage::Garage;
use garage_web::run_web_server;

#[cfg(feature = "k2v")]
use garage_api::k2v::api_server::K2VApiServer;

use crate::admin::*;

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

	info!("Initialize admin web server and metric backend...");
	let admin_server_init = AdminServer::init();

	info!("Initializing background runner...");
	let watch_cancel = netapp::util::watch_ctrl_c();
	let (background, await_background_done) = BackgroundRunner::new(16, watch_cancel.clone());

	info!("Initializing Garage main data store...");
	let garage = Garage::new(config.clone(), db, background);

	info!("Initialize tracing...");
	if let Some(export_to) = config.admin.trace_sink {
		init_tracing(&export_to, garage.system.id)?;
	}

	let run_system = tokio::spawn(garage.system.clone().run(watch_cancel.clone()));

	info!("Create admin RPC handler...");
	AdminRpcHandler::new(garage.clone());

	info!("Initializing S3 API server...");
	let s3_api_server = tokio::spawn(S3ApiServer::run(
		garage.clone(),
		wait_from(watch_cancel.clone()),
	));

	#[cfg(feature = "k2v")]
	let k2v_api_server = {
		info!("Initializing K2V API server...");
		tokio::spawn(K2VApiServer::run(
			garage.clone(),
			wait_from(watch_cancel.clone()),
		))
	};

	info!("Initializing web server...");
	let web_server = tokio::spawn(run_web_server(
		garage.clone(),
		wait_from(watch_cancel.clone()),
	));

	let admin_server = if let Some(admin_bind_addr) = config.admin.api_bind_addr {
		info!("Configure and run admin web server...");
		Some(tokio::spawn(
			admin_server_init.run(admin_bind_addr, wait_from(watch_cancel.clone())),
		))
	} else {
		None
	};

	// Stuff runs

	// When a cancel signal is sent, stuff stops
	if let Err(e) = s3_api_server.await? {
		warn!("S3 API server exited with error: {}", e);
	}
	#[cfg(feature = "k2v")]
	if let Err(e) = k2v_api_server.await? {
		warn!("K2V API server exited with error: {}", e);
	}
	if let Err(e) = web_server.await? {
		warn!("Web server exited with error: {}", e);
	}
	if let Some(a) = admin_server {
		if let Err(e) = a.await? {
			warn!("Admin web server exited with error: {}", e);
		}
	}

	// Remove RPC handlers for system to break reference cycles
	garage.system.netapp.drop_all_handlers();

	// Await for netapp RPC system to end
	run_system.await?;

	// Drop all references so that stuff can terminate properly
	drop(garage);

	// Await for all background tasks to end
	await_background_done.await?;

	info!("Cleaning up...");

	Ok(())
}
