use std::path::PathBuf;

use tokio::sync::watch;

use garage_util::background::*;
use garage_util::config::*;
use garage_util::error::Error;

use garage_api::admin::api_server::AdminApiServer;
use garage_api::s3::api_server::S3ApiServer;
use garage_model::garage::Garage;
use garage_web::run_web_server;

#[cfg(feature = "k2v")]
use garage_api::k2v::api_server::K2VApiServer;

use crate::admin::*;
use crate::tracing_setup::*;

async fn wait_from(mut chan: watch::Receiver<bool>) {
	while !*chan.borrow() {
		if chan.changed().await.is_err() {
			return;
		}
	}
}

pub async fn run_server(config_file: PathBuf) -> Result<(), Error> {
	info!("Loading configuration...");
	let config = read_config(config_file)?;

	info!("Initializing background runner...");
	let watch_cancel = netapp::util::watch_ctrl_c();
	let (background, await_background_done) = BackgroundRunner::new(16, watch_cancel.clone());

	info!("Initializing Garage main data store...");
	let garage = Garage::new(config.clone(), background)?;

	info!("Initialize tracing...");
	if let Some(export_to) = config.admin.trace_sink {
		init_tracing(&export_to, garage.system.id)?;
	}

	info!("Initialize Admin API server and metrics collector...");
	let admin_server = AdminApiServer::new(garage.clone());

	info!("Launching internal Garage cluster communications...");
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

	info!("Launching Admin API server...");
	let admin_server = tokio::spawn(admin_server.run(wait_from(watch_cancel.clone())));

	// Stuff runs

	// When a cancel signal is sent, stuff stops
	if let Err(e) = s3_api_server.await? {
		warn!("S3 API server exited with error: {}", e);
	} else {
		info!("S3 API server exited without error.");
	}
	#[cfg(feature = "k2v")]
	if let Err(e) = k2v_api_server.await? {
		warn!("K2V API server exited with error: {}", e);
	} else {
		info!("K2V API server exited without error.");
	}
	if let Err(e) = web_server.await? {
		warn!("Web server exited with error: {}", e);
	} else {
		info!("Web server exited without error.");
	}
	if let Err(e) = admin_server.await? {
		warn!("Admin web server exited with error: {}", e);
	} else {
		info!("Admin API server exited without error.");
	}

	// Remove RPC handlers for system to break reference cycles
	garage.system.netapp.drop_all_handlers();
	opentelemetry::global::shutdown_tracer_provider();

	// Await for netapp RPC system to end
	run_system.await?;
	info!("Netapp exited");

	// Drop all references so that stuff can terminate properly
	drop(garage);

	// Await for all background tasks to end
	await_background_done.await?;

	info!("Cleaning up...");

	Ok(())
}
