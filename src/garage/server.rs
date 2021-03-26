use std::path::PathBuf;
use std::sync::Arc;

use futures_util::future::*;
use tokio::sync::watch;

use garage_util::background::*;
use garage_util::config::*;
use garage_util::error::Error;

use garage_api::run_api_server;
use garage_model::garage::Garage;
use garage_rpc::rpc_server::RpcServer;
use garage_web::run_web_server;

use crate::admin_rpc::*;

async fn shutdown_signal(send_cancel: watch::Sender<bool>) -> Result<(), Error> {
	// Wait for the CTRL+C signal
	tokio::signal::ctrl_c()
		.await
		.expect("failed to install CTRL+C signal handler");
	info!("Received CTRL+C, shutting down.");
	send_cancel.send(true)?;
	Ok(())
}

async fn wait_from(mut chan: watch::Receiver<bool>) -> () {
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
	let db = sled::open(&db_path).expect("Unable to open sled DB");

	info!("Initialize RPC server...");
	let mut rpc_server = RpcServer::new(config.rpc_bind_addr.clone(), config.rpc_tls.clone());

	info!("Initializing background runner...");
	let (send_cancel, watch_cancel) = watch::channel(false);
	let (background, await_background_done) = BackgroundRunner::new(16, watch_cancel.clone());

	info!("Initializing Garage main data store...");
	let garage = Garage::new(config.clone(), db, background, &mut rpc_server);
	let bootstrap = garage.system.clone().bootstrap(
		config.bootstrap_peers,
		config.consul_host,
		config.consul_service_name,
	);

	info!("Crate admin RPC handler...");
	AdminRpcHandler::new(garage.clone()).register_handler(&mut rpc_server);

	info!("Initializing RPC and API servers...");
	let run_rpc_server = Arc::new(rpc_server).run(wait_from(watch_cancel.clone()));
	let api_server = run_api_server(garage.clone(), wait_from(watch_cancel.clone()));
	let web_server = run_web_server(garage, wait_from(watch_cancel.clone()));

	futures::try_join!(
		bootstrap.map(|rv| {
			info!("Bootstrap done");
			Ok(rv)
		}),
		run_rpc_server.map(|rv| {
			info!("RPC server exited");
			rv
		}),
		api_server.map(|rv| {
			info!("API server exited");
			rv
		}),
		web_server.map(|rv| {
			info!("Web server exited");
			rv
		}),
		await_background_done.map(|rv| {
			info!("Background runner exited: {:?}", rv);
			Ok(())
		}),
		shutdown_signal(send_cancel),
	)?;

	info!("Cleaning up...");

	Ok(())
}
