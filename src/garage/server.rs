use std::path::PathBuf;

use tokio::sync::watch;

use garage_util::background::*;
use garage_util::config::*;
use garage_util::error::Error;

use garage_api_admin::api_server::AdminApiServer;
use garage_api_s3::api_server::S3ApiServer;
use garage_model::garage::Garage;
use garage_web::WebServer;

#[cfg(feature = "k2v")]
use garage_api_k2v::api_server::K2VApiServer;

use crate::admin::*;
use crate::secrets::{fill_secrets, Secrets};
#[cfg(feature = "telemetry-otlp")]
use crate::tracing_setup::*;

async fn wait_from(mut chan: watch::Receiver<bool>) {
	while !*chan.borrow() {
		if chan.changed().await.is_err() {
			return;
		}
	}
}

pub async fn run_server(config_file: PathBuf, secrets: Secrets) -> Result<(), Error> {
	info!("Loading configuration...");
	let config = fill_secrets(read_config(config_file)?, secrets)?;

	// ---- Initialize Garage internals ----

	#[cfg(feature = "metrics")]
	let metrics_exporter = opentelemetry_prometheus::exporter()
		.with_default_summary_quantiles(vec![0.25, 0.5, 0.75, 0.9, 0.95, 0.99])
		.with_default_histogram_boundaries(vec![
			0.001, 0.0015, 0.002, 0.003, 0.005, 0.007, 0.01, 0.015, 0.02, 0.03, 0.05, 0.07, 0.1,
			0.15, 0.2, 0.3, 0.5, 0.7, 1., 1.5, 2., 3., 5., 7., 10., 15., 20., 30., 40., 50., 60.,
			70., 100.,
		])
		.init();

	info!("Initializing Garage main data store...");
	let garage = Garage::new(config.clone())?;

	info!("Initializing background runner...");
	let watch_cancel = watch_shutdown_signal();
	let (background, await_background_done) = BackgroundRunner::new(watch_cancel.clone());

	info!("Spawning Garage workers...");
	garage.spawn_workers(&background)?;

	if config.admin.trace_sink.is_some() {
		info!("Initialize tracing...");

		#[cfg(feature = "telemetry-otlp")]
		init_tracing(config.admin.trace_sink.as_ref().unwrap(), garage.system.id)?;

		#[cfg(not(feature = "telemetry-otlp"))]
		error!("Garage was built without OTLP exporter, admin.trace_sink is ignored.");
	}

	info!("Initialize Admin API server and metrics collector...");
	let admin_server = AdminApiServer::new(
		garage.clone(),
		#[cfg(feature = "metrics")]
		metrics_exporter,
	);

	info!("Launching internal Garage cluster communications...");
	let run_system = tokio::spawn(garage.system.clone().run(watch_cancel.clone()));

	info!("Create admin RPC handler...");
	AdminRpcHandler::new(garage.clone(), background.clone());

	// ---- Launch public-facing API servers ----

	let mut servers = vec![];

	if let Some(s3_bind_addr) = &config.s3_api.api_bind_addr {
		info!("Initializing S3 API server...");
		servers.push((
			"S3 API",
			tokio::spawn(S3ApiServer::run(
				garage.clone(),
				s3_bind_addr.clone(),
				config.s3_api.s3_region.clone(),
				watch_cancel.clone(),
			)),
		));
	}

	if config.k2v_api.is_some() {
		#[cfg(feature = "k2v")]
		{
			info!("Initializing K2V API server...");
			servers.push((
				"K2V API",
				tokio::spawn(K2VApiServer::run(
					garage.clone(),
					config.k2v_api.as_ref().unwrap().api_bind_addr.clone(),
					config.s3_api.s3_region.clone(),
					watch_cancel.clone(),
				)),
			));
		}
		#[cfg(not(feature = "k2v"))]
		error!("K2V is not enabled in this build, cannot start K2V API server");
	}

	if let Some(web_config) = &config.s3_web {
		info!("Initializing web server...");
		let web_server = WebServer::new(garage.clone(), &web_config);
		servers.push((
			"Web",
			tokio::spawn(web_server.run(web_config.bind_addr.clone(), watch_cancel.clone())),
		));
	}

	if let Some(admin_bind_addr) = &config.admin.api_bind_addr {
		info!("Launching Admin API server...");
		servers.push((
			"Admin",
			tokio::spawn(admin_server.run(admin_bind_addr.clone(), watch_cancel.clone())),
		));
	}

	#[cfg(not(feature = "metrics"))]
	if config.admin.metrics_token.is_some() {
		warn!("This Garage version is built without the metrics feature");
	}

	if servers.is_empty() {
		// Nothing runs except netapp (not in servers)
		// Await shutdown signal before proceeding to shutting down netapp
		wait_from(watch_cancel).await;
	} else {
		// Stuff runs

		// When a cancel signal is sent, stuff stops

		// Collect stuff
		for (desc, join_handle) in servers {
			if let Err(e) = join_handle.await? {
				error!("{} server exited with error: {}", desc, e);
			} else {
				info!("{} server exited without error.", desc);
			}
		}
	}

	// Remove RPC handlers for system to break reference cycles
	info!("Deregistering RPC handlers for shutdown...");
	garage.system.netapp.drop_all_handlers();
	opentelemetry::global::shutdown_tracer_provider();

	// Await for netapp RPC system to end
	run_system.await?;
	info!("Netapp exited");

	// Drop all references so that stuff can terminate properly
	garage.system.cleanup();
	drop(garage);

	// Await for all background tasks to end
	await_background_done.await?;

	info!("Cleaning up...");

	Ok(())
}

#[cfg(unix)]
fn watch_shutdown_signal() -> watch::Receiver<bool> {
	use tokio::signal::unix::*;

	let (send_cancel, watch_cancel) = watch::channel(false);
	tokio::spawn(async move {
		let mut sigint = signal(SignalKind::interrupt()).expect("Failed to install SIGINT handler");
		let mut sigterm =
			signal(SignalKind::terminate()).expect("Failed to install SIGTERM handler");
		let mut sighup = signal(SignalKind::hangup()).expect("Failed to install SIGHUP handler");
		tokio::select! {
			_ = sigint.recv() => info!("Received SIGINT, shutting down."),
			_ = sigterm.recv() => info!("Received SIGTERM, shutting down."),
			_ = sighup.recv() => info!("Received SIGHUP, shutting down."),
		}
		send_cancel.send(true).unwrap();
	});
	watch_cancel
}

#[cfg(windows)]
fn watch_shutdown_signal() -> watch::Receiver<bool> {
	use tokio::signal::windows::*;

	let (send_cancel, watch_cancel) = watch::channel(false);
	tokio::spawn(async move {
		let mut sigint = ctrl_c().expect("Failed to install Ctrl-C handler");
		let mut sigclose = ctrl_close().expect("Failed to install Ctrl-Close handler");
		let mut siglogoff = ctrl_logoff().expect("Failed to install Ctrl-Logoff handler");
		let mut sigsdown = ctrl_shutdown().expect("Failed to install Ctrl-Shutdown handler");
		tokio::select! {
			_ = sigint.recv() => info!("Received Ctrl-C, shutting down."),
			_ = sigclose.recv() => info!("Received Ctrl-Close, shutting down."),
			_ = siglogoff.recv() => info!("Received Ctrl-Logoff, shutting down."),
			_ = sigsdown.recv() => info!("Received Ctrl-Shutdown, shutting down."),
		}
		send_cancel.send(true).unwrap();
	});
	watch_cancel
}
