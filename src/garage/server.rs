use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::watch;

use garage_util::background::*;
use garage_util::config::*;
use garage_util::error::Error;
use garage_util::rabbitmq::RabbitClient;

use garage_api_admin::api_server::AdminApiServer;
use garage_api_s3::api_server::S3ApiServer;
use garage_model::garage::Garage;
use garage_web::WebServer;

#[cfg(feature = "k2v")]
use garage_api_k2v::api_server::K2VApiServer;

use crate::secrets::{fill_secrets, Secrets};
use crate::tracing_setup::init_tracing;
use crate::ServerOpt;

async fn wait_from(mut chan: watch::Receiver<bool>) {
	while !*chan.borrow() {
		if chan.changed().await.is_err() {
			return;
		}
	}
}

pub async fn run_server(
	config_file: PathBuf,
	secrets: Secrets,
	opt: ServerOpt,
) -> Result<(), Error> {
	info!("Loading configuration from {}...", config_file.display());
	let config = fill_secrets(read_config(config_file)?, secrets)?;

	// Optional RabbitMQ client for integration events
	let object_events = if let Some(rabbit_cfg) = &config.rabbitmq {
		if rabbit_cfg.publish_object_created {
			match RabbitClient::new(rabbit_cfg.clone()).await {
				Ok(client) => Some(Arc::new(client)),
				Err(e) => {
					error!("Failed to initialize RabbitMQ client, integration events will be disabled: {}", e);
					None
				}
			}
		} else {
			None
		}
	} else {
		None
	};

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

	// Handle --single-node, --default-bucket and --default-access-key
	initial_config(&garage, opt).await?;

	info!("Initializing background runner...");
	let watch_cancel = watch_shutdown_signal();
	let (background, await_background_done) = BackgroundRunner::new(watch_cancel.clone());

	info!("Spawning Garage workers...");
	garage.spawn_workers(&background)?;

	if let Some(admin_trace_sink) = &config.admin.trace_sink {
		info!("Initialize tracing...");
		init_tracing(admin_trace_sink, garage.system.id)?;
	}

	info!("Initialize Admin API server and metrics collector...");
	let admin_server = AdminApiServer::new(
		garage.clone(),
		background.clone(),
		#[cfg(feature = "metrics")]
		metrics_exporter,
	);

	info!("Launching internal Garage cluster communications...");
	let run_system = tokio::spawn(garage.system.clone().run(watch_cancel.clone()));

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
				object_events.clone(),
				watch_cancel.clone(),
			)),
		));
	}

	if let Some(k2v_api) = &config.k2v_api {
		#[cfg(feature = "k2v")]
		{
			info!("Initializing K2V API server...");
			servers.push((
				"K2V API",
				tokio::spawn(K2VApiServer::run(
					garage.clone(),
					k2v_api.api_bind_addr.clone(),
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
		let web_server = WebServer::new(garage.clone(), web_config);
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

async fn initial_config(garage: &Arc<Garage>, opt: ServerOpt) -> Result<(), Error> {
	use garage_model::bucket_alias_table::is_valid_bucket_name;
	use garage_model::bucket_table::Bucket;
	use garage_model::key_table::*;
	use garage_model::permission::BucketKeyPerm;
	use garage_rpc::layout::*;
	use garage_rpc::replication_mode::ReplicationFactor;
	use garage_table::*;
	use garage_util::time::now_msec;

	if opt.single_node {
		if garage.replication_factor != ReplicationFactor::new(1).unwrap() {
			return Err(Error::Message(
				"Single-node mode requires replication_factor = 1 in the configuration file."
					.into(),
			));
		}

		let layout_version = garage.system.cluster_layout().inner().current().version;

		if layout_version > 1 {
			return Err(Error::Message("Refusing to run in single-node mode: layout version is already superior to 1. Remove the --single-node flag to run the server in full mode.".into()));
		}

		if layout_version == 0 {
			// Setup initial layout
			let mut layout = garage.system.cluster_layout().inner().clone();
			let our_id = garage.system.id;

			// Check no other nodes are present in the system
			let nodes = garage.system.get_known_nodes();
			if nodes.iter().any(|x| x.id != our_id) {
				return Err(Error::Message("Refusing to run in single-node mode: more nodes are already present in the cluster.".into()));
			}

			// Automatically determine this node's capacity
			let capacity = garage
				.system
				.local_status()
				.data_disk_avail
				.map(|(_avail, total)| total)
				.unwrap_or(1024 * 1024 * 1024); // Default to 1GB

			assert!(layout.current().roles.is_empty());

			layout.staging.get_mut().roles.clear();
			layout.staging.get_mut().roles.update_in_place(
				our_id,
				NodeRoleV(Some(NodeRole {
					zone: "dc1".to_string(),
					capacity: Some(capacity),
					tags: vec!["default".to_string()],
				})),
			);

			let (layout, msg) = layout.apply_staged_changes(1)?;
			info!(
				"Created initial layout for single-node configuration:\n{}",
				msg.join("\n")
			);

			garage
				.system
				.layout_manager
				.update_cluster_layout(&layout)
				.await?;
		}
	}

	if (opt.default_bucket || opt.default_access_key) && !opt.single_node {
		return Err(Error::Message(
			"Flags --default-access-key and --default-bucket can only be used in single-node mode."
				.into(),
		));
	}

	if opt.default_access_key || opt.default_bucket {
		let rdenv = |name: &str| {
			std::env::var(name)
				.map_err(|_| Error::Message(format!("Environment variable `{}` is not set", name)))
		};

		// Create default access key if it does not exist
		let key_id = rdenv("GARAGE_DEFAULT_ACCESS_KEY")?;
		let secret_key = rdenv("GARAGE_DEFAULT_SECRET_KEY")?;

		let existing_key = garage.key_table.get(&EmptyKey, &key_id).await?;

		let key = match existing_key {
			Some(key) => {
				match key.state.as_option() {
                    None => return Err(Error::Message(format!("Access key {} was deleted in the cluster, cannot add it back", key_id))),
                    Some(st) if st.secret_key != secret_key => return Err(Error::Message(format!("Access key {} is associated with a secret key different than the one given in GARAGE_DEFAULT_SECRET_KEY", key_id))),
                    _ => (),
                }

				key
			}
			None => {
				info!("Creating default access key `{}`", key_id);

				let mut key = Key::import(&key_id, &secret_key, "default access key")
					.map_err(|e| Error::Message(format!("Invalid default access key: {}", e)))?;
				key.state
					.as_option_mut()
					.unwrap()
					.allow_create_bucket
					.update(true);
				garage.key_table.insert(&key).await?;

				key
			}
		};

		if opt.default_bucket {
			// Create default bucket if it does not exist
			let bucket_name = rdenv("GARAGE_DEFAULT_BUCKET")?;

			if !is_valid_bucket_name(&bucket_name, garage.config.allow_punycode) {
				return Err(Error::Message(
					"Invalid default bucket name, see S3 specification for allowed bucket names."
						.into(),
				));
			}

			let helper = garage.locked_helper().await;

			let bucket = match helper.bucket().resolve_global_bucket_fast(&bucket_name)? {
				Some(bucket) => bucket,
				None => {
					info!("Creating default bucket `{}`", bucket_name);

					let bucket = Bucket::new();
					garage.bucket_table.insert(&bucket).await?;

					helper
						.set_global_bucket_alias(bucket.id, &bucket_name)
						.await
						.map_err(|e| {
							Error::Message(format!("Cannot create default bucket: {}", e))
						})?;

					bucket
				}
			};

			helper
				.set_bucket_key_permissions(
					bucket.id,
					&key.key_id,
					BucketKeyPerm {
						timestamp: now_msec(),
						allow_read: true,
						allow_write: true,
						allow_owner: true,
					},
				)
				.await
				.map_err(|e| {
					Error::Message(format!(
						"Cannot configure permissions on default bucket: {}",
						e
					))
				})?;
		}
	}

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
		loop {
			tokio::select! {
					_ = sigint.recv() => {
						info!("Received SIGINT, shutting down.");
						break
					}
					_ = sigterm.recv() => {
						info!("Received SIGTERM, shutting down.");
						break
					}
					_ = sighup.recv() => {
						info!("Received SIGHUP, reload not supported.");
						continue
					}
			}
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
