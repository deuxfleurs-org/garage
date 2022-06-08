use std::path::PathBuf;

use tokio::sync::watch;

use garage_db as db;

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
	let config = read_config(config_file).expect("Unable to read config file");

	info!("Opening database...");
	let mut db_path = config.metadata_dir.clone();
	std::fs::create_dir_all(&db_path).expect("Unable to create Garage meta data directory");
	let db = match config.db_engine.as_str() {
		"sled" => {
			db_path.push("db");
			info!("Opening Sled database at: {}", db_path.display());
			let db = db::sled_adapter::sled::Config::default()
				.path(&db_path)
				.cache_capacity(config.sled_cache_capacity)
				.flush_every_ms(Some(config.sled_flush_every_ms))
				.open()
				.expect("Unable to open sled DB");
			db::sled_adapter::SledDb::init(db)
		}
		"sqlite" | "sqlite3" | "rusqlite" => {
			db_path.push("db.sqlite");
			info!("Opening Sqlite database at: {}", db_path.display());
			let db = db::sqlite_adapter::rusqlite::Connection::open(db_path)
				.expect("Unable to open sqlite DB");
			db::sqlite_adapter::SqliteDb::init(db)
		}
		"lmdb" | "heed" => {
			db_path.push("db.lmdb");
			info!("Opening LMDB database at: {}", db_path.display());
			std::fs::create_dir_all(&db_path).expect("Unable to create LMDB data directory");
			let map_size = if u32::MAX as usize == usize::MAX {
				warn!("LMDB is not recommended on 32-bit systems, database size will be limited");
				1usize << 30 // 1GB for 32-bit systems
			} else {
				1usize << 40 // 1TB for 64-bit systems
			};

			let db = db::lmdb_adapter::heed::EnvOpenOptions::new()
				.max_dbs(100)
				.map_size(map_size)
				.open(&db_path)
				.expect("Unable to open LMDB DB");
			db::lmdb_adapter::LmdbDb::init(db)
		}
		e => {
			return Err(Error::Message(format!(
				"Unsupported DB engine: {} (options: sled, sqlite, lmdb)",
				e
			)));
		}
	};

	info!("Initializing background runner...");
	let watch_cancel = netapp::util::watch_ctrl_c();
	let (background, await_background_done) = BackgroundRunner::new(16, watch_cancel.clone());

	info!("Initializing Garage main data store...");
	let garage = Garage::new(config.clone(), db, background);

	info!("Initialize tracing...");
	if let Some(export_to) = config.admin.trace_sink {
		init_tracing(&export_to, garage.system.id)?;
	}

	info!("Initialize Admin API server and metrics collector...");
	let admin_server = AdminApiServer::new(garage.clone());

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
