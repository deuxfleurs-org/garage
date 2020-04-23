use std::path::PathBuf;
use std::sync::Arc;

use futures_util::future::*;
use tokio::sync::watch;

use crate::background::*;
use crate::config::*;
use crate::error::Error;

use crate::rpc::membership::System;
use crate::rpc::rpc_client::RpcHttpClient;
use crate::rpc::rpc_server::RpcServer;

use crate::table::table_fullcopy::*;
use crate::table::table_sharded::*;
use crate::table::*;

use crate::store::block::*;
use crate::store::block_ref_table::*;
use crate::store::bucket_table::*;
use crate::store::key_table::*;
use crate::store::object_table::*;
use crate::store::version_table::*;

use crate::api::api_server;

use crate::admin_rpc::*;

pub struct Garage {
	pub config: Config,

	pub db: sled::Db,
	pub background: Arc<BackgroundRunner>,
	pub system: Arc<System>,
	pub block_manager: Arc<BlockManager>,

	pub bucket_table: Arc<Table<BucketTable, TableFullReplication>>,
	pub key_table: Arc<Table<KeyTable, TableFullReplication>>,

	pub object_table: Arc<Table<ObjectTable, TableShardedReplication>>,
	pub version_table: Arc<Table<VersionTable, TableShardedReplication>>,
	pub block_ref_table: Arc<Table<BlockRefTable, TableShardedReplication>>,
}

impl Garage {
	pub async fn new(
		config: Config,
		db: sled::Db,
		background: Arc<BackgroundRunner>,
		rpc_server: &mut RpcServer,
	) -> Arc<Self> {
		info!("Initialize membership management system...");
		let rpc_http_client = Arc::new(
			RpcHttpClient::new(config.max_concurrent_rpc_requests, &config.rpc_tls)
				.expect("Could not create RPC client"),
		);
		let system = System::new(
			config.metadata_dir.clone(),
			rpc_http_client,
			background.clone(),
			rpc_server,
		);

		let data_rep_param = TableShardedReplication {
			replication_factor: config.data_replication_factor,
			write_quorum: (config.data_replication_factor + 1) / 2,
			read_quorum: 1,
		};

		let meta_rep_param = TableShardedReplication {
			replication_factor: config.meta_replication_factor,
			write_quorum: (config.meta_replication_factor + 1) / 2,
			read_quorum: (config.meta_replication_factor + 1) / 2,
		};

		let control_rep_param = TableFullReplication::new(
			config.meta_epidemic_factor,
			(config.meta_epidemic_factor + 1) / 2,
		);

		info!("Initialize block manager...");
		let block_manager = BlockManager::new(
			&db,
			config.data_dir.clone(),
			data_rep_param.clone(),
			system.clone(),
			rpc_server,
		);

		info!("Initialize block_ref_table...");
		let block_ref_table = Table::new(
			BlockRefTable {
				background: background.clone(),
				block_manager: block_manager.clone(),
			},
			data_rep_param.clone(),
			system.clone(),
			&db,
			"block_ref".to_string(),
			rpc_server,
		)
		.await;

		info!("Initialize version_table...");
		let version_table = Table::new(
			VersionTable {
				background: background.clone(),
				block_ref_table: block_ref_table.clone(),
			},
			meta_rep_param.clone(),
			system.clone(),
			&db,
			"version".to_string(),
			rpc_server,
		)
		.await;

		info!("Initialize object_table...");
		let object_table = Table::new(
			ObjectTable {
				background: background.clone(),
				version_table: version_table.clone(),
			},
			meta_rep_param.clone(),
			system.clone(),
			&db,
			"object".to_string(),
			rpc_server,
		)
		.await;

		info!("Initialize bucket_table...");
		let bucket_table = Table::new(
			BucketTable,
			control_rep_param.clone(),
			system.clone(),
			&db,
			"bucket".to_string(),
			rpc_server,
		)
		.await;

		info!("Initialize key_table_table...");
		let key_table = Table::new(
			KeyTable,
			control_rep_param.clone(),
			system.clone(),
			&db,
			"key".to_string(),
			rpc_server,
		)
		.await;

		info!("Initialize Garage...");
		let garage = Arc::new(Self {
			config,
			db,
			system: system.clone(),
			block_manager,
			background,
			bucket_table,
			key_table,
			object_table,
			version_table,
			block_ref_table,
		});

		info!("Crate admin RPC handler...");
		AdminRpcHandler::new(garage.clone()).register_handler(rpc_server);

		info!("Start block manager background thread...");
		garage.block_manager.garage.swap(Some(garage.clone()));
		garage.block_manager.clone().spawn_background_worker().await;

		garage
	}
}

async fn shutdown_signal(send_cancel: watch::Sender<bool>) -> Result<(), Error> {
	// Wait for the CTRL+C signal
	tokio::signal::ctrl_c()
		.await
		.expect("failed to install CTRL+C signal handler");
	info!("Received CTRL+C, shutting down.");
	send_cancel.broadcast(true)?;
	Ok(())
}

async fn wait_from(mut chan: watch::Receiver<bool>) -> () {
	while let Some(exit_now) = chan.recv().await {
		if exit_now {
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
	let db = sled::open(db_path).expect("Unable to open DB");

	info!("Initialize RPC server...");
	let mut rpc_server = RpcServer::new(config.rpc_bind_addr.clone(), config.rpc_tls.clone());

	info!("Initializing background runner...");
	let (send_cancel, watch_cancel) = watch::channel(false);
	let background = BackgroundRunner::new(16, watch_cancel.clone());

	let garage = Garage::new(config, db, background.clone(), &mut rpc_server).await;

	info!("Initializing RPC and API servers...");
	let run_rpc_server = Arc::new(rpc_server).run(wait_from(watch_cancel.clone()));
	let api_server = api_server::run_api_server(garage.clone(), wait_from(watch_cancel.clone()));

	futures::try_join!(
		garage
			.system
			.clone()
			.bootstrap(&garage.config.bootstrap_peers[..])
			.map(|rv| {
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
		background.run().map(|rv| {
			info!("Background runner exited");
			Ok(rv)
		}),
		shutdown_signal(send_cancel),
	)?;

	info!("Cleaning up...");

	Ok(())
}
