use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use futures_util::future::*;
use serde::Deserialize;
use tokio::sync::watch;

use crate::background::*;
use crate::data::*;
use crate::error::Error;
use crate::membership::System;
use crate::rpc_server::RpcServer;
use crate::table::*;
use crate::table_fullcopy::*;
use crate::table_sharded::*;

use crate::block::*;
use crate::block_ref_table::*;
use crate::bucket_table::*;
use crate::object_table::*;
use crate::version_table::*;

use crate::admin_rpc::*;
use crate::api_server;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
	pub metadata_dir: PathBuf,
	pub data_dir: PathBuf,

	pub api_bind_addr: SocketAddr,
	pub rpc_bind_addr: SocketAddr,

	pub bootstrap_peers: Vec<SocketAddr>,

	#[serde(default = "default_block_size")]
	pub block_size: usize,

	#[serde(default = "default_replication_factor")]
	pub meta_replication_factor: usize,

	#[serde(default = "default_epidemic_factor")]
	pub meta_epidemic_factor: usize,

	#[serde(default = "default_replication_factor")]
	pub data_replication_factor: usize,

	pub rpc_tls: Option<TlsConfig>,
}

fn default_block_size() -> usize {
	1048576
}
fn default_replication_factor() -> usize {
	3
}
fn default_epidemic_factor() -> usize {
	3
}

#[derive(Deserialize, Debug, Clone)]
pub struct TlsConfig {
	pub ca_cert: String,
	pub node_cert: String,
	pub node_key: String,
}

pub struct Garage {
	pub db: sled::Db,
	pub background: Arc<BackgroundRunner>,
	pub system: Arc<System>,
	pub block_manager: Arc<BlockManager>,

	pub bucket_table: Arc<Table<BucketTable, TableFullReplication>>,
	pub object_table: Arc<Table<ObjectTable, TableShardedReplication>>,
	pub version_table: Arc<Table<VersionTable, TableShardedReplication>>,
	pub block_ref_table: Arc<Table<BlockRefTable, TableShardedReplication>>,
}

impl Garage {
	pub async fn new(
		config: Config,
		id: UUID,
		db: sled::Db,
		background: Arc<BackgroundRunner>,
		rpc_server: &mut RpcServer,
	) -> Arc<Self> {
		info!("Initialize membership management system...");
		let system = System::new(config.clone(), id, background.clone(), rpc_server);

		info!("Initialize block manager...");
		let block_manager =
			BlockManager::new(&db, config.data_dir.clone(), system.clone(), rpc_server);

		let data_rep_param = TableShardedReplication {
			replication_factor: system.config.data_replication_factor,
			write_quorum: (system.config.data_replication_factor + 1) / 2,
			read_quorum: 1,
		};

		let meta_rep_param = TableShardedReplication {
			replication_factor: system.config.meta_replication_factor,
			write_quorum: (system.config.meta_replication_factor + 1) / 2,
			read_quorum: (system.config.meta_replication_factor + 1) / 2,
		};

		let control_rep_param = TableFullReplication::new(
			system.config.meta_epidemic_factor,
			(system.config.meta_epidemic_factor + 1) / 2,
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

		info!("Initialize Garage...");
		let garage = Arc::new(Self {
			db,
			system: system.clone(),
			block_manager,
			background,
			bucket_table,
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

fn read_config(config_file: PathBuf) -> Result<Config, Error> {
	let mut file = std::fs::OpenOptions::new()
		.read(true)
		.open(config_file.as_path())?;

	let mut config = String::new();
	file.read_to_string(&mut config)?;

	Ok(toml::from_str(&config)?)
}

fn gen_node_id(metadata_dir: &PathBuf) -> Result<UUID, Error> {
	let mut id_file = metadata_dir.clone();
	id_file.push("node_id");
	if id_file.as_path().exists() {
		let mut f = std::fs::File::open(id_file.as_path())?;
		let mut d = vec![];
		f.read_to_end(&mut d)?;
		if d.len() != 32 {
			return Err(Error::Message(format!("Corrupt node_id file")));
		}

		let mut id = [0u8; 32];
		id.copy_from_slice(&d[..]);
		Ok(id.into())
	} else {
		let id = gen_uuid();

		let mut f = std::fs::File::create(id_file.as_path())?;
		f.write_all(id.as_slice())?;
		Ok(id)
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

	let id = gen_node_id(&config.metadata_dir).expect("Unable to read or generate node ID");
	info!("Node ID: {}", hex::encode(&id));

	info!("Opening database...");
	let mut db_path = config.metadata_dir.clone();
	db_path.push("db");
	let db = sled::open(db_path).expect("Unable to open DB");

	info!("Initialize RPC server...");
	let mut rpc_server = RpcServer::new(config.rpc_bind_addr.clone(), config.rpc_tls.clone());

	info!("Initializing background runner...");
	let (send_cancel, watch_cancel) = watch::channel(false);
	let background = BackgroundRunner::new(8, watch_cancel.clone());

	let garage = Garage::new(config, id, db, background.clone(), &mut rpc_server).await;

	info!("Initializing RPC and API servers...");
	let run_rpc_server = Arc::new(rpc_server).run(wait_from(watch_cancel.clone()));
	let api_server = api_server::run_api_server(garage.clone(), wait_from(watch_cancel.clone()));

	futures::try_join!(
		garage.system.clone().bootstrap().map(|rv| {
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
