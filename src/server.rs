use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

pub use futures_util::future::FutureExt;
use serde::Deserialize;
use tokio::sync::watch;

use crate::api_server;
use crate::background::*;
use crate::block::*;
use crate::data::*;
use crate::error::Error;
use crate::membership::System;
use crate::proto::*;
use crate::rpc_server::RpcServer;
use crate::table::*;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
	pub metadata_dir: PathBuf,
	pub data_dir: PathBuf,

	pub api_port: u16,
	pub rpc_port: u16,

	pub bootstrap_peers: Vec<SocketAddr>,

	#[serde(default = "default_block_size")]
	pub block_size: usize,

	#[serde(default = "default_replication_factor")]
	pub meta_replication_factor: usize,

	#[serde(default = "default_replication_factor")]
	pub data_replication_factor: usize,

	pub rpc_tls: Option<TlsConfig>,
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

	pub object_table: Arc<Table<ObjectTable>>,
	pub version_table: Arc<Table<VersionTable>>,
	pub block_ref_table: Arc<Table<BlockRefTable>>,
}

impl Garage {
	pub async fn new(
		config: Config,
		id: UUID,
		db: sled::Db,
		background: Arc<BackgroundRunner>,
		rpc_server: &mut RpcServer,
	) -> Arc<Self> {
		println!("Initialize membership management system...");
		let system = System::new(config.clone(), id, background.clone(), rpc_server);

		println!("Initialize block manager...");
		let block_manager =
			BlockManager::new(&db, config.data_dir.clone(), system.clone(), rpc_server);

		let data_rep_param = TableReplicationParams {
			replication_factor: system.config.data_replication_factor,
			write_quorum: (system.config.data_replication_factor + 1) / 2,
			read_quorum: 1,
			timeout: DEFAULT_TIMEOUT,
		};

		let meta_rep_param = TableReplicationParams {
			replication_factor: system.config.meta_replication_factor,
			write_quorum: (system.config.meta_replication_factor + 1) / 2,
			read_quorum: (system.config.meta_replication_factor + 1) / 2,
			timeout: DEFAULT_TIMEOUT,
		};

		println!("Initialize block_ref_table...");
		let block_ref_table = Table::new(
			BlockRefTable {
				background: background.clone(),
				block_manager: block_manager.clone(),
			},
			system.clone(),
			&db,
			"block_ref".to_string(),
			data_rep_param.clone(),
			rpc_server,
		)
		.await;

		println!("Initialize version_table...");
		let version_table = Table::new(
			VersionTable {
				background: background.clone(),
				block_ref_table: block_ref_table.clone(),
			},
			system.clone(),
			&db,
			"version".to_string(),
			meta_rep_param.clone(),
			rpc_server,
		)
		.await;

		println!("Initialize object_table...");
		let object_table = Table::new(
			ObjectTable {
				background: background.clone(),
				version_table: version_table.clone(),
			},
			system.clone(),
			&db,
			"object".to_string(),
			meta_rep_param.clone(),
			rpc_server,
		)
		.await;

		println!("Initialize Garage...");
		let garage = Arc::new(Self {
			db,
			system: system.clone(),
			block_manager,
			background,
			object_table,
			version_table,
			block_ref_table,
		});

		println!("Start block manager background thread...");
		garage.block_manager.garage.swap(Some(garage.clone()));
		garage.block_manager.clone().spawn_background_worker().await;

		garage
	}
}

fn default_block_size() -> usize {
	1048576
}
fn default_replication_factor() -> usize {
	3
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
	println!("Received CTRL+C, shutting down.");
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
	println!("Loading configuration...");
	let config = read_config(config_file).expect("Unable to read config file");

	let id = gen_node_id(&config.metadata_dir).expect("Unable to read or generate node ID");
	println!("Node ID: {}", hex::encode(&id));

	println!("Opening database...");
	let mut db_path = config.metadata_dir.clone();
	db_path.push("db");
	let db = sled::open(db_path).expect("Unable to open DB");

	println!("Initialize RPC server...");
	let rpc_bind_addr = ([0, 0, 0, 0, 0, 0, 0, 0], config.rpc_port).into();
	let mut rpc_server = RpcServer::new(rpc_bind_addr, config.rpc_tls.clone());

	println!("Initializing background runner...");
	let (send_cancel, watch_cancel) = watch::channel(false);
	let background = BackgroundRunner::new(8, watch_cancel.clone());

	let garage = Garage::new(config, id, db, background.clone(), &mut rpc_server).await;

	println!("Initializing RPC and API servers...");
	let run_rpc_server = Arc::new(rpc_server).run(wait_from(watch_cancel.clone()));
	let api_server = api_server::run_api_server(garage.clone(), wait_from(watch_cancel.clone()));

	futures::try_join!(
		garage.system.clone().bootstrap().map(Ok),
		run_rpc_server,
		api_server,
		background.run().map(Ok),
		shutdown_signal(send_cancel),
	)?;
	Ok(())
}
