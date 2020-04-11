use futures_util::future::FutureExt;
use serde::Deserialize;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::watch;
use tokio::sync::{Mutex, RwLock};

use crate::api_server;
use crate::background::*;
use crate::data::*;
use crate::error::Error;
use crate::membership::System;
use crate::proto::*;
use crate::rpc_server;
use crate::table::*;

#[derive(Deserialize, Debug)]
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
}

pub struct Garage {
	pub db: sled::Db,
	pub system: Arc<System>,
	pub fs_lock: Mutex<()>,
	pub background: Arc<BackgroundRunner>,

	pub table_rpc_handlers: HashMap<String, Box<dyn TableRpcHandler + Sync + Send>>,

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
	) -> Arc<Self> {
		let system = Arc::new(System::new(config, id, background.clone()));

		let meta_rep_param = TableReplicationParams {
			replication_factor: system.config.meta_replication_factor,
			write_quorum: (system.config.meta_replication_factor + 1) / 2,
			read_quorum: (system.config.meta_replication_factor + 1) / 2,
			timeout: DEFAULT_TIMEOUT,
		};

		let object_table = Arc::new(Table::new(
			ObjectTable {
				garage: RwLock::new(None),
			},
			system.clone(),
			&db,
			"object".to_string(),
			meta_rep_param.clone(),
		));
		let version_table = Arc::new(Table::new(
			VersionTable {
				garage: RwLock::new(None),
			},
			system.clone(),
			&db,
			"version".to_string(),
			meta_rep_param.clone(),
		));

		let data_rep_param = TableReplicationParams {
			replication_factor: system.config.data_replication_factor,
			write_quorum: (system.config.data_replication_factor + 1) / 2,
			read_quorum: 1,
			timeout: DEFAULT_TIMEOUT,
		};

		let block_ref_table = Arc::new(Table::new(
			BlockRefTable {
				garage: RwLock::new(None),
			},
			system.clone(),
			&db,
			"block_ref".to_string(),
			data_rep_param.clone(),
		));

		let mut garage = Self {
			db,
			system: system.clone(),
			fs_lock: Mutex::new(()),
			background,
			table_rpc_handlers: HashMap::new(),
			object_table,
			version_table,
			block_ref_table,
		};

		garage.table_rpc_handlers.insert(
			garage.object_table.name.clone(),
			garage.object_table.clone().rpc_handler(),
		);
		garage.table_rpc_handlers.insert(
			garage.version_table.name.clone(),
			garage.version_table.clone().rpc_handler(),
		);
		garage.table_rpc_handlers.insert(
			garage.block_ref_table.name.clone(),
			garage.block_ref_table.clone().rpc_handler(),
		);

		let garage = Arc::new(garage);

		*garage.object_table.instance.garage.write().await = Some(garage.clone());
		*garage.version_table.instance.garage.write().await = Some(garage.clone());
		*garage.block_ref_table.instance.garage.write().await = Some(garage.clone());

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
	let config = read_config(config_file).expect("Unable to read config file");

	let mut db_path = config.metadata_dir.clone();
	db_path.push("db");
	let db = sled::open(db_path).expect("Unable to open DB");

	let id = gen_node_id(&config.metadata_dir).expect("Unable to read or generate node ID");
	println!("Node ID: {}", hex::encode(&id));

	let (send_cancel, watch_cancel) = watch::channel(false);

	let background = BackgroundRunner::new(8, watch_cancel.clone());
	let garage = Garage::new(config, id, db, background.clone()).await;

	let rpc_server = rpc_server::run_rpc_server(garage.clone(), wait_from(watch_cancel.clone()));
	let api_server = api_server::run_api_server(garage.clone(), wait_from(watch_cancel.clone()));

	futures::try_join!(
		garage.system.clone().bootstrap().map(Ok),
		rpc_server,
		api_server,
		background.run().map(Ok),
		shutdown_signal(send_cancel),
	)?;
	Ok(())
}
