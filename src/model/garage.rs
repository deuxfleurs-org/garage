use std::sync::Arc;

use netapp::NetworkKey;

use garage_util::background::*;
use garage_util::config::*;

use garage_rpc::system::System;

use garage_table::replication::ReplicationMode;
use garage_table::replication::TableFullReplication;
use garage_table::replication::TableShardedReplication;
use garage_table::*;

use crate::block::*;
use crate::block_ref_table::*;
use crate::bucket_table::*;
use crate::key_table::*;
use crate::object_table::*;
use crate::version_table::*;

/// An entire Garage full of data
pub struct Garage {
	/// The parsed configuration Garage is running
	pub config: Config,

	/// The local database
	pub db: sled::Db,
	/// A background job runner
	pub background: Arc<BackgroundRunner>,
	/// The membership manager
	pub system: Arc<System>,
	/// The block manager
	pub block_manager: Arc<BlockManager>,

	/// Table containing informations about buckets
	pub bucket_table: Arc<Table<BucketTable, TableFullReplication>>,
	/// Table containing informations about api keys
	pub key_table: Arc<Table<KeyTable, TableFullReplication>>,

	pub object_table: Arc<Table<ObjectTable, TableShardedReplication>>,
	pub version_table: Arc<Table<VersionTable, TableShardedReplication>>,
	pub block_ref_table: Arc<Table<BlockRefTable, TableShardedReplication>>,
}

impl Garage {
	/// Create and run garage
	pub fn new(config: Config, db: sled::Db, background: Arc<BackgroundRunner>) -> Arc<Self> {
		let network_key = NetworkKey::from_slice(
			&hex::decode(&config.rpc_secret).expect("Invalid RPC secret key")[..],
		)
		.expect("Invalid RPC secret key");

		let replication_mode = ReplicationMode::parse(&config.replication_mode)
			.expect("Invalid replication_mode in config file.");

		info!("Initialize membership management system...");
		let system = System::new(
			network_key,
			config.metadata_dir.clone(),
			background.clone(),
			replication_mode.replication_factor(),
			config.rpc_bind_addr,
			config.rpc_public_addr,
			config.bootstrap_peers.clone(),
			config.consul_host.clone(),
			config.consul_service_name.clone(),
		);

		let data_rep_param = TableShardedReplication {
			system: system.clone(),
			replication_factor: replication_mode.replication_factor(),
			write_quorum: replication_mode.write_quorum(),
			read_quorum: 1,
		};

		let meta_rep_param = TableShardedReplication {
			system: system.clone(),
			replication_factor: replication_mode.replication_factor(),
			write_quorum: replication_mode.write_quorum(),
			read_quorum: replication_mode.read_quorum(),
		};

		let control_rep_param = TableFullReplication {
			system: system.clone(),
			max_faults: replication_mode.control_write_max_faults(),
		};

		info!("Initialize block manager...");
		let block_manager =
			BlockManager::new(&db, config.data_dir.clone(), data_rep_param, system.clone());

		info!("Initialize block_ref_table...");
		let block_ref_table = Table::new(
			BlockRefTable {
				block_manager: block_manager.clone(),
			},
			meta_rep_param.clone(),
			system.clone(),
			&db,
			"block_ref".to_string(),
		);

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
		);

		info!("Initialize object_table...");
		let object_table = Table::new(
			ObjectTable {
				background: background.clone(),
				version_table: version_table.clone(),
			},
			meta_rep_param,
			system.clone(),
			&db,
			"object".to_string(),
		);

		info!("Initialize bucket_table...");
		let bucket_table = Table::new(
			BucketTable,
			control_rep_param.clone(),
			system.clone(),
			&db,
			"bucket".to_string(),
		);

		info!("Initialize key_table_table...");
		let key_table = Table::new(
			KeyTable,
			control_rep_param,
			system.clone(),
			&db,
			"key".to_string(),
		);

		info!("Initialize Garage...");
		let garage = Arc::new(Self {
			config,
			db,
			background,
			system,
			block_manager,
			bucket_table,
			key_table,
			object_table,
			version_table,
			block_ref_table,
		});

		info!("Start block manager background thread...");
		garage.block_manager.garage.swap(Some(garage.clone()));
		garage.block_manager.clone().spawn_background_worker();

		garage
	}

	/// Use this for shutdown
	pub fn break_reference_cycles(&self) {
		self.block_manager.garage.swap(None);
	}
}
