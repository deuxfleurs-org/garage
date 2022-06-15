use std::sync::Arc;

use netapp::NetworkKey;

use garage_db as db;

use garage_util::background::*;
use garage_util::config::*;
use garage_util::error::Error;

use garage_rpc::system::System;

use garage_block::manager::*;
use garage_table::replication::ReplicationMode;
use garage_table::replication::TableFullReplication;
use garage_table::replication::TableShardedReplication;
use garage_table::*;

use crate::s3::block_ref_table::*;
use crate::s3::object_table::*;
use crate::s3::version_table::*;

use crate::bucket_alias_table::*;
use crate::bucket_table::*;
use crate::helper;
use crate::index_counter::*;
use crate::key_table::*;

#[cfg(feature = "k2v")]
use crate::k2v::{item_table::*, poll::*, rpc::*};

/// An entire Garage full of data
pub struct Garage {
	/// The parsed configuration Garage is running
	pub config: Config,

	/// The local database
	pub db: db::Db,
	/// A background job runner
	pub background: Arc<BackgroundRunner>,
	/// The membership manager
	pub system: Arc<System>,
	/// The block manager
	pub block_manager: Arc<BlockManager>,

	/// Table containing buckets
	pub bucket_table: Arc<Table<BucketTable, TableFullReplication>>,
	/// Table containing bucket aliases
	pub bucket_alias_table: Arc<Table<BucketAliasTable, TableFullReplication>>,
	/// Table containing api keys
	pub key_table: Arc<Table<KeyTable, TableFullReplication>>,

	/// Table containing S3 objects
	pub object_table: Arc<Table<ObjectTable, TableShardedReplication>>,
	/// Counting table containing object counters
	pub object_counter_table: Arc<IndexCounter<Object>>,
	/// Table containing S3 object versions
	pub version_table: Arc<Table<VersionTable, TableShardedReplication>>,
	/// Table containing S3 block references (not blocks themselves)
	pub block_ref_table: Arc<Table<BlockRefTable, TableShardedReplication>>,

	#[cfg(feature = "k2v")]
	pub k2v: GarageK2V,
}

#[cfg(feature = "k2v")]
pub struct GarageK2V {
	/// Table containing K2V items
	pub item_table: Arc<Table<K2VItemTable, TableShardedReplication>>,
	/// Indexing table containing K2V item counters
	pub counter_table: Arc<IndexCounter<K2VItem>>,
	/// K2V RPC handler
	pub rpc: Arc<K2VRpcHandler>,
}

impl Garage {
	/// Create and run garage
	pub fn new(config: Config, background: Arc<BackgroundRunner>) -> Result<Arc<Self>, Error> {
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
				let map_size = garage_db::lmdb_adapter::recommended_map_size();

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

		let network_key = NetworkKey::from_slice(
			&hex::decode(&config.rpc_secret).expect("Invalid RPC secret key")[..],
		)
		.expect("Invalid RPC secret key");

		let replication_mode = ReplicationMode::parse(&config.replication_mode)
			.expect("Invalid replication_mode in config file.");

		info!("Initialize membership management system...");
		let system = System::new(
			network_key,
			background.clone(),
			replication_mode.replication_factor(),
			&config,
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
		let block_manager = BlockManager::new(
			&db,
			config.data_dir.clone(),
			config.compression_level,
			config.block_manager_background_tranquility,
			data_rep_param,
			system.clone(),
		);

		// ---- admin tables ----
		info!("Initialize bucket_table...");
		let bucket_table = Table::new(BucketTable, control_rep_param.clone(), system.clone(), &db);

		info!("Initialize bucket_alias_table...");
		let bucket_alias_table = Table::new(
			BucketAliasTable,
			control_rep_param.clone(),
			system.clone(),
			&db,
		);
		info!("Initialize key_table_table...");
		let key_table = Table::new(KeyTable, control_rep_param, system.clone(), &db);

		// ---- S3 tables ----
		info!("Initialize block_ref_table...");
		let block_ref_table = Table::new(
			BlockRefTable {
				block_manager: block_manager.clone(),
			},
			meta_rep_param.clone(),
			system.clone(),
			&db,
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
		);

		info!("Initialize object counter table...");
		let object_counter_table = IndexCounter::new(system.clone(), meta_rep_param.clone(), &db);

		info!("Initialize object_table...");
		#[allow(clippy::redundant_clone)]
		let object_table = Table::new(
			ObjectTable {
				background: background.clone(),
				version_table: version_table.clone(),
				object_counter_table: object_counter_table.clone(),
			},
			meta_rep_param.clone(),
			system.clone(),
			&db,
		);

		// ---- K2V ----
		#[cfg(feature = "k2v")]
		let k2v = GarageK2V::new(system.clone(), &db, meta_rep_param);

		// -- done --
		Ok(Arc::new(Self {
			config,
			db,
			background,
			system,
			block_manager,
			bucket_table,
			bucket_alias_table,
			key_table,
			object_table,
			object_counter_table,
			version_table,
			block_ref_table,
			#[cfg(feature = "k2v")]
			k2v,
		}))
	}

	pub fn bucket_helper(&self) -> helper::bucket::BucketHelper {
		helper::bucket::BucketHelper(self)
	}

	pub fn key_helper(&self) -> helper::key::KeyHelper {
		helper::key::KeyHelper(self)
	}
}

#[cfg(feature = "k2v")]
impl GarageK2V {
	fn new(system: Arc<System>, db: &db::Db, meta_rep_param: TableShardedReplication) -> Self {
		info!("Initialize K2V counter table...");
		let counter_table = IndexCounter::new(system.clone(), meta_rep_param.clone(), db);
		info!("Initialize K2V subscription manager...");
		let subscriptions = Arc::new(SubscriptionManager::new());
		info!("Initialize K2V item table...");
		let item_table = Table::new(
			K2VItemTable {
				counter_table: counter_table.clone(),
				subscriptions: subscriptions.clone(),
			},
			meta_rep_param,
			system.clone(),
			db,
		);
		let rpc = K2VRpcHandler::new(system, item_table.clone(), subscriptions);

		Self {
			item_table,
			counter_table,
			rpc,
		}
	}
}
