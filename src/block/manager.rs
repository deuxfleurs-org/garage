use std::convert::TryInto;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::{ArcSwap, ArcSwapOption};
use bytes::Bytes;
use rand::prelude::*;
use serde::{Deserialize, Serialize};

use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, Mutex, MutexGuard, Semaphore};

use opentelemetry::{
	trace::{FutureExt as OtelFutureExt, TraceContextExt, Tracer},
	Context,
};

use garage_net::stream::{read_stream_to_end, stream_asyncread, ByteStream};

use garage_db as db;

use garage_util::background::{vars, BackgroundRunner};
use garage_util::config::Config;
use garage_util::data::*;
use garage_util::error::*;
use garage_util::metrics::RecordDuration;
use garage_util::persister::{Persister, PersisterShared};
use garage_util::time::msec_to_rfc3339;

use garage_rpc::rpc_helper::OrderTag;
use garage_rpc::system::System;
use garage_rpc::*;

use garage_table::replication::{TableReplication, TableShardedReplication};

use crate::block::*;
use crate::layout::*;
use crate::metrics::*;
use crate::rc::*;
use crate::repair::*;
use crate::resync::*;

/// Size under which data will be stored inlined in database instead of as files
pub const INLINE_THRESHOLD: usize = 3072;

// The delay between the moment when the reference counter
// drops to zero, and the moment where we allow ourselves
// to delete the block locally.
pub(crate) const BLOCK_GC_DELAY: Duration = Duration::from_secs(600);

/// RPC messages used to share blocks of data between nodes
#[derive(Debug, Serialize, Deserialize)]
pub enum BlockRpc {
	Ok,
	/// Message to ask for a block of data, by hash
	GetBlock(Hash, Option<OrderTag>),
	/// Message to send a block of data, either because requested, of for first delivery of new
	/// block
	PutBlock {
		hash: Hash,
		header: DataBlockHeader,
	},
	/// Ask other node if they should have this block, but don't actually have it
	NeedBlockQuery(Hash),
	/// Response : whether the node do require that block
	NeedBlockReply(bool),
}

impl Rpc for BlockRpc {
	type Response = Result<BlockRpc, Error>;
}

/// The block manager, handling block exchange between nodes, and block storage on local node
pub struct BlockManager {
	/// Replication strategy, allowing to find on which node blocks should be located
	pub replication: TableShardedReplication,

	/// Data layout
	pub(crate) data_layout: ArcSwap<DataLayout>,
	/// Data layout persister
	pub(crate) data_layout_persister: Persister<DataLayout>,

	data_fsync: bool,
	compression_level: Option<i32>,
	disable_scrub: bool,

	mutation_lock: Vec<Mutex<BlockManagerLocked>>,

	pub rc: BlockRc,
	pub resync: BlockResyncManager,

	pub(crate) system: Arc<System>,
	pub(crate) endpoint: Arc<Endpoint<BlockRpc, Self>>,
	buffer_kb_semaphore: Arc<Semaphore>,

	pub(crate) metrics: BlockManagerMetrics,

	pub scrub_persister: PersisterShared<ScrubWorkerPersisted>,
	tx_scrub_command: ArcSwapOption<mpsc::Sender<ScrubWorkerCommand>>,
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct BlockResyncErrorInfo {
	pub hash: Hash,
	pub refcount: u64,
	pub error_count: u64,
	pub last_try: u64,
	pub next_try: u64,
}

// The number of different mutexes used to parallelize write access to data blocks
const MUTEX_COUNT: usize = 256;

// This custom struct contains functions that must only be ran
// when the lock is held. We ensure that it is the case by storing
// it INSIDE a Mutex.
struct BlockManagerLocked();

impl BlockManager {
	pub fn new(
		db: &db::Db,
		config: &Config,
		replication: TableShardedReplication,
		system: Arc<System>,
	) -> Result<Arc<Self>, Error> {
		// Load or compute layout, i.e. assignment of data blocks to the different data directories
		let data_layout_persister: Persister<DataLayout> =
			Persister::new(&system.metadata_dir, "data_layout");
		let mut data_layout = match data_layout_persister.load() {
			Ok(layout) => layout
				.update(&config.data_dir)
				.ok_or_message("invalid data_dir config")?,
			Err(_) => {
				DataLayout::initialize(&config.data_dir).ok_or_message("invalid data_dir config")?
			}
		};
		data_layout.check_markers()?;
		data_layout_persister
			.save(&data_layout)
			.expect("cannot save data_layout");

		// Open metadata tables
		let rc = db
			.open_tree("block_local_rc")
			.expect("Unable to open block_local_rc tree");
		let rc = BlockRc::new(rc);

		let resync = BlockResyncManager::new(db, &system);

		let endpoint = system
			.netapp
			.endpoint("garage_block/manager.rs/Rpc".to_string());

		let buffer_kb_semaphore = Arc::new(Semaphore::new(config.block_ram_buffer_max / 1024));

		let metrics = BlockManagerMetrics::new(
			config.compression_level,
			rc.rc_table.clone(),
			resync.queue.clone(),
			resync.errors.clone(),
			buffer_kb_semaphore.clone(),
		);

		let scrub_persister = PersisterShared::new(&system.metadata_dir, "scrub_info");

		let block_manager = Arc::new(Self {
			replication,
			data_layout: ArcSwap::new(Arc::new(data_layout)),
			data_layout_persister,
			data_fsync: config.data_fsync,
			disable_scrub: config.disable_scrub,
			compression_level: config.compression_level,
			mutation_lock: vec![(); MUTEX_COUNT]
				.iter()
				.map(|_| Mutex::new(BlockManagerLocked()))
				.collect::<Vec<_>>(),
			rc,
			resync,
			system,
			endpoint,
			buffer_kb_semaphore,
			metrics,
			scrub_persister,
			tx_scrub_command: ArcSwapOption::new(None),
		});
		block_manager.endpoint.set_handler(block_manager.clone());
		block_manager.scrub_persister.set_with(|_| ()).unwrap();

		Ok(block_manager)
	}

	pub fn spawn_workers(self: &Arc<Self>, bg: &BackgroundRunner) {
		// Spawn a bunch of resync workers
		for index in 0..MAX_RESYNC_WORKERS {
			let worker = ResyncWorker::new(index, self.clone());
			bg.spawn_worker(worker);
		}

		// Spawn scrub worker
		if !self.disable_scrub {
			let (scrub_tx, scrub_rx) = mpsc::channel(1);
			self.tx_scrub_command.store(Some(Arc::new(scrub_tx)));
			bg.spawn_worker(ScrubWorker::new(
				self.clone(),
				scrub_rx,
				self.scrub_persister.clone(),
			));
		}
	}

	pub fn register_bg_vars(&self, vars: &mut vars::BgVars) {
		self.resync.register_bg_vars(vars);

		if !self.disable_scrub {
			vars.register_rw(
				&self.scrub_persister,
				"scrub-tranquility",
				|p| p.get_with(|x| x.tranquility),
				|p, tranquility| p.set_with(|x| x.tranquility = tranquility),
			);
			vars.register_ro(&self.scrub_persister, "scrub-last-completed", |p| {
				p.get_with(|x| msec_to_rfc3339(x.time_last_complete_scrub))
			});
			vars.register_ro(&self.scrub_persister, "scrub-next-run", |p| {
				p.get_with(|x| msec_to_rfc3339(x.time_next_run_scrub))
			});
			vars.register_ro(&self.scrub_persister, "scrub-corruptions_detected", |p| {
				p.get_with(|x| x.corruptions_detected)
			});
		}
	}

	/// Initialization: set how block references are recalculated
	/// for repair operations
	pub fn set_recalc_rc(&self, recalc: Vec<CalculateRefcount>) {
		self.rc.recalc_rc.store(Some(Arc::new(recalc)));
	}

	/// Ask nodes that might have a (possibly compressed) block for it
	/// Return it as a stream with a header
	async fn rpc_get_raw_block_streaming(
		&self,
		hash: &Hash,
		priority: RequestPriority,
		order_tag: Option<OrderTag>,
	) -> Result<DataBlockStream, Error> {
		self.rpc_get_raw_block_internal(
			hash,
			priority,
			order_tag,
			|stream| async move { Ok(stream) },
		)
		.await
	}

	/// Ask nodes that might have a (possibly compressed) block for it
	/// Return its entire body
	pub(crate) async fn rpc_get_raw_block(
		&self,
		hash: &Hash,
		priority: RequestPriority,
		order_tag: Option<OrderTag>,
	) -> Result<DataBlock, Error> {
		self.rpc_get_raw_block_internal(hash, priority, order_tag, |block_stream| async move {
			let (header, stream) = block_stream.into_parts();
			read_stream_to_end(stream)
				.await
				.err_context("error in block data stream")
				.map(|data| DataBlock::from_parts(header, data.into_bytes()))
		})
		.await
	}

	async fn rpc_get_raw_block_internal<F, Fut, T>(
		&self,
		hash: &Hash,
		priority: RequestPriority,
		order_tag: Option<OrderTag>,
		f: F,
	) -> Result<T, Error>
	where
		F: Fn(DataBlockStream) -> Fut,
		Fut: futures::Future<Output = Result<T, Error>>,
	{
		let who = self
			.system
			.rpc_helper()
			.block_read_nodes_of(hash, self.system.rpc_helper());

		for node in who.iter() {
			let node_id = NodeID::from(*node);
			let rpc = self.endpoint.call_streaming(
				&node_id,
				BlockRpc::GetBlock(*hash, order_tag),
				priority,
			);
			tokio::select! {
				res = rpc => {
					let res = match res {
						Ok(res) => res,
						Err(e) => {
							debug!("Get block {:?}: node {:?} could not be contacted: {}", hash, node, e);
							continue;
						}
					};
					let block_stream = match res.into_parts() {
						(Ok(BlockRpc::PutBlock { hash: _, header }), Some(stream)) => DataBlockStream::from_parts(header, stream),
						(Ok(_), _) => {
							debug!("Get block {:?}: node {:?} returned a malformed response", hash, node);
							continue;
						}
						(Err(e), _) => {
							debug!("Get block {:?}: node {:?} returned error: {}", hash, node, e);
							continue;
						}
					};
					match f(block_stream).await {
						Ok(ret) => return Ok(ret),
						Err(e) => {
							debug!("Get block {:?}: error reading stream from node {:?}: {}", hash, node, e);
						}
					}
				}
				// TODO: sleep less long (fail early), initiate a second request earlier
				// if the first one doesn't succeed rapidly
				// TODO: keep first request running when initiating a new one and take the
				// one that finishes earlier
				_ = tokio::time::sleep(self.system.rpc_helper().rpc_timeout()) => {
					debug!("Get block {:?}: node {:?} didn't return block in time, trying next.", hash, node);
				}
			};
		}

		let err = Error::MissingBlock(*hash);
		debug!("{}", err);
		Err(err)
	}

	// ---- Public interface ----

	/// Ask nodes that might have a block for it, return it as a stream
	pub async fn rpc_get_block_streaming(
		&self,
		hash: &Hash,
		order_tag: Option<OrderTag>,
	) -> Result<ByteStream, Error> {
		let block_stream = self
			.rpc_get_raw_block_streaming(hash, PRIO_NORMAL | PRIO_SECONDARY, order_tag)
			.await?;
		let (header, stream) = block_stream.into_parts();
		match header {
			DataBlockHeader::Plain => Ok(stream),
			DataBlockHeader::Compressed => {
				// Too many things, I hate it.
				let reader = stream_asyncread(stream);
				let reader = BufReader::new(reader);
				let reader = async_compression::tokio::bufread::ZstdDecoder::new(reader);
				Ok(Box::pin(tokio_util::io::ReaderStream::new(reader)))
			}
		}
	}

	/// Send block to nodes that should have it
	pub async fn rpc_put_block(
		&self,
		hash: Hash,
		data: Bytes,
		prevent_compression: bool,
		order_tag: Option<OrderTag>,
	) -> Result<(), Error> {
		let who = self.system.cluster_layout().current_storage_nodes_of(&hash);

		let compression_level = self.compression_level.filter(|_| !prevent_compression);
		let (header, bytes) = DataBlock::from_buffer(data, compression_level)
			.await
			.into_parts();

		let permit = self
			.buffer_kb_semaphore
			.clone()
			.acquire_many_owned((bytes.len() / 1024).try_into().unwrap())
			.await
			.ok_or_message("could not reserve space for buffer of data to send to remote nodes")?;

		let put_block_rpc =
			Req::new(BlockRpc::PutBlock { hash, header })?.with_stream_from_buffer(bytes);
		let put_block_rpc = if let Some(tag) = order_tag {
			put_block_rpc.with_order_tag(tag)
		} else {
			put_block_rpc
		};

		self.system
			.rpc_helper()
			.try_write_many_sets(
				&self.endpoint,
				&[who],
				put_block_rpc,
				RequestStrategy::with_priority(PRIO_NORMAL | PRIO_SECONDARY)
					.with_drop_on_completion(permit)
					.with_quorum(self.replication.write_quorum()),
			)
			.await?;

		Ok(())
	}

	/// Get number of items in the refcount table
	pub fn rc_len(&self) -> Result<usize, Error> {
		Ok(self.rc.rc_table.len()?)
	}

	/// Send command to start/stop/manager scrub worker
	pub async fn send_scrub_command(&self, cmd: ScrubWorkerCommand) -> Result<(), Error> {
		let tx = self.tx_scrub_command.load();
		let tx = tx.as_ref().ok_or_message("scrub worker is not running")?;
		tx.send(cmd).await.ok_or_message("send error")?;
		Ok(())
	}

	/// Get the reference count of a block
	pub fn get_block_rc(&self, hash: &Hash) -> Result<u64, Error> {
		Ok(self.rc.get_block_rc(hash)?.as_u64())
	}

	/// List all resync errors
	pub fn list_resync_errors(&self) -> Result<Vec<BlockResyncErrorInfo>, Error> {
		let mut blocks = Vec::with_capacity(self.resync.errors.len()?);
		for ent in self.resync.errors.iter()? {
			let (hash, cnt) = ent?;
			let cnt = ErrorCounter::decode(&cnt);
			blocks.push(BlockResyncErrorInfo {
				hash: Hash::try_from(&hash).unwrap(),
				refcount: 0,
				error_count: cnt.errors,
				last_try: cnt.last_try,
				next_try: cnt.next_try(),
			});
		}
		for block in blocks.iter_mut() {
			block.refcount = self.get_block_rc(&block.hash)?;
		}
		Ok(blocks)
	}

	//// ----- Managing the reference counter ----

	/// Increment the number of time a block is used, putting it to resynchronization if it is
	/// required, but not known
	pub fn block_incref(
		self: &Arc<Self>,
		tx: &mut db::Transaction,
		hash: Hash,
	) -> db::TxOpResult<()> {
		if self.rc.block_incref(tx, &hash)? {
			// When the reference counter is incremented, there is
			// normally a node that is responsible for sending us the
			// data of the block. However that operation may fail,
			// so in all cases we add the block here to the todo list
			// to check later that it arrived correctly, and if not
			// we will fecth it from someone.
			let this = self.clone();
			tokio::spawn(async move {
				if let Err(e) = this
					.resync
					.put_to_resync(&hash, 2 * this.system.rpc_helper().rpc_timeout())
				{
					error!("Block {:?} could not be put in resync queue: {}.", hash, e);
				}
			});
		}
		Ok(())
	}

	/// Decrement the number of time a block is used
	pub fn block_decref(
		self: &Arc<Self>,
		tx: &mut db::Transaction,
		hash: Hash,
	) -> db::TxOpResult<()> {
		if self.rc.block_decref(tx, &hash)? {
			// When the RC is decremented, it might drop to zero,
			// indicating that we don't need the block.
			// There is a delay before we garbage collect it;
			// make sure that it is handled in the resync loop
			// after that delay has passed.
			let this = self.clone();
			tokio::spawn(async move {
				if let Err(e) = this
					.resync
					.put_to_resync(&hash, BLOCK_GC_DELAY + Duration::from_secs(10))
				{
					error!("Block {:?} could not be put in resync queue: {}.", hash, e);
				}
			});
		}
		Ok(())
	}

	// ---- Reading and writing blocks locally ----

	async fn handle_put_block(
		&self,
		hash: Hash,
		header: DataBlockHeader,
		stream: Option<ByteStream>,
	) -> Result<(), Error> {
		let stream = stream.ok_or_message("missing stream")?;
		let bytes = read_stream_to_end(stream).await?.into_bytes();
		let data = DataBlock::from_parts(header, bytes);
		self.write_block(&hash, &data).await
	}

	/// Write a block to disk
	pub(crate) async fn write_block(&self, hash: &Hash, data: &DataBlock) -> Result<(), Error> {
		let tracer = opentelemetry::global::tracer("garage");

		self.lock_mutate(hash)
			.await
			.write_block(hash, data, self)
			.bound_record_duration(&self.metrics.block_write_duration)
			.with_context(Context::current_with_span(
				tracer.start("BlockManagerLocked::write_block"),
			))
			.await?;

		Ok(())
	}

	async fn handle_get_block(&self, hash: &Hash, order_tag: Option<OrderTag>) -> Resp<BlockRpc> {
		let block = match self.read_block(hash).await {
			Ok(data) => data,
			Err(e) => return Resp::new(Err(e)),
		};

		let (header, data) = block.into_parts();

		let resp = Resp::new(Ok(BlockRpc::PutBlock {
			hash: *hash,
			header,
		}))
		.with_stream_from_buffer(data);

		if let Some(order_tag) = order_tag {
			resp.with_order_tag(order_tag)
		} else {
			resp
		}
	}

	/// Read block from disk, verifying it's integrity
	pub(crate) async fn read_block(&self, hash: &Hash) -> Result<DataBlock, Error> {
		let tracer = opentelemetry::global::tracer("garage");
		async {
			match self.find_block(hash).await {
				Some(p) => self.read_block_from(hash, &p).await,
				None => {
					// Not found but maybe we should have had it ??
					self.resync
						.put_to_resync(hash, 2 * self.system.rpc_helper().rpc_timeout())?;
					return Err(Error::Message(format!(
						"block {:?} not found on node",
						hash
					)));
				}
			}
		}
		.bound_record_duration(&self.metrics.block_read_duration)
		.with_context(Context::current_with_span(
			tracer.start("BlockManager::read_block"),
		))
		.await
	}

	pub(crate) async fn read_block_from(
		&self,
		hash: &Hash,
		block_path: &DataBlockPath,
	) -> Result<DataBlock, Error> {
		let (header, path) = block_path.as_parts_ref();

		let mut f = fs::File::open(&path).await?;
		let mut data = vec![];
		f.read_to_end(&mut data).await?;
		self.metrics.bytes_read.add(data.len() as u64);
		drop(f);

		let data = DataBlock::from_parts(header, data.into());

		if data.verify(*hash).is_err() {
			self.metrics.corruption_counter.add(1);

			warn!(
				"Block {:?} is corrupted. Renaming to .corrupted and resyncing.",
				hash
			);
			self.lock_mutate(hash)
				.await
				.move_block_to_corrupted(block_path)
				.await?;
			self.resync.put_to_resync(hash, Duration::from_millis(0))?;

			return Err(Error::CorruptData(*hash));
		}

		Ok(data)
	}

	/// Check if this node should have a block, but don't actually have it
	async fn need_block(&self, hash: &Hash) -> Result<bool, Error> {
		let rc = self.rc.get_block_rc(hash)?;
		let exists = self.find_block(hash).await.is_some();
		Ok(rc.is_nonzero() && !exists)
	}

	/// Delete block if it is not needed anymore
	pub(crate) async fn delete_if_unneeded(&self, hash: &Hash) -> Result<(), Error> {
		self.lock_mutate(hash)
			.await
			.delete_if_unneeded(hash, self)
			.await
	}

	/// Find the path where a block is currently stored
	pub(crate) async fn find_block(&self, hash: &Hash) -> Option<DataBlockPath> {
		let data_layout = self.data_layout.load_full();
		let dirs = Some(data_layout.primary_block_dir(hash))
			.into_iter()
			.chain(data_layout.secondary_block_dirs(hash));
		let filename = hex::encode(hash.as_ref());

		for dir in dirs {
			let mut path = dir;
			path.push(&filename);

			if self.compression_level.is_none() {
				// If compression is disabled on node - check for the raw block
				// first and then a compressed one (as compression may have been
				// previously enabled).
				if fs::metadata(&path).await.is_ok() {
					return Some(DataBlockPath::plain(path));
				}
				path.set_extension("zst");
				if fs::metadata(&path).await.is_ok() {
					return Some(DataBlockPath::compressed(path));
				}
			} else {
				path.set_extension("zst");
				if fs::metadata(&path).await.is_ok() {
					return Some(DataBlockPath::compressed(path));
				}
				path.set_extension("");
				if fs::metadata(&path).await.is_ok() {
					return Some(DataBlockPath::plain(path));
				}
			}
		}

		None
	}

	/// Rewrite a block at the primary location for its path and delete the old path.
	/// Returns the number of bytes read/written
	pub(crate) async fn fix_block_location(
		&self,
		hash: &Hash,
		wrong_path: DataBlockPath,
	) -> Result<usize, Error> {
		let data = self.read_block_from(hash, &wrong_path).await?;
		self.lock_mutate(hash)
			.await
			.write_block_inner(hash, &data, self, Some(wrong_path))
			.await?;
		Ok(data.as_parts_ref().1.len())
	}

	async fn lock_mutate(&self, hash: &Hash) -> MutexGuard<'_, BlockManagerLocked> {
		let tracer = opentelemetry::global::tracer("garage");
		let ilock = u16::from_be_bytes([hash.as_slice()[0], hash.as_slice()[1]]) as usize
			% self.mutation_lock.len();
		self.mutation_lock[ilock]
			.lock()
			.with_context(Context::current_with_span(
				tracer.start(format!("Acquire mutation_lock #{}", ilock)),
			))
			.await
	}
}

impl StreamingEndpointHandler<BlockRpc> for BlockManager {
	async fn handle(self: &Arc<Self>, mut message: Req<BlockRpc>, _from: NodeID) -> Resp<BlockRpc> {
		match message.msg() {
			BlockRpc::PutBlock { hash, header } => Resp::new(
				self.handle_put_block(*hash, *header, message.take_stream())
					.await
					.map(|()| BlockRpc::Ok),
			),
			BlockRpc::GetBlock(h, order_tag) => self.handle_get_block(h, *order_tag).await,
			BlockRpc::NeedBlockQuery(h) => {
				Resp::new(self.need_block(h).await.map(BlockRpc::NeedBlockReply))
			}
			m => Resp::new(Err(Error::unexpected_rpc_message(m))),
		}
	}
}

impl BlockManagerLocked {
	async fn write_block(
		&self,
		hash: &Hash,
		data: &DataBlock,
		mgr: &BlockManager,
	) -> Result<(), Error> {
		let existing_path = mgr.find_block(hash).await;
		self.write_block_inner(hash, data, mgr, existing_path).await
	}

	async fn write_block_inner(
		&self,
		hash: &Hash,
		data: &DataBlock,
		mgr: &BlockManager,
		existing_path: Option<DataBlockPath>,
	) -> Result<(), Error> {
		let (header, data) = data.as_parts_ref();
		let compressed = header.is_compressed();

		let directory = mgr.data_layout.load().primary_block_dir(hash);

		let mut tgt_path = directory.clone();
		tgt_path.push(hex::encode(hash));
		if compressed {
			tgt_path.set_extension("zst");
		}

		let existing_info = existing_path.map(|x| x.into_parts());
		let to_delete = match (existing_info, compressed) {
			// If the block is stored in the wrong directory,
			// write it again at the correct path and delete the old path
			(Some((DataBlockHeader::Plain, p)), false) if p != tgt_path => Some(p),
			(Some((DataBlockHeader::Compressed, p)), true) if p != tgt_path => Some(p),

			// If the block is already stored not compressed but we have a compressed
			// copy, write the compressed copy and delete the uncompressed one
			(Some((DataBlockHeader::Plain, plain_path)), true) => Some(plain_path),

			// If the block is already stored compressed,
			// keep the stored copy, we have nothing to do
			(Some((DataBlockHeader::Compressed, _)), _) => return Ok(()),

			// If the block is already stored not compressed,
			// and we don't have a compressed copy either,
			// keep the stored copy, we have nothing to do
			(Some((DataBlockHeader::Plain, _)), false) => return Ok(()),

			// If the block isn't stored already, just store what is given to us
			(None, _) => None,
		};
		assert!(to_delete.as_ref() != Some(&tgt_path));

		let mut path_tmp = tgt_path.clone();
		let tmp_extension = format!("tmp{}", hex::encode(thread_rng().gen::<[u8; 4]>()));
		path_tmp.set_extension(tmp_extension);

		fs::create_dir_all(&directory).await?;

		let mut delete_on_drop = DeleteOnDrop(Some(path_tmp.clone()));

		let mut f = fs::File::create(&path_tmp).await?;
		f.write_all(data).await?;
		mgr.metrics.bytes_written.add(data.len() as u64);

		if mgr.data_fsync {
			f.sync_all().await?;
		}

		drop(f);

		fs::rename(path_tmp, tgt_path).await?;

		delete_on_drop.cancel();

		if let Some(to_delete) = to_delete {
			fs::remove_file(to_delete).await?;
		}

		if mgr.data_fsync {
			// We want to ensure that when this function returns, data is properly persisted
			// to disk. The first step is the sync_all above that does an fsync on the data file.
			// Now, we do an fsync on the containing directory, to ensure that the rename
			// is persisted properly. See:
			// http://thedjbway.b0llix.net/qmail/syncdir.html
			let dir = fs::OpenOptions::new()
				.read(true)
				.mode(0)
				.open(directory)
				.await?;
			dir.sync_all().await?;
			drop(dir);
		}

		Ok(())
	}

	async fn move_block_to_corrupted(&self, block_path: &DataBlockPath) -> Result<(), Error> {
		let (header, path) = block_path.as_parts_ref();

		let mut path2 = path.clone();
		if header.is_compressed() {
			path2.set_extension("zst.corrupted");
		} else {
			path2.set_extension("corrupted");
		}

		fs::rename(path, path2).await?;
		Ok(())
	}

	async fn delete_if_unneeded(&self, hash: &Hash, mgr: &BlockManager) -> Result<(), Error> {
		let rc = mgr.rc.get_block_rc(hash)?;
		if rc.is_deletable() {
			while let Some(path) = mgr.find_block(hash).await {
				let (_header, path) = path.as_parts_ref();
				fs::remove_file(path).await?;
				mgr.metrics.delete_counter.add(1);
			}
		}
		Ok(())
	}
}

struct DeleteOnDrop(Option<PathBuf>);

impl DeleteOnDrop {
	fn cancel(&mut self) {
		drop(self.0.take());
	}
}

impl Drop for DeleteOnDrop {
	fn drop(&mut self) {
		if let Some(path) = self.0.take() {
			tokio::spawn(async move {
				if let Err(e) = fs::remove_file(&path).await {
					debug!("DeleteOnDrop failed for {}: {}", path.display(), e);
				}
			});
		}
	}
}
