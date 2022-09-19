use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use futures::Stream;
use futures_util::stream::StreamExt;
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, Mutex, MutexGuard};

use opentelemetry::{
	trace::{FutureExt as OtelFutureExt, TraceContextExt, Tracer},
	Context,
};

use garage_rpc::rpc_helper::netapp::stream::{stream_asyncread, ByteStream};

use garage_db as db;

use garage_util::data::*;
use garage_util::error::*;
use garage_util::metrics::RecordDuration;

use garage_rpc::rpc_helper::OrderTag;
use garage_rpc::system::System;
use garage_rpc::*;

use garage_table::replication::{TableReplication, TableShardedReplication};

use crate::block::*;
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
	/// Directory in which block are stored
	pub data_dir: PathBuf,

	compression_level: Option<i32>,

	mutation_lock: [Mutex<BlockManagerLocked>; 256],

	pub(crate) rc: BlockRc,
	pub resync: BlockResyncManager,

	pub(crate) system: Arc<System>,
	pub(crate) endpoint: Arc<Endpoint<BlockRpc, Self>>,

	pub(crate) metrics: BlockManagerMetrics,

	tx_scrub_command: mpsc::Sender<ScrubWorkerCommand>,
}

// This custom struct contains functions that must only be ran
// when the lock is held. We ensure that it is the case by storing
// it INSIDE a Mutex.
struct BlockManagerLocked();

impl BlockManager {
	pub fn new(
		db: &db::Db,
		data_dir: PathBuf,
		compression_level: Option<i32>,
		replication: TableShardedReplication,
		system: Arc<System>,
	) -> Arc<Self> {
		let rc = db
			.open_tree("block_local_rc")
			.expect("Unable to open block_local_rc tree");
		let rc = BlockRc::new(rc);

		let resync = BlockResyncManager::new(db, &system);

		let endpoint = system
			.netapp
			.endpoint("garage_block/manager.rs/Rpc".to_string());

		let metrics = BlockManagerMetrics::new(resync.queue.clone(), resync.errors.clone());

		let (scrub_tx, scrub_rx) = mpsc::channel(1);

		let block_manager = Arc::new(Self {
			replication,
			data_dir,
			compression_level,
			mutation_lock: [(); 256].map(|_| Mutex::new(BlockManagerLocked())),
			rc,
			resync,
			system,
			endpoint,
			metrics,
			tx_scrub_command: scrub_tx,
		});
		block_manager.endpoint.set_handler(block_manager.clone());

		// Spawn a bunch of resync workers
		for index in 0..MAX_RESYNC_WORKERS {
			let worker = ResyncWorker::new(index, block_manager.clone());
			block_manager.system.background.spawn_worker(worker);
		}

		// Spawn scrub worker
		let scrub_worker = ScrubWorker::new(block_manager.clone(), scrub_rx);
		block_manager.system.background.spawn_worker(scrub_worker);

		block_manager
	}

	/// Ask nodes that might have a (possibly compressed) block for it
	/// Return it as a stream with a header
	async fn rpc_get_raw_block_streaming(
		&self,
		hash: &Hash,
		order_tag: Option<OrderTag>,
	) -> Result<(DataBlockHeader, ByteStream), Error> {
		let who = self.replication.read_nodes(hash);
		let who = self.system.rpc.request_order(&who);

		for node in who.iter() {
			let node_id = NodeID::from(*node);
			let rpc = self.endpoint.call_streaming(
				&node_id,
				BlockRpc::GetBlock(*hash, order_tag),
				PRIO_NORMAL | PRIO_SECONDARY,
			);
			tokio::select! {
				res = rpc => {
					let res = match res {
						Ok(res) => res,
						Err(e) => {
							debug!("Node {:?} returned error: {}", node, e);
							continue;
						}
					};
					let (header, stream) = match res.into_parts() {
						(Ok(BlockRpc::PutBlock { hash: _, header }), Some(stream)) => (header, stream),
						_ => {
							debug!("Node {:?} returned a malformed response", node);
							continue;
						}
					};
					return Ok((header, stream));
				}
				_ = tokio::time::sleep(self.system.rpc.rpc_timeout()) => {
					debug!("Node {:?} didn't return block in time, trying next.", node);
				}
			};
		}

		Err(Error::Message(format!(
			"Unable to read block {:?}: no node returned a valid block",
			hash
		)))
	}

	/// Ask nodes that might have a (possibly compressed) block for it
	/// Return its entire body
	pub(crate) async fn rpc_get_raw_block(
		&self,
		hash: &Hash,
		order_tag: Option<OrderTag>,
	) -> Result<DataBlock, Error> {
		let who = self.replication.read_nodes(hash);
		let who = self.system.rpc.request_order(&who);

		for node in who.iter() {
			let node_id = NodeID::from(*node);
			let rpc = self.endpoint.call_streaming(
				&node_id,
				BlockRpc::GetBlock(*hash, order_tag),
				PRIO_NORMAL | PRIO_SECONDARY,
			);
			tokio::select! {
				res = rpc => {
					let res = match res {
						Ok(res) => res,
						Err(e) => {
							debug!("Node {:?} returned error: {}", node, e);
							continue;
						}
					};
					let (header, stream) = match res.into_parts() {
						(Ok(BlockRpc::PutBlock { hash: _, header }), Some(stream)) => (header, stream),
						_ => {
							debug!("Node {:?} returned a malformed response", node);
							continue;
						}
					};
					match read_stream_to_end(stream).await {
						Ok(bytes) => return Ok(DataBlock::from_parts(header, bytes)),
						Err(e) => {
							debug!("Error reading stream from node {:?}: {}", node, e);
						}
					}
				}
				_ = tokio::time::sleep(self.system.rpc.rpc_timeout()) => {
					debug!("Node {:?} didn't return block in time, trying next.", node);
				}
			};
		}

		Err(Error::Message(format!(
			"Unable to read block {:?}: no node returned a valid block",
			hash
		)))
	}

	// ---- Public interface ----

	/// Ask nodes that might have a block for it,
	/// return it as a stream
	pub async fn rpc_get_block_streaming(
		&self,
		hash: &Hash,
		order_tag: Option<OrderTag>,
	) -> Result<
		Pin<Box<dyn Stream<Item = Result<Bytes, std::io::Error>> + Send + Sync + 'static>>,
		Error,
	> {
		let (header, stream) = self.rpc_get_raw_block_streaming(hash, order_tag).await?;
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

	/// Ask nodes that might have a block for it
	pub async fn rpc_get_block(
		&self,
		hash: &Hash,
		order_tag: Option<OrderTag>,
	) -> Result<Bytes, Error> {
		self.rpc_get_raw_block(hash, order_tag)
			.await?
			.verify_get(*hash)
	}

	/// Send block to nodes that should have it
	pub async fn rpc_put_block(&self, hash: Hash, data: Bytes) -> Result<(), Error> {
		let who = self.replication.write_nodes(&hash);

		let (header, bytes) = DataBlock::from_buffer(data, self.compression_level)
			.await
			.into_parts();
		let put_block_rpc =
			Req::new(BlockRpc::PutBlock { hash, header })?.with_stream_from_buffer(bytes);

		self.system
			.rpc
			.try_call_many(
				&self.endpoint,
				&who[..],
				put_block_rpc,
				RequestStrategy::with_priority(PRIO_NORMAL | PRIO_SECONDARY)
					.with_quorum(self.replication.write_quorum()),
			)
			.await?;

		Ok(())
	}

	/// Get number of items in the refcount table
	pub fn rc_len(&self) -> Result<usize, Error> {
		Ok(self.rc.rc.len()?)
	}

	/// Send command to start/stop/manager scrub worker
	pub async fn send_scrub_command(&self, cmd: ScrubWorkerCommand) {
		let _ = self.tx_scrub_command.send(cmd).await;
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
					.put_to_resync(&hash, 2 * this.system.rpc.rpc_timeout())
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
		let bytes = read_stream_to_end(stream).await?;
		let data = DataBlock::from_parts(header, bytes);
		self.write_block(&hash, &data).await
	}

	/// Write a block to disk
	pub(crate) async fn write_block(&self, hash: &Hash, data: &DataBlock) -> Result<(), Error> {
		let tracer = opentelemetry::global::tracer("garage");

		let write_size = data.inner_buffer().len() as u64;

		self.lock_mutate(hash)
			.await
			.write_block(hash, data, self)
			.bound_record_duration(&self.metrics.block_write_duration)
			.with_context(Context::current_with_span(
				tracer.start("BlockManagerLocked::write_block"),
			))
			.await?;

		self.metrics.bytes_written.add(write_size);

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
		let data = self
			.read_block_internal(hash)
			.bound_record_duration(&self.metrics.block_read_duration)
			.await?;

		self.metrics
			.bytes_read
			.add(data.inner_buffer().len() as u64);

		Ok(data)
	}

	async fn read_block_internal(&self, hash: &Hash) -> Result<DataBlock, Error> {
		let mut path = self.block_path(hash);
		let compressed = match self.is_block_compressed(hash).await {
			Ok(c) => c,
			Err(e) => {
				// Not found but maybe we should have had it ??
				self.resync
					.put_to_resync(hash, 2 * self.system.rpc.rpc_timeout())?;
				return Err(Into::into(e));
			}
		};
		if compressed {
			path.set_extension("zst");
		}
		let mut f = fs::File::open(&path).await?;

		let mut data = vec![];
		f.read_to_end(&mut data).await?;
		drop(f);

		let data = if compressed {
			DataBlock::Compressed(data.into())
		} else {
			DataBlock::Plain(data.into())
		};

		if data.verify(*hash).is_err() {
			self.metrics.corruption_counter.add(1);

			self.lock_mutate(hash)
				.await
				.move_block_to_corrupted(hash, self)
				.await?;
			self.resync.put_to_resync(hash, Duration::from_millis(0))?;
			return Err(Error::CorruptData(*hash));
		}

		Ok(data)
	}

	/// Check if this node has a block and whether it needs it
	pub(crate) async fn check_block_status(&self, hash: &Hash) -> Result<BlockStatus, Error> {
		self.lock_mutate(hash)
			.await
			.check_block_status(hash, self)
			.await
	}

	/// Check if this node should have a block, but don't actually have it
	async fn need_block(&self, hash: &Hash) -> Result<bool, Error> {
		let BlockStatus { exists, needed } = self.check_block_status(hash).await?;
		Ok(needed.is_nonzero() && !exists)
	}

	/// Delete block if it is not needed anymore
	pub(crate) async fn delete_if_unneeded(&self, hash: &Hash) -> Result<(), Error> {
		self.lock_mutate(hash)
			.await
			.delete_if_unneeded(hash, self)
			.await
	}

	/// Utility: gives the path of the directory in which a block should be found
	fn block_dir(&self, hash: &Hash) -> PathBuf {
		let mut path = self.data_dir.clone();
		path.push(hex::encode(&hash.as_slice()[0..1]));
		path.push(hex::encode(&hash.as_slice()[1..2]));
		path
	}

	/// Utility: give the full path where a block should be found, minus extension if block is
	/// compressed
	fn block_path(&self, hash: &Hash) -> PathBuf {
		let mut path = self.block_dir(hash);
		path.push(hex::encode(hash.as_ref()));
		path
	}

	/// Utility: check if block is stored compressed. Error if block is not stored
	async fn is_block_compressed(&self, hash: &Hash) -> Result<bool, Error> {
		let mut path = self.block_path(hash);
		path.set_extension("zst");
		if fs::metadata(&path).await.is_ok() {
			return Ok(true);
		}
		path.set_extension("");
		fs::metadata(&path).await.map(|_| false).map_err(Into::into)
	}

	async fn lock_mutate(&self, hash: &Hash) -> MutexGuard<'_, BlockManagerLocked> {
		let tracer = opentelemetry::global::tracer("garage");
		self.mutation_lock[hash.as_slice()[0] as usize]
			.lock()
			.with_context(Context::current_with_span(
				tracer.start("Acquire mutation_lock"),
			))
			.await
	}
}

#[async_trait]
impl StreamingEndpointHandler<BlockRpc> for BlockManager {
	async fn handle(self: &Arc<Self>, mut message: Req<BlockRpc>, _from: NodeID) -> Resp<BlockRpc> {
		match message.msg() {
			BlockRpc::PutBlock { hash, header } => Resp::new(
				self.handle_put_block(*hash, *header, message.take_stream())
					.await
					.map(|_| BlockRpc::Ok),
			),
			BlockRpc::GetBlock(h, order_tag) => self.handle_get_block(h, *order_tag).await,
			BlockRpc::NeedBlockQuery(h) => {
				Resp::new(self.need_block(h).await.map(BlockRpc::NeedBlockReply))
			}
			m => Resp::new(Err(Error::unexpected_rpc_message(m))),
		}
	}
}

pub(crate) struct BlockStatus {
	pub(crate) exists: bool,
	pub(crate) needed: RcEntry,
}

impl BlockManagerLocked {
	async fn check_block_status(
		&self,
		hash: &Hash,
		mgr: &BlockManager,
	) -> Result<BlockStatus, Error> {
		let exists = mgr.is_block_compressed(hash).await.is_ok();
		let needed = mgr.rc.get_block_rc(hash)?;

		Ok(BlockStatus { exists, needed })
	}

	async fn write_block(
		&self,
		hash: &Hash,
		data: &DataBlock,
		mgr: &BlockManager,
	) -> Result<(), Error> {
		let compressed = data.is_compressed();
		let data = data.inner_buffer();

		let mut path = mgr.block_dir(hash);
		let directory = path.clone();
		path.push(hex::encode(hash));

		fs::create_dir_all(&directory).await?;

		let to_delete = match (mgr.is_block_compressed(hash).await, compressed) {
			(Ok(true), _) => return Ok(()),
			(Ok(false), false) => return Ok(()),
			(Ok(false), true) => {
				let path_to_delete = path.clone();
				path.set_extension("zst");
				Some(path_to_delete)
			}
			(Err(_), compressed) => {
				if compressed {
					path.set_extension("zst");
				}
				None
			}
		};

		let mut path2 = path.clone();
		path2.set_extension("tmp");
		let mut f = fs::File::create(&path2).await?;
		f.write_all(data).await?;
		f.sync_all().await?;
		drop(f);

		fs::rename(path2, path).await?;
		if let Some(to_delete) = to_delete {
			fs::remove_file(to_delete).await?;
		}

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

		Ok(())
	}

	async fn move_block_to_corrupted(&self, hash: &Hash, mgr: &BlockManager) -> Result<(), Error> {
		warn!(
			"Block {:?} is corrupted. Renaming to .corrupted and resyncing.",
			hash
		);
		let mut path = mgr.block_path(hash);
		let mut path2 = path.clone();
		if mgr.is_block_compressed(hash).await? {
			path.set_extension("zst");
			path2.set_extension("zst.corrupted");
		} else {
			path2.set_extension("corrupted");
		}
		fs::rename(path, path2).await?;
		Ok(())
	}

	async fn delete_if_unneeded(&self, hash: &Hash, mgr: &BlockManager) -> Result<(), Error> {
		let BlockStatus { exists, needed } = self.check_block_status(hash, mgr).await?;

		if exists && needed.is_deletable() {
			let mut path = mgr.block_path(hash);
			if mgr.is_block_compressed(hash).await? {
				path.set_extension("zst");
			}
			fs::remove_file(path).await?;
			mgr.metrics.delete_counter.add(1);
		}
		Ok(())
	}
}

async fn read_stream_to_end(mut stream: ByteStream) -> Result<Bytes, Error> {
	let mut parts: Vec<Bytes> = vec![];
	while let Some(part) = stream.next().await {
		parts.push(part.ok_or_message("error in stream")?);
	}

	Ok(parts
		.iter()
		.map(|x| &x[..])
		.collect::<Vec<_>>()
		.concat()
		.into())
}
