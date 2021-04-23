use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwapOption;
use futures::future::*;
use futures::select;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{watch, Mutex, Notify};

use garage_util::data::*;
use garage_util::error::Error;
use garage_util::time::*;

use garage_rpc::membership::System;
use garage_rpc::rpc_client::*;
use garage_rpc::rpc_server::*;

use garage_table::replication::{TableReplication, TableShardedReplication};

use crate::block_ref_table::*;

use crate::garage::Garage;

/// Size under which data will be stored inlined in database instead of as files
pub const INLINE_THRESHOLD: usize = 3072;

pub const BACKGROUND_WORKERS: u64 = 1;

const BLOCK_RW_TIMEOUT: Duration = Duration::from_secs(42);
const BLOCK_GC_TIMEOUT: Duration = Duration::from_secs(60);
const NEED_BLOCK_QUERY_TIMEOUT: Duration = Duration::from_secs(5);
const RESYNC_RETRY_TIMEOUT: Duration = Duration::from_secs(10);

/// RPC messages used to share blocks of data between nodes
#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
	Ok,
	/// Message to ask for a block of data, by hash
	GetBlock(Hash),
	/// Message to send a block of data, either because requested, of for first delivery of new
	/// block
	PutBlock(PutBlockMessage),
	/// Ask other node if they should have this block, but don't actually have it
	NeedBlockQuery(Hash),
	/// Response : whether the node do require that block
	NeedBlockReply(bool),
}

/// Structure used to send a block
#[derive(Debug, Serialize, Deserialize)]
pub struct PutBlockMessage {
	/// Hash of the block
	pub hash: Hash,

	/// Content of the block
	#[serde(with = "serde_bytes")]
	pub data: Vec<u8>,
}

impl RpcMessage for Message {}

/// The block manager, handling block exchange between nodes, and block storage on local node
pub struct BlockManager {
	/// Replication strategy, allowing to find on which node blocks should be located
	pub replication: TableShardedReplication,
	/// Directory in which block are stored
	pub data_dir: PathBuf,
	/// Lock to prevent concurrent edition of the directory
	pub data_dir_lock: Mutex<()>,

	rc: sled::Tree,

	resync_queue: sled::Tree,
	resync_notify: Notify,

	system: Arc<System>,
	rpc_client: Arc<RpcClient<Message>>,
	pub(crate) garage: ArcSwapOption<Garage>,
}

impl BlockManager {
	pub fn new(
		db: &sled::Db,
		data_dir: PathBuf,
		replication: TableShardedReplication,
		system: Arc<System>,
		rpc_server: &mut RpcServer,
	) -> Arc<Self> {
		let rc = db
			.open_tree("block_local_rc")
			.expect("Unable to open block_local_rc tree");

		let resync_queue = db
			.open_tree("block_local_resync_queue")
			.expect("Unable to open block_local_resync_queue tree");

		let rpc_path = "block_manager";
		let rpc_client = system.rpc_client::<Message>(rpc_path);

		let block_manager = Arc::new(Self {
			replication,
			data_dir,
			data_dir_lock: Mutex::new(()),
			rc,
			resync_queue,
			resync_notify: Notify::new(),
			system,
			rpc_client,
			garage: ArcSwapOption::from(None),
		});
		block_manager
			.clone()
			.register_handler(rpc_server, rpc_path.into());
		block_manager
	}

	fn register_handler(self: Arc<Self>, rpc_server: &mut RpcServer, path: String) {
		let self2 = self.clone();
		rpc_server.add_handler::<Message, _, _>(path, move |msg, _addr| {
			let self2 = self2.clone();
			async move { self2.handle(&msg).await }
		});

		let self2 = self.clone();
		self.rpc_client
			.set_local_handler(self.system.id, move |msg| {
				let self2 = self2.clone();
				async move { self2.handle(&msg).await }
			});
	}

	async fn handle(self: Arc<Self>, msg: &Message) -> Result<Message, Error> {
		match msg {
			Message::PutBlock(m) => self.write_block(&m.hash, &m.data).await,
			Message::GetBlock(h) => self.read_block(h).await,
			Message::NeedBlockQuery(h) => self.need_block(h).await.map(Message::NeedBlockReply),
			_ => Err(Error::BadRPC("Unexpected RPC message".to_string())),
		}
	}

	pub fn spawn_background_worker(self: Arc<Self>) {
		// Launch 2 simultaneous workers for background resync loop preprocessing <= TODO actually this
		// launches only one worker with current value of BACKGROUND_WORKERS
		for i in 0..BACKGROUND_WORKERS {
			let bm2 = self.clone();
			let background = self.system.background.clone();
			tokio::spawn(async move {
				tokio::time::sleep(Duration::from_secs(10 * (i + 1))).await;
				background.spawn_worker(format!("block resync worker {}", i), move |must_exit| {
					bm2.resync_loop(must_exit)
				});
			});
		}
	}

	/// Write a block to disk
	async fn write_block(&self, hash: &Hash, data: &[u8]) -> Result<Message, Error> {
		let _lock = self.data_dir_lock.lock().await;

		let mut path = self.block_dir(hash);
		fs::create_dir_all(&path).await?;

		path.push(hex::encode(hash));
		if fs::metadata(&path).await.is_ok() {
			return Ok(Message::Ok);
		}

		let mut f = fs::File::create(path).await?;
		f.write_all(data).await?;
		drop(f);

		Ok(Message::Ok)
	}

	/// Read block from disk, verifying it's integrity
	async fn read_block(&self, hash: &Hash) -> Result<Message, Error> {
		let path = self.block_path(hash);

		let mut f = match fs::File::open(&path).await {
			Ok(f) => f,
			Err(e) => {
				// Not found but maybe we should have had it ??
				self.put_to_resync(hash, Duration::from_millis(0))?;
				return Err(Into::into(e));
			}
		};
		let mut data = vec![];
		f.read_to_end(&mut data).await?;
		drop(f);

		if blake2sum(&data[..]) != *hash {
			let _lock = self.data_dir_lock.lock().await;
			warn!(
				"Block {:?} is corrupted. Renaming to .corrupted and resyncing.",
				hash
			);
			let mut path2 = path.clone();
			path2.set_extension(".corrupted");
			fs::rename(path, path2).await?;
			self.put_to_resync(&hash, Duration::from_millis(0))?;
			return Err(Error::CorruptData(*hash));
		}

		Ok(Message::PutBlock(PutBlockMessage { hash: *hash, data }))
	}

	/// Check if this node should have a block, but don't actually have it
	async fn need_block(&self, hash: &Hash) -> Result<bool, Error> {
		let needed = self
			.rc
			.get(hash.as_ref())?
			.map(|x| u64_from_be_bytes(x) > 0)
			.unwrap_or(false);
		if needed {
			let path = self.block_path(hash);
			let exists = fs::metadata(&path).await.is_ok();
			Ok(!exists)
		} else {
			Ok(false)
		}
	}

	fn block_dir(&self, hash: &Hash) -> PathBuf {
		let mut path = self.data_dir.clone();
		path.push(hex::encode(&hash.as_slice()[0..1]));
		path.push(hex::encode(&hash.as_slice()[1..2]));
		path
	}
	fn block_path(&self, hash: &Hash) -> PathBuf {
		let mut path = self.block_dir(hash);
		path.push(hex::encode(hash.as_ref()));
		path
	}

	/// Increment the number of time a block is used, putting it to resynchronization if it is
	/// required, but not known
	pub fn block_incref(&self, hash: &Hash) -> Result<(), Error> {
		let old_rc = self.rc.fetch_and_update(&hash, |old| {
			let old_v = old.map(u64_from_be_bytes).unwrap_or(0);
			Some(u64::to_be_bytes(old_v + 1).to_vec())
		})?;
		let old_rc = old_rc.map(u64_from_be_bytes).unwrap_or(0);
		if old_rc == 0 {
			self.put_to_resync(&hash, BLOCK_RW_TIMEOUT)?;
		}
		Ok(())
	}

	/// Decrement the number of time a block is used
	pub fn block_decref(&self, hash: &Hash) -> Result<(), Error> {
		let new_rc = self.rc.update_and_fetch(&hash, |old| {
			let old_v = old.map(u64_from_be_bytes).unwrap_or(0);
			if old_v > 1 {
				Some(u64::to_be_bytes(old_v - 1).to_vec())
			} else {
				None
			}
		})?;
		if new_rc.is_none() {
			self.put_to_resync(&hash, BLOCK_GC_TIMEOUT)?;
		}
		Ok(())
	}

	fn put_to_resync(&self, hash: &Hash, delay: Duration) -> Result<(), Error> {
		let when = now_msec() + delay.as_millis() as u64;
		trace!("Put resync_queue: {} {:?}", when, hash);
		let mut key = u64::to_be_bytes(when).to_vec();
		key.extend(hash.as_ref());
		self.resync_queue.insert(key, hash.as_ref())?;
		self.resync_notify.notify_waiters();
		Ok(())
	}

	async fn resync_loop(self: Arc<Self>, mut must_exit: watch::Receiver<bool>) {
		while !*must_exit.borrow() {
			if let Err(e) = self.resync_iter(&mut must_exit).await {
				warn!("Error in block resync loop: {}", e);
				select! {
					_ = tokio::time::sleep(Duration::from_secs(1)).fuse() => {},
					_ = must_exit.changed().fuse() => {},
				}
			}
		}
	}

	async fn resync_iter(&self, must_exit: &mut watch::Receiver<bool>) -> Result<(), Error> {
		if let Some(first_item) = self.resync_queue.iter().next() {
			let (time_bytes, hash_bytes) = first_item?;
			let time_msec = u64_from_be_bytes(&time_bytes[0..8]);
			let now = now_msec();
			if now >= time_msec {
				let hash = Hash::try_from(&hash_bytes[..]).unwrap();
				let res = self.resync_block(&hash).await;
				if let Err(e) = &res {
					warn!("Error when resyncing {:?}: {}", hash, e);
					self.put_to_resync(&hash, RESYNC_RETRY_TIMEOUT)?;
				}
				self.resync_queue.remove(&time_bytes)?;
				res?; // propagate error to delay main loop
			} else {
				let delay = tokio::time::sleep(Duration::from_millis(time_msec - now));
				select! {
					_ = delay.fuse() => {},
					_ = self.resync_notify.notified().fuse() => {},
					_ = must_exit.changed().fuse() => {},
				}
			}
		} else {
			select! {
				_ = self.resync_notify.notified().fuse() => {},
				_ = must_exit.changed().fuse() => {},
			}
		}
		Ok(())
	}

	async fn resync_block(&self, hash: &Hash) -> Result<(), Error> {
		let lock = self.data_dir_lock.lock().await;

		let path = self.block_path(hash);

		let exists = fs::metadata(&path).await.is_ok();
		let needed = self
			.rc
			.get(hash.as_ref())?
			.map(|x| u64_from_be_bytes(x) > 0)
			.unwrap_or(false);

		if exists != needed {
			info!(
				"Resync block {:?}: exists {}, needed {}",
				hash, exists, needed
			);
		}

		if exists && !needed {
			trace!("Offloading block {:?}", hash);

			let mut who = self.replication.write_nodes(&hash);
			if who.len() < self.replication.write_quorum() {
				return Err(Error::Message("Not trying to offload block because we don't have a quorum of nodes to write to".to_string()));
			}
			who.retain(|id| *id != self.system.id);

			let msg = Arc::new(Message::NeedBlockQuery(*hash));
			let who_needs_fut = who.iter().map(|to| {
				self.rpc_client
					.call_arc(*to, msg.clone(), NEED_BLOCK_QUERY_TIMEOUT)
			});
			let who_needs_resps = join_all(who_needs_fut).await;

			let mut need_nodes = vec![];
			for (node, needed) in who.iter().zip(who_needs_resps.into_iter()) {
				match needed? {
					Message::NeedBlockReply(needed) => {
						if needed {
							need_nodes.push(*node);
						}
					}
					_ => {
						return Err(Error::Message(
							"Unexpected response to NeedBlockQuery RPC".to_string(),
						));
					}
				}
			}

			if !need_nodes.is_empty() {
				trace!(
					"Block {:?} needed by {} nodes, sending",
					hash,
					need_nodes.len()
				);

				let put_block_message = self.read_block(hash).await?;
				self.rpc_client
					.try_call_many(
						&need_nodes[..],
						put_block_message,
						RequestStrategy::with_quorum(need_nodes.len())
							.with_timeout(BLOCK_RW_TIMEOUT),
					)
					.await?;
			}
			info!(
				"Deleting block {:?}, offload finished ({} / {})",
				hash,
				need_nodes.len(),
				who.len()
			);

			fs::remove_file(path).await?;
		}

		if needed && !exists {
			drop(lock);

			// TODO find a way to not do this if they are sending it to us
			// Let's suppose this isn't an issue for now with the BLOCK_RW_TIMEOUT delay
			// between the RC being incremented and this part being called.
			let block_data = self.rpc_get_block(&hash).await?;
			self.write_block(hash, &block_data[..]).await?;
		}

		Ok(())
	}

	/// Ask nodes that might have a block for it
	pub async fn rpc_get_block(&self, hash: &Hash) -> Result<Vec<u8>, Error> {
		let who = self.replication.read_nodes(&hash);
		let resps = self
			.rpc_client
			.try_call_many(
				&who[..],
				Message::GetBlock(*hash),
				RequestStrategy::with_quorum(1)
					.with_timeout(BLOCK_RW_TIMEOUT)
					.interrupt_after_quorum(true),
			)
			.await?;

		for resp in resps {
			if let Message::PutBlock(msg) = resp {
				return Ok(msg.data);
			}
		}
		Err(Error::Message(format!(
			"Unable to read block {:?}: no valid blocks returned",
			hash
		)))
	}

	/// Send block to nodes that should have it
	pub async fn rpc_put_block(&self, hash: Hash, data: Vec<u8>) -> Result<(), Error> {
		let who = self.replication.write_nodes(&hash);
		self.rpc_client
			.try_call_many(
				&who[..],
				Message::PutBlock(PutBlockMessage { hash, data }),
				RequestStrategy::with_quorum(self.replication.write_quorum())
					.with_timeout(BLOCK_RW_TIMEOUT),
			)
			.await?;
		Ok(())
	}

	pub async fn repair_data_store(&self, must_exit: &watch::Receiver<bool>) -> Result<(), Error> {
		// 1. Repair blocks from RC table
		let garage = self.garage.load_full().unwrap();
		let mut last_hash = None;
		let mut i = 0usize;
		for entry in garage.block_ref_table.data.store.iter() {
			let (_k, v_bytes) = entry?;
			let block_ref = rmp_serde::decode::from_read_ref::<_, BlockRef>(v_bytes.as_ref())?;
			if Some(&block_ref.block) == last_hash.as_ref() {
				continue;
			}
			if !block_ref.deleted.get() {
				last_hash = Some(block_ref.block);
				self.put_to_resync(&block_ref.block, Duration::from_secs(0))?;
			}
			i += 1;
			if i & 0xFF == 0 && *must_exit.borrow() {
				return Ok(());
			}
		}

		// 2. Repair blocks actually on disk
		self.repair_aux_read_dir_rec(&self.data_dir, must_exit)
			.await?;

		Ok(())
	}

	fn repair_aux_read_dir_rec<'a>(
		&'a self,
		path: &'a Path,
		must_exit: &'a watch::Receiver<bool>,
	) -> BoxFuture<'a, Result<(), Error>> {
		// Lists all blocks on disk and adds them to the resync queue.
		// This allows us to find blocks we are storing but don't actually need,
		// so that we can offload them if necessary and then delete them locally.
		async move {
			let mut ls_data_dir = fs::read_dir(path).await?;
			loop {
				let data_dir_ent = ls_data_dir.next_entry().await?;
				let data_dir_ent = match data_dir_ent {
					Some(x) => x,
					None => break,
				};
				let name = data_dir_ent.file_name();
				let name = match name.into_string() {
					Ok(x) => x,
					Err(_) => continue,
				};
				let ent_type = data_dir_ent.file_type().await?;

				if name.len() == 2 && hex::decode(&name).is_ok() && ent_type.is_dir() {
					self.repair_aux_read_dir_rec(&data_dir_ent.path(), must_exit)
						.await?;
				} else if name.len() == 64 {
					let hash_bytes = match hex::decode(&name) {
						Ok(h) => h,
						Err(_) => continue,
					};
					let mut hash = [0u8; 32];
					hash.copy_from_slice(&hash_bytes[..]);
					self.put_to_resync(&hash.into(), Duration::from_secs(0))?;
				}

				if *must_exit.borrow() {
					break;
				}
			}
			Ok(())
		}
		.boxed()
	}

	/// Get lenght of resync queue
	pub fn resync_queue_len(&self) -> usize {
		self.resync_queue.len()
	}

	pub fn rc_len(&self) -> usize {
		self.rc.len()
	}
}

fn u64_from_be_bytes<T: AsRef<[u8]>>(bytes: T) -> u64 {
	assert!(bytes.as_ref().len() == 8);
	let mut x8 = [0u8; 8];
	x8.copy_from_slice(bytes.as_ref());
	u64::from_be_bytes(x8)
}
