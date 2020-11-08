use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwapOption;
use futures::future::*;
use futures::select;
use futures::stream::*;
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::prelude::*;
use tokio::sync::{watch, Mutex, Notify};

use garage_util::data;
use garage_util::data::*;
use garage_util::error::Error;

use garage_rpc::membership::System;
use garage_rpc::rpc_client::*;
use garage_rpc::rpc_server::*;

use garage_table::table_sharded::TableShardedReplication;
use garage_table::TableReplication;

use crate::block_ref_table::*;

use crate::garage::Garage;

pub const INLINE_THRESHOLD: usize = 3072;

const BLOCK_RW_TIMEOUT: Duration = Duration::from_secs(42);
const NEED_BLOCK_QUERY_TIMEOUT: Duration = Duration::from_secs(5);
const RESYNC_RETRY_TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
	Ok,
	GetBlock(Hash),
	PutBlock(PutBlockMessage),
	NeedBlockQuery(Hash),
	NeedBlockReply(bool),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PutBlockMessage {
	pub hash: Hash,

	#[serde(with = "serde_bytes")]
	pub data: Vec<u8>,
}

impl RpcMessage for Message {}

pub struct BlockManager {
	pub replication: TableShardedReplication,
	pub data_dir: PathBuf,
	pub data_dir_lock: Mutex<()>,

	pub rc: sled::Tree,

	pub resync_queue: sled::Tree,
	pub resync_notify: Notify,

	pub system: Arc<System>,
	rpc_client: Arc<RpcClient<Message>>,
	pub garage: ArcSwapOption<Garage>,
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
		rc.set_merge_operator(rc_merge);

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
			_ => Err(Error::BadRPC(format!("Unexpected RPC message"))),
		}
	}

	pub async fn spawn_background_worker(self: Arc<Self>) {
		// Launch 2 simultaneous workers for background resync loop preprocessing
		for i in 0..2usize {
			let bm2 = self.clone();
			let background = self.system.background.clone();
			tokio::spawn(async move {
				tokio::time::delay_for(Duration::from_secs(10)).await;
				background
					.spawn_worker(format!("block resync worker {}", i), move |must_exit| {
						bm2.resync_loop(must_exit)
					})
					.await;
			});
		}
	}

	pub async fn write_block(&self, hash: &Hash, data: &[u8]) -> Result<Message, Error> {
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

	pub async fn read_block(&self, hash: &Hash) -> Result<Message, Error> {
		let path = self.block_path(hash);

		let mut f = match fs::File::open(&path).await {
			Ok(f) => f,
			Err(e) => {
				// Not found but maybe we should have had it ??
				self.put_to_resync(hash, 0)?;
				return Err(Into::into(e));
			}
		};
		let mut data = vec![];
		f.read_to_end(&mut data).await?;
		drop(f);

		if data::hash(&data[..]) != *hash {
			let _lock = self.data_dir_lock.lock().await;
			warn!("Block {:?} is corrupted. Deleting and resyncing.", hash);
			fs::remove_file(path).await?;
			self.put_to_resync(&hash, 0)?;
			return Err(Error::CorruptData(*hash));
		}

		Ok(Message::PutBlock(PutBlockMessage { hash: *hash, data }))
	}

	pub async fn need_block(&self, hash: &Hash) -> Result<bool, Error> {
		let needed = self
			.rc
			.get(hash.as_ref())?
			.map(|x| u64_from_bytes(x.as_ref()) > 0)
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

	pub fn block_incref(&self, hash: &Hash) -> Result<(), Error> {
		let old_rc = self.rc.get(&hash)?;
		self.rc.merge(&hash, vec![1])?;
		if old_rc.map(|x| u64_from_bytes(&x[..]) == 0).unwrap_or(true) {
			self.put_to_resync(&hash, BLOCK_RW_TIMEOUT.as_millis() as u64)?;
		}
		Ok(())
	}

	pub fn block_decref(&self, hash: &Hash) -> Result<(), Error> {
		let new_rc = self.rc.merge(&hash, vec![0])?;
		if new_rc.map(|x| u64_from_bytes(&x[..]) == 0).unwrap_or(true) {
			self.put_to_resync(&hash, 0)?;
		}
		Ok(())
	}

	fn put_to_resync(&self, hash: &Hash, delay_millis: u64) -> Result<(), Error> {
		let when = now_msec() + delay_millis;
		trace!("Put resync_queue: {} {:?}", when, hash);
		let mut key = u64::to_be_bytes(when).to_vec();
		key.extend(hash.as_ref());
		self.resync_queue.insert(key, hash.as_ref())?;
		self.resync_notify.notify();
		Ok(())
	}

	async fn resync_loop(
		self: Arc<Self>,
		mut must_exit: watch::Receiver<bool>,
	) -> Result<(), Error> {
		let mut n_failures = 0usize;
		while !*must_exit.borrow() {
			if let Some((time_bytes, hash_bytes)) = self.resync_queue.pop_min()? {
				let time_msec = u64_from_bytes(&time_bytes[0..8]);
				let now = now_msec();
				if now >= time_msec {
					let mut hash = [0u8; 32];
					hash.copy_from_slice(hash_bytes.as_ref());
					let hash = Hash::from(hash);

					if let Err(e) = self.resync_iter(&hash).await {
						warn!("Failed to resync block {:?}, retrying later: {}", hash, e);
						self.put_to_resync(&hash, RESYNC_RETRY_TIMEOUT.as_millis() as u64)?;
						n_failures += 1;
						if n_failures >= 10 {
							warn!("Too many resync failures, throttling.");
							tokio::time::delay_for(Duration::from_secs(1)).await;
						}
					} else {
						n_failures = 0;
					}
				} else {
					self.resync_queue.insert(time_bytes, hash_bytes)?;
					let delay = tokio::time::delay_for(Duration::from_millis(time_msec - now));
					select! {
						_ = delay.fuse() => (),
						_ = self.resync_notify.notified().fuse() => (),
						_ = must_exit.recv().fuse() => (),
					}
				}
			} else {
				select! {
					_ = self.resync_notify.notified().fuse() => (),
					_ = must_exit.recv().fuse() => (),
				}
			}
		}
		Ok(())
	}

	async fn resync_iter(&self, hash: &Hash) -> Result<(), Error> {
		let path = self.block_path(hash);

		let exists = fs::metadata(&path).await.is_ok();
		let needed = self
			.rc
			.get(hash.as_ref())?
			.map(|x| u64_from_bytes(x.as_ref()) > 0)
			.unwrap_or(false);

		if exists != needed {
			info!(
				"Resync block {:?}: exists {}, needed {}",
				hash, exists, needed
			);
		}

		if exists && !needed {
			let garage = self.garage.load_full().unwrap();
			let active_refs = garage
				.block_ref_table
				.get_range(&hash, None, Some(()), 1)
				.await?;
			let needed_by_others = !active_refs.is_empty();
			if needed_by_others {
				let ring = self.system.ring.borrow().clone();
				let who = self.replication.replication_nodes(&hash, &ring);
				let msg = Arc::new(Message::NeedBlockQuery(*hash));
				let who_needs_fut = who.iter().map(|to| {
					self.rpc_client
						.call_arc(*to, msg.clone(), NEED_BLOCK_QUERY_TIMEOUT)
				});
				let who_needs = join_all(who_needs_fut).await;

				let mut need_nodes = vec![];
				for (node, needed) in who.into_iter().zip(who_needs.iter()) {
					match needed {
						Ok(Message::NeedBlockReply(needed)) => {
							if *needed {
								need_nodes.push(node);
							}
						}
						Err(e) => {
							return Err(Error::Message(format!(
								"Should delete block, but unable to confirm that all other nodes that need it have it: {}",
								e
							)));
						}
						Ok(_) => {
							return Err(Error::Message(format!(
								"Unexpected response to NeedBlockQuery RPC"
							)));
						}
					}
				}

				if need_nodes.len() > 0 {
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
			}
			fs::remove_file(path).await?;
			self.resync_queue.remove(&hash)?;
		}

		if needed && !exists {
			// TODO find a way to not do this if they are sending it to us
			// Let's suppose this isn't an issue for now with the BLOCK_RW_TIMEOUT delay
			// between the RC being incremented and this part being called.
			let block_data = self.rpc_get_block(&hash).await?;
			self.write_block(hash, &block_data[..]).await?;
		}

		Ok(())
	}

	pub async fn rpc_get_block(&self, hash: &Hash) -> Result<Vec<u8>, Error> {
		let who = self.replication.read_nodes(&hash, &self.system);
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

	pub async fn rpc_put_block(&self, hash: Hash, data: Vec<u8>) -> Result<(), Error> {
		let who = self.replication.write_nodes(&hash, &self.system);
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
		for entry in garage.block_ref_table.store.iter() {
			let (_k, v_bytes) = entry?;
			let block_ref = rmp_serde::decode::from_read_ref::<_, BlockRef>(v_bytes.as_ref())?;
			if Some(&block_ref.block) == last_hash.as_ref() {
				continue;
			}
			if !block_ref.deleted {
				last_hash = Some(block_ref.block);
				self.put_to_resync(&block_ref.block, 0)?;
			}
			i += 1;
			if i & 0xFF == 0 && *must_exit.borrow() {
				return Ok(());
			}
		}

		// 2. Repair blocks actually on disk
		let mut ls_data_dir = fs::read_dir(&self.data_dir).await?;
		while let Some(data_dir_ent) = ls_data_dir.next().await {
			let data_dir_ent = data_dir_ent?;
			let dir_name = data_dir_ent.file_name();
			let dir_name = match dir_name.into_string() {
				Ok(x) => x,
				Err(_) => continue,
			};
			if dir_name.len() != 2 || hex::decode(&dir_name).is_err() {
				continue;
			}

			let mut ls_data_dir_2 = match fs::read_dir(data_dir_ent.path()).await {
				Err(e) => {
					warn!(
						"Warning: could not list dir {:?}: {}",
						data_dir_ent.path().to_str(),
						e
					);
					continue;
				}
				Ok(x) => x,
			};
			while let Some(file) = ls_data_dir_2.next().await {
				let file = file?;
				let file_name = file.file_name();
				let file_name = match file_name.into_string() {
					Ok(x) => x,
					Err(_) => continue,
				};
				if file_name.len() != 64 {
					continue;
				}
				let hash_bytes = match hex::decode(&file_name) {
					Ok(h) => h,
					Err(_) => continue,
				};
				let mut hash = [0u8; 32];
				hash.copy_from_slice(&hash_bytes[..]);
				self.put_to_resync(&hash.into(), 0)?;

				if *must_exit.borrow() {
					return Ok(());
				}
			}
		}
		Ok(())
	}
}

fn u64_from_bytes(bytes: &[u8]) -> u64 {
	assert!(bytes.len() == 8);
	let mut x8 = [0u8; 8];
	x8.copy_from_slice(bytes);
	u64::from_be_bytes(x8)
}

fn rc_merge(_key: &[u8], old: Option<&[u8]>, new: &[u8]) -> Option<Vec<u8>> {
	let old = old.map(u64_from_bytes).unwrap_or(0);
	assert!(new.len() == 1);
	let new = match new[0] {
		0 => {
			if old > 0 {
				old - 1
			} else {
				0
			}
		}
		1 => old + 1,
		_ => unreachable!(),
	};
	if new == 0 {
		None
	} else {
		Some(u64::to_be_bytes(new).to_vec())
	}
}
