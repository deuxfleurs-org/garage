use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::*;
use tokio::fs;
use tokio::prelude::*;
use tokio::sync::{watch, Mutex};

use crate::data;
use crate::data::*;
use crate::error::Error;
use crate::membership::System;
use crate::proto::*;
use crate::rpc_client::*;

pub struct BlockManager {
	pub data_dir: PathBuf,
	pub rc: sled::Tree,
	pub resync_queue: sled::Tree,
	pub lock: Mutex<()>,
	pub system: Arc<System>,
}

impl BlockManager {
	pub async fn new(db: &sled::Db, data_dir: PathBuf, system: Arc<System>) -> Arc<Self> {
		let rc = db
			.open_tree("block_local_rc")
			.expect("Unable to open block_local_rc tree");
		rc.set_merge_operator(rc_merge);

		let resync_queue = db
			.open_tree("block_local_resync_queue")
			.expect("Unable to open block_local_resync_queue tree");

		let block_manager = Arc::new(Self {
			rc,
			resync_queue,
			data_dir,
			lock: Mutex::new(()),
			system,
		});
		let bm2 = block_manager.clone();
		block_manager
			.system
			.background
			.spawn_worker(move |must_exit| bm2.resync_loop(must_exit))
			.await;
		block_manager
	}

	pub async fn write_block(&self, hash: &Hash, data: &[u8]) -> Result<Message, Error> {
		let _lock = self.lock.lock().await;

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
		let mut path = self.block_dir(hash);
		path.push(hex::encode(hash));

		let mut f = fs::File::open(&path).await?;
		let mut data = vec![];
		f.read_to_end(&mut data).await?;
		drop(f);

		if data::hash(&data[..]) != *hash {
			let _lock = self.lock.lock().await;
			eprintln!("Block {:?} is corrupted. Deleting and resyncing.", hash);
			fs::remove_file(path).await?;
			self.resync_queue.insert(hash.to_vec(), vec![1u8])?;
			return Err(Error::CorruptData(hash.clone()));
		}

		Ok(Message::PutBlock(PutBlockMessage {
			hash: hash.clone(),
			data,
		}))
	}

	fn block_dir(&self, hash: &Hash) -> PathBuf {
		let mut path = self.data_dir.clone();
		path.push(hex::encode(&hash.as_slice()[0..1]));
		path.push(hex::encode(&hash.as_slice()[1..2]));
		path
	}

	pub fn block_incref(&self, hash: &Hash) -> Result<(), Error> {
		self.rc.merge(&hash, vec![1])?;
		Ok(())
	}

	pub fn block_decref(&self, hash: &Hash) -> Result<(), Error> {
		if self.rc.merge(&hash, vec![0])?.is_none() {
			self.resync_queue.insert(hash.to_vec(), vec![1u8])?;
		}
		Ok(())
	}

	async fn resync_loop(self: Arc<Self>, must_exit: watch::Receiver<bool>) -> Result<(), Error> {
		while !*must_exit.borrow() {
			if let Some((hash_bytes, _v)) = self.resync_queue.get_gt(&[])? {
				let mut hash = [0u8; 32];
				hash.copy_from_slice(hash_bytes.as_ref());
				let hash = Hash::from(hash);

				match self.resync_iter(&hash).await {
					Ok(_) => {
						self.resync_queue.remove(&hash_bytes)?;
					}
					Err(e) => {
						eprintln!(
							"Failed to resync hash {:?}, leaving it in queue: {}",
							hash, e
						);
					}
				}
			} else {
				tokio::time::delay_for(Duration::from_secs(1)).await;
			}
		}
		Ok(())
	}

	async fn resync_iter(&self, hash: &Hash) -> Result<(), Error> {
		let mut path = self.data_dir.clone();
		path.push(hex::encode(hash.as_ref()));

		let exists = fs::metadata(&path).await.is_ok();
		let needed = self
			.rc
			.get(hash.as_ref())?
			.map(|x| u64_from_bytes(x.as_ref()) > 0)
			.unwrap_or(false);

		if exists && !needed {
			// TODO: verify that other nodes that might need it have it ?
			fs::remove_file(path).await?;
			self.resync_queue.remove(&hash)?;
		}

		if needed && !exists {
			// TODO find a way to not do this if they are sending it to us
			let block_data = rpc_get_block(&self.system, &hash).await?;
			self.write_block(hash, &block_data[..]).await?;
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

pub async fn rpc_get_block(system: &Arc<System>, hash: &Hash) -> Result<Vec<u8>, Error> {
	let who = system
		.ring
		.borrow()
		.clone()
		.walk_ring(&hash, system.config.data_replication_factor);
	let msg = Message::GetBlock(hash.clone());
	let mut resp_stream = who
		.iter()
		.map(|to| rpc_call(system.clone(), to, &msg, BLOCK_RW_TIMEOUT))
		.collect::<FuturesUnordered<_>>();

	while let Some(resp) = resp_stream.next().await {
		if let Ok(Message::PutBlock(msg)) = resp {
			if data::hash(&msg.data[..]) == *hash {
				return Ok(msg.data);
			}
		}
	}
	Err(Error::Message(format!(
		"Unable to read block {:?}: no valid blocks returned",
		hash
	)))
}

pub async fn rpc_put_block(system: &Arc<System>, hash: Hash, data: Vec<u8>) -> Result<(), Error> {
	let who = system
		.ring
		.borrow()
		.clone()
		.walk_ring(&hash, system.config.data_replication_factor);
	rpc_try_call_many(
		system.clone(),
		&who[..],
		Message::PutBlock(PutBlockMessage { hash, data }),
		(system.config.data_replication_factor + 1) / 2,
		BLOCK_RW_TIMEOUT,
	)
	.await?;
	Ok(())
}
