use std::path::PathBuf;

use futures_util::future::*;
use tokio::fs;
use tokio::prelude::*;
use tokio::sync::Mutex;

use crate::background::*;
use crate::data::*;
use crate::error::Error;
use crate::proto::*;

pub struct BlockManager {
	pub data_dir: PathBuf,
	pub rc: sled::Tree,
	pub lock: Mutex<()>,
}

impl BlockManager {
	pub fn new(db: &sled::Db, data_dir: PathBuf) -> Self {
		let rc = db
			.open_tree("block_local_rc")
			.expect("Unable to open block_local_rc tree");
		rc.set_merge_operator(rc_merge);
		Self {
			rc,
			data_dir,
			lock: Mutex::new(()),
		}
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

		let mut f = fs::File::open(path).await?;
		let mut data = vec![];
		f.read_to_end(&mut data).await?;

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

	pub fn block_decref(&self, hash: &Hash, background: &BackgroundRunner) -> Result<(), Error> {
		match self.rc.merge(&hash, vec![0])? {
			None => {
				let mut path = self.block_dir(hash);
				path.push(hex::encode(hash));
				background.spawn(tokio::fs::remove_file(path).map_err(Into::into));
				Ok(())
			}
			Some(_) => Ok(()),
		}
	}
}

fn rc_merge(_key: &[u8], old: Option<&[u8]>, new: &[u8]) -> Option<Vec<u8>> {
	let old = old
		.map(|x| {
			assert!(x.len() == 8);
			let mut x8 = [0u8; 8];
			x8.copy_from_slice(x);
			u64::from_be_bytes(x8)
		})
		.unwrap_or(0);
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
