use std::sync::Arc;
use std::path::PathBuf;

use tokio::fs;
use tokio::prelude::*;

use crate::error::Error;
use crate::server::Garage;
use crate::proto::*;
use crate::data::*;

fn block_dir(garage: &Garage, hash: &Hash) -> PathBuf {
	let mut path = garage.system.config.data_dir.clone();
	path.push(hex::encode(&hash.as_slice()[0..1]));
	path.push(hex::encode(&hash.as_slice()[1..2]));
	path
}

pub async fn write_block(garage: Arc<Garage>, hash: &Hash, data: &[u8]) -> Result<Message, Error> {
	garage.fs_lock.lock().await;

	let mut path = block_dir(&garage, hash);
	fs::create_dir_all(&path).await?;

	path.push(hex::encode(hash));
	if fs::metadata(&path).await.is_ok() {
		return Ok(Message::Ok)
	}

	let mut f = fs::File::create(path).await?;
	f.write_all(data).await?;
	drop(f);

	Ok(Message::Ok)
}

pub async fn read_block(garage: Arc<Garage>, hash: &Hash) -> Result<Message, Error> {
	let mut path = block_dir(&garage, hash);
	path.push(hex::encode(hash));

	let mut f = fs::File::open(path).await?;
	let mut data = vec![];
	f.read_to_end(&mut data).await?;

	Ok(Message::PutBlock(PutBlockMessage{
		hash: hash.clone(),
		data,
	}))
}
