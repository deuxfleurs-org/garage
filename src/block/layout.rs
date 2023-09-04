use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use garage_util::config::DataDirEnum;
use garage_util::data::Hash;
use garage_util::migrate::*;

pub const DRIVE_NPART: usize = 1024;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct DataLayout {
	pub(crate) data_dirs: Vec<DataDir>,
	pub(crate) partitions: Vec<Partition>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct DataDir {
	pub(crate) path: PathBuf,
	pub(crate) state: DataDirState,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) enum DataDirState {
	Active { capacity: u64 },
	ReadOnly,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub(crate) struct Partition {
	pub(crate) prim: usize,
	pub(crate) sec: Vec<usize>,
}

impl DataLayout {
	pub(crate) fn initialize(dirs: &DataDirEnum) -> Self {
		todo!()
	}

	pub(crate) fn update(&mut self, dirs: &DataDirEnum) -> Self {
		todo!()
	}

	pub(crate) fn data_dir(&self, hash: &Hash) -> PathBuf {
		todo!()
		/*
		let mut path = self.data_dir.clone();
		path.push(hex::encode(&hash.as_slice()[0..1]));
		path.push(hex::encode(&hash.as_slice()[1..2]));
		path
		*/
	}
}

impl InitialFormat for DataLayout {
	const VERSION_MARKER: &'static [u8] = b"G09bmdl";
}
