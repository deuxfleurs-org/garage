use std::marker::PhantomData;
use std::time::Duration;
use std::sync::Arc;
use serde::{Serialize, Deserialize};
use async_trait::async_trait;

use crate::error::Error;
use crate::proto::*;
use crate::data::*;
use crate::membership::System;
use crate::rpc_client::*;


pub struct Table<F: TableFormat> {
	phantom: PhantomData<F>,

	pub name: String,

	pub system: Arc<System>,
	pub store: sled::Tree,
	pub partitions: Vec<Partition>,

	pub param: TableReplicationParams,
}

#[derive(Clone)]
pub struct TableReplicationParams {
	pub replication_factor: usize,
	pub read_quorum: usize,
	pub write_quorum: usize,
	pub timeout: Duration,
}

#[async_trait]
pub trait TableRpcHandler {
	async fn handle(&self, rpc: &[u8]) -> Result<Vec<u8>, Error>;
}

struct TableRpcHandlerAdapter<F: TableFormat> {
	table: Arc<Table<F>>,
}

#[async_trait]
impl<F: TableFormat + 'static> TableRpcHandler for TableRpcHandlerAdapter<F> {
	async fn handle(&self, rpc: &[u8]) -> Result<Vec<u8>, Error> {
		let msg = rmp_serde::decode::from_read_ref::<_, TableRPC<F>>(rpc)?;
		let rep = self.table.handle(msg).await?;
		Ok(rmp_serde::encode::to_vec_named(&rep)?)
	}
}

#[derive(Debug, Serialize, Deserialize)]
pub enum TableRPC<F: TableFormat> {
	Update(F::K, F::V),
}

pub struct Partition {
	pub begin: Hash,
	pub end: Hash,
	pub other_nodes: Vec<UUID>,
}

pub trait KeyHash {
	fn hash(&self) -> Hash;
}

pub trait ValueMerge {
	fn merge(&mut self, other: &Self);
}

#[async_trait]
pub trait TableFormat: Send + Sync {
	type K: Clone + Serialize + for<'de> Deserialize<'de> + KeyHash + Send + Sync;
	type V: Clone + Serialize + for<'de> Deserialize<'de> + ValueMerge + Send + Sync;

	async fn updated(&self, key: &Self::K, old: Option<&Self::V>, new: &Self::V);
}

impl<F: TableFormat + 'static> Table<F> {
	pub fn new(system: Arc<System>, db: &sled::Db, name: String, param: TableReplicationParams) -> Self {
		let store = db.open_tree(&name)
				.expect("Unable to open DB tree");
		Self{
			phantom: PhantomData::default(),
			name,
			system,
			store,
			partitions: Vec::new(),
			param,
		}
	}

	pub fn rpc_handler(self: Arc<Self>) -> Box<dyn TableRpcHandler + Send + Sync> {
		Box::new(TableRpcHandlerAdapter::<F>{ table: self })
	}

	pub async fn insert(&self, k: &F::K, v: &F::V) -> Result<(), Error> {
		unimplemented!();

		let hash = k.hash();
		let who = self.system.members.read().await
			.walk_ring(&hash, self.param.replication_factor);

		let msg = rmp_serde::encode::to_vec_named(&TableRPC::<F>::Update(k.clone(), v.clone()))?;
		rpc_try_call_many(self.system.clone(),
						  &who[..],
						  &Message::TableRPC(self.name.to_string(), msg),
						  self.param.write_quorum,
						  self.param.timeout).await?;
		Ok(())
	}

	async fn handle(&self, msg: TableRPC<F>) -> Result<TableRPC<F>, Error> {
		unimplemented!()
	}
}
