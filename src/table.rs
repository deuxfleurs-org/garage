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
	pub instance: F,

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

#[derive(Serialize, Deserialize)]
pub enum TableRPC<F: TableFormat> {
	Ok,

	ReadEntry(F::K, Vec<u8>),
	ReadEntryResponse(Option<F::V>),

	Update(Vec<(F::K, F::V)>),
}

pub struct Partition {
	pub begin: Hash,
	pub end: Hash,
	pub other_nodes: Vec<UUID>,
}

pub trait TableKey {
	fn hash(&self) -> Hash;
}

pub trait TableValue {
	fn sort_key(&self) -> Vec<u8>;
	fn merge(&mut self, other: &Self);
}

#[async_trait]
pub trait TableFormat: Send + Sync {
	type K: Clone + PartialEq + Serialize + for<'de> Deserialize<'de> + TableKey + Send + Sync;
	type V: Clone + Serialize + for<'de> Deserialize<'de> + TableValue + Send + Sync;

	async fn updated(&self, key: &Self::K, old: Option<&Self::V>, new: &Self::V);
}

impl<F: TableFormat + 'static> Table<F> {
	pub fn new(instance: F, system: Arc<System>, db: &sled::Db, name: String, param: TableReplicationParams) -> Self {
		let store = db.open_tree(&name)
				.expect("Unable to open DB tree");
		Self{
			instance,
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
		let hash = k.hash();
		let who = self.system.members.read().await
			.walk_ring(&hash, self.param.replication_factor);

		let rpc = &TableRPC::<F>::Update(vec![(k.clone(), v.clone())]);
		
		self.rpc_try_call_many(&who[..],
							   &rpc,
							   self.param.write_quorum).await?;
		Ok(())
	}

	pub async fn get(&self, k: &F::K, sort_key: &[u8]) -> Result<Option<F::V>, Error> {
		let hash = k.hash();
		let who = self.system.members.read().await
			.walk_ring(&hash, self.param.replication_factor);

		let rpc = &TableRPC::<F>::ReadEntry(k.clone(), sort_key.to_vec());
		let resps = self.rpc_try_call_many(&who[..],
										   &rpc,
										   self.param.read_quorum)
			.await?;

		let mut ret = None;
		for resp in resps {
			if let TableRPC::ReadEntryResponse(value) = resp {
				if let Some(v) = value {
					ret = match ret {
						None => Some(v),
						Some(mut x) => {
							x.merge(&v);
							Some(x)
						}
					}
				}
			} else {
				return Err(Error::Message(format!("Invalid return value to read")));
			}
		}
		Ok(ret)
	}

	async fn rpc_try_call_many(&self, who: &[UUID], rpc: &TableRPC<F>, quorum: usize) -> Result<Vec<TableRPC<F>>, Error> {
		let rpc_bytes = rmp_serde::encode::to_vec_named(rpc)?;
		let rpc_msg = Message::TableRPC(self.name.to_string(), rpc_bytes);

		let resps = rpc_try_call_many(self.system.clone(),
						  who,
						  &rpc_msg,
						  quorum,
						  self.param.timeout).await?;

		let mut resps_vals = vec![];
		for resp in resps {
			if let Message::TableRPC(tbl, rep_by) = &resp {
				if *tbl == self.name {
					resps_vals.push(rmp_serde::decode::from_read_ref(&rep_by)?);
					continue;
				}
			}
			return Err(Error::Message(format!("Invalid reply to TableRPC: {:?}", resp)))
		}
		Ok(resps_vals)
	}

	async fn handle(&self, msg: TableRPC<F>) -> Result<TableRPC<F>, Error> {
		match msg {
			TableRPC::ReadEntry(key, sort_key) => {
				let value = self.handle_read_entry(&key, &sort_key)?;
				Ok(TableRPC::ReadEntryResponse(value))
			}
			TableRPC::Update(pairs) => {
				self.handle_update(pairs).await?;
				Ok(TableRPC::Ok)
			}
			_ => Err(Error::RPCError(format!("Unexpected table RPC")))
		}
	}

	fn handle_read_entry(&self, key: &F::K, sort_key: &[u8]) -> Result<Option<F::V>, Error> {
		let mut tree_key = key.hash().to_vec();
		tree_key.extend(sort_key);
		if let Some(bytes) = self.store.get(&tree_key)? {
			let (_, v) = rmp_serde::decode::from_read_ref::<_, (F::K, F::V)>(&bytes)?;
			Ok(Some(v))
		} else {
			Ok(None)
		}
	}

	async fn handle_update(&self, mut pairs: Vec<(F::K, F::V)>) -> Result<(), Error> {
		for mut pair in pairs.drain(..) {
			let mut tree_key = pair.0.hash().to_vec();
			tree_key.extend(pair.1.sort_key());

			let old_val = match self.store.get(&tree_key)? {
				Some(prev_bytes) => {
					let (_, old_val) = rmp_serde::decode::from_read_ref::<_, (F::K, F::V)>(&prev_bytes)?;
					pair.1.merge(&old_val);
					Some(old_val)
				}
				None => None
			};

			let new_bytes = rmp_serde::encode::to_vec_named(&pair)?;
			self.store.insert(&tree_key, new_bytes)?;

			self.instance.updated(&pair.0, old_val.as_ref(), &pair.1).await;
		}
		Ok(())
	}
}
