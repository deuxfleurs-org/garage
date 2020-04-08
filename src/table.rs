use std::time::Duration;
use std::sync::Arc;
use serde::{Serialize, Deserialize};
use async_trait::async_trait;
use reduce::Reduce;

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
	Read(Vec<F::K>),
	Update(Vec<(F::K, F::V)>),
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
	type K: Clone + PartialEq + Serialize + for<'de> Deserialize<'de> + KeyHash + Send + Sync;
	type V: Clone + Serialize + for<'de> Deserialize<'de> + ValueMerge + Send + Sync;

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

	pub async fn get(&self, k: &F::K) -> Result<F::V, Error> {
		let hash = k.hash();
		let who = self.system.members.read().await
			.walk_ring(&hash, self.param.replication_factor);

		let rpc = &TableRPC::<F>::Read(vec![k.clone()]);
		let resps = self.rpc_try_call_many(&who[..],
										   &rpc,
										   self.param.read_quorum)
			.await?;

		let mut values = vec![];
		for resp in resps {
			if let TableRPC::Update(mut pairs) = resp {
				if pairs.len() == 0 {
					continue;
				} else if pairs.len() == 1 && pairs[0].0 == *k {
					values.push(pairs.drain(..).next().unwrap().1);
					continue;
				}
			}
			return Err(Error::Message(format!("Invalid return value to read")));
		}
		values.drain(..)
			.reduce(|mut x, y| { x.merge(&y); x })
			.map(Ok)
			.unwrap_or(Err(Error::NotFound))
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
			TableRPC::Read(keys) => {
				Ok(TableRPC::Update(self.handle_read(&keys)?))
			}
			TableRPC::Update(pairs) => {
				self.handle_write(pairs).await?;
				Ok(TableRPC::Ok)
			}
			_ => Err(Error::RPCError(format!("Unexpected table RPC")))
		}
	}

	fn handle_read(&self, keys: &[F::K]) -> Result<Vec<(F::K, F::V)>, Error> {
		let mut results = vec![];
		for key in keys.iter() {
			if let Some(bytes) = self.store.get(&key.hash())? {
				let pair = rmp_serde::decode::from_read_ref::<_, (F::K, F::V)>(bytes.as_ref())?;
				results.push(pair);
			}
		}
		Ok(results)
	}

	async fn handle_write(&self, mut pairs: Vec<(F::K, F::V)>) -> Result<(), Error> {
		for mut pair in pairs.drain(..) {
			let hash = pair.0.hash();

			let old_val = match self.store.get(&hash)? {
				Some(prev_bytes) => {
					let (_, old_val) = rmp_serde::decode::from_read_ref::<_, (F::K, F::V)>(&prev_bytes)?;
					pair.1.merge(&old_val);
					Some(old_val)
				}
				None => None
			};

			let new_bytes = rmp_serde::encode::to_vec_named(&pair)?;
			self.store.insert(&hash, new_bytes)?;

			self.instance.updated(&pair.0, old_val.as_ref(), &pair.1).await;
		}
		Ok(())
	}
}
