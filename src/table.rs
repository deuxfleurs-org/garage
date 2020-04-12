use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use futures::stream::*;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

use crate::data::*;
use crate::error::Error;
use crate::membership::System;
use crate::proto::*;
use crate::rpc_client::*;

pub struct Table<F: TableSchema> {
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

struct TableRpcHandlerAdapter<F: TableSchema> {
	table: Arc<Table<F>>,
}

#[async_trait]
impl<F: TableSchema + 'static> TableRpcHandler for TableRpcHandlerAdapter<F> {
	async fn handle(&self, rpc: &[u8]) -> Result<Vec<u8>, Error> {
		let msg = rmp_serde::decode::from_read_ref::<_, TableRPC<F>>(rpc)?;
		let rep = self.table.handle(msg).await?;
		Ok(rmp_to_vec_all_named(&rep)?)
	}
}

#[derive(Serialize, Deserialize)]
pub enum TableRPC<F: TableSchema> {
	Ok,

	ReadEntry(F::P, F::S),
	ReadEntryResponse(Option<ByteBuf>),

	Update(Vec<Arc<ByteBuf>>),
}

pub struct Partition {
	pub begin: Hash,
	pub end: Hash,
	pub other_nodes: Vec<UUID>,
}

pub trait PartitionKey {
	fn hash(&self) -> Hash;
}

pub trait SortKey {
	fn sort_key(&self) -> &[u8];
}

pub trait Entry<P: PartitionKey, S: SortKey>:
	PartialEq + Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync
{
	fn partition_key(&self) -> &P;
	fn sort_key(&self) -> &S;

	fn merge(&mut self, other: &Self);
}

#[derive(Clone, Serialize, Deserialize)]
pub struct EmptySortKey;
impl SortKey for EmptySortKey {
	fn sort_key(&self) -> &[u8] {
		&[]
	}
}

impl<T: AsRef<str>> PartitionKey for T {
	fn hash(&self) -> Hash {
		hash(self.as_ref().as_bytes())
	}
}
impl<T: AsRef<str>> SortKey for T {
	fn sort_key(&self) -> &[u8] {
		self.as_ref().as_bytes()
	}
}

impl PartitionKey for Hash {
	fn hash(&self) -> Hash {
		self.clone()
	}
}
impl SortKey for Hash {
	fn sort_key(&self) -> &[u8] {
		self.as_slice()
	}
}

#[async_trait]
pub trait TableSchema: Send + Sync {
	type P: PartitionKey + Clone + PartialEq + Serialize + for<'de> Deserialize<'de> + Send + Sync;
	type S: SortKey + Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync;
	type E: Entry<Self::P, Self::S>;

	async fn updated(&self, old: Option<Self::E>, new: Self::E);
}

impl<F: TableSchema + 'static> Table<F> {
	pub fn new(
		instance: F,
		system: Arc<System>,
		db: &sled::Db,
		name: String,
		param: TableReplicationParams,
	) -> Arc<Self> {
		let store = db.open_tree(&name).expect("Unable to open DB tree");
		Arc::new(Self {
			instance,
			name,
			system,
			store,
			partitions: Vec::new(),
			param,
		})
	}

	pub fn rpc_handler(self: Arc<Self>) -> Box<dyn TableRpcHandler + Send + Sync> {
		Box::new(TableRpcHandlerAdapter::<F> { table: self })
	}

	pub async fn insert(&self, e: &F::E) -> Result<(), Error> {
		let hash = e.partition_key().hash();
		let who = self
			.system
			.ring
			.borrow()
			.clone()
			.walk_ring(&hash, self.param.replication_factor);
		eprintln!("insert who: {:?}", who);

		let e_enc = Arc::new(ByteBuf::from(rmp_to_vec_all_named(e)?));
		let rpc = &TableRPC::<F>::Update(vec![e_enc]);

		self.rpc_try_call_many(&who[..], &rpc, self.param.write_quorum)
			.await?;
		Ok(())
	}

	pub async fn insert_many(&self, entries: &[F::E]) -> Result<(), Error> {
		let mut call_list = HashMap::new();

		for entry in entries.iter() {
			let hash = entry.partition_key().hash();
			let who = self
				.system
				.ring
				.borrow()
				.clone()
				.walk_ring(&hash, self.param.replication_factor);
			let e_enc = Arc::new(ByteBuf::from(rmp_to_vec_all_named(entry)?));
			for node in who {
				if !call_list.contains_key(&node) {
					call_list.insert(node.clone(), vec![]);
				}
				call_list.get_mut(&node).unwrap().push(e_enc.clone());
			}
		}

		let call_futures = call_list.drain().map(|(node, entries)| async move {
			let rpc = TableRPC::<F>::Update(entries);
			let rpc_bytes = rmp_to_vec_all_named(&rpc)?;
			let message = Message::TableRPC(self.name.to_string(), rpc_bytes);

			let resp = rpc_call(self.system.clone(), &node, &message, self.param.timeout).await?;
			Ok::<_, Error>((node, resp))
		});
		let mut resps = call_futures.collect::<FuturesUnordered<_>>();
		let mut errors = vec![];

		while let Some(resp) = resps.next().await {
			if let Err(e) = resp {
				errors.push(e);
			}
		}
		if errors.len() > self.param.replication_factor - self.param.write_quorum {
			Err(Error::Message("Too many errors".into()))
		} else {
			Ok(())
		}
	}

	pub async fn get(&self, partition_key: &F::P, sort_key: &F::S) -> Result<Option<F::E>, Error> {
		let hash = partition_key.hash();
		let who = self
			.system
			.ring
			.borrow()
			.clone()
			.walk_ring(&hash, self.param.replication_factor);
		eprintln!("get who: {:?}", who);

		let rpc = &TableRPC::<F>::ReadEntry(partition_key.clone(), sort_key.clone());
		let resps = self
			.rpc_try_call_many(&who[..], &rpc, self.param.read_quorum)
			.await?;

		let mut ret = None;
		let mut not_all_same = false;
		for resp in resps {
			if let TableRPC::ReadEntryResponse(value) = resp {
				if let Some(v_bytes) = value {
					let v = rmp_serde::decode::from_read_ref::<_, F::E>(v_bytes.as_slice())?;
					ret = match ret {
						None => Some(v),
						Some(mut x) => {
							if x != v {
								not_all_same = true;
								x.merge(&v);
							}
							Some(x)
						}
					}
				}
			} else {
				return Err(Error::Message(format!("Invalid return value to read")));
			}
		}
		if let Some(ret_entry) = &ret {
			if not_all_same {
				let _: Result<_, _> = self.repair_on_read(&who[..], &ret_entry).await;
			}
		}
		Ok(ret)
	}

	async fn repair_on_read(&self, who: &[UUID], what: &F::E) -> Result<(), Error> {
		let what_enc = Arc::new(ByteBuf::from(rmp_to_vec_all_named(what)?));
		self.rpc_try_call_many(&who[..], &TableRPC::<F>::Update(vec![what_enc]), who.len())
			.await
			.map(|_| ())
	}

	async fn rpc_try_call_many(
		&self,
		who: &[UUID],
		rpc: &TableRPC<F>,
		quorum: usize,
	) -> Result<Vec<TableRPC<F>>, Error> {
		eprintln!("Table RPC to {:?}: {}", who, serde_json::to_string(&rpc)?);

		let rpc_bytes = rmp_to_vec_all_named(rpc)?;
		let rpc_msg = Message::TableRPC(self.name.to_string(), rpc_bytes);

		let resps = rpc_try_call_many(
			self.system.clone(),
			who,
			&rpc_msg,
			quorum,
			self.param.timeout,
		)
		.await?;

		let mut resps_vals = vec![];
		for resp in resps {
			if let Message::TableRPC(tbl, rep_by) = &resp {
				if *tbl == self.name {
					resps_vals.push(rmp_serde::decode::from_read_ref(&rep_by)?);
					continue;
				}
			}
			return Err(Error::Message(format!(
				"Invalid reply to TableRPC: {:?}",
				resp
			)));
		}
		eprintln!(
			"Table RPC responses: {}",
			serde_json::to_string(&resps_vals)?
		);
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
			_ => Err(Error::RPCError(format!("Unexpected table RPC"))),
		}
	}

	fn handle_read_entry(&self, p: &F::P, s: &F::S) -> Result<Option<ByteBuf>, Error> {
		let tree_key = self.tree_key(p, s);
		if let Some(bytes) = self.store.get(&tree_key)? {
			Ok(Some(ByteBuf::from(bytes.to_vec())))
		} else {
			Ok(None)
		}
	}

	async fn handle_update(&self, mut entries: Vec<Arc<ByteBuf>>) -> Result<(), Error> {
		for update_bytes in entries.drain(..) {
			let update = rmp_serde::decode::from_read_ref::<_, F::E>(update_bytes.as_slice())?;

			let tree_key = self.tree_key(update.partition_key(), update.sort_key());

			let (old_entry, new_entry) = self.store.transaction(|db| {
				let (old_entry, new_entry) = match db.get(&tree_key)? {
					Some(prev_bytes) => {
						let old_entry = rmp_serde::decode::from_read_ref::<_, F::E>(&prev_bytes)
							.map_err(Error::RMPDecode)
							.map_err(sled::ConflictableTransactionError::Abort)?;
						let mut new_entry = old_entry.clone();
						new_entry.merge(&update);
						(Some(old_entry), new_entry)
					}
					None => (None, update.clone()),
				};

				let new_bytes = rmp_to_vec_all_named(&new_entry)
					.map_err(Error::RMPEncode)
					.map_err(sled::ConflictableTransactionError::Abort)?;
				db.insert(tree_key.clone(), new_bytes)?;
				Ok((old_entry, new_entry))
			})?;

			self.instance.updated(old_entry, new_entry).await;
		}
		Ok(())
	}

	fn tree_key(&self, p: &F::P, s: &F::S) -> Vec<u8> {
		let mut ret = p.hash().to_vec();
		ret.extend(s.sort_key());
		ret
	}
}
