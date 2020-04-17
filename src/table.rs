use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use futures::stream::*;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

use crate::data::*;
use crate::error::Error;
use crate::membership::System;
use crate::proto::*;
use crate::rpc_client::*;
use crate::table_sync::*;

pub struct Table<F: TableSchema> {
	pub instance: F,

	pub name: String,

	pub system: Arc<System>,
	pub store: sled::Tree,
	pub syncer: ArcSwapOption<TableSyncer<F>>,

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

	ReadRange(F::P, F::S, Option<F::Filter>, usize),

	Update(Vec<Arc<ByteBuf>>),

	SyncRPC(SyncRPC),
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
	type Filter: Clone + Serialize + for<'de> Deserialize<'de> + Send + Sync;

	async fn updated(&self, old: Option<Self::E>, new: Option<Self::E>);
	fn matches_filter(_entry: &Self::E, _filter: &Self::Filter) -> bool {
		true
	}
}

impl<F: TableSchema + 'static> Table<F> {
	// =============== PUBLIC INTERFACE FUNCTIONS (new, insert, get, etc) ===============

	pub async fn new(
		instance: F,
		system: Arc<System>,
		db: &sled::Db,
		name: String,
		param: TableReplicationParams,
	) -> Arc<Self> {
		let store = db.open_tree(&name).expect("Unable to open DB tree");
		let table = Arc::new(Self {
			instance,
			name,
			system,
			store,
			param,
			syncer: ArcSwapOption::from(None),
		});
		let syncer = TableSyncer::launch(table.clone()).await;
		table.syncer.swap(Some(syncer));
		table
	}

	pub async fn insert(&self, e: &F::E) -> Result<(), Error> {
		let hash = e.partition_key().hash();
		let ring = self.system.ring.borrow().clone();
		let who = ring.walk_ring(&hash, self.param.replication_factor);
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
			let ring = self.system.ring.borrow().clone();
			let who = ring.walk_ring(&hash, self.param.replication_factor);
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

	pub async fn get(
		self: &Arc<Self>,
		partition_key: &F::P,
		sort_key: &F::S,
	) -> Result<Option<F::E>, Error> {
		let hash = partition_key.hash();
		let ring = self.system.ring.borrow().clone();
		let who = ring.walk_ring(&hash, self.param.replication_factor);
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
				let self2 = self.clone();
				let ent2 = ret_entry.clone();
				self.system
					.background
					.spawn(async move { self2.repair_on_read(&who[..], ent2).await });
			}
		}
		Ok(ret)
	}

	pub async fn get_range(
		self: &Arc<Self>,
		partition_key: &F::P,
		begin_sort_key: &F::S,
		filter: Option<F::Filter>,
		limit: usize,
	) -> Result<Vec<F::E>, Error> {
		let hash = partition_key.hash();
		let ring = self.system.ring.borrow().clone();
		let who = ring.walk_ring(&hash, self.param.replication_factor);

		let rpc =
			&TableRPC::<F>::ReadRange(partition_key.clone(), begin_sort_key.clone(), filter, limit);
		let resps = self
			.rpc_try_call_many(&who[..], &rpc, self.param.read_quorum)
			.await?;

		let mut ret = BTreeMap::new();
		let mut to_repair = BTreeMap::new();
		for resp in resps {
			if let TableRPC::Update(entries) = resp {
				for entry_bytes in entries.iter() {
					let entry =
						rmp_serde::decode::from_read_ref::<_, F::E>(entry_bytes.as_slice())?;
					let entry_key = self.tree_key(entry.partition_key(), entry.sort_key());
					match ret.remove(&entry_key) {
						None => {
							ret.insert(entry_key, Some(entry));
						}
						Some(Some(mut prev)) => {
							let must_repair = prev != entry;
							prev.merge(&entry);
							if must_repair {
								to_repair.insert(entry_key.clone(), Some(prev.clone()));
							}
							ret.insert(entry_key, Some(prev));
						}
						Some(None) => unreachable!(),
					}
				}
			}
		}
		if !to_repair.is_empty() {
			let self2 = self.clone();
			self.system.background.spawn(async move {
				for (_, v) in to_repair.iter_mut() {
					self2.repair_on_read(&who[..], v.take().unwrap()).await?;
				}
				Ok(())
			});
		}
		let ret_vec = ret
			.iter_mut()
			.take(limit)
			.map(|(_k, v)| v.take().unwrap())
			.collect::<Vec<_>>();
		Ok(ret_vec)
	}

	// =============== UTILITY FUNCTION FOR CLIENT OPERATIONS ===============

	async fn repair_on_read(&self, who: &[UUID], what: F::E) -> Result<(), Error> {
		let what_enc = Arc::new(ByteBuf::from(rmp_to_vec_all_named(&what)?));
		self.rpc_try_call_many(&who[..], &TableRPC::<F>::Update(vec![what_enc]), who.len())
			.await?;
		Ok(())
	}

	async fn rpc_try_call_many(
		&self,
		who: &[UUID],
		rpc: &TableRPC<F>,
		quorum: usize,
	) -> Result<Vec<TableRPC<F>>, Error> {
		//eprintln!("Table RPC to {:?}: {}", who, serde_json::to_string(&rpc)?);

		let rpc_bytes = rmp_to_vec_all_named(rpc)?;
		let rpc_msg = Message::TableRPC(self.name.to_string(), rpc_bytes);

		let resps = rpc_try_call_many(
			self.system.clone(),
			who,
			rpc_msg,
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
		//eprintln!(
		//	"Table RPC responses: {}",
		//	serde_json::to_string(&resps_vals)?
		//);
		Ok(resps_vals)
	}

	pub async fn rpc_call(&self, who: &UUID, rpc: &TableRPC<F>) -> Result<TableRPC<F>, Error> {
		let rpc_bytes = rmp_to_vec_all_named(rpc)?;
		let rpc_msg = Message::TableRPC(self.name.to_string(), rpc_bytes);

		let resp = rpc_call(self.system.clone(), who, &rpc_msg, self.param.timeout).await?;
		if let Message::TableRPC(tbl, rep_by) = &resp {
			if *tbl == self.name {
				return Ok(rmp_serde::decode::from_read_ref(&rep_by)?);
			}
		}
		Err(Error::Message(format!(
			"Invalid reply to TableRPC: {:?}",
			resp
		)))
	}

	// =============== HANDLERS FOR RPC OPERATIONS (SERVER SIDE) ==============

	pub fn rpc_handler(self: Arc<Self>) -> Box<dyn TableRpcHandler + Send + Sync> {
		Box::new(TableRpcHandlerAdapter::<F> { table: self })
	}

	async fn handle(self: &Arc<Self>, msg: TableRPC<F>) -> Result<TableRPC<F>, Error> {
		match msg {
			TableRPC::ReadEntry(key, sort_key) => {
				let value = self.handle_read_entry(&key, &sort_key)?;
				Ok(TableRPC::ReadEntryResponse(value))
			}
			TableRPC::ReadRange(key, begin_sort_key, filter, limit) => {
				let values = self.handle_read_range(&key, &begin_sort_key, &filter, limit)?;
				Ok(TableRPC::Update(values))
			}
			TableRPC::Update(pairs) => {
				self.handle_update(pairs).await?;
				Ok(TableRPC::Ok)
			}
			TableRPC::SyncRPC(rpc) => {
				let syncer = self.syncer.load_full().unwrap();
				let response = syncer
					.handle_rpc(&rpc, self.system.background.stop_signal.clone())
					.await?;
				Ok(TableRPC::SyncRPC(response))
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

	fn handle_read_range(
		&self,
		p: &F::P,
		s: &F::S,
		filter: &Option<F::Filter>,
		limit: usize,
	) -> Result<Vec<Arc<ByteBuf>>, Error> {
		let partition_hash = p.hash();
		let first_key = self.tree_key(p, s);
		let mut ret = vec![];
		for item in self.store.range(first_key..) {
			let (key, value) = item?;
			if &key[..32] != partition_hash.as_slice() {
				break;
			}
			let keep = match filter {
				None => true,
				Some(f) => {
					let entry = rmp_serde::decode::from_read_ref::<_, F::E>(value.as_ref())?;
					F::matches_filter(&entry, f)
				}
			};
			if keep {
				ret.push(Arc::new(ByteBuf::from(value.as_ref())));
			}
			if ret.len() >= limit {
				break;
			}
		}
		Ok(ret)
	}

	pub async fn handle_update(
		self: &Arc<Self>,
		mut entries: Vec<Arc<ByteBuf>>,
	) -> Result<(), Error> {
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
				Ok((old_entry, Some(new_entry)))
			})?;

			if old_entry != new_entry {
				self.instance.updated(old_entry, new_entry).await;

				let syncer = self.syncer.load_full().unwrap();
				self.system.background.spawn(syncer.invalidate(tree_key));
			}
		}
		Ok(())
	}

	pub async fn delete_range(&self, begin: &Hash, end: &Hash) -> Result<(), Error> {
		eprintln!("({}) Deleting range {:?} - {:?}", self.name, begin, end);
		let mut count = 0;
		while let Some((key, _value)) = self.store.get_lt(end.as_slice())? {
			if key.as_ref() < begin.as_slice() {
				break;
			}
			if let Some(old_val) = self.store.remove(&key)? {
				let old_entry = rmp_serde::decode::from_read_ref::<_, F::E>(&old_val)?;
				self.instance.updated(Some(old_entry), None).await;
				count += 1;
			}
		}
		eprintln!("({}) {} entries deleted", self.name, count);
		Ok(())
	}

	fn tree_key(&self, p: &F::P, s: &F::S) -> Vec<u8> {
		let mut ret = p.hash().to_vec();
		ret.extend(s.sort_key());
		ret
	}
}
