use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwapOption;
use futures::stream::*;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

use garage_util::data::*;
use garage_util::error::Error;

use garage_rpc::membership::{Ring, System};
use garage_rpc::rpc_client::*;
use garage_rpc::rpc_server::*;

use crate::schema::*;
use crate::table_sync::*;

const TABLE_RPC_TIMEOUT: Duration = Duration::from_secs(10);

pub struct Table<F: TableSchema, R: TableReplication> {
	pub instance: F,
	pub replication: R,

	pub name: String,
	pub rpc_client: Arc<RpcClient<TableRPC<F>>>,

	pub system: Arc<System>,
	pub store: sled::Tree,
	pub syncer: ArcSwapOption<TableSyncer<F, R>>,
}

#[derive(Serialize, Deserialize)]
pub enum TableRPC<F: TableSchema> {
	Ok,

	ReadEntry(F::P, F::S),
	ReadEntryResponse(Option<ByteBuf>),

	// Read range: read all keys in partition P, possibly starting at a certain sort key offset
	ReadRange(F::P, Option<F::S>, Option<F::Filter>, usize),

	Update(Vec<Arc<ByteBuf>>),

	SyncRPC(SyncRPC),
}

impl<F: TableSchema> RpcMessage for TableRPC<F> {}

pub trait TableReplication: Send + Sync {
	// See examples in table_sharded.rs and table_fullcopy.rs
	// To understand various replication methods

	// Which nodes to send reads from
	fn read_nodes(&self, hash: &Hash, system: &System) -> Vec<UUID>;
	fn read_quorum(&self) -> usize;

	// Which nodes to send writes to
	fn write_nodes(&self, hash: &Hash, system: &System) -> Vec<UUID>;
	fn write_quorum(&self) -> usize;
	fn max_write_errors(&self) -> usize;
	fn epidemic_writes(&self) -> bool;

	// Which are the nodes that do actually replicate the data
	fn replication_nodes(&self, hash: &Hash, ring: &Ring) -> Vec<UUID>;
	fn split_points(&self, ring: &Ring) -> Vec<Hash>;
}

impl<F, R> Table<F, R>
where
	F: TableSchema + 'static,
	R: TableReplication + 'static,
{
	// =============== PUBLIC INTERFACE FUNCTIONS (new, insert, get, etc) ===============

	pub async fn new(
		instance: F,
		replication: R,
		system: Arc<System>,
		db: &sled::Db,
		name: String,
		rpc_server: &mut RpcServer,
	) -> Arc<Self> {
		let store = db.open_tree(&name).expect("Unable to open DB tree");

		let rpc_path = format!("table_{}", name);
		let rpc_client = system.rpc_client::<TableRPC<F>>(&rpc_path);

		let table = Arc::new(Self {
			instance,
			replication,
			name,
			rpc_client,
			system,
			store,
			syncer: ArcSwapOption::from(None),
		});
		table.clone().register_handler(rpc_server, rpc_path);

		let syncer = TableSyncer::launch(table.clone()).await;
		table.syncer.swap(Some(syncer));

		table
	}

	pub async fn insert(&self, e: &F::E) -> Result<(), Error> {
		let hash = e.partition_key().hash();
		let who = self.replication.write_nodes(&hash, &self.system);
		//eprintln!("insert who: {:?}", who);

		let e_enc = Arc::new(ByteBuf::from(rmp_to_vec_all_named(e)?));
		let rpc = TableRPC::<F>::Update(vec![e_enc]);

		self.rpc_client
			.try_call_many(
				&who[..],
				rpc,
				RequestStrategy::with_quorum(self.replication.write_quorum())
					.with_timeout(TABLE_RPC_TIMEOUT),
			)
			.await?;
		Ok(())
	}

	pub async fn insert_many(&self, entries: &[F::E]) -> Result<(), Error> {
		let mut call_list = HashMap::new();

		for entry in entries.iter() {
			let hash = entry.partition_key().hash();
			let who = self.replication.write_nodes(&hash, &self.system);
			let e_enc = Arc::new(ByteBuf::from(rmp_to_vec_all_named(entry)?));
			for node in who {
				if !call_list.contains_key(&node) {
					call_list.insert(node, vec![]);
				}
				call_list.get_mut(&node).unwrap().push(e_enc.clone());
			}
		}

		let call_futures = call_list.drain().map(|(node, entries)| async move {
			let rpc = TableRPC::<F>::Update(entries);

			let resp = self.rpc_client.call(node, rpc, TABLE_RPC_TIMEOUT).await?;
			Ok::<_, Error>((node, resp))
		});
		let mut resps = call_futures.collect::<FuturesUnordered<_>>();
		let mut errors = vec![];

		while let Some(resp) = resps.next().await {
			if let Err(e) = resp {
				errors.push(e);
			}
		}
		if errors.len() > self.replication.max_write_errors() {
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
		let who = self.replication.read_nodes(&hash, &self.system);
		//eprintln!("get who: {:?}", who);

		let rpc = TableRPC::<F>::ReadEntry(partition_key.clone(), sort_key.clone());
		let resps = self
			.rpc_client
			.try_call_many(
				&who[..],
				rpc,
				RequestStrategy::with_quorum(self.replication.read_quorum())
					.with_timeout(TABLE_RPC_TIMEOUT)
					.interrupt_after_quorum(true),
			)
			.await?;

		let mut ret = None;
		let mut not_all_same = false;
		for resp in resps {
			if let TableRPC::ReadEntryResponse(value) = resp {
				if let Some(v_bytes) = value {
					let v = Self::decode_entry(v_bytes.as_slice())?;
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
					.spawn_cancellable(async move { self2.repair_on_read(&who[..], ent2).await });
			}
		}
		Ok(ret)
	}

	pub async fn get_range(
		self: &Arc<Self>,
		partition_key: &F::P,
		begin_sort_key: Option<F::S>,
		filter: Option<F::Filter>,
		limit: usize,
	) -> Result<Vec<F::E>, Error> {
		let hash = partition_key.hash();
		let who = self.replication.read_nodes(&hash, &self.system);

		let rpc = TableRPC::<F>::ReadRange(partition_key.clone(), begin_sort_key, filter, limit);

		let resps = self
			.rpc_client
			.try_call_many(
				&who[..],
				rpc,
				RequestStrategy::with_quorum(self.replication.read_quorum())
					.with_timeout(TABLE_RPC_TIMEOUT)
					.interrupt_after_quorum(true),
			)
			.await?;

		let mut ret = BTreeMap::new();
		let mut to_repair = BTreeMap::new();
		for resp in resps {
			if let TableRPC::Update(entries) = resp {
				for entry_bytes in entries.iter() {
					let entry = Self::decode_entry(entry_bytes.as_slice())?;
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
			self.system.background.spawn_cancellable(async move {
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
		self.rpc_client
			.try_call_many(
				&who[..],
				TableRPC::<F>::Update(vec![what_enc]),
				RequestStrategy::with_quorum(who.len()).with_timeout(TABLE_RPC_TIMEOUT),
			)
			.await?;
		Ok(())
	}

	// =============== HANDLERS FOR RPC OPERATIONS (SERVER SIDE) ==============

	fn register_handler(self: Arc<Self>, rpc_server: &mut RpcServer, path: String) {
		let self2 = self.clone();
		rpc_server.add_handler::<TableRPC<F>, _, _>(path, move |msg, _addr| {
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

	async fn handle(self: &Arc<Self>, msg: &TableRPC<F>) -> Result<TableRPC<F>, Error> {
		match msg {
			TableRPC::ReadEntry(key, sort_key) => {
				let value = self.handle_read_entry(key, sort_key)?;
				Ok(TableRPC::ReadEntryResponse(value))
			}
			TableRPC::ReadRange(key, begin_sort_key, filter, limit) => {
				let values = self.handle_read_range(key, begin_sort_key, filter, *limit)?;
				Ok(TableRPC::Update(values))
			}
			TableRPC::Update(pairs) => {
				self.handle_update(pairs).await?;
				Ok(TableRPC::Ok)
			}
			TableRPC::SyncRPC(rpc) => {
				let syncer = self.syncer.load_full().unwrap();
				let response = syncer
					.handle_rpc(rpc, self.system.background.stop_signal.clone())
					.await?;
				Ok(TableRPC::SyncRPC(response))
			}
			_ => Err(Error::BadRPC(format!("Unexpected table RPC"))),
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
		s: &Option<F::S>,
		filter: &Option<F::Filter>,
		limit: usize,
	) -> Result<Vec<Arc<ByteBuf>>, Error> {
		let partition_hash = p.hash();
		let first_key = match s {
			None => partition_hash.to_vec(),
			Some(sk) => self.tree_key(p, sk),
		};
		let mut ret = vec![];
		for item in self.store.range(first_key..) {
			let (key, value) = item?;
			if &key[..32] != partition_hash.as_slice() {
				break;
			}
			let keep = match filter {
				None => true,
				Some(f) => {
					let entry = Self::decode_entry(value.as_ref())?;
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

	pub async fn handle_update(self: &Arc<Self>, entries: &[Arc<ByteBuf>]) -> Result<(), Error> {
		let syncer = self.syncer.load_full().unwrap();
		let mut epidemic_propagate = vec![];

		for update_bytes in entries.iter() {
			let update = Self::decode_entry(update_bytes.as_slice())?;

			let tree_key = self.tree_key(update.partition_key(), update.sort_key());

			let (old_entry, new_entry) = self.store.transaction(|db| {
				let (old_entry, new_entry) = match db.get(&tree_key)? {
					Some(prev_bytes) => {
						let old_entry = Self::decode_entry(&prev_bytes)
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

			if old_entry.as_ref() != Some(&new_entry) {
				if self.replication.epidemic_writes() {
					epidemic_propagate.push(new_entry.clone());
				}

				self.instance.updated(old_entry, Some(new_entry)).await?;
				self.system
					.background
					.spawn_cancellable(syncer.clone().invalidate(tree_key));
			}
		}

		if epidemic_propagate.len() > 0 {
			let self2 = self.clone();
			self.system
				.background
				.spawn_cancellable(async move { self2.insert_many(&epidemic_propagate[..]).await });
		}

		Ok(())
	}

	pub async fn delete_range(&self, begin: &Hash, end: &Hash) -> Result<(), Error> {
		let syncer = self.syncer.load_full().unwrap();

		debug!("({}) Deleting range {:?} - {:?}", self.name, begin, end);
		let mut count = 0;
		while let Some((key, _value)) = self.store.get_lt(end.as_slice())? {
			if key.as_ref() < begin.as_slice() {
				break;
			}
			if let Some(old_val) = self.store.remove(&key)? {
				let old_entry = Self::decode_entry(&old_val)?;
				self.instance.updated(Some(old_entry), None).await?;
				self.system
					.background
					.spawn_cancellable(syncer.clone().invalidate(key.to_vec()));
				count += 1;
			}
		}
		debug!("({}) {} entries deleted", self.name, count);
		Ok(())
	}

	fn tree_key(&self, p: &F::P, s: &F::S) -> Vec<u8> {
		let mut ret = p.hash().to_vec();
		ret.extend(s.sort_key());
		ret
	}

	fn decode_entry(bytes: &[u8]) -> Result<F::E, Error> {
		match rmp_serde::decode::from_read_ref::<_, F::E>(bytes) {
			Ok(x) => Ok(x),
			Err(e) => match F::try_migrate(bytes) {
				Some(x) => Ok(x),
				None => Err(e.into()),
			},
		}
	}
}
