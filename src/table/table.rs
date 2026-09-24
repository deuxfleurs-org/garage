use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;

use futures::stream::*;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

use opentelemetry::{
	trace::{FutureExt, TraceContextExt, Tracer},
	Context,
};

use garage_db as db;

use garage_util::background::BackgroundRunner;
use garage_util::data::*;
use garage_util::error::Error;
use garage_util::metrics::RecordDuration;
use garage_util::migrate::Migrate;

use garage_rpc::rpc_helper::QuorumSetResultTracker;
use garage_rpc::system::System;
use garage_rpc::*;

use crate::crdt::Crdt;
use crate::data::*;
use crate::gc::*;
use crate::merkle::*;
use crate::queue::InsertQueueWorker;
use crate::replication::*;
use crate::schema::*;
use crate::sync::*;
use crate::util::*;

pub struct Table<F: TableSchema, R: TableReplication> {
	pub system: Arc<System>,
	pub data: Arc<TableData<F, R>>,
	pub merkle_updater: Arc<MerkleUpdater<F, R>>,
	pub syncer: Arc<TableSyncer<F, R>>,
	gc: Arc<TableGc<F, R>>,
	endpoint: Arc<Endpoint<TableRpc<F>, Self>>,
}

#[derive(Serialize, Deserialize)]
pub(crate) enum TableRpc<F: TableSchema> {
	Ok,

	ReadEntry(F::P, F::S),
	ReadEntryResponse(Option<ByteBuf>),

	// Read range: read all keys in partition P, possibly starting at a certain sort key offset
	ReadRange {
		partition: F::P,
		begin_sort_key: Option<F::S>,
		filter: Option<F::Filter>,
		limit: usize,
		enumeration_order: EnumerationOrder,
	},

	Update(Vec<Arc<ByteBuf>>),
}

impl<F: TableSchema> Rpc for TableRpc<F> {
	type Response = Result<TableRpc<F>, Error>;
}

impl<F: TableSchema, R: TableReplication> Table<F, R> {
	// =============== PUBLIC INTERFACE FUNCTIONS (new, insert, get, etc) ===============

	pub fn new(instance: F, replication: R, system: Arc<System>, db: &db::Db) -> Arc<Self> {
		let endpoint = system
			.netapp
			.endpoint(format!("garage_table/table.rs/Rpc:{}", F::TABLE_NAME));

		let data = TableData::new(system.clone(), instance, replication, db);

		let merkle_updater = MerkleUpdater::new(data.clone());

		let syncer = TableSyncer::new(system.clone(), data.clone(), merkle_updater.clone());
		let gc = TableGc::new(system.clone(), data.clone());

		system.layout_manager.add_table(F::TABLE_NAME);

		let table = Arc::new(Self {
			system,
			data,
			merkle_updater,
			gc,
			syncer,
			endpoint,
		});

		table.endpoint.set_handler(table.clone());

		table
	}

	pub fn spawn_workers(self: &Arc<Self>, bg: &BackgroundRunner) {
		self.merkle_updater.spawn_workers(bg);
		self.syncer.spawn_workers(bg);
		self.gc.spawn_workers(bg);
		bg.spawn_worker(InsertQueueWorker(self.clone()));
	}

	pub async fn insert(&self, e: &F::E) -> Result<(), Error> {
		let tracer = opentelemetry::global::tracer("garage_table");
		let span = tracer.start(format!("{} insert", F::TABLE_NAME));

		self.insert_internal(e)
			.record_duration(
				&self.data.metrics.put_request_duration,
				&self.data.metrics.attrs,
			)
			.with_context(Context::current_with_span(span))
			.await?;

		self.data
			.metrics
			.put_request_counter
			.add(1, &self.data.metrics.attrs);

		Ok(())
	}

	async fn insert_internal(&self, e: &F::E) -> Result<(), Error> {
		let hash = e.partition_key().hash();
		let who = self.data.replication.write_sets(&hash)?;

		let e_enc = Arc::new(ByteBuf::from(e.encode()?));
		let rpc = TableRpc::<F>::Update(vec![e_enc]);

		self.system
			.rpc_helper()
			.try_write_many_sets(
				&self.endpoint,
				who.as_ref(),
				rpc,
				RequestStrategy::with_priority(PRIO_NORMAL)
					.with_quorum(self.data.replication.write_quorum()?),
			)
			.await?;

		Ok(())
	}

	/// Insert item locally
	pub fn queue_insert(&self, tx: &mut db::Transaction, e: &F::E) -> db::TxResult<(), Error> {
		self.data.queue_insert(tx, e)
	}

	pub async fn insert_many<I, IE>(self: &Arc<Self>, entries: I) -> Result<(), Error>
	where
		I: IntoIterator<Item = IE> + Send + Sync,
		IE: Borrow<F::E> + Send + Sync,
	{
		let tracer = opentelemetry::global::tracer("garage_table");
		let span = tracer.start(format!("{} insert_many", F::TABLE_NAME));

		self.insert_many_internal(entries)
			.record_duration(
				&self.data.metrics.put_request_duration,
				&self.data.metrics.attrs,
			)
			.with_context(Context::current_with_span(span))
			.await?;

		self.data
			.metrics
			.put_request_counter
			.add(1, &self.data.metrics.attrs);

		Ok(())
	}

	async fn insert_many_internal<I, IE>(self: &Arc<Self>, entries: I) -> Result<(), Error>
	where
		I: IntoIterator<Item = IE> + Send + Sync,
		IE: Borrow<F::E> + Send + Sync,
	{
		// The different items will have to be stored on possibly different nodes.
		// We will here batch all items into a single request for each concerned
		// node, with all of the entries it must store within that request.
		// Each entry has to be saved to a specific list of "write sets", i.e. a set
		// of node within which a quorum must be achieved. In normal operation, there
		// is a single write set which corresponds to the quorum in the current
		// cluster layout, but when the layout is updated, multiple write sets might
		// have to be handled at once. Here, since we are sending many entries, we
		// will have to handle many write sets in all cases. The algorithm is thus
		// to send one request to each node with all the items it must save,
		// and keep track of the OK responses within each write set: if for all sets
		// a quorum of nodes has answered OK, then the insert has succeeded and
		// consistency properties (read-after-write) are preserved.

		let quorum = self.data.replication.write_quorum()?;

		// Serialize all entries and compute the write sets for each of them.
		// In the case of sharded table replication, this also takes an "ack lock"
		// to the layout manager to avoid ack'ing newer versions which are not
		// taken into account by writes in progress (the ack can happen later, once
		// all writes that didn't take the new layout into account are finished).
		// These locks are released when entries_vec is dropped, i.e. when this
		// function returns.
		let mut entries_vec = Vec::new();
		for entry in entries.into_iter() {
			let entry = entry.borrow();
			let hash = entry.partition_key().hash();
			let mut write_sets = self.data.replication.write_sets(&hash)?;
			for set in write_sets.as_mut().iter_mut() {
				// Sort nodes in each write sets to merge write sets with same
				// nodes but in possibly different orders
				set.sort();
			}
			let e_enc = Arc::new(ByteBuf::from(entry.encode()?));
			entries_vec.push((write_sets, e_enc));
		}

		if entries_vec.is_empty() {
			return Ok(());
		}

		// Compute a deduplicated list of all of the write sets,
		// and compute an index from each node to the position of the sets in which
		// it takes part, to optimize the detection of a quorum.
		let mut write_sets = entries_vec
			.iter()
			.flat_map(|(wss, _)| wss.as_ref().iter().map(|ws| ws.as_slice()))
			.collect::<Vec<&[Uuid]>>();
		write_sets.sort();
		write_sets.dedup();

		let mut result_tracker = QuorumSetResultTracker::new(&write_sets, quorum);

		// Build a map of all nodes to the entries that must be sent to that node.
		let mut call_list: HashMap<Uuid, Vec<_>> = HashMap::new();
		for (write_sets, entry_enc) in entries_vec.iter() {
			for write_set in write_sets.as_ref().iter() {
				for node in write_set.iter() {
					let node_entries = call_list.entry(*node).or_default();
					match node_entries.last() {
						Some(x) if Arc::ptr_eq(x, entry_enc) => {
							// skip if entry already in list to send to this node
							// (could happen if node is in several write sets for this entry)
						}
						_ => {
							node_entries.push(entry_enc.clone());
						}
					}
				}
			}
		}

		// Build futures to actually perform each of the corresponding RPC calls
		let call_futures = call_list.into_iter().map(|(node, entries)| {
			let this = self.clone();
			async move {
				let rpc = TableRpc::<F>::Update(entries);
				let resp = this
					.system
					.rpc_helper()
					.call(
						&this.endpoint,
						node,
						rpc,
						RequestStrategy::with_priority(PRIO_NORMAL).with_quorum(quorum),
					)
					.await;
				(node, resp)
			}
		});

		// Run all requests in parallel thanks to FuturesUnordered, and collect results.
		let mut resps = call_futures.collect::<FuturesUnordered<_>>();

		while let Some((node, resp)) = resps.next().await {
			result_tracker.register_result(node, resp.map(|_| ()));

			if result_tracker.all_quorums_ok() {
				// Success

				// Continue all other requests in background
				tokio::spawn(async move {
					resps.collect::<Vec<(Uuid, Result<_, _>)>>().await;
				});

				return Ok(());
			}

			if result_tracker.too_many_failures() {
				// Too many errors in this set, we know we won't get a quorum
				break;
			}
		}

		// Failure, could not get quorum within at least one set
		Err(result_tracker.quorum_error())
	}

	pub async fn get(
		self: &Arc<Self>,
		partition_key: &F::P,
		sort_key: &F::S,
	) -> Result<Option<F::E>, Error> {
		let tracer = opentelemetry::global::tracer("garage_table");
		let span = tracer.start(format!("{} get", F::TABLE_NAME));

		let res = self
			.get_internal(partition_key, sort_key, false)
			.record_duration(
				&self.data.metrics.get_request_duration,
				&self.data.metrics.attrs,
			)
			.with_context(Context::current_with_span(span))
			.await?;

		self.data
			.metrics
			.get_request_counter
			.add(1, &self.data.metrics.attrs);

		Ok(res)
	}

	pub async fn get_monotonic(
		self: &Arc<Self>,
		partition_key: &F::P,
		sort_key: &F::S,
	) -> Result<Option<F::E>, Error> {
		let tracer = opentelemetry::global::tracer("garage_table");
		let span = tracer.start(format!("{} get_monotonic", F::TABLE_NAME));

		let res = self
			.get_internal(partition_key, sort_key, true)
			.record_duration(
				&self.data.metrics.get_request_duration,
				&self.data.metrics.attrs,
			)
			.with_context(Context::current_with_span(span))
			.await?;

		self.data
			.metrics
			.get_request_counter
			.add(1, &self.data.metrics.attrs);

		Ok(res)
	}

	async fn get_internal(
		self: &Arc<Self>,
		partition_key: &F::P,
		sort_key: &F::S,
		monotonic_read: bool,
	) -> Result<Option<F::E>, Error> {
		let hash = partition_key.hash();
		let who = self.data.replication.read_nodes(&hash)?;

		let rpc = TableRpc::<F>::ReadEntry(partition_key.clone(), sort_key.clone());
		let resps = self
			.system
			.rpc_helper()
			.try_call_many(
				&self.endpoint,
				&who,
				rpc,
				RequestStrategy::with_priority(PRIO_NORMAL)
					.with_quorum(self.data.replication.read_quorum()?),
			)
			.await?;

		let mut ret = None;
		let mut not_all_same = false;
		{
			let mut vals_nb = 0;
			for resp in &resps {
				if let TableRpc::ReadEntryResponse(value) = resp {
					if let Some(v_bytes) = value {
						vals_nb += 1;
						let v = self.data.decode_entry(v_bytes.as_slice())?;
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
					return Err(Error::Message("Invalid return value to read".to_string()));
				}
			}
			// Only some nodes store this value; we must propagate it during repair
			if vals_nb < resps.len() {
				not_all_same = true;
			}
		}

		if let Some(ret_entry) = &ret {
			if monotonic_read && not_all_same {
				self.repair_on_read(&who, &[ret_entry]).await?;
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
		enumeration_order: EnumerationOrder,
	) -> Result<Vec<F::E>, Error> {
		let tracer = opentelemetry::global::tracer("garage_table");
		let span = tracer.start(format!("{} get_range", F::TABLE_NAME));

		let res = self
			.get_range_internal(
				partition_key,
				begin_sort_key,
				filter,
				limit,
				enumeration_order,
				false,
			)
			.record_duration(
				&self.data.metrics.get_request_duration,
				&self.data.metrics.attrs,
			)
			.with_context(Context::current_with_span(span))
			.await?;

		self.data
			.metrics
			.get_request_counter
			.add(1, &self.data.metrics.attrs);

		Ok(res)
	}

	pub async fn get_range_monotonic(
		self: &Arc<Self>,
		partition_key: &F::P,
		begin_sort_key: Option<F::S>,
		filter: Option<F::Filter>,
		limit: usize,
		enumeration_order: EnumerationOrder,
	) -> Result<Vec<F::E>, Error> {
		let tracer = opentelemetry::global::tracer("garage_table");
		let span = tracer.start(format!("{} get_range_monotonic", F::TABLE_NAME));

		let res = self
			.get_range_internal(
				partition_key,
				begin_sort_key,
				filter,
				limit,
				enumeration_order,
				true,
			)
			.record_duration(
				&self.data.metrics.get_request_duration,
				&self.data.metrics.attrs,
			)
			.with_context(Context::current_with_span(span))
			.await?;

		self.data
			.metrics
			.get_request_counter
			.add(1, &self.data.metrics.attrs);

		Ok(res)
	}

	async fn get_range_internal(
		self: &Arc<Self>,
		partition_key: &F::P,
		begin_sort_key: Option<F::S>,
		filter: Option<F::Filter>,
		limit: usize,
		enumeration_order: EnumerationOrder,
		monotonic_read: bool,
	) -> Result<Vec<F::E>, Error> {
		let hash = partition_key.hash();
		let who = self.data.replication.read_nodes(&hash)?;

		let rpc = TableRpc::<F>::ReadRange {
			partition: partition_key.clone(),
			begin_sort_key,
			filter,
			limit,
			enumeration_order,
		};

		let resps = self
			.system
			.rpc_helper()
			.try_call_many(
				&self.endpoint,
				&who,
				rpc,
				RequestStrategy::with_priority(PRIO_NORMAL)
					.with_quorum(self.data.replication.read_quorum()?),
			)
			.await?;

		let mut ret: BTreeMap<Vec<u8>, F::E> = BTreeMap::new();
		let mut to_repair = BTreeSet::new();
		{
			let mut all_entries: BTreeMap<Vec<u8>, Vec<F::E>> = BTreeMap::new();
			for resp in &resps {
				if let TableRpc::Update(entries) = resp {
					for entry_bytes in entries.iter() {
						let entry = self.data.decode_entry(entry_bytes.as_slice())?;
						let entry_key = self.data.tree_key(entry.partition_key(), entry.sort_key());
						all_entries.entry(entry_key).or_default().push(entry);
					}
				} else {
					return Err(Error::unexpected_rpc_message(resp));
				}
			}
			for (entry_key, entries) in all_entries {
				// Only some nodes store this entry; we must propagate it during repair
				if entries.len() < resps.len() {
					to_repair.insert(entry_key.clone());
				}
				// Merge all entries for this key together
				for entry in entries {
					match ret.get_mut(&entry_key) {
						Some(e) => {
							if *e != entry {
								e.merge(&entry);
								to_repair.insert(entry_key.clone());
							}
						}
						None => {
							ret.insert(entry_key.clone(), entry);
						}
					}
				}
			}
		}

		if monotonic_read && !to_repair.is_empty() {
			let to_repair: Vec<_> = to_repair
				.into_iter()
				.map(|k| ret.get(&k).unwrap())
				.collect();
			self.repair_on_read(&who, &to_repair).await?;
		}

		// At this point, the `ret` btreemap might contain more than `limit`
		// items, because nodes might have returned us each `limit` items
		// but for different keys. We have to take only the first `limit` items
		// in this map, in the specified enumeration order, for two reasons:
		// 1. To return to the user no more than the number of items that they requested
		// 2. To return only items for which we have a read quorum: we do not know
		//    that we have a read quorum for the items after the first `limit`
		//    of them
		let ret_vec = match enumeration_order {
			EnumerationOrder::Forward => ret
				.into_iter()
				.take(limit)
				.map(|(_k, v)| v)
				.collect::<Vec<_>>(),
			EnumerationOrder::Reverse => ret
				.into_iter()
				.rev()
				.take(limit)
				.map(|(_k, v)| v)
				.collect::<Vec<_>>(),
		};
		Ok(ret_vec)
	}

	pub fn get_local(
		self: &Arc<Self>,
		partition_key: &F::P,
		sort_key: &F::S,
	) -> Result<Option<F::E>, Error> {
		let bytes = self.data.read_entry(partition_key, sort_key)?;
		bytes.map(|b| self.data.decode_entry(&b)).transpose()
	}

	// =============== UTILITY FUNCTION FOR CLIENT OPERATIONS ===============

	pub async fn repair_on_read(&self, who: &[Uuid], what: &[&F::E]) -> Result<(), Error> {
		let what_enc = what
			.iter()
			.map(|v| Ok(Arc::new(ByteBuf::from(v.encode()?))))
			.collect::<Result<Vec<_>, Error>>()?;
		self.system
			.rpc_helper()
			.try_call_many(
				&self.endpoint,
				who,
				TableRpc::<F>::Update(what_enc),
				RequestStrategy::with_priority(PRIO_NORMAL).with_quorum(who.len()),
			)
			.await?;
		Ok(())
	}
}

impl<F: TableSchema, R: TableReplication> EndpointHandler<TableRpc<F>> for Table<F, R> {
	async fn handle(
		self: &Arc<Self>,
		msg: &TableRpc<F>,
		_from: NodeID,
	) -> Result<TableRpc<F>, Error> {
		match msg {
			TableRpc::ReadEntry(key, sort_key) => {
				let value = self.data.read_entry(key, sort_key)?;
				Ok(TableRpc::ReadEntryResponse(value))
			}
			TableRpc::ReadRange {
				partition,
				begin_sort_key,
				filter,
				limit,
				enumeration_order,
			} => {
				let values = self.data.read_range(
					partition,
					begin_sort_key,
					filter,
					*limit,
					*enumeration_order,
				)?;
				Ok(TableRpc::Update(values))
			}
			TableRpc::Update(pairs) => {
				self.data.update_many(pairs)?;
				Ok(TableRpc::Ok)
			}
			m => Err(Error::unexpected_rpc_message(m)),
		}
	}
}
