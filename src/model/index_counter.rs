use core::ops::Bound;
use std::collections::{hash_map, BTreeMap, HashMap};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};

use garage_db as db;

use garage_rpc::ring::Ring;
use garage_rpc::system::System;
use garage_util::data::*;
use garage_util::error::*;
use garage_util::time::*;

use garage_table::crdt::*;
use garage_table::replication::*;
use garage_table::*;

pub trait CountedItem: Clone + PartialEq + Send + Sync + 'static {
	const COUNTER_TABLE_NAME: &'static str;

	type CP: PartitionKey + Clone + PartialEq + Serialize + for<'de> Deserialize<'de> + Send + Sync;
	type CS: SortKey + Clone + PartialEq + Serialize + for<'de> Deserialize<'de> + Send + Sync;

	fn counter_partition_key(&self) -> &Self::CP;
	fn counter_sort_key(&self) -> &Self::CS;
	fn counts(&self) -> Vec<(&'static str, i64)>;
}

/// A counter entry in the global table
#[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
pub struct CounterEntry<T: CountedItem> {
	pub pk: T::CP,
	pub sk: T::CS,
	pub values: BTreeMap<String, CounterValue>,
}

impl<T: CountedItem> Entry<T::CP, T::CS> for CounterEntry<T> {
	fn partition_key(&self) -> &T::CP {
		&self.pk
	}
	fn sort_key(&self) -> &T::CS {
		&self.sk
	}
	fn is_tombstone(&self) -> bool {
		self.values
			.iter()
			.all(|(_, v)| v.node_values.iter().all(|(_, (_, v))| *v == 0))
	}
}

impl<T: CountedItem> CounterEntry<T> {
	pub fn filtered_values(&self, ring: &Ring) -> HashMap<String, i64> {
		let nodes = &ring.layout.node_id_vec[..];
		self.filtered_values_with_nodes(nodes)
	}

	pub fn filtered_values_with_nodes(&self, nodes: &[Uuid]) -> HashMap<String, i64> {
		let mut ret = HashMap::new();
		for (name, vals) in self.values.iter() {
			let new_vals = vals
				.node_values
				.iter()
				.filter(|(n, _)| nodes.contains(n))
				.map(|(_, (_, v))| *v)
				.collect::<Vec<_>>();
			if !new_vals.is_empty() {
				ret.insert(
					name.clone(),
					new_vals.iter().fold(i64::MIN, |a, b| std::cmp::max(a, *b)),
				);
			}
		}

		ret
	}
}

/// A counter entry in the global table
#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct CounterValue {
	pub node_values: BTreeMap<Uuid, (u64, i64)>,
}

impl<T: CountedItem> Crdt for CounterEntry<T> {
	fn merge(&mut self, other: &Self) {
		for (name, e2) in other.values.iter() {
			if let Some(e) = self.values.get_mut(name) {
				e.merge(e2);
			} else {
				self.values.insert(name.clone(), e2.clone());
			}
		}
	}
}

impl Crdt for CounterValue {
	fn merge(&mut self, other: &Self) {
		for (node, (t2, e2)) in other.node_values.iter() {
			if let Some((t, e)) = self.node_values.get_mut(node) {
				if t2 > t {
					*e = *e2;
				}
			} else {
				self.node_values.insert(*node, (*t2, *e2));
			}
		}
	}
}

pub struct CounterTable<T: CountedItem> {
	_phantom_t: PhantomData<T>,
}

impl<T: CountedItem> TableSchema for CounterTable<T> {
	const TABLE_NAME: &'static str = T::COUNTER_TABLE_NAME;

	type P = T::CP;
	type S = T::CS;
	type E = CounterEntry<T>;
	type Filter = (DeletedFilter, Vec<Uuid>);

	fn matches_filter(entry: &Self::E, filter: &Self::Filter) -> bool {
		if filter.0 == DeletedFilter::Any {
			return true;
		}

		let is_tombstone = entry
			.filtered_values_with_nodes(&filter.1[..])
			.iter()
			.all(|(_, v)| *v == 0);
		filter.0.apply(is_tombstone)
	}
}

// ----

pub struct IndexCounter<T: CountedItem> {
	this_node: Uuid,
	local_counter: db::Tree,
	propagate_tx: mpsc::UnboundedSender<(T::CP, T::CS, LocalCounterEntry<T>)>,
	pub table: Arc<Table<CounterTable<T>, TableShardedReplication>>,
}

impl<T: CountedItem> IndexCounter<T> {
	pub fn new(
		system: Arc<System>,
		replication: TableShardedReplication,
		db: &db::Db,
	) -> Arc<Self> {
		let background = system.background.clone();

		let (propagate_tx, propagate_rx) = mpsc::unbounded_channel();

		let this = Arc::new(Self {
			this_node: system.id,
			local_counter: db
				.open_tree(format!("local_counter_v2:{}", T::COUNTER_TABLE_NAME))
				.expect("Unable to open local counter tree"),
			propagate_tx,
			table: Table::new(
				CounterTable {
					_phantom_t: Default::default(),
				},
				replication,
				system,
				db,
			),
		});

		let this2 = this.clone();
		background.spawn_worker(
			format!("{} index counter propagator", T::COUNTER_TABLE_NAME),
			move |must_exit| this2.clone().propagate_loop(propagate_rx, must_exit),
		);
		this
	}

	pub fn count(
		&self,
		tx: &mut db::Transaction,
		old: Option<&T>,
		new: Option<&T>,
	) -> db::TxResult<(), Error> {
		let pk = old
			.map(|e| e.counter_partition_key())
			.unwrap_or_else(|| new.unwrap().counter_partition_key());
		let sk = old
			.map(|e| e.counter_sort_key())
			.unwrap_or_else(|| new.unwrap().counter_sort_key());

		// calculate counter differences
		let mut counts = HashMap::new();
		for (k, v) in old.map(|x| x.counts()).unwrap_or_default() {
			*counts.entry(k).or_insert(0) -= v;
		}
		for (k, v) in new.map(|x| x.counts()).unwrap_or_default() {
			*counts.entry(k).or_insert(0) += v;
		}

		// update local counter table
		let tree_key = self.table.data.tree_key(pk, sk);

		let mut entry = match tx.get(&self.local_counter, &tree_key[..])? {
			Some(old_bytes) => {
				rmp_serde::decode::from_read_ref::<_, LocalCounterEntry<T>>(&old_bytes)
					.map_err(Error::RmpDecode)
					.map_err(db::TxError::Abort)?
			}
			None => LocalCounterEntry {
				pk: pk.clone(),
				sk: sk.clone(),
				values: BTreeMap::new(),
			},
		};

		let now = now_msec();
		for (s, inc) in counts.iter() {
			let mut ent = entry.values.entry(s.to_string()).or_insert((0, 0));
			ent.0 = std::cmp::max(ent.0 + 1, now);
			ent.1 += *inc;
		}

		let new_entry_bytes = rmp_to_vec_all_named(&entry)
			.map_err(Error::RmpEncode)
			.map_err(db::TxError::Abort)?;
		tx.insert(&self.local_counter, &tree_key[..], new_entry_bytes)?;

		if let Err(e) = self.propagate_tx.send((pk.clone(), sk.clone(), entry)) {
			error!(
				"Could not propagate updated counter values, failed to send to channel: {}",
				e
			);
		}

		Ok(())
	}

	async fn propagate_loop(
		self: Arc<Self>,
		mut propagate_rx: mpsc::UnboundedReceiver<(T::CP, T::CS, LocalCounterEntry<T>)>,
		must_exit: watch::Receiver<bool>,
	) {
		// This loop batches updates to counters to be sent all at once.
		// They are sent once the propagate_rx channel has been emptied (or is closed).
		let mut buf = HashMap::new();
		let mut errors = 0;

		loop {
			let (ent, closed) = match propagate_rx.try_recv() {
				Ok(ent) => (Some(ent), false),
				Err(mpsc::error::TryRecvError::Empty) if buf.is_empty() => {
					match propagate_rx.recv().await {
						Some(ent) => (Some(ent), false),
						None => (None, true),
					}
				}
				Err(mpsc::error::TryRecvError::Empty) => (None, false),
				Err(mpsc::error::TryRecvError::Disconnected) => (None, true),
			};

			if let Some((pk, sk, counters)) = ent {
				let tree_key = self.table.data.tree_key(&pk, &sk);
				let dist_entry = counters.into_counter_entry(self.this_node);
				match buf.entry(tree_key) {
					hash_map::Entry::Vacant(e) => {
						e.insert(dist_entry);
					}
					hash_map::Entry::Occupied(mut e) => {
						e.get_mut().merge(&dist_entry);
					}
				}
				// As long as we can add entries, loop back and add them to batch
				// before sending batch to other nodes
				continue;
			}

			if !buf.is_empty() {
				let entries = buf.iter().map(|(_k, v)| v);
				if let Err(e) = self.table.insert_many(entries).await {
					errors += 1;
					if errors >= 2 && *must_exit.borrow() {
						error!("({}) Could not propagate {} counter values: {}, these counters will not be updated correctly.", T::COUNTER_TABLE_NAME, buf.len(), e);
						break;
					}
					warn!("({}) Could not propagate {} counter values: {}, retrying in 5 seconds (retry #{})", T::COUNTER_TABLE_NAME, buf.len(), e, errors);
					tokio::time::sleep(Duration::from_secs(5)).await;
					continue;
				}

				buf.clear();
				errors = 0;
			}

			if closed || *must_exit.borrow() {
				break;
			}
		}
	}

	pub fn offline_recount_all<TS, TR>(
		&self,
		counted_table: &Arc<Table<TS, TR>>,
	) -> Result<(), Error>
	where
		TS: TableSchema<E = T>,
		TR: TableReplication,
	{
		let save_counter_entry = |entry: CounterEntry<T>| -> Result<(), Error> {
			let entry_k = self
				.table
				.data
				.tree_key(entry.partition_key(), entry.sort_key());
			self.table
				.data
				.update_entry_with(&entry_k, |ent| match ent {
					Some(mut ent) => {
						ent.merge(&entry);
						ent
					}
					None => entry.clone(),
				})?;
			Ok(())
		};

		// 1. Set all old local counters to zero
		let now = now_msec();
		let mut next_start: Option<Vec<u8>> = None;
		loop {
			let low_bound = match next_start.take() {
				Some(v) => Bound::Excluded(v),
				None => Bound::Unbounded,
			};
			let mut batch = vec![];
			for item in self.local_counter.range((low_bound, Bound::Unbounded))? {
				batch.push(item?);
				if batch.len() > 1000 {
					break;
				}
			}

			if batch.is_empty() {
				break;
			}

			info!("zeroing old counters... ({})", hex::encode(&batch[0].0));
			for (local_counter_k, local_counter) in batch {
				let mut local_counter =
					rmp_serde::decode::from_read_ref::<_, LocalCounterEntry<T>>(&local_counter)?;

				for (_, tv) in local_counter.values.iter_mut() {
					tv.0 = std::cmp::max(tv.0 + 1, now);
					tv.1 = 0;
				}

				let local_counter_bytes = rmp_to_vec_all_named(&local_counter)?;
				self.local_counter
					.insert(&local_counter_k, &local_counter_bytes)?;

				let counter_entry = local_counter.into_counter_entry(self.this_node);
				save_counter_entry(counter_entry)?;

				next_start = Some(local_counter_k);
			}
		}

		// 2. Recount all table entries
		let now = now_msec();
		let mut next_start: Option<Vec<u8>> = None;
		loop {
			let low_bound = match next_start.take() {
				Some(v) => Bound::Excluded(v),
				None => Bound::Unbounded,
			};
			let mut batch = vec![];
			for item in counted_table
				.data
				.store
				.range((low_bound, Bound::Unbounded))?
			{
				batch.push(item?);
				if batch.len() > 1000 {
					break;
				}
			}

			if batch.is_empty() {
				break;
			}

			info!("counting entries... ({})", hex::encode(&batch[0].0));
			for (counted_entry_k, counted_entry) in batch {
				let counted_entry = counted_table.data.decode_entry(&counted_entry)?;

				let pk = counted_entry.counter_partition_key();
				let sk = counted_entry.counter_sort_key();
				let counts = counted_entry.counts();

				let local_counter_key = self.table.data.tree_key(pk, sk);
				let mut local_counter = match self.local_counter.get(&local_counter_key)? {
					Some(old_bytes) => {
						let ent = rmp_serde::decode::from_read_ref::<_, LocalCounterEntry<T>>(
							&old_bytes,
						)?;
						assert!(ent.pk == *pk);
						assert!(ent.sk == *sk);
						ent
					}
					None => LocalCounterEntry {
						pk: pk.clone(),
						sk: sk.clone(),
						values: BTreeMap::new(),
					},
				};
				for (s, v) in counts.iter() {
					let mut tv = local_counter.values.entry(s.to_string()).or_insert((0, 0));
					tv.0 = std::cmp::max(tv.0 + 1, now);
					tv.1 += v;
				}

				let local_counter_bytes = rmp_to_vec_all_named(&local_counter)?;
				self.local_counter
					.insert(&local_counter_key, local_counter_bytes)?;

				let counter_entry = local_counter.into_counter_entry(self.this_node);
				save_counter_entry(counter_entry)?;

				next_start = Some(counted_entry_k);
			}
		}

		// Done
		Ok(())
	}
}

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
struct LocalCounterEntry<T: CountedItem> {
	pk: T::CP,
	sk: T::CS,
	values: BTreeMap<String, (u64, i64)>,
}

impl<T: CountedItem> LocalCounterEntry<T> {
	fn into_counter_entry(self, this_node: Uuid) -> CounterEntry<T> {
		CounterEntry {
			pk: self.pk,
			sk: self.sk,
			values: self
				.values
				.into_iter()
				.map(|(name, (ts, v))| {
					let mut node_values = BTreeMap::new();
					node_values.insert(this_node, (ts, v));
					(name, CounterValue { node_values })
				})
				.collect(),
		}
	}
}
