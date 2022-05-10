use std::collections::{hash_map, BTreeMap, HashMap};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, watch};

use garage_rpc::ring::Ring;
use garage_rpc::system::System;
use garage_util::data::*;
use garage_util::error::*;

use garage_table::crdt::*;
use garage_table::replication::TableShardedReplication;
use garage_table::*;

pub trait CounterSchema: Clone + PartialEq + Send + Sync + 'static {
	const NAME: &'static str;
	type P: PartitionKey + Clone + PartialEq + Serialize + for<'de> Deserialize<'de> + Send + Sync;
	type S: SortKey + Clone + PartialEq + Serialize + for<'de> Deserialize<'de> + Send + Sync;
}

/// A counter entry in the global table
#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct CounterEntry<T: CounterSchema> {
	pub pk: T::P,
	pub sk: T::S,
	pub values: BTreeMap<String, CounterValue>,
}

impl<T: CounterSchema> Entry<T::P, T::S> for CounterEntry<T> {
	fn partition_key(&self) -> &T::P {
		&self.pk
	}
	fn sort_key(&self) -> &T::S {
		&self.sk
	}
	fn is_tombstone(&self) -> bool {
		self.values
			.iter()
			.all(|(_, v)| v.node_values.iter().all(|(_, (_, v))| *v == 0))
	}
}

impl<T: CounterSchema> CounterEntry<T> {
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

impl<T: CounterSchema> Crdt for CounterEntry<T> {
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

pub struct CounterTable<T: CounterSchema> {
	_phantom_t: PhantomData<T>,
}

impl<T: CounterSchema> TableSchema for CounterTable<T> {
	const TABLE_NAME: &'static str = T::NAME;

	type P = T::P;
	type S = T::S;
	type E = CounterEntry<T>;
	type Filter = (DeletedFilter, Vec<Uuid>);

	fn updated(&self, _old: Option<&Self::E>, _new: Option<&Self::E>) {
		// nothing for now
	}

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

pub struct IndexCounter<T: CounterSchema> {
	this_node: Uuid,
	local_counter: sled::Tree,
	propagate_tx: mpsc::UnboundedSender<(T::P, T::S, LocalCounterEntry)>,
	pub table: Arc<Table<CounterTable<T>, TableShardedReplication>>,
}

impl<T: CounterSchema> IndexCounter<T> {
	pub fn new(
		system: Arc<System>,
		replication: TableShardedReplication,
		db: &sled::Db,
	) -> Arc<Self> {
		let background = system.background.clone();

		let (propagate_tx, propagate_rx) = mpsc::unbounded_channel();

		let this = Arc::new(Self {
			this_node: system.id,
			local_counter: db
				.open_tree(format!("local_counter:{}", T::NAME))
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
			format!("{} index counter propagator", T::NAME),
			move |must_exit| this2.clone().propagate_loop(propagate_rx, must_exit),
		);
		this
	}

	pub fn count(&self, pk: &T::P, sk: &T::S, counts: &[(&str, i64)]) -> Result<(), Error> {
		let tree_key = self.table.data.tree_key(pk, sk);

		let new_entry = self.local_counter.transaction(|tx| {
			let mut entry = match tx.get(&tree_key[..])? {
				Some(old_bytes) => {
					rmp_serde::decode::from_read_ref::<_, LocalCounterEntry>(&old_bytes)
						.map_err(Error::RmpDecode)
						.map_err(sled::transaction::ConflictableTransactionError::Abort)?
				}
				None => LocalCounterEntry {
					values: BTreeMap::new(),
				},
			};

			for (s, inc) in counts.iter() {
				let mut ent = entry.values.entry(s.to_string()).or_insert((0, 0));
				ent.0 += 1;
				ent.1 += *inc;
			}

			let new_entry_bytes = rmp_to_vec_all_named(&entry)
				.map_err(Error::RmpEncode)
				.map_err(sled::transaction::ConflictableTransactionError::Abort)?;
			tx.insert(&tree_key[..], new_entry_bytes)?;

			Ok(entry)
		})?;

		if let Err(e) = self.propagate_tx.send((pk.clone(), sk.clone(), new_entry)) {
			error!(
				"Could not propagate updated counter values, failed to send to channel: {}",
				e
			);
		}

		Ok(())
	}

	async fn propagate_loop(
		self: Arc<Self>,
		mut propagate_rx: mpsc::UnboundedReceiver<(T::P, T::S, LocalCounterEntry)>,
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
				let dist_entry = counters.into_counter_entry::<T>(self.this_node, pk, sk);
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
						error!("({}) Could not propagate {} counter values: {}, these counters will not be updated correctly.", T::NAME, buf.len(), e);
						break;
					}
					warn!("({}) Could not propagate {} counter values: {}, retrying in 5 seconds (retry #{})", T::NAME, buf.len(), e, errors);
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
}

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
struct LocalCounterEntry {
	values: BTreeMap<String, (u64, i64)>,
}

impl LocalCounterEntry {
	fn into_counter_entry<T: CounterSchema>(
		self,
		this_node: Uuid,
		pk: T::P,
		sk: T::S,
	) -> CounterEntry<T> {
		CounterEntry {
			pk,
			sk,
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
