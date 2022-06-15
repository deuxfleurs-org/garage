use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

use garage_db as db;
use garage_util::data::*;

use garage_table::crdt::*;
use garage_table::*;

use crate::index_counter::*;
use crate::k2v::causality::*;
use crate::k2v::poll::*;

pub const ENTRIES: &str = "entries";
pub const CONFLICTS: &str = "conflicts";
pub const VALUES: &str = "values";
pub const BYTES: &str = "bytes";

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub struct K2VItem {
	pub partition: K2VItemPartition,
	pub sort_key: String,

	items: BTreeMap<K2VNodeId, DvvsEntry>,
}

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize, Hash, Eq)]
pub struct K2VItemPartition {
	pub bucket_id: Uuid,
	pub partition_key: String,
}

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
struct DvvsEntry {
	t_discard: u64,
	values: Vec<(u64, DvvsValue)>,
}

#[derive(PartialEq, Clone, Debug, Serialize, Deserialize)]
pub enum DvvsValue {
	Value(#[serde(with = "serde_bytes")] Vec<u8>),
	Deleted,
}

impl K2VItem {
	/// Creates a new K2VItem when no previous entry existed in the db
	pub fn new(bucket_id: Uuid, partition_key: String, sort_key: String) -> Self {
		Self {
			partition: K2VItemPartition {
				bucket_id,
				partition_key,
			},
			sort_key,
			items: BTreeMap::new(),
		}
	}
	/// Updates a K2VItem with a new value or a deletion event
	pub fn update(
		&mut self,
		this_node: Uuid,
		context: &Option<CausalContext>,
		new_value: DvvsValue,
	) {
		if let Some(context) = context {
			for (node, t_discard) in context.vector_clock.iter() {
				if let Some(e) = self.items.get_mut(node) {
					e.t_discard = std::cmp::max(e.t_discard, *t_discard);
				} else {
					self.items.insert(
						*node,
						DvvsEntry {
							t_discard: *t_discard,
							values: vec![],
						},
					);
				}
			}
		}

		self.discard();

		let node_id = make_node_id(this_node);
		let e = self.items.entry(node_id).or_insert(DvvsEntry {
			t_discard: 0,
			values: vec![],
		});
		let t_prev = e.max_time();
		e.values.push((t_prev + 1, new_value));
	}

	/// Extract the causality context of a K2V Item
	pub fn causal_context(&self) -> CausalContext {
		let mut cc = CausalContext::new_empty();
		for (node, ent) in self.items.iter() {
			cc.vector_clock.insert(*node, ent.max_time());
		}
		cc
	}

	/// Extract the list of values
	pub fn values(&'_ self) -> Vec<&'_ DvvsValue> {
		let mut ret = vec![];
		for (_, ent) in self.items.iter() {
			for (_, v) in ent.values.iter() {
				if !ret.contains(&v) {
					ret.push(v);
				}
			}
		}
		ret
	}

	fn discard(&mut self) {
		for (_, ent) in self.items.iter_mut() {
			ent.discard();
		}
	}
}

impl DvvsEntry {
	fn max_time(&self) -> u64 {
		self.values
			.iter()
			.fold(self.t_discard, |acc, (vts, _)| std::cmp::max(acc, *vts))
	}

	fn discard(&mut self) {
		self.values = std::mem::take(&mut self.values)
			.into_iter()
			.filter(|(t, _)| *t > self.t_discard)
			.collect::<Vec<_>>();
	}
}

impl Crdt for K2VItem {
	fn merge(&mut self, other: &Self) {
		for (node, e2) in other.items.iter() {
			if let Some(e) = self.items.get_mut(node) {
				e.merge(e2);
			} else {
				self.items.insert(*node, e2.clone());
			}
		}
	}
}

impl Crdt for DvvsEntry {
	fn merge(&mut self, other: &Self) {
		self.t_discard = std::cmp::max(self.t_discard, other.t_discard);
		self.discard();

		let t_max = self.max_time();
		for (vt, vv) in other.values.iter() {
			if *vt > t_max {
				self.values.push((*vt, vv.clone()));
			}
		}
	}
}

impl PartitionKey for K2VItemPartition {
	fn hash(&self) -> Hash {
		use blake2::{Blake2b, Digest};

		let mut hasher = Blake2b::new();
		hasher.update(self.bucket_id.as_slice());
		hasher.update(self.partition_key.as_bytes());
		let mut hash = [0u8; 32];
		hash.copy_from_slice(&hasher.finalize()[..32]);
		hash.into()
	}
}

impl Entry<K2VItemPartition, String> for K2VItem {
	fn partition_key(&self) -> &K2VItemPartition {
		&self.partition
	}
	fn sort_key(&self) -> &String {
		&self.sort_key
	}
	fn is_tombstone(&self) -> bool {
		self.values()
			.iter()
			.all(|v| matches!(v, DvvsValue::Deleted))
	}
}

pub struct K2VItemTable {
	pub(crate) counter_table: Arc<IndexCounter<K2VItem>>,
	pub(crate) subscriptions: Arc<SubscriptionManager>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct ItemFilter {
	pub exclude_only_tombstones: bool,
	pub conflicts_only: bool,
}

impl TableSchema for K2VItemTable {
	const TABLE_NAME: &'static str = "k2v_item";

	type P = K2VItemPartition;
	type S = String;
	type E = K2VItem;
	type Filter = ItemFilter;

	fn updated(
		&self,
		tx: &mut db::Transaction,
		old: Option<&Self::E>,
		new: Option<&Self::E>,
	) -> db::TxOpResult<()> {
		// 1. Count
		let counter_res = self.counter_table.count(tx, old, new);
		if let Err(e) = db::unabort(counter_res)? {
			// This result can be returned by `counter_table.count()` for instance
			// if messagepack serialization or deserialization fails at some step.
			// Warn admin but ignore this error for now, that's all we can do.
			error!(
				"Unable to update K2V item counter: {}. Index values will be wrong!",
				e
			);
		}

		// 2. Notify
		if let Some(new_ent) = new {
			self.subscriptions.notify(new_ent);
		}

		Ok(())
	}

	#[allow(clippy::nonminimal_bool)]
	fn matches_filter(entry: &Self::E, filter: &Self::Filter) -> bool {
		let v = entry.values();
		!(filter.conflicts_only && v.len() < 2)
			&& !(filter.exclude_only_tombstones && entry.is_tombstone())
	}
}

impl CountedItem for K2VItem {
	const COUNTER_TABLE_NAME: &'static str = "k2v_index_counter_v2";

	// Partition key = bucket id
	type CP = Uuid;
	// Sort key = K2V item's partition key
	type CS = String;

	fn counter_partition_key(&self) -> &Uuid {
		&self.partition.bucket_id
	}
	fn counter_sort_key(&self) -> &String {
		&self.partition.partition_key
	}

	fn counts(&self) -> Vec<(&'static str, i64)> {
		let values = self.values();

		let n_entries = if self.is_tombstone() { 0 } else { 1 };
		let n_conflicts = if values.len() > 1 { 1 } else { 0 };
		let n_values = values
			.iter()
			.filter(|v| matches!(v, DvvsValue::Value(_)))
			.count() as i64;
		let n_bytes = values
			.iter()
			.map(|v| match v {
				DvvsValue::Deleted => 0,
				DvvsValue::Value(v) => v.len() as i64,
			})
			.sum();

		vec![
			(ENTRIES, n_entries),
			(CONFLICTS, n_conflicts),
			(VALUES, n_values),
			(BYTES, n_bytes),
		]
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn test_dvvsentry_merge_simple() {
		let e1 = DvvsEntry {
			t_discard: 4,
			values: vec![
				(5, DvvsValue::Value(vec![15])),
				(6, DvvsValue::Value(vec![16])),
			],
		};
		let e2 = DvvsEntry {
			t_discard: 5,
			values: vec![(6, DvvsValue::Value(vec![16])), (7, DvvsValue::Deleted)],
		};

		let mut e3 = e1.clone();
		e3.merge(&e2);
		assert_eq!(e2, e3);
	}
}
