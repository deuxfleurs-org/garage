use std::sync::Arc;

use garage_db as db;

use garage_table::crdt::*;
use garage_table::*;

use crate::k2v::poll::*;

mod v08 {
	use crate::k2v::causality::K2VNodeId;
	pub use crate::k2v::item_table::v08::{DvvsValue, K2VItemPartition};
	use garage_util::crdt;
	use serde::{Deserialize, Serialize};

	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct K2VHistoryEntry {
		/// Partition key: a K2V partition
		pub partition: K2VItemPartition,
		/// Sort key: the node ID and its local counter
		pub node_counter: K2VHistorySortKey,

		/// The value of the node's local counter before this entry was updated
		pub prev_counter: u64,
		/// The timesamp of the update (!= counter, counters are incremented
		/// by one, timestamps are real clock timestamps)
		pub timestamp: u64,
		/// The sort key of the item that was inserted
		pub ins_sort_key: String,
		/// The inserted value
		pub ins_value: DvvsValue,

		/// Whether this history entry is too old and should be deleted
		pub deleted: crdt::Bool,
	}

	#[derive(PartialEq, Eq, Clone, Debug, Serialize, Deserialize)]
	pub struct K2VHistorySortKey {
		pub node: K2VNodeId,
		pub counter: u64,
	}

	impl garage_util::migrate::InitialFormat for K2VHistoryEntry {
		const VERSION_MARKER: &'static [u8] = b"Gk2vhe08";
	}
}

pub use v08::*;

impl Crdt for K2VHistoryEntry {
	fn merge(&mut self, other: &Self) {
		self.deleted.merge(&other.deleted);
	}
}

impl SortKey for K2VHistorySortKey {
	type B<'a> = [u8; 16];

	fn sort_key(&self) -> [u8; 16] {
		let mut ret = [0u8; 16];
		ret[0..8].copy_from_slice(&u64::to_be_bytes(self.node));
		ret[8..16].copy_from_slice(&u64::to_be_bytes(self.counter));
		ret
	}
}

impl Entry<K2VItemPartition, K2VHistorySortKey> for K2VHistoryEntry {
	fn partition_key(&self) -> &K2VItemPartition {
		&self.partition
	}
	fn sort_key(&self) -> &K2VHistorySortKey {
		&self.node_counter
	}
	fn is_tombstone(&self) -> bool {
		self.deleted.get()
	}
}

pub struct K2VHistoryTable {
	pub(crate) subscriptions: Arc<SubscriptionManager>,
}

impl TableSchema for K2VHistoryTable {
	const TABLE_NAME: &'static str = "k2v_history";

	type P = K2VItemPartition;
	type S = K2VHistorySortKey;
	type E = K2VHistoryEntry;
	type Filter = DeletedFilter;

	fn updated(
		&self,
		_tx: &mut db::Transaction,
		_old: Option<&Self::E>,
		new: Option<&Self::E>,
	) -> db::TxOpResult<()> {
		if let Some(new_ent) = new {
			self.subscriptions.notify_range(new_ent);
		}

		Ok(())
	}

	fn matches_filter(entry: &Self::E, filter: &Self::Filter) -> bool {
		filter.apply(entry.deleted.get())
	}
}
