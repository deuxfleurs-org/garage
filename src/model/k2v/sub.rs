use std::collections::HashMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

use crate::k2v::history_table::*;
use crate::k2v::item_table::*;

#[derive(Debug, Hash, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PollKey {
	pub partition: K2VItemPartition,
	pub sort_key: String,
}

#[derive(Debug, Hash, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PollRange {
	pub partition: K2VItemPartition,
	pub prefix: Option<String>,
	pub start: Option<String>,
	pub end: Option<String>,
}

#[derive(Default)]
pub struct SubscriptionManager {
	item_subscriptions: Mutex<HashMap<PollKey, broadcast::Sender<K2VItem>>>,
	range_subscriptions: Mutex<HashMap<PollRange, broadcast::Sender<K2VHistoryEntry>>>,
}

impl SubscriptionManager {
	pub fn new() -> Self {
		Self::default()
	}

	// ---- simple item polling ----

	pub fn subscribe_item(&self, key: &PollKey) -> broadcast::Receiver<K2VItem> {
		let mut subs = self.item_subscriptions.lock().unwrap();
		if let Some(s) = subs.get(key) {
			s.subscribe()
		} else {
			let (tx, rx) = broadcast::channel(8);
			subs.insert(key.clone(), tx);
			rx
		}
	}

	pub fn notify_item(&self, item: &K2VItem) {
		let key = PollKey {
			partition: item.partition.clone(),
			sort_key: item.sort_key.clone(),
		};
		let mut subs = self.item_subscriptions.lock().unwrap();
		if let Some(s) = subs.get(&key) {
			if s.send(item.clone()).is_err() {
				// no more subscribers, remove channel from here
				// (we will re-create it later if we need to subscribe again)
				subs.remove(&key);
			}
		}
	}

	// ---- range polling ----

	pub fn subscribe_range(&self, key: &PollRange) -> broadcast::Receiver<K2VHistoryEntry> {
		let mut subs = self.range_subscriptions.lock().unwrap();
		if let Some(s) = subs.get(key) {
			s.subscribe()
		} else {
			let (tx, rx) = broadcast::channel(8);
			subs.insert(key.clone(), tx);
			rx
		}
	}

	pub fn notify_range(&self, entry: &K2VHistoryEntry) {
		let mut subs = self.range_subscriptions.lock().unwrap();
		let mut dead_subs = vec![];

		for (sub, chan) in subs.iter() {
			if sub.matches(&entry) {
				if chan.send(entry.clone()).is_err() {
					dead_subs.push(sub.clone());
				}
			} else if chan.receiver_count() == 0 {
				dead_subs.push(sub.clone());
			}
		}

		for sub in dead_subs.iter() {
			subs.remove(sub);
		}
	}
}

impl PollRange {
	fn matches(&self, entry: &K2VHistoryEntry) -> bool {
		entry.ins_item.partition == self.partition
			&& self
				.prefix
				.as_ref()
				.map(|x| entry.ins_item.sort_key.starts_with(x))
				.unwrap_or(true)
			&& self
				.start
				.as_ref()
				.map(|x| entry.ins_item.sort_key >= *x)
				.unwrap_or(true)
			&& self
				.end
				.as_ref()
				.map(|x| entry.ins_item.sort_key < *x)
				.unwrap_or(true)
	}
}
