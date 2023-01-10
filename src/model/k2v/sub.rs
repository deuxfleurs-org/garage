use std::collections::HashMap;
use std::sync::Mutex;

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;

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
pub struct SubscriptionManager(Mutex<SubscriptionManagerInner>);

#[derive(Default)]
pub struct SubscriptionManagerInner {
	item_subscriptions: HashMap<PollKey, broadcast::Sender<K2VItem>>,
	part_subscriptions: HashMap<K2VItemPartition, broadcast::Sender<K2VItem>>,
}

impl SubscriptionManager {
	pub fn new() -> Self {
		Self::default()
	}

	pub(crate) fn subscribe_item(&self, key: &PollKey) -> broadcast::Receiver<K2VItem> {
		let mut inner = self.0.lock().unwrap();
		if let Some(s) = inner.item_subscriptions.get(key) {
			s.subscribe()
		} else {
			let (tx, rx) = broadcast::channel(8);
			inner.item_subscriptions.insert(key.clone(), tx);
			rx
		}
	}

	pub(crate) fn subscribe_partition(
		&self,
		part: &K2VItemPartition,
	) -> broadcast::Receiver<K2VItem> {
		let mut inner = self.0.lock().unwrap();
		if let Some(s) = inner.part_subscriptions.get(part) {
			s.subscribe()
		} else {
			let (tx, rx) = broadcast::channel(8);
			inner.part_subscriptions.insert(part.clone(), tx);
			rx
		}
	}

	pub(crate) fn notify(&self, item: &K2VItem) {
		let mut inner = self.0.lock().unwrap();

		// 1. Notify single item subscribers,
		// removing subscriptions with no more listeners if any
		let key = PollKey {
			partition: item.partition.clone(),
			sort_key: item.sort_key.clone(),
		};
		if let Some(s) = inner.item_subscriptions.get(&key) {
			if s.send(item.clone()).is_err() {
				// no more subscribers, remove channel from here
				// (we will re-create it later if we need to subscribe again)
				inner.item_subscriptions.remove(&key);
			}
		}

		// 2. Notify partition subscribers,
		// removing subscriptions with no more listeners if any
		if let Some(s) = inner.part_subscriptions.get(&item.partition) {
			if s.send(item.clone()).is_err() {
				// no more subscribers, remove channel from here
				// (we will re-create it later if we need to subscribe again)
				inner.part_subscriptions.remove(&item.partition);
			}
		}
	}
}

impl PollRange {
	pub fn matches(&self, item: &K2VItem) -> bool {
		item.partition == self.partition
			&& self
				.prefix
				.as_ref()
				.map(|x| item.sort_key.starts_with(x))
				.unwrap_or(true)
			&& self
				.start
				.as_ref()
				.map(|x| item.sort_key >= *x)
				.unwrap_or(true)
			&& self
				.end
				.as_ref()
				.map(|x| item.sort_key < *x)
				.unwrap_or(true)
	}
}
