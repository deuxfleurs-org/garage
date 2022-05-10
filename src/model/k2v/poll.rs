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

#[derive(Default)]
pub struct SubscriptionManager {
	subscriptions: Mutex<HashMap<PollKey, broadcast::Sender<K2VItem>>>,
}

impl SubscriptionManager {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn subscribe(&self, key: &PollKey) -> broadcast::Receiver<K2VItem> {
		let mut subs = self.subscriptions.lock().unwrap();
		if let Some(s) = subs.get(key) {
			s.subscribe()
		} else {
			let (tx, rx) = broadcast::channel(8);
			subs.insert(key.clone(), tx);
			rx
		}
	}

	pub fn notify(&self, item: &K2VItem) {
		let key = PollKey {
			partition: item.partition.clone(),
			sort_key: item.sort_key.clone(),
		};
		let mut subs = self.subscriptions.lock().unwrap();
		if let Some(s) = subs.get(&key) {
			if s.send(item.clone()).is_err() {
				// no more subscribers, remove channel from here
				// (we will re-create it later if we need to subscribe again)
				subs.remove(&key);
			}
		}
	}
}
