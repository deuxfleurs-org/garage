//! Module that implements RPCs specific to K2V.
//! This is necessary for insertions into the K2V store,
//! as they have to be transmitted to one of the nodes responsible
//! for storing the entry to be processed (the API entry
//! node does not process the entry directly, as this would
//! mean the vector clock gets much larger than needed).

use std::collections::{BTreeMap, HashMap};
use std::convert::TryInto;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::{Duration, Instant};

use futures::stream::FuturesUnordered;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tokio::select;

use garage_db as db;

use garage_util::crdt::*;
use garage_util::data::*;
use garage_util::error::*;
use garage_util::time::now_msec;

use garage_rpc::system::System;
use garage_rpc::*;

use garage_table::replication::{TableReplication, TableShardedReplication};
use garage_table::{PartitionKey, Table};

use crate::helper::error::Error as HelperError;
use crate::k2v::causality::*;
use crate::k2v::item_table::*;
use crate::k2v::seen::*;
use crate::k2v::sub::*;

const POLL_RANGE_EXTRA_DELAY: Duration = Duration::from_millis(200);

const TIMESTAMP_KEY: &[u8] = b"timestamp";

/// RPC messages for K2V
#[derive(Debug, Serialize, Deserialize)]
enum K2VRpc {
	Ok,
	InsertItem(InsertedItem),
	InsertManyItems(Vec<InsertedItem>),
	PollItem {
		key: PollKey,
		causal_context: CausalContext,
		timeout_msec: u64,
	},
	PollRange {
		range: PollRange,
		seen_str: Option<String>,
		timeout_msec: u64,
	},
	PollItemResponse(Option<K2VItem>),
	PollRangeResponse(Uuid, Vec<K2VItem>),
}

#[derive(Debug, Serialize, Deserialize)]
struct InsertedItem {
	partition: K2VItemPartition,
	sort_key: String,
	causal_context: Option<CausalContext>,
	value: DvvsValue,
}

impl Rpc for K2VRpc {
	type Response = Result<K2VRpc, Error>;
}

/// The block manager, handling block exchange between nodes, and block storage on local node
pub struct K2VRpcHandler {
	system: Arc<System>,
	item_table: Arc<Table<K2VItemTable, TableShardedReplication>>,

	// Using a mutex on the local_timestamp_tree is not strictly necessary,
	// but it helps to not try to do several inserts at the same time,
	// which would create transaction conflicts and force many useless retries.
	local_timestamp_tree: Mutex<db::Tree>,

	endpoint: Arc<Endpoint<K2VRpc, Self>>,
	subscriptions: Arc<SubscriptionManager>,
}

impl K2VRpcHandler {
	pub fn new(
		system: Arc<System>,
		db: &db::Db,
		item_table: Arc<Table<K2VItemTable, TableShardedReplication>>,
		subscriptions: Arc<SubscriptionManager>,
	) -> Arc<Self> {
		let local_timestamp_tree = db
			.open_tree("k2v_local_timestamp")
			.expect("Unable to open DB tree for k2v local timestamp");
		let endpoint = system.netapp.endpoint("garage_model/k2v/Rpc".to_string());

		let rpc_handler = Arc::new(Self {
			system,
			item_table,
			local_timestamp_tree: Mutex::new(local_timestamp_tree),
			endpoint,
			subscriptions,
		});
		rpc_handler.endpoint.set_handler(rpc_handler.clone());

		rpc_handler
	}

	// ---- public interface ----

	pub async fn insert(
		&self,
		bucket_id: Uuid,
		partition_key: String,
		sort_key: String,
		causal_context: Option<CausalContext>,
		value: DvvsValue,
	) -> Result<(), Error> {
		let partition = K2VItemPartition {
			bucket_id,
			partition_key,
		};
		let mut who = self
			.item_table
			.data
			.replication
			.storage_nodes(&partition.hash());
		who.sort();

		self.system
			.rpc_helper()
			.try_call_many(
				&self.endpoint,
				&who,
				K2VRpc::InsertItem(InsertedItem {
					partition,
					sort_key,
					causal_context,
					value,
				}),
				RequestStrategy::with_priority(PRIO_NORMAL).with_quorum(1),
			)
			.await?;

		Ok(())
	}

	pub async fn insert_batch(
		&self,
		bucket_id: Uuid,
		items: Vec<(String, String, Option<CausalContext>, DvvsValue)>,
	) -> Result<(), Error> {
		let n_items = items.len();

		let mut call_list: HashMap<_, Vec<_>> = HashMap::new();

		for (partition_key, sort_key, causal_context, value) in items {
			let partition = K2VItemPartition {
				bucket_id,
				partition_key,
			};
			let mut who = self
				.item_table
				.data
				.replication
				.storage_nodes(&partition.hash());
			who.sort();

			call_list.entry(who).or_default().push(InsertedItem {
				partition,
				sort_key,
				causal_context,
				value,
			});
		}

		debug!(
			"K2V insert_batch: {} requests to insert {} items",
			call_list.len(),
			n_items
		);
		let call_futures = call_list.into_iter().map(|(nodes, items)| async move {
			let resp = self
				.system
				.rpc_helper()
				.try_call_many(
					&self.endpoint,
					&nodes[..],
					K2VRpc::InsertManyItems(items),
					RequestStrategy::with_priority(PRIO_NORMAL).with_quorum(1),
				)
				.await?;
			Ok::<_, Error>((nodes, resp))
		});

		let mut resps = call_futures.collect::<FuturesUnordered<_>>();
		while let Some(resp) = resps.next().await {
			resp?;
		}

		Ok(())
	}

	pub async fn poll_item(
		&self,
		bucket_id: Uuid,
		partition_key: String,
		sort_key: String,
		causal_context: CausalContext,
		timeout_msec: u64,
	) -> Result<Option<K2VItem>, Error> {
		let poll_key = PollKey {
			partition: K2VItemPartition {
				bucket_id,
				partition_key,
			},
			sort_key,
		};
		let nodes = self
			.item_table
			.data
			.replication
			.storage_nodes(&poll_key.partition.hash());

		let rpc = self.system.rpc_helper().try_call_many(
			&self.endpoint,
			&nodes,
			K2VRpc::PollItem {
				key: poll_key,
				causal_context,
				timeout_msec,
			},
			RequestStrategy::with_priority(PRIO_NORMAL)
				.with_quorum(self.item_table.data.replication.read_quorum())
				.send_all_at_once(true)
				.without_timeout(),
		);
		let timeout_duration = Duration::from_millis(timeout_msec);
		let resps = select! {
			r = rpc => r?,
			_ = tokio::time::sleep(timeout_duration) => return Ok(None),
		};

		let mut resp: Option<K2VItem> = None;
		for v in resps {
			match v {
				K2VRpc::PollItemResponse(Some(x)) => {
					if let Some(y) = &mut resp {
						y.merge(&x);
					} else {
						resp = Some(x);
					}
				}
				K2VRpc::PollItemResponse(None) => (),
				v => return Err(Error::unexpected_rpc_message(v)),
			}
		}

		Ok(resp)
	}

	pub async fn poll_range(
		&self,
		range: PollRange,
		seen_str: Option<String>,
		timeout_msec: u64,
	) -> Result<Option<(BTreeMap<String, K2VItem>, String)>, HelperError> {
		let has_seen_marker = seen_str.is_some();

		// Parse seen marker, we will use it below. This is also the first check
		// that it is valid, which returns a bad request error if not.
		let mut seen = seen_str
			.as_deref()
			.map(RangeSeenMarker::decode_helper)
			.transpose()?
			.unwrap_or_default();
		seen.restrict(&range);

		// Prepare PollRange RPC to send to the storage nodes responsible for the parititon
		let nodes = self
			.item_table
			.data
			.replication
			.storage_nodes(&range.partition.hash());
		let quorum = self.item_table.data.replication.read_quorum();
		let msg = K2VRpc::PollRange {
			range,
			seen_str,
			timeout_msec,
		};

		// Send the request to all nodes, use FuturesUnordered to get the responses in any order
		let msg = msg.into_req().map_err(garage_net::error::Error::from)?;
		let rs = RequestStrategy::with_priority(PRIO_NORMAL).without_timeout();
		let mut requests = nodes
			.iter()
			.map(|node| {
				self.system
					.rpc_helper()
					.call(&self.endpoint, *node, msg.clone(), rs.clone())
			})
			.collect::<FuturesUnordered<_>>();

		// Fetch responses. This procedure stops fetching responses when any of the following
		// conditions arise:
		// - we have a response to all requests
		// - we have a response to a read quorum of requests (e.g. 2/3), and an extra delay
		//   has passed since the quorum was achieved
		// - a global RPC timeout expired
		// The extra delay after a quorum was received is useful if the third response was to
		// arrive during this short interval: this would allow us to consider all the data seen
		// by that last node in the response we produce, and would likely help reduce the
		// size of the seen marker that we will return (because we would have an info of the
		// kind: all items produced by that node until time ts have been returned, so we can
		// bump the entry in the global vector clock and possibly remove some item-specific
		// vector clocks)
		let mut deadline = Instant::now() + Duration::from_millis(timeout_msec);
		let mut resps = vec![];
		let mut errors = vec![];
		loop {
			select! {
				_ =	tokio::time::sleep_until(deadline.into()) => {
					break;
				}
				res = requests.next() => match res {
					None => break,
					Some(Err(e)) => errors.push(e),
					Some(Ok(r)) => {
						resps.push(r);
						if resps.len() >= quorum {
							deadline = std::cmp::min(deadline, Instant::now() + POLL_RANGE_EXTRA_DELAY);
						}
					}
				}
			}
		}
		if errors.len() > nodes.len() - quorum {
			let errors = errors.iter().map(|e| format!("{}", e)).collect::<Vec<_>>();
			return Err(Error::Quorum(quorum, None, resps.len(), nodes.len(), errors).into());
		}

		// Take all returned items into account to produce the response.
		let mut new_items = BTreeMap::<String, K2VItem>::new();
		for v in resps {
			if let K2VRpc::PollRangeResponse(node, items) = v {
				seen.mark_seen_node_items(node, items.iter());
				for item in items.into_iter() {
					match new_items.get_mut(&item.sort_key) {
						Some(ent) => {
							ent.merge(&item);
						}
						None => {
							new_items.insert(item.sort_key.clone(), item);
						}
					}
				}
			} else {
				return Err(Error::unexpected_rpc_message(v).into());
			}
		}

		if new_items.is_empty() && has_seen_marker {
			Ok(None)
		} else {
			Ok(Some((new_items, seen.encode()?)))
		}
	}

	// ---- internal handlers ----

	async fn handle_insert(&self, item: &InsertedItem) -> Result<K2VRpc, Error> {
		let new = {
			let local_timestamp_tree = self.local_timestamp_tree.lock().unwrap();
			self.local_insert(&local_timestamp_tree, item)?
		};

		// Propagate to rest of network
		if let Some(updated) = new {
			self.item_table.insert(&updated).await?;
		}

		Ok(K2VRpc::Ok)
	}

	async fn handle_insert_many(&self, items: &[InsertedItem]) -> Result<K2VRpc, Error> {
		let mut updated_vec = vec![];

		{
			let local_timestamp_tree = self.local_timestamp_tree.lock().unwrap();
			for item in items {
				let new = self.local_insert(&local_timestamp_tree, item)?;

				if let Some(updated) = new {
					updated_vec.push(updated);
				}
			}
		}

		// Propagate to rest of network
		if !updated_vec.is_empty() {
			self.item_table.insert_many(&updated_vec).await?;
		}

		Ok(K2VRpc::Ok)
	}

	fn local_insert(
		&self,
		local_timestamp_tree: &MutexGuard<'_, db::Tree>,
		item: &InsertedItem,
	) -> Result<Option<K2VItem>, Error> {
		let now = now_msec();

		self.item_table
			.data
			.update_entry_with(&item.partition, &item.sort_key, |tx, ent| {
				let old_local_timestamp = tx
					.get(local_timestamp_tree, TIMESTAMP_KEY)?
					.and_then(|x| x.try_into().ok())
					.map(u64::from_be_bytes)
					.unwrap_or_default();

				let mut ent = ent.unwrap_or_else(|| {
					K2VItem::new(
						item.partition.bucket_id,
						item.partition.partition_key.clone(),
						item.sort_key.clone(),
					)
				});
				let new_local_timestamp = ent.update(
					self.system.id,
					&item.causal_context,
					item.value.clone(),
					std::cmp::max(old_local_timestamp, now),
				);

				tx.insert(
					local_timestamp_tree,
					TIMESTAMP_KEY,
					u64::to_be_bytes(new_local_timestamp),
				)?;

				Ok(ent)
			})
	}

	async fn handle_poll_item(&self, key: &PollKey, ct: &CausalContext) -> Result<K2VItem, Error> {
		let mut chan = self.subscriptions.subscribe_item(key);

		let mut value = self
			.item_table
			.data
			.read_entry(&key.partition, &key.sort_key)?
			.map(|bytes| self.item_table.data.decode_entry(&bytes[..]))
			.transpose()?
			.unwrap_or_else(|| {
				K2VItem::new(
					key.partition.bucket_id,
					key.partition.partition_key.clone(),
					key.sort_key.clone(),
				)
			});

		while !value.causal_context().is_newer_than(ct) {
			value = chan.recv().await?;
		}

		Ok(value)
	}

	async fn handle_poll_range(
		&self,
		range: &PollRange,
		seen_str: &Option<String>,
	) -> Result<Vec<K2VItem>, Error> {
		if let Some(seen_str) = seen_str {
			let seen = RangeSeenMarker::decode(seen_str).ok_or_message("Invalid seenMarker")?;

			// Subscribe now to all changes on that partition,
			// so that new items that are inserted while we are reading the range
			// will be seen in the loop below
			let mut chan = self.subscriptions.subscribe_partition(&range.partition);

			// Check for the presence of any new items already stored in the item table
			let mut new_items = self.poll_range_read_range(range, &seen)?;

			// If we found no new items, wait for a matching item to arrive
			// on the channel
			while new_items.is_empty() {
				let item = chan.recv().await?;
				if range.matches(&item) && seen.is_new_item(&item) {
					new_items.push(item);
				}
			}

			Ok(new_items)
		} else {
			// If no seen marker was specified, we do not poll for anything.
			// We return immediately with the set of known items (even if
			// it is empty), which will give the client an initial view of
			// the dataset and an initial seen marker for further
			// PollRange calls.
			self.poll_range_read_range(range, &RangeSeenMarker::default())
		}
	}

	fn poll_range_read_range(
		&self,
		range: &PollRange,
		seen: &RangeSeenMarker,
	) -> Result<Vec<K2VItem>, Error> {
		let mut new_items = vec![];

		let partition_hash = range.partition.hash();
		let first_key = match &range.start {
			None => partition_hash.to_vec(),
			Some(sk) => self.item_table.data.tree_key(&range.partition, sk),
		};
		for item in self.item_table.data.store.range(first_key..)? {
			let (key, value) = item?;
			if &key[..32] != partition_hash.as_slice() {
				break;
			}
			let item = self.item_table.data.decode_entry(&value)?;
			if !range.matches(&item) {
				break;
			}
			if seen.is_new_item(&item) {
				new_items.push(item);
			}
		}

		Ok(new_items)
	}
}

impl EndpointHandler<K2VRpc> for K2VRpcHandler {
	async fn handle(self: &Arc<Self>, message: &K2VRpc, _from: NodeID) -> Result<K2VRpc, Error> {
		match message {
			K2VRpc::InsertItem(item) => self.handle_insert(item).await,
			K2VRpc::InsertManyItems(items) => self.handle_insert_many(&items[..]).await,
			K2VRpc::PollItem {
				key,
				causal_context,
				timeout_msec,
			} => {
				let delay = tokio::time::sleep(Duration::from_millis(*timeout_msec));
				select! {
					ret = self.handle_poll_item(key, causal_context) => ret.map(Some).map(K2VRpc::PollItemResponse),
					_ = delay => Ok(K2VRpc::PollItemResponse(None)),
				}
			}
			K2VRpc::PollRange {
				range,
				seen_str,
				timeout_msec,
			} => {
				let delay = tokio::time::sleep(Duration::from_millis(*timeout_msec));
				select! {
					ret = self.handle_poll_range(range, seen_str) => ret.map(|items| K2VRpc::PollRangeResponse(self.system.id, items)),
					_ = delay => Ok(K2VRpc::PollRangeResponse(self.system.id, vec![])),
				}
			}
			m => Err(Error::unexpected_rpc_message(m)),
		}
	}
}
