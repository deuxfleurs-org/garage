use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use async_trait::async_trait;
use futures_util::stream::*;
use opentelemetry::KeyValue;
use rand::prelude::*;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use tokio::select;
use tokio::sync::{mpsc, watch, Notify};

use garage_util::background::*;
use garage_util::data::*;
use garage_util::encode::{debug_serialize, nonversioned_encode};
use garage_util::error::{Error, OkOrMessage};

use garage_rpc::layout::*;
use garage_rpc::rpc_helper::QuorumSetResultTracker;
use garage_rpc::system::System;
use garage_rpc::*;

use crate::data::*;
use crate::merkle::*;
use crate::replication::*;
use crate::*;

// Do anti-entropy every 10 minutes
const ANTI_ENTROPY_INTERVAL: Duration = Duration::from_secs(10 * 60);

pub struct TableSyncer<F: TableSchema, R: TableReplication> {
	system: Arc<System>,
	data: Arc<TableData<F, R>>,
	merkle: Arc<MerkleUpdater<F, R>>,

	add_full_sync_tx: ArcSwapOption<mpsc::UnboundedSender<()>>,
	endpoint: Arc<Endpoint<SyncRpc, Self>>,
}

#[derive(Serialize, Deserialize)]
pub(crate) enum SyncRpc {
	RootCkHash(Partition, Hash),
	RootCkDifferent(bool),
	GetNode(MerkleNodeKey),
	Node(MerkleNodeKey, MerkleNode),
	Items(Vec<Arc<ByteBuf>>),
	Ok,
}

impl Rpc for SyncRpc {
	type Response = Result<SyncRpc, Error>;
}

impl<F: TableSchema, R: TableReplication> TableSyncer<F, R> {
	pub(crate) fn new(
		system: Arc<System>,
		data: Arc<TableData<F, R>>,
		merkle: Arc<MerkleUpdater<F, R>>,
	) -> Arc<Self> {
		let endpoint = system
			.netapp
			.endpoint(format!("garage_table/sync.rs/Rpc:{}", F::TABLE_NAME));

		let syncer = Arc::new(Self {
			system,
			data,
			merkle,
			add_full_sync_tx: ArcSwapOption::new(None),
			endpoint,
		});
		syncer.endpoint.set_handler(syncer.clone());

		syncer
	}

	pub(crate) fn spawn_workers(self: &Arc<Self>, bg: &BackgroundRunner) {
		let (add_full_sync_tx, add_full_sync_rx) = mpsc::unbounded_channel();
		self.add_full_sync_tx
			.store(Some(Arc::new(add_full_sync_tx)));

		bg.spawn_worker(SyncWorker {
			syncer: self.clone(),
			layout_notify: self.system.layout_notify(),
			layout_digest: self.system.cluster_layout().sync_digest(),
			add_full_sync_rx,
			todo: None,
			next_full_sync: Instant::now() + Duration::from_secs(20),
		});
	}

	pub fn add_full_sync(&self) -> Result<(), Error> {
		let tx = self.add_full_sync_tx.load();
		let tx = tx
			.as_ref()
			.ok_or_message("table sync worker is not running")?;
		tx.send(()).ok_or_message("send error")?;
		Ok(())
	}

	// ----

	async fn sync_partition(
		self: &Arc<Self>,
		partition: &SyncPartition,
		must_exit: &mut watch::Receiver<bool>,
	) -> Result<(), Error> {
		let my_id = self.system.id;
		let retain = partition.storage_sets.iter().any(|x| x.contains(&my_id));

		if retain {
			debug!(
				"({}) Syncing {:?} with {:?}...",
				F::TABLE_NAME,
				partition,
				partition.storage_sets
			);
			let mut result_tracker = QuorumSetResultTracker::new(
				&partition.storage_sets,
				self.data.replication.write_quorum(),
			);

			let mut sync_futures = result_tracker
				.nodes
				.keys()
				.copied()
				.map(|node| {
					let must_exit = must_exit.clone();
					async move {
						if node == my_id {
							(node, Ok(()))
						} else {
							(node, self.do_sync_with(partition, node, must_exit).await)
						}
					}
				})
				.collect::<FuturesUnordered<_>>();

			while let Some((node, res)) = sync_futures.next().await {
				if let Err(e) = &res {
					warn!("({}) Sync error with {:?}: {}", F::TABLE_NAME, node, e);
				}
				result_tracker.register_result(node, res);
			}

			if result_tracker.too_many_failures() {
				Err(result_tracker.quorum_error())
			} else {
				Ok(())
			}
		} else {
			self.offload_partition(&partition.first_hash, &partition.last_hash, must_exit)
				.await
		}
	}

	// Offload partition: this partition is not something we are storing,
	// so send it out to all other nodes that store it and delete items locally.
	// We don't bother checking if the remote nodes already have the items,
	// we just batch-send everything. Offloading isn't supposed to happen very often.
	// If any of the nodes that are supposed to store the items is unable to
	// save them, we interrupt the process.
	async fn offload_partition(
		self: &Arc<Self>,
		begin: &Hash,
		end: &Hash,
		must_exit: &mut watch::Receiver<bool>,
	) -> Result<(), Error> {
		let mut counter: usize = 0;

		while !*must_exit.borrow() {
			let mut items = Vec::new();

			for item in self.data.store.range(begin.to_vec()..end.to_vec())? {
				let (key, value) = item?;
				items.push((key.to_vec(), Arc::new(ByteBuf::from(value))));

				if items.len() >= 1024 {
					break;
				}
			}

			if !items.is_empty() {
				let nodes = self.data.replication.storage_nodes(begin);
				if nodes.contains(&self.system.id) {
					warn!(
						"({}) Interrupting offload as partitions seem to have changed",
						F::TABLE_NAME
					);
					break;
				}
				if nodes.len() < self.data.replication.write_quorum() {
					return Err(Error::Message(
						"Not offloading as we don't have a quorum of nodes to write to."
							.to_string(),
					));
				}

				counter += 1;
				info!(
					"({}) Offloading {} items from {:?}..{:?} ({})",
					F::TABLE_NAME,
					items.len(),
					begin,
					end,
					counter
				);
				self.offload_items(&items, &nodes).await?;
			} else {
				break;
			}
		}

		Ok(())
	}

	async fn offload_items(
		self: &Arc<Self>,
		items: &[(Vec<u8>, Arc<ByteBuf>)],
		nodes: &[Uuid],
	) -> Result<(), Error> {
		let values = items.iter().map(|(_k, v)| v.clone()).collect::<Vec<_>>();

		for to in nodes.iter() {
			self.data.metrics.sync_items_sent.add(
				values.len() as u64,
				&[
					KeyValue::new("table_name", F::TABLE_NAME),
					KeyValue::new("to", format!("{:?}", to)),
				],
			);
		}

		self.system
			.rpc_helper()
			.try_call_many(
				&self.endpoint,
				nodes,
				SyncRpc::Items(values),
				RequestStrategy::with_priority(PRIO_BACKGROUND).with_quorum(nodes.len()),
			)
			.await?;

		// All remote nodes have written those items, now we can delete them locally
		let mut not_removed = 0;
		for (k, v) in items.iter() {
			if !self.data.delete_if_equal(&k[..], &v[..])? {
				not_removed += 1;
			}
		}

		if not_removed > 0 {
			debug!("({}) {} items not removed during offload because they changed in between (trying again...)", F::TABLE_NAME, not_removed);
		}

		Ok(())
	}

	// ======= SYNCHRONIZATION PROCEDURE -- DRIVER SIDE ======
	// The driver side is only concerned with sending out the item it has
	// and the other side might not have. Receiving items that differ from one
	// side to the other will happen when the other side syncs with us,
	// which they also do regularly.

	fn get_root_ck(&self, partition: Partition) -> Result<(MerkleNodeKey, MerkleNode), Error> {
		let key = MerkleNodeKey {
			partition,
			prefix: vec![],
		};
		let node = self.merkle.read_node(&key)?;
		Ok((key, node))
	}

	async fn do_sync_with(
		self: &Arc<Self>,
		partition: &SyncPartition,
		who: Uuid,
		must_exit: watch::Receiver<bool>,
	) -> Result<(), Error> {
		let (root_ck_key, root_ck) = self.get_root_ck(partition.partition)?;
		if root_ck.is_empty() {
			debug!(
				"({}) Sync {:?} with {:?}: partition is empty.",
				F::TABLE_NAME,
				partition,
				who
			);
			return Ok(());
		}
		let root_ck_hash = hash_of_merkle_node(&root_ck)?;

		// Check if they have the same root checksum
		// If so, do nothing.
		let root_resp = self
			.system
			.rpc_helper()
			.call(
				&self.endpoint,
				who,
				SyncRpc::RootCkHash(partition.partition, root_ck_hash),
				RequestStrategy::with_priority(PRIO_BACKGROUND),
			)
			.await?;

		let mut todo = match root_resp {
			SyncRpc::RootCkDifferent(false) => {
				debug!(
					"({}) Sync {:?} with {:?}: no difference",
					F::TABLE_NAME,
					partition,
					who
				);
				return Ok(());
			}
			SyncRpc::RootCkDifferent(true) => VecDeque::from(vec![root_ck_key]),
			x => {
				return Err(Error::Message(format!(
					"Invalid response to RootCkHash RPC: {}",
					debug_serialize(x)
				)));
			}
		};

		let mut todo_items = vec![];

		while !todo.is_empty() && !*must_exit.borrow() {
			let key = todo.pop_front().unwrap();
			let node = self.merkle.read_node(&key)?;

			match node {
				MerkleNode::Empty => {
					// They have items we don't have.
					// We don't request those items from them, they will send them.
					// We only bother with pushing items that differ
				}
				MerkleNode::Leaf(ik, ivhash) => {
					// Just send that item directly
					if let Some(val) = self.data.store.get(&ik[..])? {
						if blake2sum(&val[..]) != ivhash {
							debug!("({}) Hashes differ between stored value and Merkle tree, key: {} (if your server is very busy, don't worry, this happens when the Merkle tree can't be updated fast enough)", F::TABLE_NAME, hex::encode(ik));
						}
						todo_items.push(val.to_vec());
					} else {
						debug!("({}) Item from Merkle tree not found in store: {} (if your server is very busy, don't worry, this happens when the Merkle tree can't be updated fast enough)", F::TABLE_NAME, hex::encode(ik));
					}
				}
				MerkleNode::Intermediate(l) => {
					// Get Merkle node for this tree position at remote node
					// and compare it with local node
					let remote_node = match self
						.system
						.rpc_helper()
						.call(
							&self.endpoint,
							who,
							SyncRpc::GetNode(key.clone()),
							RequestStrategy::with_priority(PRIO_BACKGROUND),
						)
						.await?
					{
						SyncRpc::Node(_, node) => node,
						x => {
							return Err(Error::Message(format!(
								"Invalid response to GetNode RPC: {}",
								debug_serialize(x)
							)));
						}
					};
					let int_l2 = match remote_node {
						// If they have an intermediate node at this tree position,
						// we can compare them to find differences
						MerkleNode::Intermediate(l2) => l2,
						// Otherwise, treat it as if they have nothing for this subtree,
						// which will have the consequence of sending them everything
						_ => vec![],
					};

					let join = join_ordered(&l[..], &int_l2[..]);
					for (p, v1, v2) in join.into_iter() {
						let diff = match (v1, v2) {
							(Some(_), None) | (None, Some(_)) => true,
							(Some(a), Some(b)) => a != b,
							_ => false,
						};
						if diff {
							todo.push_back(key.add_byte(*p));
						}
					}
				}
			}

			if todo_items.len() >= 256 {
				self.send_items(who, std::mem::take(&mut todo_items))
					.await?;
			}
		}

		if !todo_items.is_empty() {
			self.send_items(who, todo_items).await?;
		}

		Ok(())
	}

	async fn send_items(&self, who: Uuid, item_value_list: Vec<Vec<u8>>) -> Result<(), Error> {
		info!(
			"({}) Sending {} items to {:?}",
			F::TABLE_NAME,
			item_value_list.len(),
			who
		);

		let values = item_value_list
			.into_iter()
			.map(|x| Arc::new(ByteBuf::from(x)))
			.collect::<Vec<_>>();

		self.data.metrics.sync_items_sent.add(
			values.len() as u64,
			&[
				KeyValue::new("table_name", F::TABLE_NAME),
				KeyValue::new("to", format!("{:?}", who)),
			],
		);

		let rpc_resp = self
			.system
			.rpc_helper()
			.call(
				&self.endpoint,
				who,
				SyncRpc::Items(values),
				RequestStrategy::with_priority(PRIO_BACKGROUND),
			)
			.await?;
		if let SyncRpc::Ok = rpc_resp {
			Ok(())
		} else {
			Err(Error::unexpected_rpc_message(rpc_resp))
		}
	}
}

// ======= SYNCHRONIZATION PROCEDURE -- RECEIVER SIDE ======

impl<F: TableSchema, R: TableReplication> EndpointHandler<SyncRpc> for TableSyncer<F, R> {
	async fn handle(self: &Arc<Self>, message: &SyncRpc, from: NodeID) -> Result<SyncRpc, Error> {
		match message {
			SyncRpc::RootCkHash(range, h) => {
				let (_root_ck_key, root_ck) = self.get_root_ck(*range)?;
				let hash = hash_of_merkle_node(&root_ck)?;
				Ok(SyncRpc::RootCkDifferent(hash != *h))
			}
			SyncRpc::GetNode(k) => {
				let node = self.merkle.read_node(k)?;
				Ok(SyncRpc::Node(k.clone(), node))
			}
			SyncRpc::Items(items) => {
				self.data.metrics.sync_items_received.add(
					items.len() as u64,
					&[
						KeyValue::new("table_name", F::TABLE_NAME),
						KeyValue::new(
							"from",
							format!("{:?}", Uuid::try_from(from.as_ref()).unwrap()),
						),
					],
				);

				self.data.update_many(items)?;
				Ok(SyncRpc::Ok)
			}
			m => Err(Error::unexpected_rpc_message(m)),
		}
	}
}

// -------- Sync Worker ---------

struct SyncWorker<F: TableSchema, R: TableReplication> {
	syncer: Arc<TableSyncer<F, R>>,

	layout_notify: Arc<Notify>,
	layout_digest: SyncLayoutDigest,

	add_full_sync_rx: mpsc::UnboundedReceiver<()>,
	next_full_sync: Instant,

	todo: Option<SyncPartitions>,
}

impl<F: TableSchema, R: TableReplication> SyncWorker<F, R> {
	fn check_add_full_sync(&mut self) {
		let layout_digest = self.syncer.system.cluster_layout().sync_digest();
		if layout_digest != self.layout_digest {
			self.layout_digest = layout_digest;
			info!(
				"({}) Layout versions changed ({:?}), adding full sync to syncer todo list",
				F::TABLE_NAME,
				layout_digest,
			);
			self.add_full_sync();
		}
	}

	fn add_full_sync(&mut self) {
		let mut partitions = self.syncer.data.replication.sync_partitions();
		info!(
			"{}: Adding full sync for ack layout version {}",
			F::TABLE_NAME,
			partitions.layout_version
		);

		partitions.partitions.shuffle(&mut thread_rng());
		self.todo = Some(partitions);
		self.next_full_sync = Instant::now() + ANTI_ENTROPY_INTERVAL;
	}
}

#[async_trait]
impl<F: TableSchema, R: TableReplication> Worker for SyncWorker<F, R> {
	fn name(&self) -> String {
		format!("{} sync", F::TABLE_NAME)
	}

	fn status(&self) -> WorkerStatus {
		WorkerStatus {
			queue_length: Some(self.todo.as_ref().map(|x| x.partitions.len()).unwrap_or(0) as u64),
			..Default::default()
		}
	}

	async fn work(&mut self, must_exit: &mut watch::Receiver<bool>) -> Result<WorkerState, Error> {
		self.check_add_full_sync();

		if let Some(todo) = &mut self.todo {
			let partition = todo.partitions.pop().unwrap();

			// process partition
			if let Err(e) = self.syncer.sync_partition(&partition, must_exit).await {
				error!(
					"{}: Failed to sync partition {:?}: {}",
					F::TABLE_NAME,
					partition,
					e
				);
				// if error, put partition back at the other side of the queue,
				// so that other partitions will be tried in the meantime
				todo.partitions.insert(0, partition);
				// TODO: returning an error here will cause the background job worker
				// to delay this task for some time, but maybe we don't want to
				// delay it if there are lots of failures from nodes that are gone
				// (we also don't want zero delays as that will cause lots of useless retries)
				return Err(e);
			}

			if todo.partitions.is_empty() {
				info!(
					"{}: Completed full sync for ack layout version {}",
					F::TABLE_NAME,
					todo.layout_version
				);
				self.syncer
					.system
					.layout_manager
					.sync_table_until(F::TABLE_NAME, todo.layout_version);
				self.todo = None;
			}

			Ok(WorkerState::Busy)
		} else {
			Ok(WorkerState::Idle)
		}
	}

	async fn wait_for_work(&mut self) -> WorkerState {
		select! {
			s = self.add_full_sync_rx.recv() => {
				if let Some(()) = s {
					self.add_full_sync();
				}
			},
			_ = self.layout_notify.notified() => {
				self.check_add_full_sync();
			},
			_ = tokio::time::sleep_until(self.next_full_sync.into()) => {
				self.add_full_sync();
			}
		}
		match self.todo.is_some() {
			true => WorkerState::Busy,
			false => WorkerState::Idle,
		}
	}
}

// ---- UTIL ----

fn hash_of_merkle_node(x: &MerkleNode) -> Result<Hash, Error> {
	Ok(blake2sum(&nonversioned_encode(x)?[..]))
}

fn join_ordered<'a, K: Ord + Eq, V1, V2>(
	x: &'a [(K, V1)],
	y: &'a [(K, V2)],
) -> Vec<(&'a K, Option<&'a V1>, Option<&'a V2>)> {
	let mut ret = vec![];
	let mut i = 0;
	let mut j = 0;
	while i < x.len() || j < y.len() {
		if i < x.len() && j < y.len() && x[i].0 == y[j].0 {
			ret.push((&x[i].0, Some(&x[i].1), Some(&y[j].1)));
			i += 1;
			j += 1;
		} else if i < x.len() && (j == y.len() || x[i].0 < y[j].0) {
			ret.push((&x[i].0, Some(&x[i].1), None));
			i += 1;
		} else if j < y.len() && (i == x.len() || x[i].0 > y[j].0) {
			ret.push((&y[j].0, None, Some(&y[j].1)));
			j += 1;
		} else {
			unreachable!();
		}
	}
	ret
}
