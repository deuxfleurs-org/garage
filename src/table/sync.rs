use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::stream::*;
use opentelemetry::KeyValue;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use tokio::select;
use tokio::sync::{mpsc, watch};

use garage_util::background::*;
use garage_util::data::*;
use garage_util::error::Error;

use garage_rpc::ring::*;
use garage_rpc::system::System;
use garage_rpc::*;

use crate::data::*;
use crate::merkle::*;
use crate::replication::*;
use crate::*;

const TABLE_SYNC_RPC_TIMEOUT: Duration = Duration::from_secs(30);

// Do anti-entropy every 10 minutes
const ANTI_ENTROPY_INTERVAL: Duration = Duration::from_secs(10 * 60);

pub struct TableSyncer<F: TableSchema + 'static, R: TableReplication + 'static> {
	system: Arc<System>,
	data: Arc<TableData<F, R>>,
	merkle: Arc<MerkleUpdater<F, R>>,

	add_full_sync_tx: mpsc::UnboundedSender<()>,
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

#[derive(Debug, Clone)]
struct TodoPartition {
	partition: Partition,
	begin: Hash,
	end: Hash,

	// Are we a node that stores this partition or not?
	retain: bool,
}

impl<F, R> TableSyncer<F, R>
where
	F: TableSchema + 'static,
	R: TableReplication + 'static,
{
	pub(crate) fn launch(
		system: Arc<System>,
		data: Arc<TableData<F, R>>,
		merkle: Arc<MerkleUpdater<F, R>>,
	) -> Arc<Self> {
		let endpoint = system
			.netapp
			.endpoint(format!("garage_table/sync.rs/Rpc:{}", F::TABLE_NAME));

		let (add_full_sync_tx, add_full_sync_rx) = mpsc::unbounded_channel();

		let syncer = Arc::new(Self {
			system: system.clone(),
			data,
			merkle,
			add_full_sync_tx,
			endpoint,
		});

		syncer.endpoint.set_handler(syncer.clone());

		system.background.spawn_worker(SyncWorker {
			syncer: syncer.clone(),
			ring_recv: system.ring.clone(),
			ring: system.ring.borrow().clone(),
			add_full_sync_rx,
			todo: vec![],
			next_full_sync: Instant::now() + Duration::from_secs(20),
		});

		syncer
	}

	pub fn add_full_sync(&self) {
		if self.add_full_sync_tx.send(()).is_err() {
			error!("({}) Could not add full sync", F::TABLE_NAME);
		}
	}

	// ----

	async fn sync_partition(
		self: &Arc<Self>,
		partition: &TodoPartition,
		must_exit: &mut watch::Receiver<bool>,
	) -> Result<(), Error> {
		if partition.retain {
			let my_id = self.system.id;

			let nodes = self
				.data
				.replication
				.write_nodes(&partition.begin)
				.into_iter()
				.filter(|node| *node != my_id)
				.collect::<Vec<_>>();

			debug!(
				"({}) Syncing {:?} with {:?}...",
				F::TABLE_NAME,
				partition,
				nodes
			);
			let mut sync_futures = nodes
				.iter()
				.map(|node| {
					self.clone()
						.do_sync_with(partition.clone(), *node, must_exit.clone())
				})
				.collect::<FuturesUnordered<_>>();

			let mut n_errors = 0;
			while let Some(r) = sync_futures.next().await {
				if let Err(e) = r {
					n_errors += 1;
					warn!("({}) Sync error: {}", F::TABLE_NAME, e);
				}
			}
			if n_errors > self.data.replication.max_write_errors() {
				return Err(Error::Message(format!(
					"Sync failed with too many nodes (should have been: {:?}).",
					nodes
				)));
			}
		} else {
			self.offload_partition(&partition.begin, &partition.end, must_exit)
				.await?;
		}

		Ok(())
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
				let nodes = self
					.data
					.replication
					.write_nodes(begin)
					.into_iter()
					.collect::<Vec<_>>();
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
				self.offload_items(&items, &nodes[..]).await?;
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
			.rpc
			.try_call_many(
				&self.endpoint,
				nodes,
				SyncRpc::Items(values),
				RequestStrategy::with_priority(PRIO_BACKGROUND)
					.with_quorum(nodes.len())
					.with_timeout(TABLE_SYNC_RPC_TIMEOUT),
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
		self: Arc<Self>,
		partition: TodoPartition,
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
		let root_ck_hash = hash_of::<MerkleNode>(&root_ck)?;

		// Check if they have the same root checksum
		// If so, do nothing.
		let root_resp = self
			.system
			.rpc
			.call(
				&self.endpoint,
				who,
				SyncRpc::RootCkHash(partition.partition, root_ck_hash),
				RequestStrategy::with_priority(PRIO_BACKGROUND)
					.with_timeout(TABLE_SYNC_RPC_TIMEOUT),
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
					"Invalid respone to RootCkHash RPC: {}",
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
							warn!("({}) Hashes differ between stored value and Merkle tree, key: {:?} (if your server is very busy, don't worry, this happens when the Merkle tree can't be updated fast enough)", F::TABLE_NAME, ik);
						}
						todo_items.push(val.to_vec());
					} else {
						warn!("({}) Item from Merkle tree not found in store: {:?} (if your server is very busy, don't worry, this happens when the Merkle tree can't be updated fast enough)", F::TABLE_NAME, ik);
					}
				}
				MerkleNode::Intermediate(l) => {
					// Get Merkle node for this tree position at remote node
					// and compare it with local node
					let remote_node = match self
						.system
						.rpc
						.call(
							&self.endpoint,
							who,
							SyncRpc::GetNode(key.clone()),
							RequestStrategy::with_priority(PRIO_BACKGROUND)
								.with_timeout(TABLE_SYNC_RPC_TIMEOUT),
						)
						.await?
					{
						SyncRpc::Node(_, node) => node,
						x => {
							return Err(Error::Message(format!(
								"Invalid respone to GetNode RPC: {}",
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
			.rpc
			.call(
				&self.endpoint,
				who,
				SyncRpc::Items(values),
				RequestStrategy::with_priority(PRIO_BACKGROUND)
					.with_timeout(TABLE_SYNC_RPC_TIMEOUT),
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

#[async_trait]
impl<F, R> EndpointHandler<SyncRpc> for TableSyncer<F, R>
where
	F: TableSchema + 'static,
	R: TableReplication + 'static,
{
	async fn handle(self: &Arc<Self>, message: &SyncRpc, from: NodeID) -> Result<SyncRpc, Error> {
		match message {
			SyncRpc::RootCkHash(range, h) => {
				let (_root_ck_key, root_ck) = self.get_root_ck(*range)?;
				let hash = hash_of::<MerkleNode>(&root_ck)?;
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

struct SyncWorker<F: TableSchema + 'static, R: TableReplication + 'static> {
	syncer: Arc<TableSyncer<F, R>>,
	ring_recv: watch::Receiver<Arc<Ring>>,
	ring: Arc<Ring>,
	add_full_sync_rx: mpsc::UnboundedReceiver<()>,
	todo: Vec<TodoPartition>,
	next_full_sync: Instant,
}

impl<F: TableSchema + 'static, R: TableReplication + 'static> SyncWorker<F, R> {
	fn add_full_sync(&mut self) {
		let system = &self.syncer.system;
		let data = &self.syncer.data;

		let my_id = system.id;

		self.todo.clear();

		let partitions = data.replication.partitions();

		for i in 0..partitions.len() {
			let begin = partitions[i].1;

			let end = if i + 1 < partitions.len() {
				partitions[i + 1].1
			} else {
				[0xFFu8; 32].into()
			};

			let nodes = data.replication.write_nodes(&begin);

			let retain = nodes.contains(&my_id);
			if !retain {
				// Check if we have some data to send, otherwise skip
				match data.store.range(begin..end) {
					Ok(mut iter) => {
						if iter.next().is_none() {
							continue;
						}
					}
					Err(e) => {
						warn!("DB error in add_full_sync: {}", e);
						continue;
					}
				}
			}

			self.todo.push(TodoPartition {
				partition: partitions[i].0,
				begin,
				end,
				retain,
			});
		}

		self.next_full_sync = Instant::now() + ANTI_ENTROPY_INTERVAL;
	}

	fn pop_task(&mut self) -> Option<TodoPartition> {
		if self.todo.is_empty() {
			return None;
		}

		let i = rand::thread_rng().gen_range(0..self.todo.len());
		if i == self.todo.len() - 1 {
			self.todo.pop()
		} else {
			let replacement = self.todo.pop().unwrap();
			let ret = std::mem::replace(&mut self.todo[i], replacement);
			Some(ret)
		}
	}
}

#[async_trait]
impl<F: TableSchema + 'static, R: TableReplication + 'static> Worker for SyncWorker<F, R> {
	fn name(&self) -> String {
		format!("{} sync", F::TABLE_NAME)
	}

	fn info(&self) -> Option<String> {
		let l = self.todo.len();
		if l > 0 {
			Some(format!("{} partitions remaining", l))
		} else {
			None
		}
	}

	async fn work(&mut self, must_exit: &mut watch::Receiver<bool>) -> Result<WorkerState, Error> {
		if let Some(partition) = self.pop_task() {
			self.syncer.sync_partition(&partition, must_exit).await?;
			Ok(WorkerState::Busy)
		} else {
			Ok(WorkerState::Idle)
		}
	}

	async fn wait_for_work(&mut self, must_exit: &watch::Receiver<bool>) -> WorkerState {
		if *must_exit.borrow() {
			return WorkerState::Done;
		}
		select! {
			s = self.add_full_sync_rx.recv() => {
				if let Some(()) = s {
					self.add_full_sync();
				}
			},
			_ = self.ring_recv.changed() => {
				let new_ring = self.ring_recv.borrow();
				if !Arc::ptr_eq(&new_ring, &self.ring) {
					self.ring = new_ring.clone();
					drop(new_ring);
					debug!("({}) Ring changed, adding full sync to syncer todo list", F::TABLE_NAME);
					self.add_full_sync();
				}
			},
			_ = tokio::time::sleep(self.next_full_sync - Instant::now()) => {
				self.add_full_sync();
			}
		}
		match self.todo.is_empty() {
			false => WorkerState::Busy,
			true => WorkerState::Idle,
		}
	}
}

// ---- UTIL ----

fn hash_of<T: Serialize>(x: &T) -> Result<Hash, Error> {
	Ok(blake2sum(&rmp_to_vec_all_named(x)?[..]))
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
