use std::collections::VecDeque;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::select;
use futures_util::future::*;
use futures_util::stream::*;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use tokio::sync::{mpsc, watch};

use garage_util::data::*;
use garage_util::error::Error;

use garage_rpc::membership::System;
use garage_rpc::ring::*;
use garage_rpc::rpc_client::*;
use garage_rpc::rpc_server::*;

use crate::data::*;
use crate::merkle::*;
use crate::replication::*;
use crate::*;

const TABLE_SYNC_RPC_TIMEOUT: Duration = Duration::from_secs(30);

// Do anti-entropy every 10 minutes
const ANTI_ENTROPY_INTERVAL: Duration = Duration::from_secs(10 * 60);

pub struct TableSyncer<F: TableSchema, R: TableReplication> {
	system: Arc<System>,
	data: Arc<TableData<F, R>>,
	merkle: Arc<MerkleUpdater<F, R>>,

	todo: Mutex<SyncTodo>,
	rpc_client: Arc<RpcClient<SyncRpc>>,
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

impl RpcMessage for SyncRpc {}

struct SyncTodo {
	todo: Vec<TodoPartition>,
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
		rpc_server: &mut RpcServer,
	) -> Arc<Self> {
		let rpc_path = format!("table_{}/sync", data.name);
		let rpc_client = system.rpc_client::<SyncRpc>(&rpc_path);

		let todo = SyncTodo { todo: vec![] };

		let syncer = Arc::new(Self {
			system: system.clone(),
			data: data.clone(),
			merkle,
			todo: Mutex::new(todo),
			rpc_client,
		});

		syncer.register_handler(rpc_server, rpc_path);

		let (busy_tx, busy_rx) = mpsc::unbounded_channel();

		let s1 = syncer.clone();
		system.background.spawn_worker(
			format!("table sync watcher for {}", data.name),
			move |must_exit: watch::Receiver<bool>| s1.watcher_task(must_exit, busy_rx),
		);

		let s2 = syncer.clone();
		system.background.spawn_worker(
			format!("table syncer for {}", data.name),
			move |must_exit: watch::Receiver<bool>| s2.syncer_task(must_exit, busy_tx),
		);

		let s3 = syncer.clone();
		tokio::spawn(async move {
			tokio::time::sleep(Duration::from_secs(20)).await;
			s3.add_full_sync();
		});

		syncer
	}

	fn register_handler(self: &Arc<Self>, rpc_server: &mut RpcServer, path: String) {
		let self2 = self.clone();
		rpc_server.add_handler::<SyncRpc, _, _>(path, move |msg, _addr| {
			let self2 = self2.clone();
			async move { self2.handle_rpc(&msg).await }
		});

		let self2 = self.clone();
		self.rpc_client
			.set_local_handler(self.system.id, move |msg| {
				let self2 = self2.clone();
				async move { self2.handle_rpc(&msg).await }
			});
	}

	async fn watcher_task(
		self: Arc<Self>,
		mut must_exit: watch::Receiver<bool>,
		mut busy_rx: mpsc::UnboundedReceiver<bool>,
	) {
		let mut prev_ring: Arc<Ring> = self.system.ring.borrow().clone();
		let mut ring_recv: watch::Receiver<Arc<Ring>> = self.system.ring.clone();
		let mut nothing_to_do_since = Some(Instant::now());

		while !*must_exit.borrow() {
			select! {
				_ = ring_recv.changed().fuse() => {
					let new_ring = ring_recv.borrow();
					if !Arc::ptr_eq(&new_ring, &prev_ring) {
						debug!("({}) Ring changed, adding full sync to syncer todo list", self.data.name);
						self.add_full_sync();
						prev_ring = new_ring.clone();
					}
				}
				busy_opt = busy_rx.recv().fuse() => {
					if let Some(busy) = busy_opt {
						if busy {
							nothing_to_do_since = None;
						} else if nothing_to_do_since.is_none() {
							nothing_to_do_since = Some(Instant::now());
						}
					}
				}
				_ = must_exit.changed().fuse() => {},
				_ = tokio::time::sleep(Duration::from_secs(1)).fuse() => {
					if nothing_to_do_since.map(|t| Instant::now() - t >= ANTI_ENTROPY_INTERVAL).unwrap_or(false) {
						nothing_to_do_since = None;
						debug!("({}) Interval passed, adding full sync to syncer todo list", self.data.name);
						self.add_full_sync();
					}
				}
			}
		}
	}

	pub fn add_full_sync(&self) {
		self.todo
			.lock()
			.unwrap()
			.add_full_sync(&self.data, &self.system);
	}

	async fn syncer_task(
		self: Arc<Self>,
		mut must_exit: watch::Receiver<bool>,
		busy_tx: mpsc::UnboundedSender<bool>,
	) {
		while !*must_exit.borrow() {
			let task = self.todo.lock().unwrap().pop_task();
			if let Some(partition) = task {
				busy_tx.send(true).unwrap();
				let res = self
					.clone()
					.sync_partition(&partition, &mut must_exit)
					.await;
				if let Err(e) = res {
					warn!(
						"({}) Error while syncing {:?}: {}",
						self.data.name, partition, e
					);
				}
			} else {
				busy_tx.send(false).unwrap();
				tokio::time::sleep(Duration::from_secs(1)).await;
			}
		}
	}

	async fn sync_partition(
		self: Arc<Self>,
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
				self.data.name, partition, nodes
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
					warn!("({}) Sync error: {}", self.data.name, e);
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

			for item in self.data.store.range(begin.to_vec()..end.to_vec()) {
				let (key, value) = item?;
				items.push((key.to_vec(), Arc::new(ByteBuf::from(value.as_ref()))));

				if items.len() >= 1024 {
					break;
				}
			}

			if !items.is_empty() {
				let nodes = self
					.data
					.replication
					.write_nodes(&begin)
					.into_iter()
					.collect::<Vec<_>>();
				if nodes.contains(&self.system.id) {
					warn!(
						"({}) Interrupting offload as partitions seem to have changed",
						self.data.name
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
					self.data.name,
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

		self.rpc_client
			.try_call_many(
				nodes,
				SyncRpc::Items(values),
				RequestStrategy::with_quorum(nodes.len()).with_timeout(TABLE_SYNC_RPC_TIMEOUT),
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
			debug!("({}) {} items not removed during offload because they changed in between (trying again...)", self.data.name, not_removed);
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
				self.data.name, partition, who
			);
			return Ok(());
		}
		let root_ck_hash = hash_of::<MerkleNode>(&root_ck)?;

		// Check if they have the same root checksum
		// If so, do nothing.
		let root_resp = self
			.rpc_client
			.call(
				who,
				SyncRpc::RootCkHash(partition.partition, root_ck_hash),
				TABLE_SYNC_RPC_TIMEOUT,
			)
			.await?;

		let mut todo = match root_resp {
			SyncRpc::RootCkDifferent(false) => {
				debug!(
					"({}) Sync {:?} with {:?}: no difference",
					self.data.name, partition, who
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
							warn!("({}) Hashes differ between stored value and Merkle tree, key: {:?} (if your server is very busy, don't worry, this happens when the Merkle tree can't be updated fast enough)", self.data.name, ik);
						}
						todo_items.push(val.to_vec());
					} else {
						warn!("({}) Item from Merkle tree not found in store: {:?} (if your server is very busy, don't worry, this happens when the Merkle tree can't be updated fast enough)", self.data.name, ik);
					}
				}
				MerkleNode::Intermediate(l) => {
					// Get Merkle node for this tree position at remote node
					// and compare it with local node
					let remote_node = match self
						.rpc_client
						.call(who, SyncRpc::GetNode(key.clone()), TABLE_SYNC_RPC_TIMEOUT)
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
			self.data.name,
			item_value_list.len(),
			who
		);

		let values = item_value_list
			.into_iter()
			.map(|x| Arc::new(ByteBuf::from(x)))
			.collect::<Vec<_>>();

		let rpc_resp = self
			.rpc_client
			.call(who, SyncRpc::Items(values), TABLE_SYNC_RPC_TIMEOUT)
			.await?;
		if let SyncRpc::Ok = rpc_resp {
			Ok(())
		} else {
			Err(Error::Message(format!(
				"Unexpected response to RPC Update: {}",
				debug_serialize(&rpc_resp)
			)))
		}
	}

	// ======= SYNCHRONIZATION PROCEDURE -- RECEIVER SIDE ======

	async fn handle_rpc(self: &Arc<Self>, message: &SyncRpc) -> Result<SyncRpc, Error> {
		match message {
			SyncRpc::RootCkHash(range, h) => {
				let (_root_ck_key, root_ck) = self.get_root_ck(*range)?;
				let hash = hash_of::<MerkleNode>(&root_ck)?;
				Ok(SyncRpc::RootCkDifferent(hash != *h))
			}
			SyncRpc::GetNode(k) => {
				let node = self.merkle.read_node(&k)?;
				Ok(SyncRpc::Node(k.clone(), node))
			}
			SyncRpc::Items(items) => {
				self.data.update_many(items)?;
				Ok(SyncRpc::Ok)
			}
			_ => Err(Error::Message("Unexpected sync RPC".to_string())),
		}
	}
}

impl SyncTodo {
	fn add_full_sync<F: TableSchema, R: TableReplication>(
		&mut self,
		data: &TableData<F, R>,
		system: &System,
	) {
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
				if data.store.range(begin..end).next().is_none() {
					continue;
				}
			}

			self.todo.push(TodoPartition {
				partition: partitions[i].0,
				begin,
				end,
				retain,
			});
		}
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
