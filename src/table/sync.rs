use std::collections::VecDeque;
use std::convert::TryInto;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::future::join_all;
use futures::{pin_mut, select};
use futures_util::future::*;
use futures_util::stream::*;
use rand::Rng;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use tokio::sync::{mpsc, watch};

use garage_rpc::ring::Ring;
use garage_util::data::*;
use garage_util::error::Error;

use crate::data::*;
use crate::merkle::*;
use crate::replication::*;
use crate::*;

const TABLE_SYNC_RPC_TIMEOUT: Duration = Duration::from_secs(30);

// Do anti-entropy every 10 minutes
const ANTI_ENTROPY_INTERVAL: Duration = Duration::from_secs(10 * 60);

pub struct TableSyncer<F: TableSchema, R: TableReplication> {
	data: Arc<TableData<F>>,
	aux: Arc<TableAux<F, R>>,

	todo: Mutex<SyncTodo>,
}

type RootCk = Vec<(MerklePartition, Hash)>;

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct PartitionRange {
	begin: MerklePartition,
	// if end is None, go all the way to partition 0xFFFF included
	end: Option<MerklePartition>,
}

#[derive(Serialize, Deserialize)]
pub(crate) enum SyncRPC {
	RootCkHash(PartitionRange, Hash),
	RootCkList(PartitionRange, RootCk),
	CkNoDifference,
	GetNode(MerkleNodeKey),
	Node(MerkleNodeKey, MerkleNode),
}

struct SyncTodo {
	todo: Vec<TodoPartition>,
}

#[derive(Debug, Clone)]
struct TodoPartition {
	range: PartitionRange,

	// Are we a node that stores this partition or not?
	retain: bool,
}

impl<F, R> TableSyncer<F, R>
where
	F: TableSchema + 'static,
	R: TableReplication + 'static,
{
	pub(crate) fn launch(data: Arc<TableData<F>>, aux: Arc<TableAux<F, R>>) -> Arc<Self> {
		let todo = SyncTodo { todo: vec![] };

		let syncer = Arc::new(Self {
			data: data.clone(),
			aux: aux.clone(),
			todo: Mutex::new(todo),
		});

		let (busy_tx, busy_rx) = mpsc::unbounded_channel();

		let s1 = syncer.clone();
		aux.system.background.spawn_worker(
			format!("table sync watcher for {}", data.name),
			move |must_exit: watch::Receiver<bool>| s1.watcher_task(must_exit, busy_rx),
		);

		let s2 = syncer.clone();
		aux.system.background.spawn_worker(
			format!("table syncer for {}", data.name),
			move |must_exit: watch::Receiver<bool>| s2.syncer_task(must_exit, busy_tx),
		);

		let s3 = syncer.clone();
		tokio::spawn(async move {
			tokio::time::delay_for(Duration::from_secs(20)).await;
			s3.add_full_sync();
		});

		syncer
	}

	async fn watcher_task(
		self: Arc<Self>,
		mut must_exit: watch::Receiver<bool>,
		mut busy_rx: mpsc::UnboundedReceiver<bool>,
	) -> Result<(), Error> {
		let mut ring_recv: watch::Receiver<Arc<Ring>> = self.aux.system.ring.clone();
		let mut nothing_to_do_since = Some(Instant::now());

		while !*must_exit.borrow() {
			let s_ring_recv = ring_recv.recv().fuse();
			let s_busy = busy_rx.recv().fuse();
			let s_must_exit = must_exit.recv().fuse();
			let s_timeout = tokio::time::delay_for(Duration::from_secs(1)).fuse();
			pin_mut!(s_ring_recv, s_busy, s_must_exit, s_timeout);

			select! {
				new_ring_r = s_ring_recv => {
					if new_ring_r.is_some() {
						debug!("({}) Ring changed, adding full sync to syncer todo list", self.data.name);
						self.add_full_sync();
					}
				}
				busy_opt = s_busy => {
					if let Some(busy) = busy_opt {
						if busy {
							nothing_to_do_since = None;
						} else {
							if nothing_to_do_since.is_none() {
								nothing_to_do_since = Some(Instant::now());
							}
						}
					}
				}
				must_exit_v = s_must_exit => {
					if must_exit_v.unwrap_or(false) {
						break;
					}
				}
				_ = s_timeout => {
					if nothing_to_do_since.map(|t| Instant::now() - t >= ANTI_ENTROPY_INTERVAL).unwrap_or(false) {
						nothing_to_do_since = None;
						debug!("({}) Interval passed, adding full sync to syncer todo list", self.data.name);
						self.add_full_sync();
					}
				}
			}
		}
		Ok(())
	}

	pub fn add_full_sync(&self) {
		self.todo
			.lock()
			.unwrap()
			.add_full_sync(&self.data, &self.aux);
	}

	async fn syncer_task(
		self: Arc<Self>,
		mut must_exit: watch::Receiver<bool>,
		busy_tx: mpsc::UnboundedSender<bool>,
	) -> Result<(), Error> {
		while !*must_exit.borrow() {
			let task = self.todo.lock().unwrap().pop_task();
			if let Some(partition) = task {
				busy_tx.send(true)?;
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
				busy_tx.send(false)?;
				tokio::time::delay_for(Duration::from_secs(1)).await;
			}
		}
		Ok(())
	}

	async fn sync_partition(
		self: Arc<Self>,
		partition: &TodoPartition,
		must_exit: &mut watch::Receiver<bool>,
	) -> Result<(), Error> {
		if partition.retain {
			let my_id = self.aux.system.id;

			let nodes = self
				.aux
				.replication
				.write_nodes(
					&hash_of_merkle_partition(partition.range.begin),
					&self.aux.system,
				)
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
			if n_errors > self.aux.replication.max_write_errors() {
				return Err(Error::Message(format!(
					"Sync failed with too many nodes (should have been: {:?}).",
					nodes
				)));
			}
		} else {
			self.offload_partition(
				&hash_of_merkle_partition(partition.range.begin),
				&hash_of_merkle_partition_opt(partition.range.end),
				must_exit,
			)
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

			if items.len() > 0 {
				let nodes = self
					.aux
					.replication
					.write_nodes(&begin, &self.aux.system)
					.into_iter()
					.collect::<Vec<_>>();
				if nodes.contains(&self.aux.system.id) {
					warn!("Interrupting offload as partitions seem to have changed");
					break;
				}

				counter += 1;
				debug!(
					"Offloading {} items from {:?}..{:?} ({})",
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
		items: &Vec<(Vec<u8>, Arc<ByteBuf>)>,
		nodes: &[UUID],
	) -> Result<(), Error> {
		let values = items.iter().map(|(_k, v)| v.clone()).collect::<Vec<_>>();
		let update_msg = Arc::new(TableRPC::<F>::Update(values));

		for res in join_all(nodes.iter().map(|to| {
			self.aux
				.rpc_client
				.call_arc(*to, update_msg.clone(), TABLE_SYNC_RPC_TIMEOUT)
		}))
		.await
		{
			res?;
		}

		// All remote nodes have written those items, now we can delete them locally
		let mut not_removed = 0;
		for (k, v) in items.iter() {
			if !self.data.delete_if_equal(&k[..], &v[..])? {
				not_removed += 1;
			}
		}

		if not_removed > 0 {
			debug!("{} items not removed during offload because they changed in between (trying again...)", not_removed);
		}

		Ok(())
	}

	// ======= SYNCHRONIZATION PROCEDURE -- DRIVER SIDE ======
	// The driver side is only concerned with sending out the item it has
	// and the other side might not have. Receiving items that differ from one
	// side to the other will happen when the other side syncs with us,
	// which they also do regularly.

	fn get_root_ck(&self, range: PartitionRange) -> Result<RootCk, Error> {
		let begin = u16::from_be_bytes(range.begin);
		let range_iter = match range.end {
			Some(end) => {
				let end = u16::from_be_bytes(end);
				begin..=(end - 1)
			}
			None => begin..=0xFFFF,
		};

		let mut ret = vec![];
		for i in range_iter {
			let key = MerkleNodeKey {
				partition: u16::to_be_bytes(i),
				prefix: vec![],
			};
			match self.data.merkle_updater.read_node(&key)? {
				MerkleNode::Empty => (),
				x => {
					ret.push((key.partition, hash_of(&x)?));
				}
			}
		}
		Ok(ret)
	}

	async fn do_sync_with(
		self: Arc<Self>,
		partition: TodoPartition,
		who: UUID,
		must_exit: watch::Receiver<bool>,
	) -> Result<(), Error> {
		let root_ck = self.get_root_ck(partition.range)?;
		let root_ck_hash = hash_of(&root_ck)?;

		// If their root checksum has level > than us, use that as a reference
		let root_resp = self
			.aux
			.rpc_client
			.call(
				who,
				TableRPC::<F>::SyncRPC(SyncRPC::RootCkHash(partition.range, root_ck_hash)),
				TABLE_SYNC_RPC_TIMEOUT,
			)
			.await?;

		let mut todo = match root_resp {
			TableRPC::<F>::SyncRPC(SyncRPC::CkNoDifference) => {
				debug!(
					"({}) Sync {:?} with {:?}: no difference",
					self.data.name, partition, who
				);
				return Ok(());
			}
			TableRPC::<F>::SyncRPC(SyncRPC::RootCkList(_, their_root_ck)) => {
				let join = join_ordered(&root_ck[..], &their_root_ck[..]);
				let mut todo = VecDeque::new();
				for (p, v1, v2) in join.iter() {
					let diff = match (v1, v2) {
						(Some(_), None) | (None, Some(_)) => true,
						(Some(a), Some(b)) => a != b,
						_ => false,
					};
					if diff {
						todo.push_back(MerkleNodeKey {
							partition: **p,
							prefix: vec![],
						});
					}
				}
				debug!(
					"({}) Sync {:?} with {:?}: todo.len() = {}",
					self.data.name,
					partition,
					who,
					todo.len()
				);
				todo
			}
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
			let node = self.data.merkle_updater.read_node(&key)?;

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
							warn!("Hashes differ between stored value and Merkle tree, key: {:?} (if your server is very busy, don't worry, this happens when the Merkle tree can't be updated fast enough)", ik);
						}
						todo_items.push(val.to_vec());
					}
				}
				MerkleNode::Intermediate(l) => {
					let remote_node = match self
						.aux
						.rpc_client
						.call(
							who,
							TableRPC::<F>::SyncRPC(SyncRPC::GetNode(key.clone())),
							TABLE_SYNC_RPC_TIMEOUT,
						)
						.await?
					{
						TableRPC::<F>::SyncRPC(SyncRPC::Node(_, node)) => node,
						x => {
							return Err(Error::Message(format!(
								"Invalid respone to GetNode RPC: {}",
								debug_serialize(x)
							)));
						}
					};
					let int_l2 = match remote_node {
						MerkleNode::Intermediate(l2) => l2,
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
				self.send_items(who, std::mem::replace(&mut todo_items, vec![]))
					.await?;
			}
		}

		if !todo_items.is_empty() {
			self.send_items(who, todo_items).await?;
		}

		Ok(())
	}

	async fn send_items(&self, who: UUID, item_list: Vec<Vec<u8>>) -> Result<(), Error> {
		info!(
			"({}) Sending {} items to {:?}",
			self.data.name,
			item_list.len(),
			who
		);

		let mut values = vec![];
		for item in item_list.iter() {
			if let Some(v) = self.data.store.get(&item[..])? {
				values.push(Arc::new(ByteBuf::from(v.as_ref())));
			}
		}
		let rpc_resp = self
			.aux
			.rpc_client
			.call(who, TableRPC::<F>::Update(values), TABLE_SYNC_RPC_TIMEOUT)
			.await?;
		if let TableRPC::<F>::Ok = rpc_resp {
			Ok(())
		} else {
			Err(Error::Message(format!(
				"Unexpected response to RPC Update: {}",
				debug_serialize(&rpc_resp)
			)))
		}
	}

	// ======= SYNCHRONIZATION PROCEDURE -- RECEIVER SIDE ======

	pub(crate) async fn handle_rpc(self: &Arc<Self>, message: &SyncRPC) -> Result<SyncRPC, Error> {
		match message {
			SyncRPC::RootCkHash(range, h) => {
				let root_ck = self.get_root_ck(*range)?;
				let hash = hash_of(&root_ck)?;
				if hash == *h {
					Ok(SyncRPC::CkNoDifference)
				} else {
					Ok(SyncRPC::RootCkList(*range, root_ck))
				}
			}
			SyncRPC::GetNode(k) => {
				let node = self.data.merkle_updater.read_node(&k)?;
				Ok(SyncRPC::Node(k.clone(), node))
			}
			_ => Err(Error::Message(format!("Unexpected sync RPC"))),
		}
	}
}

impl SyncTodo {
	fn add_full_sync<F: TableSchema, R: TableReplication>(
		&mut self,
		data: &TableData<F>,
		aux: &TableAux<F, R>,
	) {
		let my_id = aux.system.id;

		self.todo.clear();

		let ring = aux.system.ring.borrow().clone();
		let split_points = aux.replication.split_points(&ring);

		for i in 0..split_points.len() {
			let begin: MerklePartition = {
				let b = split_points[i];
				assert_eq!(b.as_slice()[2..], [0u8; 30][..]);
				b.as_slice()[..2].try_into().unwrap()
			};

			let end: Option<MerklePartition> = if i + 1 < split_points.len() {
				let e = split_points[i + 1];
				assert_eq!(e.as_slice()[2..], [0u8; 30][..]);
				Some(e.as_slice()[..2].try_into().unwrap())
			} else {
				None
			};

			let begin_hash = hash_of_merkle_partition(begin);
			let end_hash = hash_of_merkle_partition_opt(end);

			let nodes = aux.replication.replication_nodes(&begin_hash, &ring);

			let retain = nodes.contains(&my_id);
			if !retain {
				// Check if we have some data to send, otherwise skip
				if data.store.range(begin_hash..end_hash).next().is_none() {
					continue;
				}
			}

			self.todo.push(TodoPartition {
				range: PartitionRange { begin, end },
				retain,
			});
		}
	}

	fn pop_task(&mut self) -> Option<TodoPartition> {
		if self.todo.is_empty() {
			return None;
		}

		let i = rand::thread_rng().gen_range::<usize, _, _>(0, self.todo.len());
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
			ret.push((&x[i].0, None, Some(&y[j].1)));
			j += 1;
		} else {
			unreachable!();
		}
	}
	ret
}
