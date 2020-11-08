use rand::Rng;
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use futures::{pin_mut, select};
use futures_util::future::*;
use futures_util::stream::*;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use tokio::sync::Mutex;
use tokio::sync::{mpsc, watch};

use garage_rpc::membership::Ring;
use garage_util::data::*;
use garage_util::error::Error;

use crate::*;

const MAX_DEPTH: usize = 16;
const SCAN_INTERVAL: Duration = Duration::from_secs(3600);
const CHECKSUM_CACHE_TIMEOUT: Duration = Duration::from_secs(1800);
const TABLE_SYNC_RPC_TIMEOUT: Duration = Duration::from_secs(30);

pub struct TableSyncer<F: TableSchema, R: TableReplication> {
	table: Arc<Table<F, R>>,
	todo: Mutex<SyncTodo>,
	cache: Vec<Mutex<BTreeMap<SyncRange, RangeChecksumCache>>>,
}

#[derive(Serialize, Deserialize)]
pub enum SyncRPC {
	GetRootChecksumRange(Hash, Hash),
	RootChecksumRange(SyncRange),
	Checksums(Vec<RangeChecksum>, bool),
	Difference(Vec<SyncRange>, Vec<Arc<ByteBuf>>),
}

pub struct SyncTodo {
	todo: Vec<TodoPartition>,
}

#[derive(Debug, Clone)]
struct TodoPartition {
	begin: Hash,
	end: Hash,
	retain: bool,
}

// A SyncRange defines a query on the dataset stored by a node, in the following way:
// - all items whose key are >= `begin`
// - stopping at the first item whose key hash has at least `level` leading zero bytes (excluded)
// - except if the first item of the range has such many leading zero bytes
// - and stopping at `end` (excluded) if such an item is not found
// The checksum itself does not store all of the items in the database, only the hashes of the "sub-ranges"
// i.e. of ranges of level `level-1` that cover the same range
// (ranges of level 0 do not exist and their hash is simply the hash of the first item >= begin)
// See RangeChecksum for the struct that stores this information.
#[derive(Hash, PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct SyncRange {
	begin: Vec<u8>,
	end: Vec<u8>,
	level: usize,
}

impl std::cmp::PartialOrd for SyncRange {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}
impl std::cmp::Ord for SyncRange {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		self.begin
			.cmp(&other.begin)
			.then(self.level.cmp(&other.level))
			.then(self.end.cmp(&other.end))
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeChecksum {
	bounds: SyncRange,
	children: Vec<(SyncRange, Hash)>,
	found_limit: Option<Vec<u8>>,

	#[serde(skip, default = "std::time::Instant::now")]
	time: Instant,
}

#[derive(Debug, Clone)]
pub struct RangeChecksumCache {
	hash: Option<Hash>, // None if no children
	found_limit: Option<Vec<u8>>,
	time: Instant,
}

impl<F, R> TableSyncer<F, R>
where
	F: TableSchema + 'static,
	R: TableReplication + 'static,
{
	pub async fn launch(table: Arc<Table<F, R>>) -> Arc<Self> {
		let todo = SyncTodo { todo: Vec::new() };
		let syncer = Arc::new(TableSyncer {
			table: table.clone(),
			todo: Mutex::new(todo),
			cache: (0..MAX_DEPTH)
				.map(|_| Mutex::new(BTreeMap::new()))
				.collect::<Vec<_>>(),
		});

		let (busy_tx, busy_rx) = mpsc::unbounded_channel();

		let s1 = syncer.clone();
		table
			.system
			.background
			.spawn_worker(
				format!("table sync watcher for {}", table.name),
				move |must_exit: watch::Receiver<bool>| s1.watcher_task(must_exit, busy_rx),
			)
			.await;

		let s2 = syncer.clone();
		table
			.system
			.background
			.spawn_worker(
				format!("table syncer for {}", table.name),
				move |must_exit: watch::Receiver<bool>| s2.syncer_task(must_exit, busy_tx),
			)
			.await;

		let s3 = syncer.clone();
		tokio::spawn(async move {
			tokio::time::delay_for(Duration::from_secs(20)).await;
			s3.add_full_scan().await;
		});

		syncer
	}

	async fn watcher_task(
		self: Arc<Self>,
		mut must_exit: watch::Receiver<bool>,
		mut busy_rx: mpsc::UnboundedReceiver<bool>,
	) -> Result<(), Error> {
		let mut prev_ring: Arc<Ring> = self.table.system.ring.borrow().clone();
		let mut ring_recv: watch::Receiver<Arc<Ring>> = self.table.system.ring.clone();
		let mut nothing_to_do_since = Some(Instant::now());

		while !*must_exit.borrow() {
			let s_ring_recv = ring_recv.recv().fuse();
			let s_busy = busy_rx.recv().fuse();
			let s_must_exit = must_exit.recv().fuse();
			let s_timeout = tokio::time::delay_for(Duration::from_secs(1)).fuse();
			pin_mut!(s_ring_recv, s_busy, s_must_exit, s_timeout);

			select! {
				new_ring_r = s_ring_recv => {
					if let Some(new_ring) = new_ring_r {
						debug!("({}) Adding ring difference to syncer todo list", self.table.name);
						self.todo.lock().await.add_ring_difference(&self.table, &prev_ring, &new_ring);
						prev_ring = new_ring;
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
					if nothing_to_do_since.map(|t| Instant::now() - t >= SCAN_INTERVAL).unwrap_or(false) {
						nothing_to_do_since = None;
						debug!("({}) Adding full scan to syncer todo list", self.table.name);
						self.add_full_scan().await;
					}
				}
			}
		}
		Ok(())
	}

	pub async fn add_full_scan(&self) {
		self.todo.lock().await.add_full_scan(&self.table);
	}

	async fn syncer_task(
		self: Arc<Self>,
		mut must_exit: watch::Receiver<bool>,
		busy_tx: mpsc::UnboundedSender<bool>,
	) -> Result<(), Error> {
		while !*must_exit.borrow() {
			if let Some(partition) = self.todo.lock().await.pop_task() {
				busy_tx.send(true)?;
				let res = self
					.clone()
					.sync_partition(&partition, &mut must_exit)
					.await;
				if let Err(e) = res {
					warn!(
						"({}) Error while syncing {:?}: {}",
						self.table.name, partition, e
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
		let my_id = self.table.system.id;
		let nodes = self
			.table
			.replication
			.write_nodes(&partition.begin, &self.table.system)
			.into_iter()
			.filter(|node| *node != my_id)
			.collect::<Vec<_>>();

		debug!(
			"({}) Preparing to sync {:?} with {:?}...",
			self.table.name, partition, nodes
		);
		let root_cks = self
			.root_checksum(&partition.begin, &partition.end, must_exit)
			.await?;

		let mut sync_futures = nodes
			.iter()
			.map(|node| {
				self.clone().do_sync_with(
					partition.clone(),
					root_cks.clone(),
					*node,
					partition.retain,
					must_exit.clone(),
				)
			})
			.collect::<FuturesUnordered<_>>();

		let mut n_errors = 0;
		while let Some(r) = sync_futures.next().await {
			if let Err(e) = r {
				n_errors += 1;
				warn!("({}) Sync error: {}", self.table.name, e);
			}
		}
		if n_errors > self.table.replication.max_write_errors() {
			return Err(Error::Message(format!(
				"Sync failed with too many nodes (should have been: {:?}).",
				nodes
			)));
		}

		if !partition.retain {
			self.table
				.delete_range(&partition.begin, &partition.end)
				.await?;
		}

		Ok(())
	}

	async fn root_checksum(
		self: &Arc<Self>,
		begin: &Hash,
		end: &Hash,
		must_exit: &mut watch::Receiver<bool>,
	) -> Result<RangeChecksum, Error> {
		for i in 1..MAX_DEPTH {
			let rc = self
				.range_checksum(
					&SyncRange {
						begin: begin.to_vec(),
						end: end.to_vec(),
						level: i,
					},
					must_exit,
				)
				.await?;
			if rc.found_limit.is_none() {
				return Ok(rc);
			}
		}
		Err(Error::Message(format!(
			"Unable to compute root checksum (this should never happen)"
		)))
	}

	async fn range_checksum(
		self: &Arc<Self>,
		range: &SyncRange,
		must_exit: &mut watch::Receiver<bool>,
	) -> Result<RangeChecksum, Error> {
		assert!(range.level != 0);

		if range.level == 1 {
			let mut children = vec![];
			for item in self
				.table
				.store
				.range(range.begin.clone()..range.end.clone())
			{
				let (key, value) = item?;
				let key_hash = hash(&key[..]);
				if children.len() > 0
					&& key_hash.as_slice()[0..range.level]
						.iter()
						.all(|x| *x == 0u8)
				{
					return Ok(RangeChecksum {
						bounds: range.clone(),
						children,
						found_limit: Some(key.to_vec()),
						time: Instant::now(),
					});
				}
				let item_range = SyncRange {
					begin: key.to_vec(),
					end: vec![],
					level: 0,
				};
				children.push((item_range, hash(&value[..])));
			}
			Ok(RangeChecksum {
				bounds: range.clone(),
				children,
				found_limit: None,
				time: Instant::now(),
			})
		} else {
			let mut children = vec![];
			let mut sub_range = SyncRange {
				begin: range.begin.clone(),
				end: range.end.clone(),
				level: range.level - 1,
			};
			let mut time = Instant::now();
			while !*must_exit.borrow() {
				let sub_ck = self
					.range_checksum_cached_hash(&sub_range, must_exit)
					.await?;

				if let Some(hash) = sub_ck.hash {
					children.push((sub_range.clone(), hash));
					if sub_ck.time < time {
						time = sub_ck.time;
					}
				}

				if sub_ck.found_limit.is_none() || sub_ck.hash.is_none() {
					return Ok(RangeChecksum {
						bounds: range.clone(),
						children,
						found_limit: None,
						time,
					});
				}
				let found_limit = sub_ck.found_limit.unwrap();

				let actual_limit_hash = hash(&found_limit[..]);
				if actual_limit_hash.as_slice()[0..range.level]
					.iter()
					.all(|x| *x == 0u8)
				{
					return Ok(RangeChecksum {
						bounds: range.clone(),
						children,
						found_limit: Some(found_limit.clone()),
						time,
					});
				}

				sub_range.begin = found_limit;
			}
			Err(Error::Message(format!("Exiting.")))
		}
	}

	fn range_checksum_cached_hash<'a>(
		self: &'a Arc<Self>,
		range: &'a SyncRange,
		must_exit: &'a mut watch::Receiver<bool>,
	) -> BoxFuture<'a, Result<RangeChecksumCache, Error>> {
		async move {
			let mut cache = self.cache[range.level].lock().await;
			if let Some(v) = cache.get(&range) {
				if Instant::now() - v.time < CHECKSUM_CACHE_TIMEOUT {
					return Ok(v.clone());
				}
			}
			cache.remove(&range);
			drop(cache);

			let v = self.range_checksum(&range, must_exit).await?;
			trace!(
				"({}) New checksum calculated for {}-{}/{}, {} children",
				self.table.name,
				hex::encode(&range.begin)
					.chars()
					.take(16)
					.collect::<String>(),
				hex::encode(&range.end).chars().take(16).collect::<String>(),
				range.level,
				v.children.len()
			);

			let hash = if v.children.len() > 0 {
				Some(hash(&rmp_to_vec_all_named(&v)?[..]))
			} else {
				None
			};
			let cache_entry = RangeChecksumCache {
				hash,
				found_limit: v.found_limit,
				time: v.time,
			};

			let mut cache = self.cache[range.level].lock().await;
			cache.insert(range.clone(), cache_entry.clone());
			Ok(cache_entry)
		}
		.boxed()
	}

	async fn do_sync_with(
		self: Arc<Self>,
		partition: TodoPartition,
		root_ck: RangeChecksum,
		who: UUID,
		retain: bool,
		mut must_exit: watch::Receiver<bool>,
	) -> Result<(), Error> {
		let mut todo = VecDeque::new();

		// If their root checksum has level > than us, use that as a reference
		let root_cks_resp = self
			.table
			.rpc_client
			.call(
				who,
				TableRPC::<F>::SyncRPC(SyncRPC::GetRootChecksumRange(
					partition.begin.clone(),
					partition.end.clone(),
				)),
				TABLE_SYNC_RPC_TIMEOUT,
			)
			.await?;
		if let TableRPC::<F>::SyncRPC(SyncRPC::RootChecksumRange(range)) = root_cks_resp {
			if range.level > root_ck.bounds.level {
				let their_root_range_ck = self.range_checksum(&range, &mut must_exit).await?;
				todo.push_back(their_root_range_ck);
			} else {
				todo.push_back(root_ck);
			}
		} else {
			return Err(Error::Message(format!(
				"Invalid respone to GetRootChecksumRange RPC: {}",
				debug_serialize(root_cks_resp)
			)));
		}

		while !todo.is_empty() && !*must_exit.borrow() {
			let total_children = todo.iter().map(|x| x.children.len()).fold(0, |x, y| x + y);
			trace!(
				"({}) Sync with {:?}: {} ({}) remaining",
				self.table.name,
				who,
				todo.len(),
				total_children
			);

			let step_size = std::cmp::min(16, todo.len());
			let step = todo.drain(..step_size).collect::<Vec<_>>();

			let rpc_resp = self
				.table
				.rpc_client
				.call(
					who,
					TableRPC::<F>::SyncRPC(SyncRPC::Checksums(step, retain)),
					TABLE_SYNC_RPC_TIMEOUT,
				)
				.await?;
			if let TableRPC::<F>::SyncRPC(SyncRPC::Difference(mut diff_ranges, diff_items)) =
				rpc_resp
			{
				if diff_ranges.len() > 0 || diff_items.len() > 0 {
					info!(
						"({}) Sync with {:?}: difference {} ranges, {} items",
						self.table.name,
						who,
						diff_ranges.len(),
						diff_items.len()
					);
				}
				let mut items_to_send = vec![];
				for differing in diff_ranges.drain(..) {
					if differing.level == 0 {
						items_to_send.push(differing.begin);
					} else {
						let checksum = self.range_checksum(&differing, &mut must_exit).await?;
						todo.push_back(checksum);
					}
				}
				if retain && diff_items.len() > 0 {
					self.table.handle_update(&diff_items[..]).await?;
				}
				if items_to_send.len() > 0 {
					self.send_items(who, items_to_send).await?;
				}
			} else {
				return Err(Error::Message(format!(
					"Unexpected response to sync RPC checksums: {}",
					debug_serialize(&rpc_resp)
				)));
			}
		}
		Ok(())
	}

	async fn send_items(&self, who: UUID, item_list: Vec<Vec<u8>>) -> Result<(), Error> {
		info!(
			"({}) Sending {} items to {:?}",
			self.table.name,
			item_list.len(),
			who
		);

		let mut values = vec![];
		for item in item_list.iter() {
			if let Some(v) = self.table.store.get(&item[..])? {
				values.push(Arc::new(ByteBuf::from(v.as_ref())));
			}
		}
		let rpc_resp = self
			.table
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

	pub async fn handle_rpc(
		self: &Arc<Self>,
		message: &SyncRPC,
		mut must_exit: watch::Receiver<bool>,
	) -> Result<SyncRPC, Error> {
		match message {
			SyncRPC::GetRootChecksumRange(begin, end) => {
				let root_cks = self.root_checksum(&begin, &end, &mut must_exit).await?;
				Ok(SyncRPC::RootChecksumRange(root_cks.bounds))
			}
			SyncRPC::Checksums(checksums, retain) => {
				self.handle_checksums_rpc(&checksums[..], *retain, &mut must_exit)
					.await
			}
			_ => Err(Error::Message(format!("Unexpected sync RPC"))),
		}
	}

	async fn handle_checksums_rpc(
		self: &Arc<Self>,
		checksums: &[RangeChecksum],
		retain: bool,
		must_exit: &mut watch::Receiver<bool>,
	) -> Result<SyncRPC, Error> {
		let mut ret_ranges = vec![];
		let mut ret_items = vec![];

		for their_ckr in checksums.iter() {
			let our_ckr = self.range_checksum(&their_ckr.bounds, must_exit).await?;
			for (their_range, their_hash) in their_ckr.children.iter() {
				let differs = match our_ckr
					.children
					.binary_search_by(|(our_range, _)| our_range.cmp(&their_range))
				{
					Err(_) => {
						if their_range.level >= 1 {
							let cached_hash = self
								.range_checksum_cached_hash(&their_range, must_exit)
								.await?;
							cached_hash.hash.map(|h| h != *their_hash).unwrap_or(true)
						} else {
							true
						}
					}
					Ok(i) => our_ckr.children[i].1 != *their_hash,
				};
				if differs {
					ret_ranges.push(their_range.clone());
					if retain && their_range.level == 0 {
						if let Some(item_bytes) =
							self.table.store.get(their_range.begin.as_slice())?
						{
							ret_items.push(Arc::new(ByteBuf::from(item_bytes.to_vec())));
						}
					}
				}
			}
			for (our_range, _hash) in our_ckr.children.iter() {
				if let Some(their_found_limit) = &their_ckr.found_limit {
					if our_range.begin.as_slice() > their_found_limit.as_slice() {
						break;
					}
				}

				let not_present = our_ckr
					.children
					.binary_search_by(|(their_range, _)| their_range.cmp(&our_range))
					.is_err();
				if not_present {
					if our_range.level > 0 {
						ret_ranges.push(our_range.clone());
					}
					if retain && our_range.level == 0 {
						if let Some(item_bytes) =
							self.table.store.get(our_range.begin.as_slice())?
						{
							ret_items.push(Arc::new(ByteBuf::from(item_bytes.to_vec())));
						}
					}
				}
			}
		}
		let n_checksums = checksums
			.iter()
			.map(|x| x.children.len())
			.fold(0, |x, y| x + y);
		if ret_ranges.len() > 0 || ret_items.len() > 0 {
			trace!(
				"({}) Checksum comparison RPC: {} different + {} items for {} received",
				self.table.name,
				ret_ranges.len(),
				ret_items.len(),
				n_checksums
			);
		}
		Ok(SyncRPC::Difference(ret_ranges, ret_items))
	}

	pub async fn invalidate(self: Arc<Self>, item_key: Vec<u8>) -> Result<(), Error> {
		for i in 1..MAX_DEPTH {
			let needle = SyncRange {
				begin: item_key.to_vec(),
				end: vec![],
				level: i,
			};
			let mut cache = self.cache[i].lock().await;
			if let Some(cache_entry) = cache.range(..=needle).rev().next() {
				if cache_entry.0.begin <= item_key && cache_entry.0.end > item_key {
					let index = cache_entry.0.clone();
					drop(cache_entry);
					cache.remove(&index);
				}
			}
		}
		Ok(())
	}
}

impl SyncTodo {
	fn add_full_scan<F: TableSchema, R: TableReplication>(&mut self, table: &Table<F, R>) {
		let my_id = table.system.id;

		self.todo.clear();

		let ring = table.system.ring.borrow().clone();
		let split_points = table.replication.split_points(&ring);

		for i in 0..split_points.len() - 1 {
			let begin = split_points[i];
			let end = split_points[i + 1];
			let nodes = table.replication.replication_nodes(&begin, &ring);

			let retain = nodes.contains(&my_id);
			if !retain {
				// Check if we have some data to send, otherwise skip
				if table.store.range(begin..end).next().is_none() {
					continue;
				}
			}

			self.todo.push(TodoPartition { begin, end, retain });
		}
	}

	fn add_ring_difference<F: TableSchema, R: TableReplication>(
		&mut self,
		table: &Table<F, R>,
		old_ring: &Ring,
		new_ring: &Ring,
	) {
		let my_id = table.system.id;

		// If it is us who are entering or leaving the system,
		// initiate a full sync instead of incremental sync
		if old_ring.config.members.contains_key(&my_id)
			!= new_ring.config.members.contains_key(&my_id)
		{
			self.add_full_scan(table);
			return;
		}

		let mut all_points = None
			.into_iter()
			.chain(table.replication.split_points(old_ring).drain(..))
			.chain(table.replication.split_points(new_ring).drain(..))
			.chain(self.todo.iter().map(|x| x.begin))
			.chain(self.todo.iter().map(|x| x.end))
			.collect::<Vec<_>>();
		all_points.sort();
		all_points.dedup();

		let mut old_todo = std::mem::replace(&mut self.todo, vec![]);
		old_todo.sort_by(|x, y| x.begin.cmp(&y.begin));
		let mut new_todo = vec![];

		for i in 0..all_points.len() - 1 {
			let begin = all_points[i];
			let end = all_points[i + 1];
			let was_ours = table
				.replication
				.replication_nodes(&begin, &old_ring)
				.contains(&my_id);
			let is_ours = table
				.replication
				.replication_nodes(&begin, &new_ring)
				.contains(&my_id);

			let was_todo = match old_todo.binary_search_by(|x| x.begin.cmp(&begin)) {
				Ok(_) => true,
				Err(j) => {
					(j > 0 && old_todo[j - 1].begin < end && begin < old_todo[j - 1].end)
						|| (j < old_todo.len()
							&& old_todo[j].begin < end && begin < old_todo[j].end)
				}
			};
			if was_todo || (is_ours && !was_ours) || (was_ours && !is_ours) {
				new_todo.push(TodoPartition {
					begin,
					end,
					retain: is_ours,
				});
			}
		}

		self.todo = new_todo;
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
