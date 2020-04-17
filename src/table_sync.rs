use rand::Rng;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use futures::{pin_mut, select};
use futures_util::future::*;
use futures_util::stream::*;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;
use tokio::sync::watch;
use tokio::sync::Mutex;

use crate::data::*;
use crate::error::Error;
use crate::membership::Ring;
use crate::table::*;

const MAX_DEPTH: usize = 16;
const SCAN_INTERVAL: Duration = Duration::from_secs(3600);
const CHECKSUM_CACHE_TIMEOUT: Duration = Duration::from_secs(1800);

pub struct TableSyncer<F: TableSchema> {
	pub table: Arc<Table<F>>,
	pub todo: Mutex<SyncTodo>,
	pub cache: Vec<Mutex<BTreeMap<SyncRange, RangeChecksum>>>,
}

#[derive(Serialize, Deserialize)]
pub enum SyncRPC {
	Checksums(Vec<RangeChecksum>, bool),
	Difference(Vec<SyncRange>, Vec<Arc<ByteBuf>>),
}

pub struct SyncTodo {
	pub todo: Vec<Partition>,
}

#[derive(Debug, Clone)]
pub struct Partition {
	pub begin: Hash,
	pub end: Hash,
	pub retain: bool,
}

#[derive(Hash, PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct SyncRange {
	pub begin: Vec<u8>,
	pub end: Vec<u8>,
	pub level: usize,
}

impl std::cmp::PartialOrd for SyncRange {
	fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
		Some(self.cmp(other))
	}
}
impl std::cmp::Ord for SyncRange {
	fn cmp(&self, other: &Self) -> std::cmp::Ordering {
		self.begin.cmp(&other.begin)
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RangeChecksum {
	pub bounds: SyncRange,
	pub children: Vec<(SyncRange, Hash)>,
	pub found_limit: Option<Vec<u8>>,

	#[serde(skip, default = "std::time::Instant::now")]
	pub time: Instant,
}

impl<F: TableSchema + 'static> TableSyncer<F> {
	pub async fn launch(table: Arc<Table<F>>) -> Arc<Self> {
		let todo = SyncTodo { todo: Vec::new() };
		let syncer = Arc::new(TableSyncer {
			table: table.clone(),
			todo: Mutex::new(todo),
			cache: (0..MAX_DEPTH)
				.map(|_| Mutex::new(BTreeMap::new()))
				.collect::<Vec<_>>(),
		});

		let s1 = syncer.clone();
		table
			.system
			.background
			.spawn_worker(move |must_exit: watch::Receiver<bool>| s1.watcher_task(must_exit))
			.await;

		let s2 = syncer.clone();
		table
			.system
			.background
			.spawn_worker(move |must_exit: watch::Receiver<bool>| s2.syncer_task(must_exit))
			.await;

		syncer
	}

	async fn watcher_task(
		self: Arc<Self>,
		mut must_exit: watch::Receiver<bool>,
	) -> Result<(), Error> {
		tokio::time::delay_for(Duration::from_secs(10)).await;

		self.todo.lock().await.add_full_scan(&self.table);
		let mut next_full_scan = tokio::time::delay_for(SCAN_INTERVAL).fuse();
		let mut prev_ring: Arc<Ring> = self.table.system.ring.borrow().clone();
		let mut ring_recv: watch::Receiver<Arc<Ring>> = self.table.system.ring.clone();

		while !*must_exit.borrow() {
			let s_ring_recv = ring_recv.recv().fuse();
			let s_must_exit = must_exit.recv().fuse();
			pin_mut!(s_ring_recv, s_must_exit);

			select! {
				_ = next_full_scan => {
					next_full_scan = tokio::time::delay_for(SCAN_INTERVAL).fuse();
					eprintln!("({}) Adding full scan to syncer todo list", self.table.name);
					self.todo.lock().await.add_full_scan(&self.table);
				}
				new_ring_r = s_ring_recv => {
					if let Some(new_ring) = new_ring_r {
						eprintln!("({}) Adding ring difference to syncer todo list", self.table.name);
						self.todo.lock().await.add_ring_difference(&self.table, &prev_ring, &new_ring);
						prev_ring = new_ring;
					}
				}
				must_exit_v = s_must_exit => {
					if must_exit_v.unwrap_or(false) {
						break;
					}
				}
			}
		}
		Ok(())
	}

	async fn syncer_task(
		self: Arc<Self>,
		mut must_exit: watch::Receiver<bool>,
	) -> Result<(), Error> {
		while !*must_exit.borrow() {
			if let Some(partition) = self.todo.lock().await.pop_task() {
				let res = self
					.clone()
					.sync_partition(&partition, &mut must_exit)
					.await;
				if let Err(e) = res {
					eprintln!(
						"({}) Error while syncing {:?}: {}",
						self.table.name, partition, e
					);
				}
			} else {
				tokio::time::delay_for(Duration::from_secs(1)).await;
			}
		}
		Ok(())
	}

	async fn sync_partition(
		self: Arc<Self>,
		partition: &Partition,
		must_exit: &mut watch::Receiver<bool>,
	) -> Result<(), Error> {
		eprintln!("({}) Preparing to sync {:?}...", self.table.name, partition);
		let root_cks = self
			.root_checksum(&partition.begin, &partition.end, must_exit)
			.await?;

		let my_id = self.table.system.id.clone();
		let ring = self.table.system.ring.borrow().clone();
		let nodes = ring.walk_ring(&partition.begin, self.table.param.replication_factor);
		let mut sync_futures = nodes
			.iter()
			.filter(|node| **node != my_id)
			.map(|node| {
				self.clone().do_sync_with(
					root_cks.clone(),
					node.clone(),
					partition.retain,
					must_exit.clone(),
				)
			})
			.collect::<FuturesUnordered<_>>();

		while let Some(r) = sync_futures.next().await {
			if let Err(e) = r {
				eprintln!("({}) Sync error: {}", self.table.name, e);
			}
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
			"Unable to compute root checksum (this should never happen"
		)))
	}

	fn range_checksum<'a>(
		self: &'a Arc<Self>,
		range: &'a SyncRange,
		must_exit: &'a mut watch::Receiver<bool>,
	) -> BoxFuture<'a, Result<RangeChecksum, Error>> {
		async move {
			let mut cache = self.cache[range.level].lock().await;
			if let Some(v) = cache.get(&range) {
				if Instant::now() - v.time < CHECKSUM_CACHE_TIMEOUT {
					return Ok(v.clone());
				}
			}
			cache.remove(&range);
			drop(cache);

			let v = self.range_checksum_inner(&range, must_exit).await?;
			eprintln!(
				"({}) New checksum calculated for {}-{}/{}, {} children",
				self.table.name,
				hex::encode(&range.begin[..]),
				hex::encode(&range.end[..]),
				range.level,
				v.children.len()
			);

			let mut cache = self.cache[range.level].lock().await;
			cache.insert(range.clone(), v.clone());
			Ok(v)
		}
		.boxed()
	}

	async fn range_checksum_inner(
		self: &Arc<Self>,
		range: &SyncRange,
		must_exit: &mut watch::Receiver<bool>,
	) -> Result<RangeChecksum, Error> {
		if range.level == 1 {
			let mut children = vec![];
			for item in self
				.table
				.store
				.range(range.begin.clone()..range.end.clone())
			{
				let (key, value) = item?;
				let key_hash = hash(&key[..]);
				if key != range.begin && key_hash.as_slice()[0..range.level].iter().all(|x| *x == 0)
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
				let sub_ck = self.range_checksum(&sub_range, must_exit).await?;

				if sub_ck.children.len() > 0 {
					let sub_ck_hash = hash(&rmp_to_vec_all_named(&sub_ck)?[..]);
					children.push((sub_range.clone(), sub_ck_hash));
					if sub_ck.time < time {
						time = sub_ck.time;
					}
				}

				if sub_ck.found_limit.is_none() || sub_ck.children.len() == 0 {
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
					.all(|x| *x == 0)
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

	async fn do_sync_with(
		self: Arc<Self>,
		root_ck: RangeChecksum,
		who: UUID,
		retain: bool,
		mut must_exit: watch::Receiver<bool>,
	) -> Result<(), Error> {
		let mut todo = VecDeque::new();
		todo.push_back(root_ck);

		while !todo.is_empty() && !*must_exit.borrow() {
			let total_children = todo.iter().map(|x| x.children.len()).fold(0, |x, y| x + y);
			eprintln!(
				"({}) Sync with {:?}: {} ({}) remaining",
				self.table.name,
				who,
				todo.len(),
				total_children
			);

			let end = std::cmp::min(16, todo.len());
			let step = todo.drain(..end).collect::<Vec<_>>();

			let rpc_resp = self
				.table
				.rpc_call(
					&who,
					&TableRPC::<F>::SyncRPC(SyncRPC::Checksums(step, retain)),
				)
				.await?;
			if let TableRPC::<F>::SyncRPC(SyncRPC::Difference(mut diff_ranges, diff_items)) =
				rpc_resp
			{
				eprintln!(
					"({}) Sync with {:?}: difference {} ranges, {} items",
					self.table.name,
					who,
					diff_ranges.len(),
					diff_items.len()
				);
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
					self.table.handle_update(diff_items).await?;
				}
				if items_to_send.len() > 0 {
					self.table
						.system
						.background
						.spawn(self.clone().send_items(who.clone(), items_to_send));
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

	async fn send_items(self: Arc<Self>, who: UUID, item_list: Vec<Vec<u8>>) -> Result<(), Error> {
		eprintln!(
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
			.rpc_call(&who, &TableRPC::<F>::Update(values))
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
		if let SyncRPC::Checksums(checksums, retain) = message {
			let mut ret_ranges = vec![];
			let mut ret_items = vec![];
			for ckr in checksums.iter() {
				let our_ckr = self.range_checksum(&ckr.bounds, &mut must_exit).await?;
				for (range, hash) in ckr.children.iter() {
					// Only consider items that are in the intersection of the two ranges
					// (other ranges will be exchanged at some point)
					if our_ckr
						.found_limit
						.as_ref()
						.map(|x| range.begin.as_slice() >= x.as_slice())
						.unwrap_or(false)
					{
						break;
					}

					let differs = match our_ckr
						.children
						.binary_search_by(|(our_range, _)| our_range.begin.cmp(&range.begin))
					{
						Err(_) => true,
						Ok(i) => our_ckr.children[i].1 != *hash,
					};
					if differs {
						ret_ranges.push(range.clone());
						if *retain && range.level == 0 {
							if let Some(item_bytes) =
								self.table.store.get(range.begin.as_slice())?
							{
								ret_items.push(Arc::new(ByteBuf::from(item_bytes.to_vec())));
							}
						}
					}
				}
				for (range, _hash) in our_ckr.children.iter() {
					if ckr
						.found_limit
						.as_ref()
						.map(|x| range.begin.as_slice() >= x.as_slice())
						.unwrap_or(false)
					{
						break;
					}

					let not_present = ckr
						.children
						.binary_search_by(|(their_range, _)| their_range.begin.cmp(&range.begin))
						.is_err();
					if not_present {
						if range.level > 0 {
							ret_ranges.push(range.clone());
						}
						if *retain && range.level == 0 {
							if let Some(item_bytes) =
								self.table.store.get(range.begin.as_slice())?
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
			eprintln!(
				"({}) Checksum comparison RPC: {} different + {} items for {} received",
				self.table.name,
				ret_ranges.len(),
				ret_items.len(),
				n_checksums
			);
			return Ok(SyncRPC::Difference(ret_ranges, ret_items));
		}
		Err(Error::Message(format!("Unexpected sync RPC")))
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
	fn add_full_scan<F: TableSchema>(&mut self, table: &Table<F>) {
		let my_id = table.system.id.clone();

		self.todo.clear();

		let ring: Arc<Ring> = table.system.ring.borrow().clone();

		for i in 0..ring.ring.len() {
			let nodes = ring.walk_ring_from_pos(i, table.param.replication_factor);
			let begin = ring.ring[i].location.clone();

			if i == 0 {
				self.add_full_scan_aux(table, [0u8; 32].into(), begin.clone(), &nodes[..], &my_id);
			}

			if i == ring.ring.len() - 1 {
				self.add_full_scan_aux(table, begin, [0xffu8; 32].into(), &nodes[..], &my_id);
			} else {
				let end = ring.ring[i + 1].location.clone();
				self.add_full_scan_aux(table, begin, end, &nodes[..], &my_id);
			}
		}
	}

	fn add_full_scan_aux<F: TableSchema>(
		&mut self,
		table: &Table<F>,
		begin: Hash,
		end: Hash,
		nodes: &[UUID],
		my_id: &UUID,
	) {
		let retain = nodes.contains(my_id);
		if !retain {
			// Check if we have some data to send, otherwise skip
			if table
				.store
				.range(begin.clone()..end.clone())
				.next()
				.is_none()
			{
				return;
			}
		}

		self.todo.push(Partition { begin, end, retain });
	}

	fn add_ring_difference<F: TableSchema>(&mut self, table: &Table<F>, old: &Ring, new: &Ring) {
		let my_id = table.system.id.clone();

		let old_ring = ring_points(old);
		let new_ring = ring_points(new);
		let both_ring = old_ring.union(&new_ring).cloned().collect::<BTreeSet<_>>();

		let prev_todo_begin = self
			.todo
			.iter()
			.map(|x| x.begin.clone())
			.collect::<BTreeSet<_>>();
		let prev_todo_end = self
			.todo
			.iter()
			.map(|x| x.end.clone())
			.collect::<BTreeSet<_>>();
		let prev_todo = prev_todo_begin
			.union(&prev_todo_end)
			.cloned()
			.collect::<BTreeSet<_>>();

		let all_points = both_ring.union(&prev_todo).cloned().collect::<Vec<_>>();

		self.todo.sort_by(|x, y| x.begin.cmp(&y.begin));
		let mut new_todo = vec![];
		for i in 0..all_points.len() - 1 {
			let begin = all_points[i].clone();
			let end = all_points[i + 1].clone();
			let was_ours = old
				.walk_ring(&begin, table.param.replication_factor)
				.contains(&my_id);
			let is_ours = new
				.walk_ring(&begin, table.param.replication_factor)
				.contains(&my_id);
			let was_todo = match self.todo.binary_search_by(|x| x.begin.cmp(&begin)) {
				Ok(_) => true,
				Err(j) => {
					(j > 0 && self.todo[j - 1].begin < end && begin < self.todo[j - 1].end)
						|| (j < self.todo.len()
							&& self.todo[j].begin < end && begin < self.todo[j].end)
				}
			};
			if was_todo || (is_ours && !was_ours) || (was_ours && !is_ours) {
				new_todo.push(Partition {
					begin,
					end,
					retain: is_ours,
				});
			}
		}

		self.todo = new_todo;
	}

	fn pop_task(&mut self) -> Option<Partition> {
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

fn ring_points(ring: &Ring) -> BTreeSet<Hash> {
	let mut ret = BTreeSet::new();
	ret.insert([0u8; 32].into());
	ret.insert([0xFFu8; 32].into());
	for i in 0..ring.ring.len() {
		ret.insert(ring.ring[i].location.clone());
	}
	ret
}
