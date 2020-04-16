use rand::Rng;
use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use futures::{pin_mut, select};
use futures_util::future::*;
use tokio::sync::watch;
use tokio::sync::Mutex;

use crate::data::*;
use crate::error::Error;
use crate::membership::Ring;
use crate::table::*;

const SCAN_INTERVAL: Duration = Duration::from_secs(3600);

pub struct TableSyncer<F: TableSchema> {
	pub table: Arc<Table<F>>,

	pub todo: Mutex<SyncTodo>,
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

impl<F: TableSchema + 'static> TableSyncer<F> {
	pub async fn launch(table: Arc<Table<F>>) -> Arc<Self> {
		let todo = SyncTodo { todo: Vec::new() };
		let syncer = Arc::new(TableSyncer {
			table: table.clone(),
			todo: Mutex::new(todo),
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
		self.todo.lock().await.add_full_scan(&self.table);
		let mut next_full_scan = tokio::time::delay_for(SCAN_INTERVAL).fuse();
		let mut prev_ring: Arc<Ring> = self.table.system.ring.borrow().clone();
		let mut ring_recv: watch::Receiver<Arc<Ring>> = self.table.system.ring.clone();

		loop {
			let s_ring_recv = ring_recv.recv().fuse();
			let s_must_exit = must_exit.recv().fuse();
			pin_mut!(s_ring_recv, s_must_exit);

			select! {
				_ = next_full_scan => {
					next_full_scan = tokio::time::delay_for(SCAN_INTERVAL).fuse();
					self.todo.lock().await.add_full_scan(&self.table);
				}
				new_ring_r = s_ring_recv => {
					if let Some(new_ring) = new_ring_r {
						self.todo.lock().await.add_ring_difference(&self.table, &prev_ring, &new_ring);
						prev_ring = new_ring;
					}
				}
				must_exit_v = s_must_exit => {
					if must_exit_v.unwrap_or(false) {
						return Ok(())
					}
				}
			}
		}
	}

	async fn syncer_task(
		self: Arc<Self>,
		mut must_exit: watch::Receiver<bool>,
	) -> Result<(), Error> {
		loop {
			let s_pop_task = self.pop_task().fuse();
			let s_must_exit = must_exit.recv().fuse();
			pin_mut!(s_must_exit, s_pop_task);

			select! {
				task = s_pop_task => {
					if let Some(partition) = task {
						let res = self.sync_partition(&partition).await;
						if let Err(e) = res {
							eprintln!("Error while syncing {:?}: {}", partition, e);
						}
					} else {
						tokio::time::delay_for(Duration::from_secs(1)).await;
					}
				}
				must_exit_v = s_must_exit => {
					if must_exit_v.unwrap_or(false) {
						return Ok(())
					}
				}
			}
		}
	}

	async fn pop_task(&self) -> Option<Partition> {
		self.todo.lock().await.pop_task()
	}

	async fn sync_partition(self: &Arc<Self>, partition: &Partition) -> Result<(), Error> {
		eprintln!("NOT IMPLEMENTED: SYNC PARTITION {:?}", partition);
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
