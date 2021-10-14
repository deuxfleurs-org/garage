use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

use futures::future::join_all;
use futures::select;
use futures_util::future::*;
use tokio::sync::watch;

use garage_util::data::*;
use garage_util::error::Error;

use garage_rpc::system::System;
use garage_rpc::*;

use crate::data::*;
use crate::replication::*;
use crate::schema::*;

const TABLE_GC_BATCH_SIZE: usize = 1024;
const TABLE_GC_RPC_TIMEOUT: Duration = Duration::from_secs(30);

pub struct TableGc<F: TableSchema + 'static, R: TableReplication + 'static> {
	system: Arc<System>,
	data: Arc<TableData<F, R>>,

	endpoint: Arc<Endpoint<GcRpc, Self>>,
}

#[derive(Serialize, Deserialize)]
enum GcRpc {
	Update(Vec<ByteBuf>),
	DeleteIfEqualHash(Vec<(ByteBuf, Hash)>),
	Ok,
	Error(String),
}

impl Message for GcRpc {
	type Response = GcRpc;
}

impl<F, R> TableGc<F, R>
where
	F: TableSchema + 'static,
	R: TableReplication + 'static,
{
	pub(crate) fn launch(system: Arc<System>, data: Arc<TableData<F, R>>) -> Arc<Self> {
		let endpoint = system
			.netapp
			.endpoint(format!("garage_table/gc.rs/Rpc:{}", data.name));

		let gc = Arc::new(Self {
			system: system.clone(),
			data: data.clone(),
			endpoint,
		});

		gc.endpoint.set_handler(gc.clone());

		let gc1 = gc.clone();
		system.background.spawn_worker(
			format!("GC loop for {}", data.name),
			move |must_exit: watch::Receiver<bool>| gc1.gc_loop(must_exit),
		);

		gc
	}

	async fn gc_loop(self: Arc<Self>, mut must_exit: watch::Receiver<bool>) {
		while !*must_exit.borrow() {
			match self.gc_loop_iter().await {
				Ok(true) => {
					// Stuff was done, loop immediately
					continue;
				}
				Ok(false) => {
					// Nothing was done, sleep for some time (below)
				}
				Err(e) => {
					warn!("({}) Error doing GC: {}", self.data.name, e);
				}
			}
			select! {
				_ = tokio::time::sleep(Duration::from_secs(10)).fuse() => {},
				_ = must_exit.changed().fuse() => {},
			}
		}
	}

	async fn gc_loop_iter(&self) -> Result<bool, Error> {
		let mut entries = vec![];
		let mut excluded = vec![];

		for item in self.data.gc_todo.iter() {
			let (k, vhash) = item?;

			let vhash = Hash::try_from(&vhash[..]).unwrap();

			let v_opt = self
				.data
				.store
				.get(&k[..])?
				.filter(|v| blake2sum(&v[..]) == vhash);

			if let Some(v) = v_opt {
				entries.push((ByteBuf::from(k.to_vec()), vhash, ByteBuf::from(v.to_vec())));
				if entries.len() >= TABLE_GC_BATCH_SIZE {
					break;
				}
			} else {
				excluded.push((k, vhash));
			}
		}

		for (k, vhash) in excluded {
			self.todo_remove_if_equal(&k[..], vhash)?;
		}

		if entries.is_empty() {
			// Nothing to do in this iteration
			return Ok(false);
		}

		debug!("({}) GC: doing {} items", self.data.name, entries.len());

		let mut partitions = HashMap::new();
		for (k, vhash, v) in entries {
			let pkh = Hash::try_from(&k[..32]).unwrap();
			let mut nodes = self.data.replication.write_nodes(&pkh);
			nodes.retain(|x| *x != self.system.id);
			nodes.sort();

			if !partitions.contains_key(&nodes) {
				partitions.insert(nodes.clone(), vec![]);
			}
			partitions.get_mut(&nodes).unwrap().push((k, vhash, v));
		}

		let resps = join_all(
			partitions
				.into_iter()
				.map(|(nodes, items)| self.try_send_and_delete(nodes, items)),
		)
		.await;

		let mut errs = vec![];
		for resp in resps {
			if let Err(e) = resp {
				errs.push(e);
			}
		}

		if errs.is_empty() {
			Ok(true)
		} else {
			Err(Error::Message(
				errs.into_iter()
					.map(|x| format!("{}", x))
					.collect::<Vec<_>>()
					.join(", "),
			))
		}
	}

	async fn try_send_and_delete(
		&self,
		nodes: Vec<NodeID>,
		items: Vec<(ByteBuf, Hash, ByteBuf)>,
	) -> Result<(), Error> {
		let n_items = items.len();

		let mut updates = vec![];
		let mut deletes = vec![];
		for (k, vhash, v) in items {
			updates.push(v);
			deletes.push((k, vhash));
		}

		self.system
			.rpc
			.try_call_many(
				&self.endpoint,
				&nodes[..],
				GcRpc::Update(updates),
				RequestStrategy::with_priority(PRIO_BACKGROUND)
					.with_quorum(nodes.len())
					.with_timeout(TABLE_GC_RPC_TIMEOUT),
			)
			.await?;

		info!(
			"({}) GC: {} items successfully pushed, will try to delete.",
			self.data.name, n_items
		);

		self.system
			.rpc
			.try_call_many(
				&self.endpoint,
				&nodes[..],
				GcRpc::DeleteIfEqualHash(deletes.clone()),
				RequestStrategy::with_priority(PRIO_BACKGROUND)
					.with_quorum(nodes.len())
					.with_timeout(TABLE_GC_RPC_TIMEOUT),
			)
			.await?;

		for (k, vhash) in deletes {
			self.data.delete_if_equal_hash(&k[..], vhash)?;
			self.todo_remove_if_equal(&k[..], vhash)?;
		}

		Ok(())
	}

	fn todo_remove_if_equal(&self, key: &[u8], vhash: Hash) -> Result<(), Error> {
		let _ = self
			.data
			.gc_todo
			.compare_and_swap::<_, _, Vec<u8>>(key, Some(vhash), None)?;
		Ok(())
	}

	async fn handle_rpc(&self, message: &GcRpc) -> Result<GcRpc, Error> {
		match message {
			GcRpc::Update(items) => {
				self.data.update_many(items)?;
				Ok(GcRpc::Ok)
			}
			GcRpc::DeleteIfEqualHash(items) => {
				for (key, vhash) in items.iter() {
					self.data.delete_if_equal_hash(&key[..], *vhash)?;
					self.todo_remove_if_equal(&key[..], *vhash)?;
				}
				Ok(GcRpc::Ok)
			}
			_ => Err(Error::Message("Unexpected GC RPC".to_string())),
		}
	}
}

#[async_trait]
impl<F, R> EndpointHandler<GcRpc> for TableGc<F, R>
where
	F: TableSchema + 'static,
	R: TableReplication + 'static,
{
	async fn handle(self: &Arc<Self>, message: &GcRpc, _from: NodeID) -> GcRpc {
		self.handle_rpc(message)
			.await
			.unwrap_or_else(|e| GcRpc::Error(format!("{}", e)))
	}
}
