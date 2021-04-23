use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use serde_bytes::ByteBuf;

use futures::future::join_all;
use futures::select;
use futures_util::future::*;
use tokio::sync::watch;

use garage_util::data::*;
use garage_util::error::Error;

use garage_rpc::membership::System;
use garage_rpc::rpc_client::*;
use garage_rpc::rpc_server::*;

use crate::data::*;
use crate::replication::*;
use crate::schema::*;

const TABLE_GC_BATCH_SIZE: usize = 1024;
const TABLE_GC_RPC_TIMEOUT: Duration = Duration::from_secs(30);

pub struct TableGC<F: TableSchema, R: TableReplication> {
	system: Arc<System>,
	data: Arc<TableData<F, R>>,

	rpc_client: Arc<RpcClient<GcRPC>>,
}

#[derive(Serialize, Deserialize)]
enum GcRPC {
	Update(Vec<ByteBuf>),
	DeleteIfEqualHash(Vec<(ByteBuf, Hash)>),
	Ok,
}

impl RpcMessage for GcRPC {}

impl<F, R> TableGC<F, R>
where
	F: TableSchema + 'static,
	R: TableReplication + 'static,
{
	pub(crate) fn launch(
		system: Arc<System>,
		data: Arc<TableData<F, R>>,
		rpc_server: &mut RpcServer,
	) -> Arc<Self> {
		let rpc_path = format!("table_{}/gc", data.name);
		let rpc_client = system.rpc_client::<GcRPC>(&rpc_path);

		let gc = Arc::new(Self {
			system: system.clone(),
			data: data.clone(),
			rpc_client,
		});

		gc.register_handler(rpc_server, rpc_path);

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
		nodes: Vec<UUID>,
		items: Vec<(ByteBuf, Hash, ByteBuf)>,
	) -> Result<(), Error> {
		let n_items = items.len();

		let mut updates = vec![];
		let mut deletes = vec![];
		for (k, vhash, v) in items {
			updates.push(v);
			deletes.push((k, vhash));
		}

		self.rpc_client
			.try_call_many(
				&nodes[..],
				GcRPC::Update(updates),
				RequestStrategy::with_quorum(nodes.len()).with_timeout(TABLE_GC_RPC_TIMEOUT),
			)
			.await?;

		info!(
			"({}) GC: {} items successfully pushed, will try to delete.",
			self.data.name, n_items
		);

		self.rpc_client
			.try_call_many(
				&nodes[..],
				GcRPC::DeleteIfEqualHash(deletes.clone()),
				RequestStrategy::with_quorum(nodes.len()).with_timeout(TABLE_GC_RPC_TIMEOUT),
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

	// ---- RPC HANDLER ----

	fn register_handler(self: &Arc<Self>, rpc_server: &mut RpcServer, path: String) {
		let self2 = self.clone();
		rpc_server.add_handler::<GcRPC, _, _>(path, move |msg, _addr| {
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

	async fn handle_rpc(self: &Arc<Self>, message: &GcRPC) -> Result<GcRPC, Error> {
		match message {
			GcRPC::Update(items) => {
				self.data.update_many(items)?;
				Ok(GcRPC::Ok)
			}
			GcRPC::DeleteIfEqualHash(items) => {
				for (key, vhash) in items.iter() {
					self.data.delete_if_equal_hash(&key[..], *vhash)?;
					self.todo_remove_if_equal(&key[..], *vhash)?;
				}
				Ok(GcRPC::Ok)
			}
			_ => Err(Error::Message("Unexpected GC RPC".to_string())),
		}
	}
}
