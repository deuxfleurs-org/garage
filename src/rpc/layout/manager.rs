use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use tokio::sync::Notify;

use netapp::endpoint::Endpoint;
use netapp::peering::fullmesh::FullMeshPeeringStrategy;
use netapp::NodeID;

use garage_util::config::Config;
use garage_util::data::*;
use garage_util::error::*;
use garage_util::persister::Persister;

use super::*;
use crate::rpc_helper::*;
use crate::system::*;

pub struct LayoutManager {
	node_id: Uuid,
	replication_factor: usize,
	persist_cluster_layout: Persister<LayoutHistory>,

	layout: Arc<RwLock<LayoutHistory>>,
	pub(crate) change_notify: Arc<Notify>,

	table_sync_version: Mutex<HashMap<String, u64>>,

	pub(crate) rpc_helper: RpcHelper,
	system_endpoint: Arc<Endpoint<SystemRpc, System>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LayoutStatus {
	/// Cluster layout version
	pub cluster_layout_version: u64,
	/// Hash of cluster layout update trackers
	pub cluster_layout_trackers_hash: Hash,
	/// Hash of cluster layout staging data
	pub cluster_layout_staging_hash: Hash,
}

impl LayoutManager {
	pub fn new(
		config: &Config,
		node_id: NodeID,
		system_endpoint: Arc<Endpoint<SystemRpc, System>>,
		fullmesh: Arc<FullMeshPeeringStrategy>,
		replication_factor: usize,
	) -> Result<Arc<Self>, Error> {
		let persist_cluster_layout: Persister<LayoutHistory> =
			Persister::new(&config.metadata_dir, "cluster_layout");

		let mut cluster_layout = match persist_cluster_layout.load() {
			Ok(x) => {
				if x.current().replication_factor != replication_factor {
					return Err(Error::Message(format!(
						"Prevous cluster layout has replication factor {}, which is different than the one specified in the config file ({}). The previous cluster layout can be purged, if you know what you are doing, simply by deleting the `cluster_layout` file in your metadata directory.",
						x.current().replication_factor,
						replication_factor
					)));
				}
				x
			}
			Err(e) => {
				info!(
					"No valid previous cluster layout stored ({}), starting fresh.",
					e
				);
				LayoutHistory::new(replication_factor)
			}
		};

		cluster_layout.update_trackers(node_id.into());

		let layout = Arc::new(RwLock::new(cluster_layout));
		let change_notify = Arc::new(Notify::new());

		let rpc_helper = RpcHelper::new(
			node_id.into(),
			fullmesh,
			layout.clone(),
			config.rpc_timeout_msec.map(Duration::from_millis),
		);

		Ok(Arc::new(Self {
			node_id: node_id.into(),
			replication_factor,
			persist_cluster_layout,
			layout,
			change_notify,
			table_sync_version: Mutex::new(HashMap::new()),
			system_endpoint,
			rpc_helper,
		}))
	}

	// ---- PUBLIC INTERFACE ----

	pub fn layout(&self) -> RwLockReadGuard<'_, LayoutHistory> {
		self.layout.read().unwrap()
	}

	pub fn status(&self) -> LayoutStatus {
		let layout = self.layout();
		LayoutStatus {
			cluster_layout_version: layout.current().version,
			cluster_layout_trackers_hash: layout.trackers_hash,
			cluster_layout_staging_hash: layout.staging_hash,
		}
	}

	pub async fn update_cluster_layout(
		self: &Arc<Self>,
		layout: &LayoutHistory,
	) -> Result<(), Error> {
		self.handle_advertise_cluster_layout(layout).await?;
		Ok(())
	}

	pub fn add_table(&self, table_name: &'static str) {
		let first_version = self.layout().versions.first().unwrap().version;

		self.table_sync_version
			.lock()
			.unwrap()
			.insert(table_name.to_string(), first_version);
	}

	pub fn sync_table_until(self: &Arc<Self>, table_name: &'static str, version: u64) {
		let mut table_sync_version = self.table_sync_version.lock().unwrap();
		*table_sync_version.get_mut(table_name).unwrap() = version;
		let sync_until = table_sync_version.iter().map(|(_, v)| *v).min().unwrap();
		drop(table_sync_version);

		let mut layout = self.layout.write().unwrap();
		if layout
			.update_trackers
			.sync_map
			.set_max(self.node_id, sync_until)
		{
			debug!("sync_until updated to {}", sync_until);
			layout.update_hashes();
			self.broadcast_update(SystemRpc::AdvertiseClusterLayoutTrackers(
				layout.update_trackers.clone(),
			));
		}
	}

	// ---- INTERNALS ---

	fn merge_layout(&self, adv: &LayoutHistory) -> Option<LayoutHistory> {
		let mut layout = self.layout.write().unwrap();
		let prev_layout_check = layout.check().is_ok();

		if !prev_layout_check || adv.check().is_ok() {
			if layout.merge(adv) {
				layout.update_trackers(self.node_id);
				if prev_layout_check && layout.check().is_err() {
					panic!("Merged two correct layouts and got an incorrect layout.");
				}
				return Some(layout.clone());
			}
		}
		None
	}

	fn merge_layout_trackers(&self, adv: &UpdateTrackers) -> Option<UpdateTrackers> {
		let mut layout = self.layout.write().unwrap();
		if layout.update_trackers != *adv {
			if layout.update_trackers.merge(adv) {
				layout.update_trackers(self.node_id);
				return Some(layout.update_trackers.clone());
			}
		}
		None
	}

	async fn pull_cluster_layout(self: &Arc<Self>, peer: Uuid) {
		let resp = self
			.rpc_helper
			.call(
				&self.system_endpoint,
				peer,
				SystemRpc::PullClusterLayout,
				RequestStrategy::with_priority(PRIO_HIGH),
			)
			.await;
		if let Ok(SystemRpc::AdvertiseClusterLayout(layout)) = resp {
			if let Err(e) = self.handle_advertise_cluster_layout(&layout).await {
				warn!("In pull_cluster_layout: {}", e);
			}
		}
	}

	async fn pull_cluster_layout_trackers(self: &Arc<Self>, peer: Uuid) {
		let resp = self
			.rpc_helper
			.call(
				&self.system_endpoint,
				peer,
				SystemRpc::PullClusterLayoutTrackers,
				RequestStrategy::with_priority(PRIO_HIGH),
			)
			.await;
		if let Ok(SystemRpc::AdvertiseClusterLayoutTrackers(trackers)) = resp {
			if let Err(e) = self
				.handle_advertise_cluster_layout_trackers(&trackers)
				.await
			{
				warn!("In pull_cluster_layout_trackers: {}", e);
			}
		}
	}

	/// Save cluster layout data to disk
	async fn save_cluster_layout(&self) -> Result<(), Error> {
		let layout = self.layout.read().unwrap().clone();
		self.persist_cluster_layout
			.save_async(&layout)
			.await
			.expect("Cannot save current cluster layout");
		Ok(())
	}

	fn broadcast_update(self: &Arc<Self>, rpc: SystemRpc) {
		tokio::spawn({
			let this = self.clone();
			async move {
				if let Err(e) = this
					.rpc_helper
					.broadcast(
						&this.system_endpoint,
						rpc,
						RequestStrategy::with_priority(PRIO_HIGH),
					)
					.await
				{
					warn!("Error while broadcasting new cluster layout: {}", e);
				}
			}
		});
	}

	// ---- RPC HANDLERS ----

	pub(crate) fn handle_advertise_status(self: &Arc<Self>, from: Uuid, remote: &LayoutStatus) {
		let local = self.status();
		if remote.cluster_layout_version > local.cluster_layout_version
			|| remote.cluster_layout_staging_hash != local.cluster_layout_staging_hash
		{
			tokio::spawn({
				let this = self.clone();
				async move { this.pull_cluster_layout(from).await }
			});
		} else if remote.cluster_layout_trackers_hash != local.cluster_layout_trackers_hash {
			tokio::spawn({
				let this = self.clone();
				async move { this.pull_cluster_layout_trackers(from).await }
			});
		}
	}

	pub(crate) fn handle_pull_cluster_layout(&self) -> SystemRpc {
		let layout = self.layout.read().unwrap().clone(); // TODO: avoid cloning
		SystemRpc::AdvertiseClusterLayout(layout)
	}

	pub(crate) fn handle_pull_cluster_layout_trackers(&self) -> SystemRpc {
		let layout = self.layout.read().unwrap();
		SystemRpc::AdvertiseClusterLayoutTrackers(layout.update_trackers.clone())
	}

	pub(crate) async fn handle_advertise_cluster_layout(
		self: &Arc<Self>,
		adv: &LayoutHistory,
	) -> Result<SystemRpc, Error> {
		debug!(
			"handle_advertise_cluster_layout: {} versions, last={}, trackers={:?}",
			adv.versions.len(),
			adv.current().version,
			adv.update_trackers
		);

		if adv.current().replication_factor != self.replication_factor {
			let msg = format!(
				"Received a cluster layout from another node with replication factor {}, which is different from what we have in our configuration ({}). Discarding the cluster layout we received.",
				adv.current().replication_factor,
				self.replication_factor
			);
			error!("{}", msg);
			return Err(Error::Message(msg));
		}

		if let Some(new_layout) = self.merge_layout(adv) {
			debug!("handle_advertise_cluster_layout: some changes were added to the current stuff");

			self.change_notify.notify_waiters();
			self.broadcast_update(SystemRpc::AdvertiseClusterLayout(new_layout));
			self.save_cluster_layout().await?;
		}

		Ok(SystemRpc::Ok)
	}

	pub(crate) async fn handle_advertise_cluster_layout_trackers(
		self: &Arc<Self>,
		trackers: &UpdateTrackers,
	) -> Result<SystemRpc, Error> {
		debug!("handle_advertise_cluster_layout_trackers: {:?}", trackers);

		if let Some(new_trackers) = self.merge_layout_trackers(trackers) {
			self.change_notify.notify_waiters();
			self.broadcast_update(SystemRpc::AdvertiseClusterLayoutTrackers(new_trackers));
			self.save_cluster_layout().await?;
		}

		Ok(SystemRpc::Ok)
	}
}
