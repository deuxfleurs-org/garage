use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use tokio::sync::watch;
use tokio::sync::Mutex;

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
	replication_factor: usize,
	persist_cluster_layout: Persister<LayoutHistory>,

	pub layout_watch: watch::Receiver<Arc<LayoutHistory>>,
	update_layout: Mutex<watch::Sender<Arc<LayoutHistory>>>,

	pub(crate) rpc_helper: RpcHelper,
	system_endpoint: Arc<Endpoint<SystemRpc, System>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct LayoutStatus {
	/// Cluster layout version
	pub cluster_layout_version: u64,
	/// Hash of cluster layout update trackers
	// (TODO) pub cluster_layout_trackers_hash: Hash,
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

		let cluster_layout = match persist_cluster_layout.load() {
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

		let (update_layout, layout_watch) = watch::channel(Arc::new(cluster_layout));

		let rpc_helper = RpcHelper::new(
			node_id.into(),
			fullmesh,
			layout_watch.clone(),
			config.rpc_timeout_msec.map(Duration::from_millis),
		);

		Ok(Arc::new(Self {
			replication_factor,
			persist_cluster_layout,
			layout_watch,
			update_layout: Mutex::new(update_layout),
			system_endpoint,
			rpc_helper,
		}))
	}

	// ---- PUBLIC INTERFACE ----

	pub fn status(&self) -> LayoutStatus {
		let layout = self.layout();
		LayoutStatus {
			cluster_layout_version: layout.current().version,
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

	pub fn layout(&self) -> watch::Ref<Arc<LayoutHistory>> {
		self.layout_watch.borrow()
	}

	pub(crate) async fn pull_cluster_layout(self: &Arc<Self>, peer: Uuid) {
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
			let _: Result<_, _> = self.handle_advertise_cluster_layout(&layout).await;
		}
	}

	// ---- INTERNALS ---

	/// Save network configuration to disc
	async fn save_cluster_layout(&self) -> Result<(), Error> {
		let layout: Arc<LayoutHistory> = self.layout_watch.borrow().clone();
		self.persist_cluster_layout
			.save_async(&layout)
			.await
			.expect("Cannot save current cluster layout");
		Ok(())
	}

	// ---- RPC HANDLERS ----

	pub(crate) fn handle_advertise_status(self: &Arc<Self>, from: Uuid, status: &LayoutStatus) {
		let local_status = self.status();
		if status.cluster_layout_version > local_status.cluster_layout_version
			|| status.cluster_layout_staging_hash != local_status.cluster_layout_staging_hash
		{
			tokio::spawn({
				let this = self.clone();
				async move { this.pull_cluster_layout(from).await }
			});
		}
	}

	pub(crate) fn handle_pull_cluster_layout(&self) -> SystemRpc {
		let layout = self.layout_watch.borrow().clone();
		SystemRpc::AdvertiseClusterLayout(layout)
	}

	pub(crate) async fn handle_advertise_cluster_layout(
		self: &Arc<Self>,
		adv: &LayoutHistory,
	) -> Result<SystemRpc, Error> {
		if adv.current().replication_factor != self.replication_factor {
			let msg = format!(
				"Received a cluster layout from another node with replication factor {}, which is different from what we have in our configuration ({}). Discarding the cluster layout we received.",
				adv.current().replication_factor,
				self.replication_factor
			);
			error!("{}", msg);
			return Err(Error::Message(msg));
		}

		if *adv != **self.layout_watch.borrow() {
			let update_layout = self.update_layout.lock().await;
			let mut layout: LayoutHistory = self.layout_watch.borrow().as_ref().clone();

			let prev_layout_check = layout.check().is_ok();
			if layout.merge(adv) {
				if prev_layout_check && layout.check().is_err() {
					error!("New cluster layout is invalid, discarding.");
					return Err(Error::Message(
						"New cluster layout is invalid, discarding.".into(),
					));
				}

				let layout = Arc::new(layout);
				update_layout.send(layout.clone())?;
				drop(update_layout); // release mutex

				tokio::spawn({
					let this = self.clone();
					async move {
						if let Err(e) = this
							.rpc_helper
							.broadcast(
								&this.system_endpoint,
								SystemRpc::AdvertiseClusterLayout(layout),
								RequestStrategy::with_priority(PRIO_HIGH),
							)
							.await
						{
							warn!("Error while broadcasting new cluster layout: {}", e);
						}
					}
				});

				self.save_cluster_layout().await?;
			}
		}

		Ok(SystemRpc::Ok)
	}
}
