//! Module containing structs related to membership management
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use futures::{join, select};
use futures_util::future::*;
use serde::{Deserialize, Serialize};
use sodiumoxide::crypto::sign::ed25519;
use tokio::sync::watch;
use tokio::sync::Mutex;

use netapp::endpoint::{Endpoint, EndpointHandler, Message};
use netapp::peering::fullmesh::FullMeshPeeringStrategy;
use netapp::proto::*;
use netapp::{NetApp, NetworkKey, NodeID, NodeKey};

use garage_util::background::BackgroundRunner;
use garage_util::error::Error;
use garage_util::persister::Persister;
//use garage_util::time::*;

//use crate::consul::get_consul_nodes;
use crate::ring::*;
use crate::rpc_helper::{RequestStrategy, RpcHelper};

const DISCOVERY_INTERVAL: Duration = Duration::from_secs(60);
const PING_TIMEOUT: Duration = Duration::from_secs(2);

/// RPC endpoint used for calls related to membership
pub const SYSTEM_RPC_PATH: &str = "garage_rpc/membership.rs/SystemRpc";

/// RPC messages related to membership
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SystemRpc {
	/// Response to successfull advertisements
	Ok,
	/// Error response
	Error(String),
	/// Ask other node its config. Answered with AdvertiseConfig
	PullConfig,
	/// Advertise Garage status. Answered with another AdvertiseStatus.
	/// Exchanged with every node on a regular basis.
	AdvertiseStatus(StateInfo),
	/// Advertisement of nodes config. Sent spontanously or in response to PullConfig
	AdvertiseConfig(NetworkConfig),
	/// Get known nodes states
	GetKnownNodes,
	/// Return known nodes
	ReturnKnownNodes(Vec<(NodeID, SocketAddr, bool)>),
}

impl Message for SystemRpc {
	type Response = SystemRpc;
}

/// This node's membership manager
pub struct System {
	/// The id of this node
	pub id: NodeID,

	persist_config: Persister<NetworkConfig>,

	state_info: ArcSwap<StateInfo>,

	pub netapp: Arc<NetApp>,
	fullmesh: Arc<FullMeshPeeringStrategy>,
	pub rpc: RpcHelper,

	system_endpoint: Arc<Endpoint<SystemRpc, System>>,

	rpc_listen_addr: SocketAddr,
	bootstrap_peers: Vec<(NodeID, SocketAddr)>,
	consul_host: Option<String>,
	consul_service_name: Option<String>,
	replication_factor: usize,

	/// The ring
	pub ring: watch::Receiver<Arc<Ring>>,
	update_ring: Mutex<watch::Sender<Arc<Ring>>>,

	/// The job runner of this node
	pub background: Arc<BackgroundRunner>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateInfo {
	/// Hostname of the node
	pub hostname: String,
	/// Replication factor configured on the node
	pub replication_factor: usize,
	/// Configuration version
	pub config_version: u64,
}

fn gen_node_key(metadata_dir: &Path) -> Result<NodeKey, Error> {
	let mut id_file = metadata_dir.to_path_buf();
	id_file.push("node_id");
	if id_file.as_path().exists() {
		let mut f = std::fs::File::open(id_file.as_path())?;
		let mut d = vec![];
		f.read_to_end(&mut d)?;
		if d.len() != 64 {
			return Err(Error::Message("Corrupt node_id file".to_string()));
		}

		let mut key = [0u8; 64];
		key.copy_from_slice(&d[..]);
		Ok(NodeKey::from_slice(&key[..]).unwrap())
	} else {
		let (key, _) = ed25519::gen_keypair();

		let mut f = std::fs::File::create(id_file.as_path())?;
		f.write_all(&key[..])?;
		Ok(NodeKey::from_slice(&key[..]).unwrap())
	}
}

impl System {
	/// Create this node's membership manager
	pub fn new(
		network_key: NetworkKey,
		metadata_dir: PathBuf,
		background: Arc<BackgroundRunner>,
		replication_factor: usize,
		rpc_listen_addr: SocketAddr,
		bootstrap_peers: Vec<(NodeID, SocketAddr)>,
		consul_host: Option<String>,
		consul_service_name: Option<String>,
	) -> Arc<Self> {
		let node_key = gen_node_key(&metadata_dir).expect("Unable to read or generate node ID");
		info!("Node public key: {}", hex::encode(&node_key.public_key()));

		let persist_config = Persister::new(&metadata_dir, "network_config");

		let net_config = match persist_config.load() {
			Ok(x) => x,
			Err(e) => {
				match Persister::<garage_rpc_021::ring::NetworkConfig>::new(
					&metadata_dir,
					"network_config",
				)
				.load()
				{
					Ok(old_config) => NetworkConfig::migrate_from_021(old_config),
					Err(e2) => {
						info!(
							"No valid previous network configuration stored ({}, {}), starting fresh.",
							e, e2
						);
						NetworkConfig::new()
					}
				}
			}
		};

		let state_info = StateInfo {
			hostname: gethostname::gethostname()
				.into_string()
				.unwrap_or_else(|_| "<invalid utf-8>".to_string()),
			replication_factor: replication_factor,
			config_version: net_config.version,
		};

		let ring = Ring::new(net_config, replication_factor);
		let (update_ring, ring) = watch::channel(Arc::new(ring));

		let netapp = NetApp::new(network_key, node_key);
		let fullmesh = FullMeshPeeringStrategy::new(netapp.clone(), bootstrap_peers.clone());

		let system_endpoint = netapp.endpoint(SYSTEM_RPC_PATH.into());

		let sys = Arc::new(System {
			id: netapp.id.clone(),
			persist_config,
			state_info: ArcSwap::new(Arc::new(state_info)),
			netapp: netapp.clone(),
			fullmesh: fullmesh.clone(),
			rpc: RpcHelper {
				fullmesh: fullmesh.clone(),
				background: background.clone(),
			},
			system_endpoint,
			replication_factor,
			rpc_listen_addr,
			bootstrap_peers,
			consul_host,
			consul_service_name,
			ring,
			update_ring: Mutex::new(update_ring),
			background: background.clone(),
		});
		sys.system_endpoint.set_handler(sys.clone());
		sys
	}

	/// Perform bootstraping, starting the ping loop
	pub async fn run(self: Arc<Self>, must_exit: watch::Receiver<bool>) {
		join!(
			self.netapp
				.clone()
				.listen(self.rpc_listen_addr, None, must_exit.clone()),
			self.fullmesh.clone().run(must_exit.clone()),
			self.discovery_loop(must_exit.clone()),
		);
	}

	// ---- INTERNALS ----

	/// Save network configuration to disc
	async fn save_network_config(self: Arc<Self>) -> Result<(), Error> {
		let ring: Arc<Ring> = self.ring.borrow().clone();
		self.persist_config
			.save_async(&ring.config)
			.await
			.expect("Cannot save current cluster configuration");
		Ok(())
	}

	fn update_state_info(&self) {
		let mut new_si: StateInfo = self.state_info.load().as_ref().clone();

		let ring = self.ring.borrow();
		new_si.config_version = ring.config.version;
		self.state_info.swap(Arc::new(new_si));
	}

	fn handle_pull_config(&self) -> SystemRpc {
		let ring = self.ring.borrow().clone();
		SystemRpc::AdvertiseConfig(ring.config.clone())
	}

	async fn handle_advertise_config(
		self: Arc<Self>,
		adv: &NetworkConfig,
	) -> Result<SystemRpc, Error> {
		let update_ring = self.update_ring.lock().await;
		let ring: Arc<Ring> = self.ring.borrow().clone();

		if adv.version > ring.config.version {
			let ring = Ring::new(adv.clone(), self.replication_factor);
			update_ring.send(Arc::new(ring))?;
			drop(update_ring);

			let self2 = self.clone();
			let adv2 = adv.clone();
			self.background.spawn_cancellable(async move {
				self2
					.rpc
					.broadcast(
						&self2.system_endpoint,
						SystemRpc::AdvertiseConfig(adv2),
						RequestStrategy::with_priority(PRIO_NORMAL),
					)
					.await;
				Ok(())
			});
			self.background.spawn(self.clone().save_network_config());
		}

		Ok(SystemRpc::Ok)
	}

	async fn discovery_loop(&self, mut stop_signal: watch::Receiver<bool>) {
		/* TODO
		let consul_config = match (&self.consul_host, &self.consul_service_name) {
			(Some(ch), Some(csn)) => Some((ch.clone(), csn.clone())),
			_ => None,
		};
		*/

		while !*stop_signal.borrow() {
			let not_configured = self.ring.borrow().config.members.is_empty();
			let no_peers = self.fullmesh.get_peer_list().len() < self.replication_factor;
			let bad_peers = self
				.fullmesh
				.get_peer_list()
				.iter()
				.filter(|p| p.is_up())
				.count() != self.ring.borrow().config.members.len();

			if not_configured || no_peers || bad_peers {
				info!("Doing a bootstrap/discovery step (not_configured: {}, no_peers: {}, bad_peers: {})", not_configured, no_peers, bad_peers);

				let ping_list = self.bootstrap_peers.clone();

				/*
				 *TODO bring this back: persisted list of peers
				if let Ok(peers) = self.persist_status.load_async().await {
					ping_list.extend(peers.iter().map(|x| (x.addr, Some(x.id))));
				}
				*/

				/*
				 * TODO bring this back: get peers from consul
				if let Some((consul_host, consul_service_name)) = &consul_config {
					match get_consul_nodes(consul_host, consul_service_name).await {
						Ok(node_list) => {
							ping_list.extend(node_list.iter().map(|a| (*a, None)));
						}
						Err(e) => {
							warn!("Could not retrieve node list from Consul: {}", e);
						}
					}
				}
				*/

				for (node_id, node_addr) in ping_list {
					tokio::spawn(self.netapp.clone().try_connect(node_addr, node_id));
				}
			}

			let restart_at = tokio::time::sleep(DISCOVERY_INTERVAL);
			select! {
				_ = restart_at.fuse() => {},
				_ = stop_signal.changed().fuse() => {},
			}
		}
	}

	async fn pull_config(self: Arc<Self>, peer: NodeID) {
		let resp = self
			.rpc
			.call(
				&self.system_endpoint,
				peer,
				SystemRpc::PullConfig,
				RequestStrategy::with_priority(PRIO_HIGH).with_timeout(PING_TIMEOUT),
			)
			.await;
		if let Ok(SystemRpc::AdvertiseConfig(config)) = resp {
			let _: Result<_, _> = self.handle_advertise_config(&config).await;
		}
	}
}

#[async_trait]
impl EndpointHandler<SystemRpc> for System {
	async fn handle(self: &Arc<Self>, msg: &SystemRpc, _from: NodeID) -> SystemRpc {
		let resp = match msg {
			SystemRpc::PullConfig => Ok(self.handle_pull_config()),
			SystemRpc::AdvertiseConfig(adv) => self.clone().handle_advertise_config(&adv).await,
			SystemRpc::GetKnownNodes => {
				let known_nodes = self
					.fullmesh
					.get_peer_list()
					.iter()
					.map(|n| (n.id, n.addr, n.is_up()))
					.collect::<Vec<_>>();
				Ok(SystemRpc::ReturnKnownNodes(known_nodes))
			}
			_ => Err(Error::BadRpc("Unexpected RPC message".to_string())),
		};
		match resp {
			Ok(r) => r,
			Err(e) => SystemRpc::Error(format!("{}", e)),
		}
	}
}
