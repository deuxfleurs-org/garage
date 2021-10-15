//! Module containing structs related to membership management
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use arc_swap::ArcSwap;
use async_trait::async_trait;
use futures::{join, select};
use futures_util::future::*;
use serde::{Deserialize, Serialize};
use sodiumoxide::crypto::sign::ed25519;
use tokio::sync::watch;
use tokio::sync::Mutex;

use netapp::endpoint::{Endpoint, EndpointHandler};
use netapp::peering::fullmesh::FullMeshPeeringStrategy;
use netapp::proto::*;
use netapp::{NetApp, NetworkKey, NodeID, NodeKey};
use netapp::util::parse_and_resolve_peer_addr;

use garage_util::background::BackgroundRunner;
use garage_util::data::Uuid;
use garage_util::error::Error;
use garage_util::persister::Persister;
use garage_util::time::*;

use crate::consul::*;
use crate::ring::*;
use crate::rpc_helper::*;

const DISCOVERY_INTERVAL: Duration = Duration::from_secs(60);
const STATUS_EXCHANGE_INTERVAL: Duration = Duration::from_secs(10);
const PING_TIMEOUT: Duration = Duration::from_secs(2);

/// RPC endpoint used for calls related to membership
pub const SYSTEM_RPC_PATH: &str = "garage_rpc/membership.rs/SystemRpc";

/// RPC messages related to membership
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SystemRpc {
	/// Response to successfull advertisements
	Ok,
	/// Request to connect to a specific node (in <pubkey>@<host>:<port> format)
	Connect(String),
	/// Ask other node its config. Answered with AdvertiseConfig
	PullConfig,
	/// Advertise Garage status. Answered with another AdvertiseStatus.
	/// Exchanged with every node on a regular basis.
	AdvertiseStatus(NodeStatus),
	/// Advertisement of nodes config. Sent spontanously or in response to PullConfig
	AdvertiseConfig(NetworkConfig),
	/// Get known nodes states
	GetKnownNodes,
	/// Return known nodes
	ReturnKnownNodes(Vec<KnownNodeInfo>),
}

impl Rpc for SystemRpc {
	type Response = Result<SystemRpc, Error>;
}

/// This node's membership manager
pub struct System {
	/// The id of this node
	pub id: Uuid,

	persist_config: Persister<NetworkConfig>,
	persist_peer_list: Persister<Vec<(Uuid, SocketAddr)>>,

	local_status: ArcSwap<NodeStatus>,
	node_status: RwLock<HashMap<Uuid, (u64, NodeStatus)>>,

	pub netapp: Arc<NetApp>,
	fullmesh: Arc<FullMeshPeeringStrategy>,
	pub rpc: RpcHelper,

	system_endpoint: Arc<Endpoint<SystemRpc, System>>,

	rpc_listen_addr: SocketAddr,
	rpc_public_addr: Option<SocketAddr>,
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
pub struct NodeStatus {
	/// Hostname of the node
	pub hostname: String,
	/// Replication factor configured on the node
	pub replication_factor: usize,
	/// Configuration version
	pub config_version: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnownNodeInfo {
	pub id: Uuid,
	pub addr: SocketAddr,
	pub is_up: bool,
	pub status: NodeStatus,
}

fn gen_node_key(metadata_dir: &Path) -> Result<NodeKey, Error> {
	let mut key_file = metadata_dir.to_path_buf();
	key_file.push("node_key");
	if key_file.as_path().exists() {
		let mut f = std::fs::File::open(key_file.as_path())?;
		let mut d = vec![];
		f.read_to_end(&mut d)?;
		if d.len() != 64 {
			return Err(Error::Message("Corrupt node_key file".to_string()));
		}

		let mut key = [0u8; 64];
		key.copy_from_slice(&d[..]);
		Ok(NodeKey::from_slice(&key[..]).unwrap())
	} else {
		let (_, key) = ed25519::gen_keypair();

		let mut f = std::fs::File::create(key_file.as_path())?;
		f.write_all(&key[..])?;
		Ok(key)
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
		rpc_public_address: Option<SocketAddr>,
		bootstrap_peers: Vec<(NodeID, SocketAddr)>,
		consul_host: Option<String>,
		consul_service_name: Option<String>,
	) -> Arc<Self> {
		let node_key = gen_node_key(&metadata_dir).expect("Unable to read or generate node ID");
		info!("Node public key: {}", hex::encode(&node_key.public_key()));

		let persist_config = Persister::new(&metadata_dir, "network_config");
		let persist_peer_list = Persister::new(&metadata_dir, "peer_list");

		let net_config = match persist_config.load() {
			Ok(x) => x,
			Err(e) => {
				info!(
					"No valid previous network configuration stored ({}), starting fresh.",
					e
				);
				NetworkConfig::new()
			}
		};

		let local_status = NodeStatus {
			hostname: gethostname::gethostname()
				.into_string()
				.unwrap_or_else(|_| "<invalid utf-8>".to_string()),
			replication_factor: replication_factor,
			config_version: net_config.version,
		};

		let ring = Ring::new(net_config, replication_factor);
		let (update_ring, ring) = watch::channel(Arc::new(ring));

		if let Some(addr) = rpc_public_address {
			println!("{}@{}", hex::encode(&node_key.public_key()), addr);
		} else {
			println!("{}", hex::encode(&node_key.public_key()));
		}

		let netapp = NetApp::new(network_key, node_key);
		let fullmesh = FullMeshPeeringStrategy::new(
			netapp.clone(),
			bootstrap_peers.clone(),
			rpc_public_address,
		);

		let system_endpoint = netapp.endpoint(SYSTEM_RPC_PATH.into());

		let sys = Arc::new(System {
			id: netapp.id.into(),
			persist_config,
			persist_peer_list,
			local_status: ArcSwap::new(Arc::new(local_status)),
			node_status: RwLock::new(HashMap::new()),
			netapp: netapp.clone(),
			fullmesh: fullmesh.clone(),
			rpc: RpcHelper {
				fullmesh: fullmesh.clone(),
				background: background.clone(),
			},
			system_endpoint,
			replication_factor,
			rpc_listen_addr,
			rpc_public_addr: rpc_public_address,
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
			self.status_exchange_loop(must_exit.clone()),
		);
	}

	// ---- INTERNALS ----

	async fn advertise_to_consul(self: Arc<Self>) -> Result<(), Error> {
		let (consul_host, consul_service_name) =
			match (&self.consul_host, &self.consul_service_name) {
				(Some(ch), Some(csn)) => (ch, csn),
				_ => return Ok(()),
			};

		let rpc_public_addr = match self.rpc_public_addr {
			Some(addr) => addr,
			None => {
				warn!("Not advertising to Consul because rpc_public_addr is not defined in config file.");
				return Ok(());
			}
		};

		publish_consul_service(
			consul_host,
			consul_service_name,
			self.netapp.id,
			&self.local_status.load_full().hostname,
			rpc_public_addr,
		)
		.await
		.map_err(|e| Error::Message(format!("Error while publishing Consul service: {}", e)))
	}

	/// Save network configuration to disc
	async fn save_network_config(self: Arc<Self>) -> Result<(), Error> {
		let ring: Arc<Ring> = self.ring.borrow().clone();
		self.persist_config
			.save_async(&ring.config)
			.await
			.expect("Cannot save current cluster configuration");
		Ok(())
	}

	fn update_local_status(&self) {
		let mut new_si: NodeStatus = self.local_status.load().as_ref().clone();

		let ring = self.ring.borrow();
		new_si.config_version = ring.config.version;
		self.local_status.swap(Arc::new(new_si));
	}

	async fn handle_connect(&self, node: &str) -> Result<SystemRpc, Error> {
		let (pubkey, addrs) = parse_and_resolve_peer_addr(node)
			.ok_or_else(|| Error::Message(format!("Unable to parse or resolve node specification: {}", node)))?;
		let mut errors = vec![];
		for ip in addrs.iter() {
			match self.netapp.clone().try_connect(*ip, pubkey).await {
				Ok(()) => return Ok(SystemRpc::Ok),
				Err(e) => {
					errors.push((*ip, e));
				}
			}
		}
		return Err(Error::Message(format!("Could not connect to specified peers. Errors: {:?}", errors)));
	}

	fn handle_pull_config(&self) -> SystemRpc {
		let ring = self.ring.borrow().clone();
		SystemRpc::AdvertiseConfig(ring.config.clone())
	}

	fn handle_get_known_nodes(&self) -> SystemRpc {
		let node_status = self.node_status.read().unwrap();
		let known_nodes =
			self.fullmesh
				.get_peer_list()
				.iter()
				.map(|n| KnownNodeInfo {
					id: n.id.into(),
					addr: n.addr,
					is_up: n.is_up(),
					status: node_status.get(&n.id.into()).cloned().map(|(_, st)| st).unwrap_or(
						NodeStatus {
							hostname: "?".to_string(),
							replication_factor: 0,
							config_version: 0,
						},
					),
				})
				.collect::<Vec<_>>();
		SystemRpc::ReturnKnownNodes(known_nodes)
	}

	async fn handle_advertise_status(
		self: &Arc<Self>,
		from: Uuid,
		info: &NodeStatus,
	) -> Result<SystemRpc, Error> {
		let local_info = self.local_status.load();

		if local_info.replication_factor < info.replication_factor {
			error!("Some node have a higher replication factor ({}) than this one ({}). This is not supported and might lead to bugs",
				info.replication_factor,
				local_info.replication_factor);
			std::process::exit(1);
		}

		if info.config_version > local_info.config_version {
			let self2 = self.clone();
			self.background.spawn_cancellable(async move {
				self2.pull_config(from).await;
				Ok(())
			});
		}

		self.node_status
			.write()
			.unwrap()
			.insert(from, (now_msec(), info.clone()));

		Ok(SystemRpc::Ok)
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

	async fn status_exchange_loop(&self, mut stop_signal: watch::Receiver<bool>) {
		while !*stop_signal.borrow() {
			let restart_at = tokio::time::sleep(STATUS_EXCHANGE_INTERVAL);

			self.update_local_status();
			let local_status: NodeStatus = self.local_status.load().as_ref().clone();
			self.rpc
				.broadcast(
					&self.system_endpoint,
					SystemRpc::AdvertiseStatus(local_status),
					RequestStrategy::with_priority(PRIO_HIGH).with_timeout(PING_TIMEOUT),
				)
				.await;

			select! {
				_ = restart_at.fuse() => {},
				_ = stop_signal.changed().fuse() => {},
			}
		}
	}

	async fn discovery_loop(self: &Arc<Self>, mut stop_signal: watch::Receiver<bool>) {
		let consul_config = match (&self.consul_host, &self.consul_service_name) {
			(Some(ch), Some(csn)) => Some((ch.clone(), csn.clone())),
			_ => None,
		};

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

				let mut ping_list = self.bootstrap_peers.clone();

				// Add peer list from list stored on disk
				if let Ok(peers) = self.persist_peer_list.load_async().await {
					ping_list.extend(peers.iter().map(|(id, addr)| ((*id).into(), *addr)))
				}

				// Fetch peer list from Consul
				if let Some((consul_host, consul_service_name)) = &consul_config {
					match get_consul_nodes(consul_host, consul_service_name).await {
						Ok(node_list) => {
							ping_list.extend(node_list);
						}
						Err(e) => {
							warn!("Could not retrieve node list from Consul: {}", e);
						}
					}
				}

				for (node_id, node_addr) in ping_list {
					tokio::spawn(self.netapp.clone().try_connect(node_addr, node_id));
				}
			}

			let peer_list = self
				.fullmesh
				.get_peer_list()
				.iter()
				.map(|n| (n.id.into(), n.addr))
				.collect::<Vec<_>>();
			if let Err(e) = self.persist_peer_list.save_async(&peer_list).await {
				warn!("Could not save peer list to file: {}", e);
			}

			self.background.spawn(self.clone().advertise_to_consul());

			let restart_at = tokio::time::sleep(DISCOVERY_INTERVAL);
			select! {
				_ = restart_at.fuse() => {},
				_ = stop_signal.changed().fuse() => {},
			}
		}
	}

	async fn pull_config(self: Arc<Self>, peer: Uuid) {
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
	async fn handle(self: &Arc<Self>, msg: &SystemRpc, from: NodeID) -> Result<SystemRpc, Error> {
		match msg {
			SystemRpc::Connect(node) => self.handle_connect(node).await,
			SystemRpc::PullConfig => Ok(self.handle_pull_config()),
			SystemRpc::AdvertiseStatus(adv) => self.handle_advertise_status(from.into(), adv).await,
			SystemRpc::AdvertiseConfig(adv) => self.clone().handle_advertise_config(&adv).await,
			SystemRpc::GetKnownNodes => Ok(self.handle_get_known_nodes()),
			_ => Err(Error::BadRpc("Unexpected RPC message".to_string())),
		}
	}
}
