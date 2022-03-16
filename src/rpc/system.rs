//! Module containing structs related to membership management
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

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
use netapp::util::parse_and_resolve_peer_addr;
use netapp::{NetApp, NetworkKey, NodeID, NodeKey};

use garage_util::background::BackgroundRunner;
use garage_util::config::Config;
use garage_util::data::*;
use garage_util::error::*;
use garage_util::persister::Persister;
use garage_util::time::*;

use crate::consul::*;
#[cfg(feature = "kubernetes-discovery")]
use crate::kubernetes::*;
use crate::layout::*;
use crate::ring::*;
use crate::rpc_helper::*;

const DISCOVERY_INTERVAL: Duration = Duration::from_secs(60);
const STATUS_EXCHANGE_INTERVAL: Duration = Duration::from_secs(10);
const PING_TIMEOUT: Duration = Duration::from_secs(2);

/// Version tag used for version check upon Netapp connection
pub const GARAGE_VERSION_TAG: u64 = 0x6761726167650007; // garage 0x0007

/// RPC endpoint used for calls related to membership
pub const SYSTEM_RPC_PATH: &str = "garage_rpc/membership.rs/SystemRpc";

pub const CONNECT_ERROR_MESSAGE: &str = "Error establishing RPC connection to remote node. This can happen if the remote node is not reachable on the network, but also if the two nodes are not configured with the same rpc_secret";

/// RPC messages related to membership
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SystemRpc {
	/// Response to successfull advertisements
	Ok,
	/// Request to connect to a specific node (in <pubkey>@<host>:<port> format)
	Connect(String),
	/// Ask other node its cluster layout. Answered with AdvertiseClusterLayout
	PullClusterLayout,
	/// Advertise Garage status. Answered with another AdvertiseStatus.
	/// Exchanged with every node on a regular basis.
	AdvertiseStatus(NodeStatus),
	/// Advertisement of cluster layout. Sent spontanously or in response to PullClusterLayout
	AdvertiseClusterLayout(ClusterLayout),
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

	persist_cluster_layout: Persister<ClusterLayout>,
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

	consul_discovery: Option<ConsulDiscoveryParam>,
	#[cfg(feature = "kubernetes-discovery")]
	kubernetes_discovery: Option<KubernetesDiscoveryParam>,

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
	/// Cluster layout version
	pub cluster_layout_version: u64,
	/// Hash of cluster layout staging data
	pub cluster_layout_staging_hash: Hash,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnownNodeInfo {
	pub id: Uuid,
	pub addr: SocketAddr,
	pub is_up: bool,
	pub last_seen_secs_ago: Option<u64>,
	pub status: NodeStatus,
}

pub fn read_node_id(metadata_dir: &Path) -> Result<NodeID, Error> {
	let mut pubkey_file = metadata_dir.to_path_buf();
	pubkey_file.push("node_key.pub");

	let mut f = std::fs::File::open(pubkey_file.as_path())?;
	let mut d = vec![];
	f.read_to_end(&mut d)?;
	if d.len() != 32 {
		return Err(Error::Message("Corrupt node_key.pub file".to_string()));
	}

	let mut key = [0u8; 32];
	key.copy_from_slice(&d[..]);
	Ok(NodeID::from_slice(&key[..]).unwrap())
}

pub fn gen_node_key(metadata_dir: &Path) -> Result<NodeKey, Error> {
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
		if !metadata_dir.exists() {
			info!("Metadata directory does not exist, creating it.");
			std::fs::create_dir(&metadata_dir)?;
		}

		info!("Generating new node key pair.");
		let (pubkey, key) = ed25519::gen_keypair();

		{
			use std::os::unix::fs::PermissionsExt;
			let mut f = std::fs::File::create(key_file.as_path())?;
			let mut perm = f.metadata()?.permissions();
			perm.set_mode(0o600);
			std::fs::set_permissions(key_file.as_path(), perm)?;
			f.write_all(&key[..])?;
		}

		{
			let mut pubkey_file = metadata_dir.to_path_buf();
			pubkey_file.push("node_key.pub");
			let mut f2 = std::fs::File::create(pubkey_file.as_path())?;
			f2.write_all(&pubkey[..])?;
		}

		Ok(key)
	}
}

impl System {
	/// Create this node's membership manager
	pub fn new(
		network_key: NetworkKey,
		background: Arc<BackgroundRunner>,
		replication_factor: usize,
		config: &Config,
	) -> Arc<Self> {
		let node_key =
			gen_node_key(&config.metadata_dir).expect("Unable to read or generate node ID");
		info!(
			"Node ID of this node: {}",
			hex::encode(&node_key.public_key()[..8])
		);

		let persist_cluster_layout = Persister::new(&config.metadata_dir, "cluster_layout");
		let persist_peer_list = Persister::new(&config.metadata_dir, "peer_list");

		let cluster_layout = match persist_cluster_layout.load() {
			Ok(x) => x,
			Err(e) => {
				info!(
					"No valid previous cluster layout stored ({}), starting fresh.",
					e
				);
				ClusterLayout::new(replication_factor)
			}
		};

		let local_status = NodeStatus {
			hostname: gethostname::gethostname()
				.into_string()
				.unwrap_or_else(|_| "<invalid utf-8>".to_string()),
			replication_factor,
			cluster_layout_version: cluster_layout.version,
			cluster_layout_staging_hash: cluster_layout.staging_hash,
		};

		let ring = Ring::new(cluster_layout, replication_factor);
		let (update_ring, ring) = watch::channel(Arc::new(ring));

		let rpc_public_addr = match config.rpc_public_addr {
			Some(a) => Some(a),
			None => {
				let addr =
					get_default_ip().map(|ip| SocketAddr::new(ip, config.rpc_bind_addr.port()));
				if let Some(a) = addr {
					warn!("Using autodetected rpc_public_addr: {}. Consider specifying it explicitly in configuration file if possible.", a);
				}
				addr
			}
		};

		let netapp = NetApp::new(GARAGE_VERSION_TAG, network_key, node_key);
		let fullmesh = FullMeshPeeringStrategy::new(
			netapp.clone(),
			config.bootstrap_peers.clone(),
			rpc_public_addr,
		);

		let system_endpoint = netapp.endpoint(SYSTEM_RPC_PATH.into());

		let consul_discovery = match (&config.consul_host, &config.consul_service_name) {
			(Some(ch), Some(csn)) => Some(ConsulDiscoveryParam {
				consul_host: ch.to_string(),
				service_name: csn.to_string(),
			}),
			_ => None,
		};

		#[cfg(feature = "kubernetes-discovery")]
		let kubernetes_discovery = match (
			&config.kubernetes_service_name,
			&config.kubernetes_namespace,
		) {
			(Some(ksn), Some(kn)) => Some(KubernetesDiscoveryParam {
				service_name: ksn.to_string(),
				namespace: kn.to_string(),
				skip_crd: config.kubernetes_skip_crd,
			}),
			_ => None,
		};

		#[cfg(not(feature = "kubernetes-discovery"))]
		if config.kubernetes_service_name.is_some() || config.kubernetes_namespace.is_some() {
			warn!("Kubernetes discovery is not enabled in this build.");
		}

		let sys = Arc::new(System {
			id: netapp.id.into(),
			persist_cluster_layout,
			persist_peer_list,
			local_status: ArcSwap::new(Arc::new(local_status)),
			node_status: RwLock::new(HashMap::new()),
			netapp: netapp.clone(),
			fullmesh: fullmesh.clone(),
			rpc: RpcHelper::new(netapp.id.into(), fullmesh, background.clone(), ring.clone()),
			system_endpoint,
			replication_factor,
			rpc_listen_addr: config.rpc_bind_addr,
			rpc_public_addr,
			bootstrap_peers: config.bootstrap_peers.clone(),
			consul_discovery,
			#[cfg(feature = "kubernetes-discovery")]
			kubernetes_discovery,

			ring,
			update_ring: Mutex::new(update_ring),
			background,
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
		let c = match &self.consul_discovery {
			Some(c) => c,
			_ => return Ok(()),
		};

		let rpc_public_addr = match self.rpc_public_addr {
			Some(addr) => addr,
			None => {
				warn!("Not advertising to Consul because rpc_public_addr is not defined in config file and could not be autodetected.");
				return Ok(());
			}
		};

		publish_consul_service(
			&c.consul_host,
			&c.service_name,
			self.netapp.id,
			&self.local_status.load_full().hostname,
			rpc_public_addr,
		)
		.await
		.err_context("Error while publishing Consul service")
	}

	#[cfg(feature = "kubernetes-discovery")]
	async fn advertise_to_kubernetes(self: Arc<Self>) -> Result<(), Error> {
		let k = match &self.kubernetes_discovery {
			Some(k) => k,
			_ => return Ok(()),
		};

		let rpc_public_addr = match self.rpc_public_addr {
			Some(addr) => addr,
			None => {
				warn!("Not advertising to Kubernetes because rpc_public_addr is not defined in config file and could not be autodetected.");
				return Ok(());
			}
		};

		publish_kubernetes_node(
			&k.service_name,
			&k.namespace,
			self.netapp.id,
			&self.local_status.load_full().hostname,
			rpc_public_addr,
		)
		.await
		.err_context("Error while publishing node to kubernetes")
	}

	/// Save network configuration to disc
	async fn save_cluster_layout(self: Arc<Self>) -> Result<(), Error> {
		let ring: Arc<Ring> = self.ring.borrow().clone();
		self.persist_cluster_layout
			.save_async(&ring.layout)
			.await
			.expect("Cannot save current cluster layout");
		Ok(())
	}

	fn update_local_status(&self) {
		let mut new_si: NodeStatus = self.local_status.load().as_ref().clone();

		let ring = self.ring.borrow();
		new_si.cluster_layout_version = ring.layout.version;
		new_si.cluster_layout_staging_hash = ring.layout.staging_hash;
		self.local_status.swap(Arc::new(new_si));
	}

	async fn handle_connect(&self, node: &str) -> Result<SystemRpc, Error> {
		let (pubkey, addrs) = parse_and_resolve_peer_addr(node).ok_or_else(|| {
			Error::Message(format!(
				"Unable to parse or resolve node specification: {}",
				node
			))
		})?;
		let mut errors = vec![];
		for ip in addrs.iter() {
			match self
				.netapp
				.clone()
				.try_connect(*ip, pubkey)
				.await
				.err_context(CONNECT_ERROR_MESSAGE)
			{
				Ok(()) => return Ok(SystemRpc::Ok),
				Err(e) => {
					errors.push((*ip, e));
				}
			}
		}
		return Err(Error::Message(format!(
			"Could not connect to specified peers. Errors: {:?}",
			errors
		)));
	}

	fn handle_pull_cluster_layout(&self) -> SystemRpc {
		let ring = self.ring.borrow().clone();
		SystemRpc::AdvertiseClusterLayout(ring.layout.clone())
	}

	fn handle_get_known_nodes(&self) -> SystemRpc {
		let node_status = self.node_status.read().unwrap();
		let known_nodes = self
			.fullmesh
			.get_peer_list()
			.iter()
			.map(|n| KnownNodeInfo {
				id: n.id.into(),
				addr: n.addr,
				is_up: n.is_up(),
				last_seen_secs_ago: n.last_seen.map(|t| (Instant::now() - t).as_secs()),
				status: node_status
					.get(&n.id.into())
					.cloned()
					.map(|(_, st)| st)
					.unwrap_or(NodeStatus {
						hostname: "?".to_string(),
						replication_factor: 0,
						cluster_layout_version: 0,
						cluster_layout_staging_hash: Hash::from([0u8; 32]),
					}),
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

		if info.cluster_layout_version > local_info.cluster_layout_version
			|| info.cluster_layout_staging_hash != local_info.cluster_layout_staging_hash
		{
			let self2 = self.clone();
			self.background.spawn_cancellable(async move {
				self2.pull_cluster_layout(from).await;
				Ok(())
			});
		}

		self.node_status
			.write()
			.unwrap()
			.insert(from, (now_msec(), info.clone()));

		Ok(SystemRpc::Ok)
	}

	async fn handle_advertise_cluster_layout(
		self: Arc<Self>,
		adv: &ClusterLayout,
	) -> Result<SystemRpc, Error> {
		let update_ring = self.update_ring.lock().await;
		let mut layout: ClusterLayout = self.ring.borrow().layout.clone();

		let prev_layout_check = layout.check();
		if layout.merge(adv) {
			if prev_layout_check && !layout.check() {
				error!("New cluster layout is invalid, discarding.");
				return Err(Error::Message(
					"New cluster layout is invalid, discarding.".into(),
				));
			}

			let ring = Ring::new(layout.clone(), self.replication_factor);
			update_ring.send(Arc::new(ring))?;
			drop(update_ring);

			let self2 = self.clone();
			self.background.spawn_cancellable(async move {
				self2
					.rpc
					.broadcast(
						&self2.system_endpoint,
						SystemRpc::AdvertiseClusterLayout(layout),
						RequestStrategy::with_priority(PRIO_HIGH),
					)
					.await;
				Ok(())
			});
			self.background.spawn(self.clone().save_cluster_layout());
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
		while !*stop_signal.borrow() {
			let not_configured = !self.ring.borrow().layout.check();
			let no_peers = self.fullmesh.get_peer_list().len() < self.replication_factor;
			let expected_n_nodes = self.ring.borrow().layout.num_nodes();
			let bad_peers = self
				.fullmesh
				.get_peer_list()
				.iter()
				.filter(|p| p.is_up())
				.count() != expected_n_nodes;

			if not_configured || no_peers || bad_peers {
				info!("Doing a bootstrap/discovery step (not_configured: {}, no_peers: {}, bad_peers: {})", not_configured, no_peers, bad_peers);

				let mut ping_list = self.bootstrap_peers.clone();

				// Add peer list from list stored on disk
				if let Ok(peers) = self.persist_peer_list.load_async().await {
					ping_list.extend(peers.iter().map(|(id, addr)| ((*id).into(), *addr)))
				}

				// Fetch peer list from Consul
				if let Some(c) = &self.consul_discovery {
					match get_consul_nodes(&c.consul_host, &c.service_name).await {
						Ok(node_list) => {
							ping_list.extend(node_list);
						}
						Err(e) => {
							warn!("Could not retrieve node list from Consul: {}", e);
						}
					}
				}

				// Fetch peer list from Kubernetes
				#[cfg(feature = "kubernetes-discovery")]
				if let Some(k) = &self.kubernetes_discovery {
					if !k.skip_crd {
						match create_kubernetes_crd().await {
							Ok(()) => (),
							Err(e) => {
								error!("Failed to create kubernetes custom resource: {}", e)
							}
						};
					}

					match get_kubernetes_nodes(&k.service_name, &k.namespace).await {
						Ok(node_list) => {
							ping_list.extend(node_list);
						}
						Err(e) => {
							warn!("Could not retrieve node list from Kubernetes: {}", e);
						}
					}
				}

				for (node_id, node_addr) in ping_list {
					tokio::spawn(
						self.netapp
							.clone()
							.try_connect(node_addr, node_id)
							.map(|r| r.err_context(CONNECT_ERROR_MESSAGE)),
					);
				}
			}

			if let Err(e) = self.save_peer_list().await {
				warn!("Could not save peer list to file: {}", e);
			}

			self.background.spawn(self.clone().advertise_to_consul());

			#[cfg(feature = "kubernetes-discovery")]
			self.background
				.spawn(self.clone().advertise_to_kubernetes());

			let restart_at = tokio::time::sleep(DISCOVERY_INTERVAL);
			select! {
				_ = restart_at.fuse() => {},
				_ = stop_signal.changed().fuse() => {},
			}
		}
	}

	async fn save_peer_list(&self) -> Result<(), Error> {
		// Prepare new peer list to save to file
		// It is a vec of tuples (node ID as Uuid, node SocketAddr)
		let mut peer_list = self
			.fullmesh
			.get_peer_list()
			.iter()
			.map(|n| (n.id.into(), n.addr))
			.collect::<Vec<_>>();

		// Before doing it, we read the current peer list file (if it exists)
		// and append it to the list we are about to save,
		// so that no peer ID gets lost in the process.
		if let Ok(mut prev_peer_list) = self.persist_peer_list.load_async().await {
			prev_peer_list.retain(|(id, _ip)| peer_list.iter().all(|(id2, _ip2)| id2 != id));
			peer_list.extend(prev_peer_list);
		}

		// Save new peer list to file
		self.persist_peer_list.save_async(&peer_list).await
	}

	async fn pull_cluster_layout(self: Arc<Self>, peer: Uuid) {
		let resp = self
			.rpc
			.call(
				&self.system_endpoint,
				peer,
				SystemRpc::PullClusterLayout,
				RequestStrategy::with_priority(PRIO_HIGH).with_timeout(PING_TIMEOUT),
			)
			.await;
		if let Ok(SystemRpc::AdvertiseClusterLayout(layout)) = resp {
			let _: Result<_, _> = self.handle_advertise_cluster_layout(&layout).await;
		}
	}
}

#[async_trait]
impl EndpointHandler<SystemRpc> for System {
	async fn handle(self: &Arc<Self>, msg: &SystemRpc, from: NodeID) -> Result<SystemRpc, Error> {
		match msg {
			SystemRpc::Connect(node) => self.handle_connect(node).await,
			SystemRpc::PullClusterLayout => Ok(self.handle_pull_cluster_layout()),
			SystemRpc::AdvertiseStatus(adv) => self.handle_advertise_status(from.into(), adv).await,
			SystemRpc::AdvertiseClusterLayout(adv) => {
				self.clone().handle_advertise_cluster_layout(adv).await
			}
			SystemRpc::GetKnownNodes => Ok(self.handle_get_known_nodes()),
			m => Err(Error::unexpected_rpc_message(m)),
		}
	}
}

fn get_default_ip() -> Option<IpAddr> {
	pnet_datalink::interfaces()
		.iter()
		.find(|e| e.is_up() && !e.is_loopback() && !e.ips.is_empty())
		.and_then(|e| e.ips.first())
		.map(|a| a.ip())
}

struct ConsulDiscoveryParam {
	consul_host: String,
	service_name: String,
}

#[cfg(feature = "kubernetes-discovery")]
struct KubernetesDiscoveryParam {
	service_name: String,
	namespace: String,
	skip_crd: bool,
}
