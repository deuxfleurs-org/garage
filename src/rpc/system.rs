//! Module containing structs related to membership management
use std::collections::{HashMap, HashSet};
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock, RwLockReadGuard};
use std::time::{Duration, Instant};

use arc_swap::ArcSwapOption;
use futures::join;
use serde::{Deserialize, Serialize};
use sodiumoxide::crypto::sign::ed25519;
use tokio::select;
use tokio::sync::{watch, Notify};

use garage_net::endpoint::{Endpoint, EndpointHandler};
use garage_net::message::*;
use garage_net::peering::{PeerConnState, PeeringManager};
use garage_net::util::parse_and_resolve_peer_addr_async;
use garage_net::{NetApp, NetworkKey, NodeID, NodeKey};

#[cfg(feature = "kubernetes-discovery")]
use garage_util::config::KubernetesDiscoveryConfig;
use garage_util::config::{Config, DataDirEnum};
use garage_util::data::*;
use garage_util::error::*;
use garage_util::persister::Persister;
use garage_util::time::*;

#[cfg(feature = "consul-discovery")]
use crate::consul::ConsulDiscovery;
#[cfg(feature = "kubernetes-discovery")]
use crate::kubernetes::*;
use crate::layout::{
	self, manager::LayoutManager, LayoutHelper, LayoutHistory, NodeRoleV, RpcLayoutDigest,
};
use crate::replication_mode::*;
use crate::rpc_helper::*;

use crate::system_metrics::*;

const DISCOVERY_INTERVAL: Duration = Duration::from_secs(60);
const STATUS_EXCHANGE_INTERVAL: Duration = Duration::from_secs(10);

/// Version tag used for version check upon Netapp connection.
/// Cluster nodes with different version tags are deemed
/// incompatible and will refuse to connect.
pub const GARAGE_VERSION_TAG: u64 = 0x6761726167650010; // garage 0x0010 (1.0)

/// RPC endpoint used for calls related to membership
pub const SYSTEM_RPC_PATH: &str = "garage_rpc/system.rs/SystemRpc";

/// RPC messages related to membership
#[derive(Debug, Serialize, Deserialize, Clone)]
pub enum SystemRpc {
	/// Response to successful advertisements
	Ok,
	/// Request to connect to a specific node (in <pubkey>@<host>:<port> format, pubkey = full-length node ID)
	Connect(String),
	/// Advertise Garage status. Answered with another AdvertiseStatus.
	/// Exchanged with every node on a regular basis.
	AdvertiseStatus(NodeStatus),
	/// Get known nodes states
	GetKnownNodes,
	/// Return known nodes
	ReturnKnownNodes(Vec<KnownNodeInfo>),

	/// Ask other node its cluster layout. Answered with AdvertiseClusterLayout
	PullClusterLayout,
	/// Advertisement of cluster layout. Sent spontanously or in response to PullClusterLayout
	AdvertiseClusterLayout(LayoutHistory),
	/// Ask other node its cluster layout update trackers.
	PullClusterLayoutTrackers,
	/// Advertisement of cluster layout update trackers.
	AdvertiseClusterLayoutTrackers(layout::UpdateTrackers),
}

impl Rpc for SystemRpc {
	type Response = Result<SystemRpc, Error>;
}

#[derive(Serialize, Deserialize)]
pub struct PeerList(Vec<(Uuid, SocketAddr)>);
impl garage_util::migrate::InitialFormat for PeerList {}

/// This node's membership manager
pub struct System {
	/// The id of this node
	pub id: Uuid,

	persist_peer_list: Persister<PeerList>,

	pub(crate) local_status: RwLock<NodeStatus>,
	node_status: RwLock<HashMap<Uuid, (u64, NodeStatus)>>,

	pub netapp: Arc<NetApp>,
	peering: Arc<PeeringManager>,

	pub(crate) system_endpoint: Arc<Endpoint<SystemRpc, System>>,

	rpc_listen_addr: SocketAddr,
	rpc_public_addr: Option<SocketAddr>,
	bootstrap_peers: Vec<String>,

	#[cfg(feature = "consul-discovery")]
	consul_discovery: Option<ConsulDiscovery>,
	#[cfg(feature = "kubernetes-discovery")]
	kubernetes_discovery: Option<KubernetesDiscoveryConfig>,

	pub layout_manager: Arc<LayoutManager>,

	metrics: ArcSwapOption<SystemMetrics>,

	pub(crate) replication_factor: ReplicationFactor,

	/// Path to metadata directory
	pub metadata_dir: PathBuf,
	/// Path to data directory
	pub data_dir: DataDirEnum,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeStatus {
	/// Hostname of the node
	pub hostname: Option<String>,

	/// Replication factor configured on the node
	pub replication_factor: usize,

	/// Cluster layout digest
	pub layout_digest: RpcLayoutDigest,

	/// Disk usage on partition containing metadata directory (tuple: `(avail, total)`)
	#[serde(default)]
	pub meta_disk_avail: Option<(u64, u64)>,
	/// Disk usage on partition containing data directory (tuple: `(avail, total)`)
	#[serde(default)]
	pub data_disk_avail: Option<(u64, u64)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KnownNodeInfo {
	pub id: Uuid,
	pub addr: Option<SocketAddr>,
	pub is_up: bool,
	pub last_seen_secs_ago: Option<u64>,
	pub status: NodeStatus,
}

#[derive(Debug, Clone, Copy)]
pub struct ClusterHealth {
	/// The current health status of the cluster (see below)
	pub status: ClusterHealthStatus,
	/// Number of nodes already seen once in the cluster
	pub known_nodes: usize,
	/// Number of nodes currently connected
	pub connected_nodes: usize,
	/// Number of storage nodes declared in the current layout
	pub storage_nodes: usize,
	/// Number of storage nodes currently connected
	pub storage_nodes_ok: usize,
	/// Number of partitions in the layout
	pub partitions: usize,
	/// Number of partitions for which we have a quorum of connected nodes
	pub partitions_quorum: usize,
	/// Number of partitions for which all storage nodes are connected
	pub partitions_all_ok: usize,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum ClusterHealthStatus {
	/// All nodes are available
	Healthy,
	/// Some storage nodes are unavailable, but quorum is still
	/// achieved for all partitions
	Degraded,
	/// Quorum is not available for some partitions
	Unavailable,
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
			std::fs::create_dir(metadata_dir)?;
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
		replication_factor: ReplicationFactor,
		consistency_mode: ConsistencyMode,
		config: &Config,
	) -> Result<Arc<Self>, Error> {
		// ---- setup netapp RPC protocol ----
		let node_key =
			gen_node_key(&config.metadata_dir).expect("Unable to read or generate node ID");
		info!(
			"Node ID of this node: {}",
			hex::encode(&node_key.public_key()[..8])
		);

		let bind_outgoing_to = Some(config)
			.filter(|x| x.rpc_bind_outgoing)
			.map(|x| x.rpc_bind_addr.ip());
		let netapp = NetApp::new(GARAGE_VERSION_TAG, network_key, node_key, bind_outgoing_to);
		let system_endpoint = netapp.endpoint(SYSTEM_RPC_PATH.into());

		// ---- setup netapp public listener and full mesh peering strategy ----
		let rpc_public_addr = get_rpc_public_addr(config);
		if rpc_public_addr.is_none() {
			warn!("This Garage node does not know its publicly reachable RPC address, this might hamper intra-cluster communication.");
		}

		let peering = PeeringManager::new(netapp.clone(), vec![], rpc_public_addr);
		if let Some(ping_timeout) = config.rpc_ping_timeout_msec {
			peering.set_ping_timeout_millis(ping_timeout);
		}

		let persist_peer_list = Persister::new(&config.metadata_dir, "peer_list");

		// ---- setup cluster layout and layout manager ----
		let layout_manager = LayoutManager::new(
			config,
			netapp.id,
			system_endpoint.clone(),
			peering.clone(),
			replication_factor,
			consistency_mode,
		)?;

		let mut local_status = NodeStatus::initial(replication_factor, &layout_manager);
		local_status.update_disk_usage(&config.metadata_dir, &config.data_dir);

		// ---- if enabled, set up additional peer discovery methods ----
		#[cfg(feature = "consul-discovery")]
		let consul_discovery = match &config.consul_discovery {
			Some(cfg) => Some(
				ConsulDiscovery::new(cfg.clone())
					.ok_or_message("Invalid Consul discovery configuration")?,
			),
			None => None,
		};
		#[cfg(not(feature = "consul-discovery"))]
		if config.consul_discovery.is_some() {
			warn!("Consul discovery is not enabled in this build.");
		}

		#[cfg(not(feature = "kubernetes-discovery"))]
		if config.kubernetes_discovery.is_some() {
			warn!("Kubernetes discovery is not enabled in this build.");
		}

		// ---- almost done ----
		let sys = Arc::new(System {
			id: netapp.id.into(),
			persist_peer_list,
			local_status: RwLock::new(local_status),
			node_status: RwLock::new(HashMap::new()),
			netapp: netapp.clone(),
			peering: peering.clone(),
			system_endpoint,
			replication_factor,
			rpc_listen_addr: config.rpc_bind_addr,
			rpc_public_addr,
			bootstrap_peers: config.bootstrap_peers.clone(),
			#[cfg(feature = "consul-discovery")]
			consul_discovery,
			#[cfg(feature = "kubernetes-discovery")]
			kubernetes_discovery: config.kubernetes_discovery.clone(),
			layout_manager,
			metrics: ArcSwapOption::new(None),

			metadata_dir: config.metadata_dir.clone(),
			data_dir: config.data_dir.clone(),
		});

		sys.system_endpoint.set_handler(sys.clone());

		let metrics = SystemMetrics::new(sys.clone());
		sys.metrics.store(Some(Arc::new(metrics)));

		Ok(sys)
	}

	/// Perform bootstrapping, starting the ping loop
	pub async fn run(self: Arc<Self>, must_exit: watch::Receiver<bool>) {
		join!(
			self.netapp.clone().listen(
				self.rpc_listen_addr,
				self.rpc_public_addr,
				must_exit.clone()
			),
			self.peering.clone().run(must_exit.clone()),
			self.discovery_loop(must_exit.clone()),
			self.status_exchange_loop(must_exit.clone()),
		);
	}

	pub fn cleanup(&self) {
		// Break reference cycle
		self.metrics.store(None);
	}

	// ---- Public utilities / accessors ----

	pub fn cluster_layout(&self) -> RwLockReadGuard<'_, LayoutHelper> {
		self.layout_manager.layout()
	}

	pub fn layout_notify(&self) -> Arc<Notify> {
		self.layout_manager.change_notify.clone()
	}

	pub fn rpc_helper(&self) -> &RpcHelper {
		&self.layout_manager.rpc_helper
	}

	// ---- Administrative operations (directly available and
	//      also available through RPC) ----

	pub fn get_known_nodes(&self) -> Vec<KnownNodeInfo> {
		let node_status = self.node_status.read().unwrap();
		let known_nodes = self
			.peering
			.get_peer_list()
			.iter()
			.map(|n| KnownNodeInfo {
				id: n.id.into(),
				addr: match n.state {
					PeerConnState::Ourself => self.rpc_public_addr,
					PeerConnState::Connected { addr } => Some(addr),
					_ => None,
				},
				is_up: n.is_up(),
				last_seen_secs_ago: n
					.last_seen
					.map(|t| (Instant::now().saturating_duration_since(t)).as_secs()),
				status: node_status
					.get(&n.id.into())
					.cloned()
					.map(|(_, st)| st)
					.unwrap_or_else(NodeStatus::unknown),
			})
			.collect::<Vec<_>>();
		known_nodes
	}

	pub async fn connect(&self, node: &str) -> Result<(), Error> {
		let (pubkey, addrs) = parse_and_resolve_peer_addr_async(node)
			.await
			.ok_or_else(|| {
				Error::Message(format!(
					"Unable to parse or resolve node specification: {}",
					node
				))
			})?;
		let mut errors = vec![];
		for addr in addrs.iter() {
			match self.netapp.clone().try_connect(*addr, pubkey).await {
				Ok(()) => return Ok(()),
				Err(e) => {
					errors.push((
						*addr,
						Error::Message(connect_error_message(*addr, pubkey, e)),
					));
				}
			}
		}
		if errors.len() == 1 {
			Err(Error::Message(errors[0].1.to_string()))
		} else {
			Err(Error::Message(format!("{:?}", errors)))
		}
	}

	pub fn health(&self) -> ClusterHealth {
		let quorum = self
			.replication_factor
			.write_quorum(ConsistencyMode::Consistent);

		// Gather information about running nodes.
		// Technically, `nodes` contains currently running nodes, as well
		// as nodes that this Garage process has been connected to at least
		// once since it started.
		let nodes = self
			.get_known_nodes()
			.into_iter()
			.map(|n| (n.id, n))
			.collect::<HashMap<Uuid, _>>();
		let connected_nodes = nodes.iter().filter(|(_, n)| n.is_up).count();
		let node_up = |x: &Uuid| nodes.get(x).map(|n| n.is_up).unwrap_or(false);

		// Acquire a rwlock read-lock to the current cluster layout
		let layout = self.cluster_layout();

		// Obtain information about nodes that have a role as storage nodes
		// in one of the active layout versions
		let mut storage_nodes = HashSet::<Uuid>::with_capacity(16);
		for ver in layout.versions().iter() {
			storage_nodes.extend(
				ver.roles
					.items()
					.iter()
					.filter(|(_, _, v)| matches!(v, NodeRoleV(Some(r)) if r.capacity.is_some()))
					.map(|(n, _, _)| *n),
			)
		}
		let storage_nodes_ok = storage_nodes.iter().filter(|x| node_up(x)).count();

		// Determine the number of partitions that have:
		// - a quorum of up nodes for all write sets (i.e. are available)
		// - for which all nodes in all write sets are up (i.e. are fully healthy)
		let partitions = layout.current().partitions().collect::<Vec<_>>();
		let mut partitions_quorum = 0;
		let mut partitions_all_ok = 0;
		for (_, hash) in partitions.iter() {
			let mut write_sets = layout
				.versions()
				.iter()
				.map(|x| x.nodes_of(hash, x.replication_factor));
			let has_quorum = write_sets
				.clone()
				.all(|set| set.filter(|x| node_up(x)).count() >= quorum);
			let all_ok = write_sets.all(|mut set| set.all(|x| node_up(&x)));
			if has_quorum {
				partitions_quorum += 1;
			}
			if all_ok {
				partitions_all_ok += 1;
			}
		}

		// Determine overall cluster status
		let status =
			if partitions_all_ok == partitions.len() && storage_nodes_ok == storage_nodes.len() {
				ClusterHealthStatus::Healthy
			} else if partitions_quorum == partitions.len() {
				ClusterHealthStatus::Degraded
			} else {
				ClusterHealthStatus::Unavailable
			};

		ClusterHealth {
			status,
			known_nodes: nodes.len(),
			connected_nodes,
			storage_nodes: storage_nodes.len(),
			storage_nodes_ok,
			partitions: partitions.len(),
			partitions_quorum,
			partitions_all_ok,
		}
	}

	// ---- INTERNALS ----

	#[cfg(feature = "consul-discovery")]
	async fn advertise_to_consul(self: Arc<Self>) {
		let c = match &self.consul_discovery {
			Some(c) => c,
			_ => return,
		};

		let rpc_public_addr = match self.rpc_public_addr {
			Some(addr) => addr,
			None => {
				warn!("Not advertising to Consul because rpc_public_addr is not defined in config file and could not be autodetected.");
				return;
			}
		};

		let hostname = self.local_status.read().unwrap().hostname.clone().unwrap();
		if let Err(e) = c
			.publish_consul_service(self.netapp.id, &hostname, rpc_public_addr)
			.await
		{
			error!("Error while publishing Consul service: {}", e);
		}
	}

	#[cfg(feature = "kubernetes-discovery")]
	async fn advertise_to_kubernetes(self: Arc<Self>) {
		let k = match &self.kubernetes_discovery {
			Some(k) => k,
			_ => return,
		};

		let rpc_public_addr = match self.rpc_public_addr {
			Some(addr) => addr,
			None => {
				warn!("Not advertising to Kubernetes because rpc_public_addr is not defined in config file and could not be autodetected.");
				return;
			}
		};

		let hostname = self.local_status.read().unwrap().hostname.clone().unwrap();
		if let Err(e) = publish_kubernetes_node(k, self.netapp.id, &hostname, rpc_public_addr).await
		{
			error!("Error while publishing node to Kubernetes: {}", e);
		}
	}

	fn update_local_status(&self) {
		let mut local_status = self.local_status.write().unwrap();
		local_status.layout_digest = self.layout_manager.layout().digest();
		local_status.update_disk_usage(&self.metadata_dir, &self.data_dir);
	}

	// --- RPC HANDLERS ---

	async fn handle_connect(&self, node: &str) -> Result<SystemRpc, Error> {
		self.connect(node).await?;
		Ok(SystemRpc::Ok)
	}

	fn handle_get_known_nodes(&self) -> SystemRpc {
		let known_nodes = self.get_known_nodes();
		SystemRpc::ReturnKnownNodes(known_nodes)
	}

	async fn handle_advertise_status(
		self: &Arc<Self>,
		from: Uuid,
		info: &NodeStatus,
	) -> Result<SystemRpc, Error> {
		let local_info = self.local_status.read().unwrap();

		if local_info.replication_factor < info.replication_factor {
			error!("Some node have a higher replication factor ({}) than this one ({}). This is not supported and will lead to data corruption. Shutting down for safety.",
				info.replication_factor,
				local_info.replication_factor);
			std::process::exit(1);
		}

		self.layout_manager
			.handle_advertise_status(from, &info.layout_digest);

		drop(local_info);

		self.node_status
			.write()
			.unwrap()
			.insert(from, (now_msec(), info.clone()));

		Ok(SystemRpc::Ok)
	}

	async fn status_exchange_loop(&self, mut stop_signal: watch::Receiver<bool>) {
		while !*stop_signal.borrow() {
			let restart_at = Instant::now() + STATUS_EXCHANGE_INTERVAL;

			// Update local node status that is exchanged.
			self.update_local_status();

			let local_status: NodeStatus = self.local_status.read().unwrap().clone();
			let _ = self
				.rpc_helper()
				.broadcast(
					&self.system_endpoint,
					SystemRpc::AdvertiseStatus(local_status),
					RequestStrategy::with_priority(PRIO_HIGH)
						.with_custom_timeout(STATUS_EXCHANGE_INTERVAL),
				)
				.await;

			select! {
				_ = tokio::time::sleep_until(restart_at.into()) => {},
				_ = stop_signal.changed() => {},
			}
		}
	}

	async fn discovery_loop(self: &Arc<Self>, mut stop_signal: watch::Receiver<bool>) {
		while !*stop_signal.borrow() {
			let n_connected = self
				.peering
				.get_peer_list()
				.iter()
				.filter(|p| p.is_up())
				.count();

			let not_configured = !self.cluster_layout().is_check_ok();
			let no_peers = n_connected < self.replication_factor.into();
			let expected_n_nodes = self.cluster_layout().all_nodes().len();
			let bad_peers = n_connected != expected_n_nodes;

			if not_configured || no_peers || bad_peers {
				info!("Doing a bootstrap/discovery step (not_configured: {}, no_peers: {}, bad_peers: {})", not_configured, no_peers, bad_peers);

				let mut ping_list = resolve_peers(&self.bootstrap_peers).await;

				// Add peer list from list stored on disk
				if let Ok(peers) = self.persist_peer_list.load_async().await {
					ping_list.extend(peers.0.iter().map(|(id, addr)| ((*id).into(), *addr)))
				}

				// Fetch peer list from Consul
				#[cfg(feature = "consul-discovery")]
				if let Some(c) = &self.consul_discovery {
					match c.get_consul_nodes().await {
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

					match get_kubernetes_nodes(k).await {
						Ok(node_list) => {
							ping_list.extend(node_list);
						}
						Err(e) => {
							warn!("Could not retrieve node list from Kubernetes: {}", e);
						}
					}
				}

				if !not_configured && !no_peers {
					// If the layout is configured, and we already have some connections
					// to other nodes in the cluster, we can skip trying to connect to
					// nodes that are not in the cluster layout.
					let layout = self.cluster_layout();
					ping_list.retain(|(id, _)| layout.all_nodes().contains(&(*id).into()));
				}

				for (node_id, node_addr) in ping_list {
					let self2 = self.clone();
					tokio::spawn(async move {
						if let Err(e) = self2.netapp.clone().try_connect(node_addr, node_id).await {
							error!("{}", connect_error_message(node_addr, node_id, e));
						}
					});
				}
			}

			if let Err(e) = self.save_peer_list().await {
				warn!("Could not save peer list to file: {}", e);
			}

			#[cfg(feature = "consul-discovery")]
			tokio::spawn(self.clone().advertise_to_consul());

			#[cfg(feature = "kubernetes-discovery")]
			tokio::spawn(self.clone().advertise_to_kubernetes());

			select! {
				_ = tokio::time::sleep(DISCOVERY_INTERVAL) => {},
				_ = stop_signal.changed() => {},
			}
		}
	}

	async fn save_peer_list(&self) -> Result<(), Error> {
		// Prepare new peer list to save to file
		// It is a vec of tuples (node ID as Uuid, node SocketAddr)
		let mut peer_list = self
			.peering
			.get_peer_list()
			.iter()
			.filter_map(|n| match n.state {
				PeerConnState::Connected { addr } => Some((n.id.into(), addr)),
				_ => None,
			})
			.collect::<Vec<_>>();

		// Before doing it, we read the current peer list file (if it exists)
		// and append it to the list we are about to save,
		// so that no peer ID gets lost in the process.
		if let Ok(mut prev_peer_list) = self.persist_peer_list.load_async().await {
			prev_peer_list
				.0
				.retain(|(id, _ip)| peer_list.iter().all(|(id2, _ip2)| id2 != id));
			peer_list.extend(prev_peer_list.0);
		}

		// Save new peer list to file
		self.persist_peer_list
			.save_async(&PeerList(peer_list))
			.await
	}
}

impl EndpointHandler<SystemRpc> for System {
	async fn handle(self: &Arc<Self>, msg: &SystemRpc, from: NodeID) -> Result<SystemRpc, Error> {
		match msg {
			// ---- system functions -> System ----
			SystemRpc::Connect(node) => self.handle_connect(node).await,
			SystemRpc::AdvertiseStatus(adv) => self.handle_advertise_status(from.into(), adv).await,
			SystemRpc::GetKnownNodes => Ok(self.handle_get_known_nodes()),

			// ---- layout functions -> LayoutManager ----
			SystemRpc::PullClusterLayout => Ok(self.layout_manager.handle_pull_cluster_layout()),
			SystemRpc::AdvertiseClusterLayout(adv) => {
				self.layout_manager
					.handle_advertise_cluster_layout(adv)
					.await
			}
			SystemRpc::PullClusterLayoutTrackers => {
				Ok(self.layout_manager.handle_pull_cluster_layout_trackers())
			}
			SystemRpc::AdvertiseClusterLayoutTrackers(adv) => {
				self.layout_manager
					.handle_advertise_cluster_layout_trackers(adv)
					.await
			}

			// ---- other -> Error ----
			m => Err(Error::unexpected_rpc_message(m)),
		}
	}
}

impl NodeStatus {
	fn initial(replication_factor: ReplicationFactor, layout_manager: &LayoutManager) -> Self {
		NodeStatus {
			hostname: Some(
				gethostname::gethostname()
					.into_string()
					.unwrap_or_else(|_| "<invalid utf-8>".to_string()),
			),
			replication_factor: replication_factor.into(),
			layout_digest: layout_manager.layout().digest(),
			meta_disk_avail: None,
			data_disk_avail: None,
		}
	}

	fn unknown() -> Self {
		NodeStatus {
			hostname: None,
			replication_factor: 0,
			layout_digest: Default::default(),
			meta_disk_avail: None,
			data_disk_avail: None,
		}
	}

	fn update_disk_usage(&mut self, meta_dir: &Path, data_dir: &DataDirEnum) {
		use nix::sys::statvfs::statvfs;

		// The HashMap used below requires a filesystem identifier from statfs (instead of statvfs) on FreeBSD, as
		// FreeBSD's statvfs filesystem identifier is "not meaningful in this implementation" (man 3 statvfs).

		#[cfg(target_os = "freebsd")]
		let get_filesystem_id = |path: &Path| match nix::sys::statfs::statfs(path) {
			Ok(fs) => Some(fs.filesystem_id()),
			Err(_) => None,
		};

		let mount_avail = |path: &Path| match statvfs(path) {
			Ok(x) => {
				let avail = x.blocks_available() as u64 * x.fragment_size() as u64;
				let total = x.blocks() as u64 * x.fragment_size() as u64;
				Some((x.filesystem_id(), avail, total))
			}
			Err(_) => None,
		};

		self.meta_disk_avail = mount_avail(meta_dir).map(|(_, a, t)| (a, t));

		self.data_disk_avail = match data_dir {
			DataDirEnum::Single(dir) => mount_avail(dir).map(|(_, a, t)| (a, t)),
			DataDirEnum::Multiple(dirs) => (|| {
				// TODO: more precise calculation that takes into account
				// how data is going to be spread among partitions
				let mut mounts = HashMap::new();
				for dir in dirs.iter() {
					if dir.capacity.is_none() {
						continue;
					}

					#[cfg(not(target_os = "freebsd"))]
					match mount_avail(&dir.path) {
						Some((fsid, avail, total)) => {
							mounts.insert(fsid, (avail, total));
						}
						None => return None,
					}

					#[cfg(target_os = "freebsd")]
					match get_filesystem_id(&dir.path) {
						Some(fsid) => match mount_avail(&dir.path) {
							Some((_, avail, total)) => {
								mounts.insert(fsid, (avail, total));
							}
							None => return None,
						},
						None => return None,
					}
				}
				Some(
					mounts
						.into_iter()
						.fold((0, 0), |(x, y), (_, (a, b))| (x + a, y + b)),
				)
			})(),
		};
	}
}

/// Obtain the list of currently available IP addresses on all non-loopback
/// interfaces, optionally filtering them to be inside a given IpNet.
fn get_default_ip(filter_ipnet: Option<ipnet::IpNet>) -> Option<IpAddr> {
	pnet_datalink::interfaces()
		.into_iter()
		// filter down and loopback interfaces
		.filter(|i| i.is_up() && !i.is_loopback())
		// get all IPs
		.flat_map(|e| e.ips)
		// optionally, filter to be inside filter_ipnet
		.find(|ipn| {
			filter_ipnet.is_some_and(|ipnet| ipnet.contains(&ipn.ip())) || filter_ipnet.is_none()
		})
		.map(|ipn| ipn.ip())
}

fn get_rpc_public_addr(config: &Config) -> Option<SocketAddr> {
	match &config.rpc_public_addr {
		Some(a_str) => {
			use std::net::ToSocketAddrs;
			match a_str.to_socket_addrs() {
				Err(e) => {
					error!(
						"Cannot resolve rpc_public_addr {} from config file: {}.",
						a_str, e
					);
					None
				}
				Ok(a) => {
					let a = a.collect::<Vec<_>>();
					if a.is_empty() {
						error!("rpc_public_addr {} resolve to no known IP address", a_str);
					}
					if a.len() > 1 {
						warn!("Multiple possible resolutions for rpc_public_addr: {:?}. Taking the first one.", a);
					}
					a.into_iter().next()
				}
			}
		}
		None => {
			// `No rpc_public_addr` specified, try to discover one, optionally filtering by `rpc_public_addr_subnet`.
			let filter_subnet: Option<ipnet::IpNet> = config
				.rpc_public_addr_subnet
				.as_ref()
				.and_then(|filter_subnet_str| match filter_subnet_str.parse::<ipnet::IpNet>() {
					Ok(filter_subnet) => {
						let filter_subnet_trunc = filter_subnet.trunc();
						if filter_subnet_trunc != filter_subnet {
							warn!("`rpc_public_addr_subnet` changed after applying netmask, continuing with {}", filter_subnet.trunc());
						}
						Some(filter_subnet_trunc)
					}
					Err(e) => {
						panic!(
							"Cannot parse rpc_public_addr_subnet {} from config file: {}. Bailing out.",
							filter_subnet_str, e
						);
					}
				});

			let addr = get_default_ip(filter_subnet)
				.map(|ip| SocketAddr::new(ip, config.rpc_bind_addr.port()));
			if let Some(a) = addr {
				warn!("Using autodetected rpc_public_addr: {}. Consider specifying it explicitly in configuration file if possible.", a);
			}
			addr
		}
	}
}

async fn resolve_peers(peers: &[String]) -> Vec<(NodeID, SocketAddr)> {
	let mut ret = vec![];

	for peer in peers.iter() {
		match parse_and_resolve_peer_addr_async(peer).await {
			Some((pubkey, addrs)) => {
				for ip in addrs {
					ret.push((pubkey, ip));
				}
			}
			None => {
				warn!("Unable to parse and/or resolve peer hostname {}", peer);
			}
		}
	}

	ret
}

fn connect_error_message(
	addr: SocketAddr,
	pubkey: ed25519::PublicKey,
	e: garage_net::error::Error,
) -> String {
	format!("Error establishing RPC connection to remote node: {}@{}.\nThis can happen if the remote node is not reachable on the network, but also if the two nodes are not configured with the same rpc_secret.\n{}", hex::encode(pubkey), addr, e)
}
