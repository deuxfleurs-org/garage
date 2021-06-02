//! Module containing structs related to membership management
use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::io::{Read, Write};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use futures::select;
use futures_util::future::*;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio::sync::Mutex;

use garage_util::background::BackgroundRunner;
use garage_util::data::*;
use garage_util::error::Error;
use garage_util::persister::Persister;
use garage_util::time::*;

use crate::consul::get_consul_nodes;
use crate::ring::*;
use crate::rpc_client::*;
use crate::rpc_server::*;

const PING_INTERVAL: Duration = Duration::from_secs(10);
const DISCOVERY_INTERVAL: Duration = Duration::from_secs(60);
const PING_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_FAILURES_BEFORE_CONSIDERED_DOWN: usize = 5;

/// RPC endpoint used for calls related to membership
pub const MEMBERSHIP_RPC_PATH: &str = "_membership";

/// RPC messages related to membership
#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
	/// Response to successfull advertisements
	Ok,
	/// Message sent to detect other nodes status
	Ping(PingMessage),
	/// Ask other node for the nodes it knows. Answered with AdvertiseNodesUp
	PullStatus,
	/// Ask other node its config. Answered with AdvertiseConfig
	PullConfig,
	/// Advertisement of nodes the host knows up. Sent spontanously or in response to PullStatus
	AdvertiseNodesUp(Vec<AdvertisedNode>),
	/// Advertisement of nodes config. Sent spontanously or in response to PullConfig
	AdvertiseConfig(NetworkConfig),
}

impl RpcMessage for Message {}

/// A ping, containing informations about status and config
#[derive(Debug, Serialize, Deserialize)]
pub struct PingMessage {
	id: Uuid,
	rpc_port: u16,

	status_hash: Hash,
	config_version: u64,

	state_info: StateInfo,
}

/// A node advertisement
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdvertisedNode {
	/// Id of the node this advertisement relates to
	pub id: Uuid,
	/// IP and port of the node
	pub addr: SocketAddr,

	/// Is the node considered up
	pub is_up: bool,
	/// When was the node last seen up, in milliseconds since UNIX epoch
	pub last_seen: u64,

	pub state_info: StateInfo,
}

/// This node's membership manager
pub struct System {
	/// The id of this node
	pub id: Uuid,

	persist_config: Persister<NetworkConfig>,
	persist_status: Persister<Vec<AdvertisedNode>>,
	rpc_local_port: u16,

	state_info: StateInfo,

	rpc_http_client: Arc<RpcHttpClient>,
	rpc_client: Arc<RpcClient<Message>>,

	replication_factor: usize,
	pub(crate) status: watch::Receiver<Arc<Status>>,
	/// The ring
	pub ring: watch::Receiver<Arc<Ring>>,

	update_lock: Mutex<Updaters>,

	/// The job runner of this node
	pub background: Arc<BackgroundRunner>,
}

struct Updaters {
	update_status: watch::Sender<Arc<Status>>,
	update_ring: watch::Sender<Arc<Ring>>,
}

/// The status of each nodes, viewed by this node
#[derive(Debug, Clone)]
pub struct Status {
	/// Mapping of each node id to its known status
	pub nodes: HashMap<Uuid, Arc<StatusEntry>>,
	/// Hash of `nodes`, used to detect when nodes have different views of the cluster
	pub hash: Hash,
}

/// The status of a single node
#[derive(Debug)]
pub struct StatusEntry {
	/// The IP and port used to connect to this node
	pub addr: SocketAddr,
	/// Last time this node was seen
	pub last_seen: u64,
	/// Number of consecutive pings sent without reply to this node
	pub num_failures: AtomicUsize,
	pub state_info: StateInfo,
}

impl StatusEntry {
	/// is the node associated to this entry considered up
	pub fn is_up(&self) -> bool {
		self.num_failures.load(Ordering::SeqCst) < MAX_FAILURES_BEFORE_CONSIDERED_DOWN
	}
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateInfo {
	/// Hostname of the node
	pub hostname: String,
	/// Replication factor configured on the node
	pub replication_factor: Option<usize>, // TODO Option is just for retrocompatibility. It should become a simple usize at some point
}

impl Status {
	fn handle_ping(&mut self, ip: IpAddr, info: &PingMessage) -> bool {
		let addr = SocketAddr::new(ip, info.rpc_port);
		let old_status = self.nodes.insert(
			info.id,
			Arc::new(StatusEntry {
				addr,
				last_seen: now_msec(),
				num_failures: AtomicUsize::from(0),
				state_info: info.state_info.clone(),
			}),
		);
		match old_status {
			None => {
				info!("Newly pingable node: {}", hex::encode(&info.id));
				true
			}
			Some(x) => x.addr != addr,
		}
	}

	fn recalculate_hash(&mut self) {
		let mut nodes = self.nodes.iter().collect::<Vec<_>>();
		nodes.sort_unstable_by_key(|(id, _status)| *id);

		let mut nodes_txt = String::new();
		debug!("Current set of pingable nodes: --");
		for (id, status) in nodes {
			debug!("{} {}", hex::encode(&id), status.addr);
			writeln!(&mut nodes_txt, "{} {}", hex::encode(&id), status.addr).unwrap();
		}
		debug!("END --");
		self.hash = blake2sum(nodes_txt.as_bytes());
	}

	fn to_serializable_membership(&self, system: &System) -> Vec<AdvertisedNode> {
		let mut mem = vec![];
		for (node, status) in self.nodes.iter() {
			let state_info = if *node == system.id {
				system.state_info.clone()
			} else {
				status.state_info.clone()
			};
			mem.push(AdvertisedNode {
				id: *node,
				addr: status.addr,
				is_up: status.is_up(),
				last_seen: status.last_seen,
				state_info,
			});
		}
		mem
	}
}

fn gen_node_id(metadata_dir: &Path) -> Result<Uuid, Error> {
	let mut id_file = metadata_dir.to_path_buf();
	id_file.push("node_id");
	if id_file.as_path().exists() {
		let mut f = std::fs::File::open(id_file.as_path())?;
		let mut d = vec![];
		f.read_to_end(&mut d)?;
		if d.len() != 32 {
			return Err(Error::Message("Corrupt node_id file".to_string()));
		}

		let mut id = [0u8; 32];
		id.copy_from_slice(&d[..]);
		Ok(id.into())
	} else {
		let id = gen_uuid();

		let mut f = std::fs::File::create(id_file.as_path())?;
		f.write_all(id.as_slice())?;
		Ok(id)
	}
}

impl System {
	/// Create this node's membership manager
	pub fn new(
		metadata_dir: PathBuf,
		rpc_http_client: Arc<RpcHttpClient>,
		background: Arc<BackgroundRunner>,
		rpc_server: &mut RpcServer,
		replication_factor: usize,
	) -> Arc<Self> {
		let id = gen_node_id(&metadata_dir).expect("Unable to read or generate node ID");
		info!("Node ID: {}", hex::encode(&id));

		let persist_config = Persister::new(&metadata_dir, "network_config");
		let persist_status = Persister::new(&metadata_dir, "peer_info");

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

		let mut status = Status {
			nodes: HashMap::new(),
			hash: Hash::default(),
		};
		status.recalculate_hash();
		let (update_status, status) = watch::channel(Arc::new(status));

		let state_info = StateInfo {
			hostname: gethostname::gethostname()
				.into_string()
				.unwrap_or_else(|_| "<invalid utf-8>".to_string()),
			replication_factor: Some(replication_factor),
		};

		let ring = Ring::new(net_config, replication_factor);
		let (update_ring, ring) = watch::channel(Arc::new(ring));

		let rpc_path = MEMBERSHIP_RPC_PATH.to_string();
		let rpc_client = RpcClient::new(
			RpcAddrClient::<Message>::new(rpc_http_client.clone(), rpc_path.clone()),
			background.clone(),
			status.clone(),
		);

		let sys = Arc::new(System {
			id,
			persist_config,
			persist_status,
			rpc_local_port: rpc_server.bind_addr.port(),
			state_info,
			rpc_http_client,
			rpc_client,
			replication_factor,
			status,
			ring,
			update_lock: Mutex::new(Updaters {
				update_status,
				update_ring,
			}),
			background,
		});
		sys.clone().register_handler(rpc_server, rpc_path);
		sys
	}

	fn register_handler(self: Arc<Self>, rpc_server: &mut RpcServer, path: String) {
		rpc_server.add_handler::<Message, _, _>(path, move |msg, addr| {
			let self2 = self.clone();
			async move {
				match msg {
					Message::Ping(ping) => self2.handle_ping(&addr, &ping).await,

					Message::PullStatus => Ok(self2.handle_pull_status()),
					Message::PullConfig => Ok(self2.handle_pull_config()),
					Message::AdvertiseNodesUp(adv) => self2.handle_advertise_nodes_up(&adv).await,
					Message::AdvertiseConfig(adv) => self2.handle_advertise_config(&adv).await,

					_ => Err(Error::BadRpc("Unexpected RPC message".to_string())),
				}
			}
		});
	}

	/// Get an RPC client
	pub fn rpc_client<M: RpcMessage + 'static>(self: &Arc<Self>, path: &str) -> Arc<RpcClient<M>> {
		RpcClient::new(
			RpcAddrClient::new(self.rpc_http_client.clone(), path.to_string()),
			self.background.clone(),
			self.status.clone(),
		)
	}

	/// Save network configuration to disc
	async fn save_network_config(self: Arc<Self>) -> Result<(), Error> {
		let ring = self.ring.borrow().clone();
		self.persist_config
			.save_async(&ring.config)
			.await
			.expect("Cannot save current cluster configuration");
		Ok(())
	}

	fn make_ping(&self) -> Message {
		let status = self.status.borrow().clone();
		let ring = self.ring.borrow().clone();
		Message::Ping(PingMessage {
			id: self.id,
			rpc_port: self.rpc_local_port,
			status_hash: status.hash,
			config_version: ring.config.version,
			state_info: self.state_info.clone(),
		})
	}

	async fn broadcast(self: Arc<Self>, msg: Message, timeout: Duration) {
		let status = self.status.borrow().clone();
		let to = status
			.nodes
			.keys()
			.filter(|x| **x != self.id)
			.cloned()
			.collect::<Vec<_>>();
		self.rpc_client.call_many(&to[..], msg, timeout).await;
	}

	/// Perform bootstraping, starting the ping loop
	pub async fn bootstrap(
		self: Arc<Self>,
		peers: Vec<SocketAddr>,
		consul_host: Option<String>,
		consul_service_name: Option<String>,
	) {
		let self2 = self.clone();
		self.background
			.spawn_worker("discovery loop".to_string(), |stop_signal| {
				self2.discovery_loop(peers, consul_host, consul_service_name, stop_signal)
			});

		let self2 = self.clone();
		self.background
			.spawn_worker("ping loop".to_string(), |stop_signal| {
				self2.ping_loop(stop_signal)
			});
	}

	async fn ping_nodes(self: Arc<Self>, peers: Vec<(SocketAddr, Option<Uuid>)>) {
		let ping_msg = self.make_ping();
		let ping_resps = join_all(peers.iter().map(|(addr, id_option)| {
			let sys = self.clone();
			let ping_msg_ref = &ping_msg;
			async move {
				(
					id_option,
					addr,
					sys.rpc_client
						.by_addr()
						.call(&addr, ping_msg_ref, PING_TIMEOUT)
						.await,
				)
			}
		}))
		.await;

		let update_locked = self.update_lock.lock().await;
		let mut status: Status = self.status.borrow().as_ref().clone();
		let ring = self.ring.borrow().clone();

		let mut has_changes = false;
		let mut to_advertise = vec![];

		for (id_option, addr, ping_resp) in ping_resps {
			if let Ok(Ok(Message::Ping(info))) = ping_resp {
				let is_new = status.handle_ping(addr.ip(), &info);
				if is_new {
					has_changes = true;
					to_advertise.push(AdvertisedNode {
						id: info.id,
						addr: *addr,
						is_up: true,
						last_seen: now_msec(),
						state_info: info.state_info.clone(),
					});
				}
				if is_new || status.hash != info.status_hash {
					self.background
						.spawn_cancellable(self.clone().pull_status(info.id).map(Ok));
				}
				if is_new || ring.config.version < info.config_version {
					self.background
						.spawn_cancellable(self.clone().pull_config(info.id).map(Ok));
				}
			} else if let Some(id) = id_option {
				if let Some(st) = status.nodes.get_mut(id) {
					// we need to increment failure counter as call was done using by_addr so the
					// counter was not auto-incremented
					st.num_failures.fetch_add(1, Ordering::SeqCst);
					if !st.is_up() {
						warn!("Node {:?} seems to be down.", id);
						if !ring.config.members.contains_key(id) {
							info!("Removing node {:?} from status (not in config and not responding to pings anymore)", id);
							status.nodes.remove(&id);
							has_changes = true;
						}
					}
				}
			}
		}
		if has_changes {
			status.recalculate_hash();
		}
		self.update_status(&update_locked, status).await;
		drop(update_locked);

		if !to_advertise.is_empty() {
			self.broadcast(Message::AdvertiseNodesUp(to_advertise), PING_TIMEOUT)
				.await;
		}
	}

	async fn handle_ping(
		self: Arc<Self>,
		from: &SocketAddr,
		ping: &PingMessage,
	) -> Result<Message, Error> {
		let update_locked = self.update_lock.lock().await;
		let mut status: Status = self.status.borrow().as_ref().clone();

		let is_new = status.handle_ping(from.ip(), ping);
		if is_new {
			status.recalculate_hash();
		}
		let status_hash = status.hash;
		let config_version = self.ring.borrow().config.version;

		self.update_status(&update_locked, status).await;
		drop(update_locked);

		if is_new || status_hash != ping.status_hash {
			self.background
				.spawn_cancellable(self.clone().pull_status(ping.id).map(Ok));
		}
		if is_new || config_version < ping.config_version {
			self.background
				.spawn_cancellable(self.clone().pull_config(ping.id).map(Ok));
		}

		Ok(self.make_ping())
	}

	fn handle_pull_status(&self) -> Message {
		Message::AdvertiseNodesUp(self.status.borrow().to_serializable_membership(self))
	}

	fn handle_pull_config(&self) -> Message {
		let ring = self.ring.borrow().clone();
		Message::AdvertiseConfig(ring.config.clone())
	}

	async fn handle_advertise_nodes_up(
		self: Arc<Self>,
		adv: &[AdvertisedNode],
	) -> Result<Message, Error> {
		let mut to_ping = vec![];

		let update_lock = self.update_lock.lock().await;
		let mut status: Status = self.status.borrow().as_ref().clone();
		let mut has_changed = false;
		let mut max_replication_factor = 0;

		for node in adv.iter() {
			if node.id == self.id {
				// learn our own ip address
				let self_addr = SocketAddr::new(node.addr.ip(), self.rpc_local_port);
				let old_self = status.nodes.insert(
					node.id,
					Arc::new(StatusEntry {
						addr: self_addr,
						last_seen: now_msec(),
						num_failures: AtomicUsize::from(0),
						state_info: self.state_info.clone(),
					}),
				);
				has_changed = match old_self {
					None => true,
					Some(x) => x.addr != self_addr,
				};
			} else {
				let ping_them = match status.nodes.get(&node.id) {
					// Case 1: new node
					None => true,
					// Case 2: the node might have changed address
					Some(our_node) => node.is_up && !our_node.is_up() && our_node.addr != node.addr,
				};
				max_replication_factor = std::cmp::max(
					max_replication_factor,
					node.state_info.replication_factor.unwrap_or_default(),
				);
				if ping_them {
					to_ping.push((node.addr, Some(node.id)));
				}
			}
		}

		if self.replication_factor < max_replication_factor {
			error!("Some node have a higher replication factor ({}) than this one ({}). This is not supported and might lead to bugs", 
					max_replication_factor,
					self.replication_factor);
			std::process::exit(1);
		}
		if has_changed {
			status.recalculate_hash();
		}
		self.update_status(&update_lock, status).await;
		drop(update_lock);

		if !to_ping.is_empty() {
			self.background
				.spawn_cancellable(self.clone().ping_nodes(to_ping).map(Ok));
		}

		Ok(Message::Ok)
	}

	async fn handle_advertise_config(
		self: Arc<Self>,
		adv: &NetworkConfig,
	) -> Result<Message, Error> {
		let update_lock = self.update_lock.lock().await;
		let ring: Arc<Ring> = self.ring.borrow().clone();

		if adv.version > ring.config.version {
			let ring = Ring::new(adv.clone(), self.replication_factor);
			update_lock.update_ring.send(Arc::new(ring))?;
			drop(update_lock);

			self.background.spawn_cancellable(
				self.clone()
					.broadcast(Message::AdvertiseConfig(adv.clone()), PING_TIMEOUT)
					.map(Ok),
			);
			self.background.spawn(self.clone().save_network_config());
		}

		Ok(Message::Ok)
	}

	async fn ping_loop(self: Arc<Self>, mut stop_signal: watch::Receiver<bool>) {
		while !*stop_signal.borrow() {
			let restart_at = tokio::time::sleep(PING_INTERVAL);

			let status = self.status.borrow().clone();
			let ping_addrs = status
				.nodes
				.iter()
				.filter(|(id, _)| **id != self.id)
				.map(|(id, status)| (status.addr, Some(*id)))
				.collect::<Vec<_>>();

			self.clone().ping_nodes(ping_addrs).await;

			select! {
				_ = restart_at.fuse() => {},
				_ = stop_signal.changed().fuse() => {},
			}
		}
	}

	async fn discovery_loop(
		self: Arc<Self>,
		bootstrap_peers: Vec<SocketAddr>,
		consul_host: Option<String>,
		consul_service_name: Option<String>,
		mut stop_signal: watch::Receiver<bool>,
	) {
		let consul_config = match (consul_host, consul_service_name) {
			(Some(ch), Some(csn)) => Some((ch, csn)),
			_ => None,
		};

		while !*stop_signal.borrow() {
			let not_configured = self.ring.borrow().config.members.is_empty();
			let no_peers = self.status.borrow().nodes.len() < 3;
			let bad_peers = self
				.status
				.borrow()
				.nodes
				.iter()
				.filter(|(_, v)| v.is_up())
				.count() != self.ring.borrow().config.members.len();

			if not_configured || no_peers || bad_peers {
				info!("Doing a bootstrap/discovery step (not_configured: {}, no_peers: {}, bad_peers: {})", not_configured, no_peers, bad_peers);

				let mut ping_list = bootstrap_peers
					.iter()
					.map(|ip| (*ip, None))
					.collect::<Vec<_>>();

				if let Ok(peers) = self.persist_status.load_async().await {
					ping_list.extend(peers.iter().map(|x| (x.addr, Some(x.id))));
				}

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

				self.clone().ping_nodes(ping_list).await;
			}

			let restart_at = tokio::time::sleep(DISCOVERY_INTERVAL);
			select! {
				_ = restart_at.fuse() => {},
				_ = stop_signal.changed().fuse() => {},
			}
		}
	}

	// for some reason fixing this is causing compilation error, see https://github.com/rust-lang/rust-clippy/issues/7052
	#[allow(clippy::manual_async_fn)]
	fn pull_status(
		self: Arc<Self>,
		peer: Uuid,
	) -> impl futures::future::Future<Output = ()> + Send + 'static {
		async move {
			let resp = self
				.rpc_client
				.call(peer, Message::PullStatus, PING_TIMEOUT)
				.await;
			if let Ok(Message::AdvertiseNodesUp(nodes)) = resp {
				let _: Result<_, _> = self.handle_advertise_nodes_up(&nodes).await;
			}
		}
	}

	async fn pull_config(self: Arc<Self>, peer: Uuid) {
		let resp = self
			.rpc_client
			.call(peer, Message::PullConfig, PING_TIMEOUT)
			.await;
		if let Ok(Message::AdvertiseConfig(config)) = resp {
			let _: Result<_, _> = self.handle_advertise_config(&config).await;
		}
	}

	async fn update_status(self: &Arc<Self>, updaters: &Updaters, status: Status) {
		if status.hash != self.status.borrow().hash {
			let mut list = status.to_serializable_membership(&self);

			// Combine with old peer list to make sure no peer is lost
			if let Ok(old_list) = self.persist_status.load_async().await {
				for pp in old_list {
					if !list.iter().any(|np| pp.id == np.id) {
						list.push(pp);
					}
				}
			}

			if !list.is_empty() {
				info!("Persisting new peer list ({} peers)", list.len());
				self.persist_status
					.save_async(&list)
					.await
					.expect("Unable to persist peer list");
			}
		}

		updaters
			.update_status
			.send(Arc::new(status))
			.expect("Could not update internal membership status");
	}
}
