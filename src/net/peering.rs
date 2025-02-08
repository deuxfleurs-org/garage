use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{self, AtomicU64};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;
use log::{debug, info, trace, warn};
use serde::{Deserialize, Serialize};

use tokio::select;
use tokio::sync::watch;

use sodiumoxide::crypto::hash;

use crate::endpoint::*;
use crate::error::*;
use crate::netapp::*;

use crate::message::*;
use crate::NodeID;

const CONN_RETRY_INTERVAL: Duration = Duration::from_secs(30);
const CONN_MAX_RETRIES: usize = 10;
const PING_INTERVAL: Duration = Duration::from_secs(15);
const LOOP_DELAY: Duration = Duration::from_secs(1);
const FAILED_PING_THRESHOLD: usize = 4;

const DEFAULT_PING_TIMEOUT_MILLIS: u64 = 10_000;

// -- Protocol messages --

#[derive(Serialize, Deserialize)]
struct PingMessage {
	pub id: u64,
	pub peer_list_hash: hash::Digest,
}

impl Message for PingMessage {
	type Response = PingMessage;
}

#[derive(Serialize, Deserialize)]
struct PeerListMessage {
	pub list: Vec<(NodeID, SocketAddr)>,
}

impl Message for PeerListMessage {
	type Response = PeerListMessage;
}

// -- Algorithm data structures --

#[derive(Debug)]
struct PeerInfoInternal {
	// known_addrs contains all of the addresses everyone gave us
	known_addrs: Vec<SocketAddr>,

	state: PeerConnState,
	last_send_ping: Option<Instant>,
	last_seen: Option<Instant>,
	ping: VecDeque<Duration>,
	failed_pings: usize,
}

impl PeerInfoInternal {
	fn new(state: PeerConnState, known_addr: Option<SocketAddr>) -> Self {
		Self {
			known_addrs: known_addr.map(|x| vec![x]).unwrap_or_default(),
			state,
			last_send_ping: None,
			last_seen: None,
			ping: VecDeque::new(),
			failed_pings: 0,
		}
	}
	fn add_addr(&mut self, addr: SocketAddr) -> bool {
		if !self.known_addrs.contains(&addr) {
			self.known_addrs.push(addr);
			// If we are learning a new address for this node,
			// we want to retry connecting
			self.state = match self.state {
				PeerConnState::Trying(_) => PeerConnState::Trying(0),
				PeerConnState::Waiting(_, _) | PeerConnState::Abandonned => {
					PeerConnState::Waiting(0, Instant::now())
				}
				x @ (PeerConnState::Ourself | PeerConnState::Connected { .. }) => x,
			};
			true
		} else {
			false
		}
	}
}

/// Information that the full mesh peering strategy can return about the peers it knows of
#[derive(Copy, Clone, Debug)]
pub struct PeerInfo {
	/// The node's identifier (its public key)
	pub id: NodeID,
	/// The current status of our connection to this node
	pub state: PeerConnState,
	/// The last time at which the node was seen
	pub last_seen: Option<Instant>,
	/// The average ping to this node  on recent observations (if at least one ping value is known)
	pub avg_ping: Option<Duration>,
	/// The maximum observed ping to this node on recent observations (if at least one
	/// ping value is known)
	pub max_ping: Option<Duration>,
	/// The median ping to this node on recent observations (if at least one ping value
	/// is known)
	pub med_ping: Option<Duration>,
}

impl PeerInfo {
	/// Returns true if we can currently send requests to this peer
	pub fn is_up(&self) -> bool {
		self.state.is_up()
	}
}

/// PeerConnState: possible states for our tentative connections to given peer
/// This structure is only interested in recording connection info for outgoing
/// TCP connections
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum PeerConnState {
	/// This entry represents ourself (the local node)
	Ourself,

	/// We currently have a connection to this peer
	Connected { addr: SocketAddr },

	/// Our next connection tentative (the nth, where n is the first value of the tuple)
	/// will be at given Instant
	Waiting(usize, Instant),

	/// A connection tentative is in progress (the nth, where n is the value stored)
	Trying(usize),

	/// We abandoned trying to connect to this peer (too many failed attempts)
	Abandonned,
}

impl PeerConnState {
	/// Returns true if we can currently send requests to this peer
	pub fn is_up(&self) -> bool {
		matches!(self, Self::Ourself | Self::Connected { .. })
	}
}

struct KnownHosts {
	list: HashMap<NodeID, PeerInfoInternal>,
	hash: hash::Digest,
}

impl KnownHosts {
	fn new() -> Self {
		let list = HashMap::new();
		let mut ret = Self {
			list,
			hash: hash::Digest::from_slice(&[0u8; 64][..]).unwrap(),
		};
		ret.update_hash();
		ret
	}
	fn update_hash(&mut self) {
		// The hash is a value that is exchanged between nodes when they ping one
		// another.  Nodes compare their known hosts hash to know if they are connected
		// to the same set of nodes. If the hashes differ, they are connected to
		// different nodes and they trigger an exchange of the full list of active
		// connections.  The hash value only represents the set of node IDs and not
		// their actual socket addresses, because nodes can be connected via different
		// addresses and that shouldn't necessarily trigger a full peer exchange.
		let mut list = self
			.list
			.iter()
			.filter(|(_, peer)| peer.state.is_up())
			.map(|(id, _)| *id)
			.collect::<Vec<_>>();
		list.sort();
		let mut hash_state = hash::State::new();
		for id in list {
			hash_state.update(&id[..]);
		}
		self.hash = hash_state.finalize();
	}
	fn connected_peers_vec(&self) -> Vec<(NodeID, SocketAddr)> {
		self.list
			.iter()
			.filter_map(|(id, peer)| match peer.state {
				PeerConnState::Connected { addr } => Some((*id, addr)),
				_ => None,
			})
			.collect::<Vec<_>>()
	}
}

/// A "Full Mesh" peering strategy is a peering strategy that tries
/// to establish and maintain a direct connection with all of the
/// known nodes in the network.
pub struct PeeringManager {
	netapp: Arc<NetApp>,
	known_hosts: RwLock<KnownHosts>,
	public_peer_list: ArcSwap<Vec<PeerInfo>>,

	next_ping_id: AtomicU64,
	ping_endpoint: Arc<Endpoint<PingMessage, Self>>,
	peer_list_endpoint: Arc<Endpoint<PeerListMessage, Self>>,

	ping_timeout_millis: AtomicU64,
}

impl PeeringManager {
	/// Create a new Full Mesh peering strategy.
	/// The strategy will not be run until `.run()` is called and awaited.
	/// Once that happens, the peering strategy will try to connect
	/// to all of the nodes specified in the bootstrap list.
	pub fn new(
		netapp: Arc<NetApp>,
		bootstrap_list: Vec<(NodeID, SocketAddr)>,
		our_addr: Option<SocketAddr>,
	) -> Arc<Self> {
		let mut known_hosts = KnownHosts::new();
		for (id, addr) in bootstrap_list {
			if id != netapp.id {
				known_hosts.list.insert(
					id,
					PeerInfoInternal::new(PeerConnState::Waiting(0, Instant::now()), Some(addr)),
				);
			}
		}

		known_hosts.list.insert(
			netapp.id,
			PeerInfoInternal::new(PeerConnState::Ourself, our_addr),
		);
		known_hosts.update_hash();

		let strat = Arc::new(Self {
			netapp: netapp.clone(),
			known_hosts: RwLock::new(known_hosts),
			public_peer_list: ArcSwap::new(Arc::new(Vec::new())),
			next_ping_id: AtomicU64::new(42),
			ping_endpoint: netapp.endpoint("garage_net/peering.rs/Ping".into()),
			peer_list_endpoint: netapp.endpoint("garage_net/peering.rs/PeerList".into()),
			ping_timeout_millis: DEFAULT_PING_TIMEOUT_MILLIS.into(),
		});

		strat.update_public_peer_list(&strat.known_hosts.read().unwrap());

		strat.ping_endpoint.set_handler(strat.clone());
		strat.peer_list_endpoint.set_handler(strat.clone());

		let strat2 = strat.clone();
		netapp.on_connected(move |id: NodeID, addr: SocketAddr, is_incoming: bool| {
			strat2.on_connected(id, addr, is_incoming);
		});

		let strat2 = strat.clone();
		netapp.on_disconnected(move |id: NodeID, is_incoming: bool| {
			strat2.on_disconnected(id, is_incoming);
		});

		strat
	}

	/// Run the full mesh peering strategy.
	/// This future exits when the `must_exit` watch becomes true.
	pub async fn run(self: Arc<Self>, must_exit: watch::Receiver<bool>) {
		while !*must_exit.borrow() {
			// 1. Read current state: get list of connected peers (ping them)
			let (to_ping, to_retry) = {
				let known_hosts = self.known_hosts.read().unwrap();
				trace!("known_hosts: {} peers", known_hosts.list.len());

				let mut to_ping = vec![];
				let mut to_retry = vec![];
				for (id, info) in known_hosts.list.iter() {
					trace!("{}, {:?}", hex::encode(&id[..8]), info);
					match info.state {
						PeerConnState::Connected { .. } => {
							let must_ping = match info.last_send_ping {
								None => true,
								Some(t) => Instant::now() - t > PING_INTERVAL,
							};
							if must_ping {
								to_ping.push(*id);
							}
						}
						PeerConnState::Waiting(_, t) => {
							if Instant::now() >= t {
								to_retry.push(*id);
							}
						}
						_ => (),
					}
				}
				(to_ping, to_retry)
			};

			// 2. Dispatch ping to hosts
			trace!("to_ping: {} peers", to_ping.len());
			if !to_ping.is_empty() {
				let mut known_hosts = self.known_hosts.write().unwrap();
				for id in to_ping.iter() {
					known_hosts.list.get_mut(id).unwrap().last_send_ping = Some(Instant::now());
				}
				drop(known_hosts);
				for id in to_ping {
					tokio::spawn(self.clone().ping(id));
				}
			}

			// 3. Try reconnects
			trace!("to_retry: {} peers", to_retry.len());
			if !to_retry.is_empty() {
				let mut known_hosts = self.known_hosts.write().unwrap();
				for id in to_retry {
					if let Some(h) = known_hosts.list.get_mut(&id) {
						if let PeerConnState::Waiting(i, _) = h.state {
							info!(
								"Retrying connection to {} at {} ({})",
								hex::encode(&id[..8]),
								h.known_addrs
									.iter()
									.map(|x| format!("{}", x))
									.collect::<Vec<_>>()
									.join(", "),
								i + 1
							);
							h.state = PeerConnState::Trying(i);

							let addresses = h.known_addrs.clone();
							tokio::spawn(self.clone().try_connect(id, addresses));
						}
					}
				}
				self.update_public_peer_list(&known_hosts);
			}

			// 4. Sleep before next loop iteration
			tokio::time::sleep(LOOP_DELAY).await;
		}
	}

	/// Returns a list of currently known peers in the network.
	pub fn get_peer_list(&self) -> Arc<Vec<PeerInfo>> {
		self.public_peer_list.load_full()
	}

	/// Set the timeout for ping messages, in milliseconds
	pub fn set_ping_timeout_millis(&self, timeout: u64) {
		self.ping_timeout_millis
			.store(timeout, atomic::Ordering::Relaxed);
	}

	// -- internal stuff --

	fn update_public_peer_list(&self, known_hosts: &KnownHosts) {
		let mut pub_peer_list = Vec::with_capacity(known_hosts.list.len());
		for (id, info) in known_hosts.list.iter() {
			if *id == self.netapp.id {
				// sanity check
				assert!(matches!(info.state, PeerConnState::Ourself));
			}
			let mut pings = info.ping.iter().cloned().collect::<Vec<_>>();
			pings.sort();
			if !pings.is_empty() {
				pub_peer_list.push(PeerInfo {
					id: *id,
					state: info.state,
					last_seen: info.last_seen,
					avg_ping: Some(pings.iter().sum::<Duration>().div_f64(pings.len() as f64)),
					max_ping: pings.last().cloned(),
					med_ping: Some(pings[pings.len() / 2]),
				});
			} else {
				pub_peer_list.push(PeerInfo {
					id: *id,
					state: info.state,
					last_seen: info.last_seen,
					avg_ping: None,
					max_ping: None,
					med_ping: None,
				});
			}
		}
		self.public_peer_list.store(Arc::new(pub_peer_list));
	}

	async fn ping(self: Arc<Self>, id: NodeID) {
		let peer_list_hash = self.known_hosts.read().unwrap().hash;
		let ping_id = self.next_ping_id.fetch_add(1u64, atomic::Ordering::Relaxed);
		let ping_time = Instant::now();
		let ping_timeout =
			Duration::from_millis(self.ping_timeout_millis.load(atomic::Ordering::Relaxed));
		let ping_msg = PingMessage {
			id: ping_id,
			peer_list_hash,
		};

		debug!(
			"Sending ping {} to {} at {:?}",
			ping_id,
			hex::encode(&id[..8]),
			ping_time
		);
		let ping_response = select! {
			r = self.ping_endpoint.call(&id, ping_msg, PRIO_HIGH) => r,
			_ = tokio::time::sleep(ping_timeout) => Err(Error::Message("Ping timeout".into())),
		};

		match ping_response {
			Err(e) => {
				warn!("Error pinging {}: {}", hex::encode(&id[..8]), e);
				let mut known_hosts = self.known_hosts.write().unwrap();
				if let Some(host) = known_hosts.list.get_mut(&id) {
					host.failed_pings += 1;
					if host.failed_pings > FAILED_PING_THRESHOLD {
						warn!(
							"Too many failed pings from {}, closing connection.",
							hex::encode(&id[..8])
						);
						// this will later update info in known_hosts
						// through the disconnection handler
						self.netapp.disconnect(&id);
					}
				}
			}
			Ok(ping_resp) => {
				let resp_time = Instant::now();
				debug!(
					"Got ping response from {} at {:?}",
					hex::encode(&id[..8]),
					resp_time
				);
				{
					let mut known_hosts = self.known_hosts.write().unwrap();
					if let Some(host) = known_hosts.list.get_mut(&id) {
						host.failed_pings = 0;
						host.last_seen = Some(resp_time);
						host.ping.push_back(resp_time - ping_time);
						while host.ping.len() > 10 {
							host.ping.pop_front();
						}
						self.update_public_peer_list(&known_hosts);
					}
				}
				if ping_resp.peer_list_hash != peer_list_hash {
					self.exchange_peers(&id).await;
				}
			}
		}
	}

	async fn exchange_peers(self: Arc<Self>, id: &NodeID) {
		let peer_list = self.known_hosts.read().unwrap().connected_peers_vec();
		let pex_message = PeerListMessage { list: peer_list };
		match self
			.peer_list_endpoint
			.call(id, pex_message, PRIO_BACKGROUND)
			.await
		{
			Err(e) => warn!("Error doing peer exchange: {}", e),
			Ok(resp) => {
				self.handle_peer_list(&resp.list[..]);
			}
		}
	}

	fn handle_peer_list(&self, list: &[(NodeID, SocketAddr)]) {
		let mut known_hosts = self.known_hosts.write().unwrap();

		let mut changed = false;
		for (id, addr) in list.iter() {
			if let Some(kh) = known_hosts.list.get_mut(id) {
				if kh.add_addr(*addr) {
					changed = true;
				}
			} else {
				known_hosts.list.insert(*id, self.new_peer(id, *addr));
				changed = true;
			}
		}

		if changed {
			known_hosts.update_hash();
			self.update_public_peer_list(&known_hosts);
		}
	}

	async fn try_connect(self: Arc<Self>, id: NodeID, addresses: Vec<SocketAddr>) {
		let conn_addr = {
			let mut ret = None;
			for addr in addresses.iter() {
				debug!("Trying address {} for peer {}", addr, hex::encode(&id[..8]));
				match self.netapp.clone().try_connect(*addr, id).await {
					Ok(()) => {
						ret = Some(*addr);
						break;
					}
					Err(e) => {
						debug!(
							"Error connecting to {} at {}: {}",
							hex::encode(&id[..8]),
							addr,
							e
						);
					}
				}
			}
			ret
		};

		if let Some(ok_addr) = conn_addr {
			self.on_connected(id, ok_addr, false);
		} else {
			warn!(
				"Could not connect to peer {} ({} addresses tried)",
				hex::encode(&id[..8]),
				addresses.len()
			);
			let mut known_hosts = self.known_hosts.write().unwrap();
			if let Some(host) = known_hosts.list.get_mut(&id) {
				host.state = match host.state {
					PeerConnState::Trying(i) => {
						if i >= CONN_MAX_RETRIES {
							PeerConnState::Abandonned
						} else {
							PeerConnState::Waiting(i + 1, Instant::now() + CONN_RETRY_INTERVAL)
						}
					}
					_ => PeerConnState::Waiting(0, Instant::now() + CONN_RETRY_INTERVAL),
				};
				self.update_public_peer_list(&known_hosts);
			}
		}
	}

	fn on_connected(self: &Arc<Self>, id: NodeID, addr: SocketAddr, is_incoming: bool) {
		if id == self.netapp.id {
			// sanity check
			panic!(
				"on_connected from local node, id={:?}, addr={}, incoming={}",
				id, addr, is_incoming
			);
		}

		let mut known_hosts = self.known_hosts.write().unwrap();
		if is_incoming {
			if let Some(host) = known_hosts.list.get_mut(&id) {
				host.add_addr(addr);
			} else {
				known_hosts.list.insert(id, self.new_peer(&id, addr));
			}
		} else {
			info!(
				"Successfully connected to {} at {}",
				hex::encode(&id[..8]),
				addr
			);
			if let Some(host) = known_hosts.list.get_mut(&id) {
				host.state = PeerConnState::Connected { addr };
				host.add_addr(addr);
			} else {
				known_hosts.list.insert(
					id,
					PeerInfoInternal::new(PeerConnState::Connected { addr }, Some(addr)),
				);
			}
		}
		known_hosts.update_hash();
		self.update_public_peer_list(&known_hosts);
	}

	fn on_disconnected(self: &Arc<Self>, id: NodeID, is_incoming: bool) {
		if !is_incoming {
			info!("Connection to {} was closed", hex::encode(&id[..8]));
			let mut known_hosts = self.known_hosts.write().unwrap();
			if let Some(host) = known_hosts.list.get_mut(&id) {
				host.state = PeerConnState::Waiting(0, Instant::now());
				known_hosts.update_hash();
				self.update_public_peer_list(&known_hosts);
			}
		}
	}

	fn new_peer(&self, id: &NodeID, addr: SocketAddr) -> PeerInfoInternal {
		assert!(*id != self.netapp.id);
		PeerInfoInternal::new(PeerConnState::Waiting(0, Instant::now()), Some(addr))
	}
}

impl EndpointHandler<PingMessage> for PeeringManager {
	async fn handle(self: &Arc<Self>, ping: &PingMessage, from: NodeID) -> PingMessage {
		let ping_resp = PingMessage {
			id: ping.id,
			peer_list_hash: self.known_hosts.read().unwrap().hash,
		};
		debug!("Ping from {}", hex::encode(&from[..8]));
		ping_resp
	}
}

impl EndpointHandler<PeerListMessage> for PeeringManager {
	async fn handle(
		self: &Arc<Self>,
		peer_list: &PeerListMessage,
		_from: NodeID,
	) -> PeerListMessage {
		self.handle_peer_list(&peer_list.list[..]);
		let peer_list = self.known_hosts.read().unwrap().connected_peers_vec();
		PeerListMessage { list: peer_list }
	}
}
