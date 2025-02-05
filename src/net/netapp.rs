use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, RwLock};

use log::{debug, error, info, trace, warn};

use arc_swap::ArcSwapOption;

use serde::{Deserialize, Serialize};
use sodiumoxide::crypto::auth;
use sodiumoxide::crypto::sign::ed25519;

use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt;
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::select;
use tokio::sync::{mpsc, watch};

use crate::client::*;
use crate::endpoint::*;
use crate::error::*;
use crate::message::*;
use crate::server::*;

/// A node's identifier, which is also its public cryptographic key
pub type NodeID = sodiumoxide::crypto::sign::ed25519::PublicKey;
/// A node's secret key
pub type NodeKey = sodiumoxide::crypto::sign::ed25519::SecretKey;
/// A network key
pub type NetworkKey = sodiumoxide::crypto::auth::Key;

/// Tag which is exchanged between client and server upon connection establishment
/// to check that they are running compatible versions of Netapp,
/// composed of 8 bytes for Netapp version and 8 bytes for client version
pub(crate) type VersionTag = [u8; 16];

/// Value of garage_net version used in the version tag
/// We are no longer using prefix `netapp` as garage_net is forked from the netapp crate.
/// Since Garage v1.0, we have replaced the prefix by `grgnet` (shorthand for garage_net).
pub(crate) const NETAPP_VERSION_TAG: u64 = 0x6772676e65740010; // grgnet 0x0010 (1.0)

/// HelloMessage is sent by the client on a Netapp connection to indicate
/// that they are also a server and ready to receive incoming connections
/// at the specified address and port. If the client doesn't know their
/// public address, they don't need to specify it and we look at the
/// remote address of the socket is used instead.
#[derive(Serialize, Deserialize, Debug)]
pub(crate) struct HelloMessage {
	pub server_addr: Option<IpAddr>,
	pub server_port: u16,
}

impl Message for HelloMessage {
	type Response = ();
}

type OnConnectHandler = Box<dyn Fn(NodeID, SocketAddr, bool) + Send + Sync>;
type OnDisconnectHandler = Box<dyn Fn(NodeID, bool) + Send + Sync>;

/// NetApp is the main class that handles incoming and outgoing connections.
///
/// NetApp can be used in a stand-alone fashion or together with a peering strategy.
/// If using it alone, you will want to set `on_connect` and `on_disconnect` events
/// in order to manage information about the current peer list.
pub struct NetApp {
	bind_outgoing_to: Option<IpAddr>,
	listen_params: ArcSwapOption<ListenParams>,

	/// Version tag, 8 bytes for netapp version, 8 bytes for app version
	pub version_tag: VersionTag,
	/// Network secret key
	pub netid: auth::Key,
	/// Our peer ID
	pub id: NodeID,
	/// Private key associated with our peer ID
	pub privkey: ed25519::SecretKey,

	pub(crate) server_conns: RwLock<HashMap<NodeID, Arc<ServerConn>>>,
	pub(crate) client_conns: RwLock<HashMap<NodeID, Arc<ClientConn>>>,

	pub(crate) endpoints: RwLock<HashMap<String, DynEndpoint>>,
	hello_endpoint: ArcSwapOption<Endpoint<HelloMessage, NetApp>>,

	on_connected_handler: ArcSwapOption<OnConnectHandler>,
	on_disconnected_handler: ArcSwapOption<OnDisconnectHandler>,
}

struct ListenParams {
	listen_addr: SocketAddr,
	public_addr: Option<SocketAddr>,
}

impl NetApp {
	/// Creates a new instance of NetApp, which can serve either as a full p2p node,
	/// or just as a passive client. To upgrade to a full p2p node, spawn a listener
	/// using `.listen()`
	///
	/// Our Peer ID is the public key associated to the secret key given here.
	pub fn new(
		app_version_tag: u64,
		netid: auth::Key,
		privkey: ed25519::SecretKey,
		bind_outgoing_to: Option<IpAddr>,
	) -> Arc<Self> {
		let mut version_tag = [0u8; 16];
		version_tag[0..8].copy_from_slice(&u64::to_be_bytes(NETAPP_VERSION_TAG)[..]);
		version_tag[8..16].copy_from_slice(&u64::to_be_bytes(app_version_tag)[..]);

		let id = privkey.public_key();
		let netapp = Arc::new(Self {
			bind_outgoing_to,
			listen_params: ArcSwapOption::new(None),
			version_tag,
			netid,
			id,
			privkey,
			server_conns: RwLock::new(HashMap::new()),
			client_conns: RwLock::new(HashMap::new()),
			endpoints: RwLock::new(HashMap::new()),
			hello_endpoint: ArcSwapOption::new(None),
			on_connected_handler: ArcSwapOption::new(None),
			on_disconnected_handler: ArcSwapOption::new(None),
		});

		netapp
			.hello_endpoint
			.swap(Some(netapp.endpoint("garage_net/netapp.rs/Hello".into())));
		netapp
			.hello_endpoint
			.load_full()
			.unwrap()
			.set_handler(netapp.clone());

		netapp
	}

	/// Set the handler to be called when a new connection (incoming or outgoing) has
	/// been successfully established. Do not set this if using a peering strategy,
	/// as the peering strategy will need to set this itself.
	pub fn on_connected<F>(&self, handler: F)
	where
		F: Fn(NodeID, SocketAddr, bool) + Sized + Send + Sync + 'static,
	{
		self.on_connected_handler
			.store(Some(Arc::new(Box::new(handler))));
	}

	/// Set the handler to be called when an existing connection (incoming or outgoing) has
	/// been closed by either party. Do not set this if using a peering strategy,
	/// as the peering strategy will need to set this itself.
	pub fn on_disconnected<F>(&self, handler: F)
	where
		F: Fn(NodeID, bool) + Sized + Send + Sync + 'static,
	{
		self.on_disconnected_handler
			.store(Some(Arc::new(Box::new(handler))));
	}

	/// Create a new endpoint with path `path`,
	/// that handles messages of type `M`.
	/// `H` is the type of the object that should handle requests
	/// to this endpoint on the local node. If you don't want
	/// to handle request on the local node (e.g. if this node
	/// is only a client in the network), define the type `H`
	/// to be `()`.
	/// This function will panic if the endpoint has already been
	/// created.
	pub fn endpoint<M, H>(self: &Arc<Self>, path: String) -> Arc<Endpoint<M, H>>
	where
		M: Message + 'static,
		H: StreamingEndpointHandler<M> + 'static,
	{
		let endpoint = Arc::new(Endpoint::<M, H>::new(self.clone(), path.clone()));
		let endpoint_arc = EndpointArc(endpoint.clone());
		if self
			.endpoints
			.write()
			.unwrap()
			.insert(path.clone(), Box::new(endpoint_arc))
			.is_some()
		{
			panic!("Redefining endpoint: {}", path);
		};
		endpoint
	}

	/// Main listening process for our app. This future runs during the whole
	/// run time of our application.
	/// If this is not called, the NetApp instance remains a passive client.
	pub async fn listen(
		self: Arc<Self>,
		listen_addr: SocketAddr,
		public_addr: Option<SocketAddr>,
		mut must_exit: watch::Receiver<bool>,
	) {
		let listen_params = ListenParams {
			listen_addr,
			public_addr,
		};
		if self
			.listen_params
			.swap(Some(Arc::new(listen_params)))
			.is_some()
		{
			error!("Trying to listen on NetApp but we're already listening!");
		}

		let listener = TcpListener::bind(listen_addr).await.unwrap();
		info!("Listening on {}", listen_addr);

		let (conn_in, mut conn_out) = mpsc::unbounded_channel();
		let connection_collector = tokio::spawn(async move {
			let mut collection = FuturesUnordered::new();
			loop {
				if collection.is_empty() {
					match conn_out.recv().await {
						Some(f) => collection.push(f),
						None => break,
					}
				} else {
					select! {
						new_fut = conn_out.recv() => {
							match new_fut {
								Some(f) => collection.push(f),
								None => break,
							}
						}
						result = collection.next() => {
							trace!("Collected connection: {:?}", result);
						}
					}
				}
			}
			debug!("Collecting last open server connections.");
			while let Some(conn_res) = collection.next().await {
				trace!("Collected connection: {:?}", conn_res);
			}
			debug!("No more server connections to collect");
		});

		while !*must_exit.borrow_and_update() {
			let (socket, peer_addr) = select! {
				sockres = listener.accept() => {
					match sockres {
						Ok(x) => x,
						Err(e) => {
							warn!("Error in listener.accept: {}", e);
							continue;
						}
					}
				},
				_ = must_exit.changed() => continue,
			};

			info!(
				"Incoming connection from {}, negotiating handshake...",
				peer_addr
			);
			let self2 = self.clone();
			let must_exit2 = must_exit.clone();
			conn_in
				.send(tokio::spawn(async move {
					ServerConn::run(self2, socket, must_exit2)
						.await
						.log_err("ServerConn::run");
				}))
				.log_err("Failed to send connection to connection collector");
		}

		drop(conn_in);

		connection_collector
			.await
			.log_err("Failed to await for connection collector");
	}

	/// Drop all endpoint handlers, as well as handlers for connection/disconnection
	/// events. (This disables the peering strategy)
	///
	/// Use this when terminating to break reference cycles
	pub fn drop_all_handlers(&self) {
		for (_, endpoint) in self.endpoints.read().unwrap().iter() {
			endpoint.drop_handler();
		}
		self.on_connected_handler.store(None);
		self.on_disconnected_handler.store(None);
	}

	/// Attempt to connect to a peer, given by its ip:port and its public key.
	/// The public key will be checked during the secret handshake process.
	/// This function returns once the connection has been established and a
	/// successful handshake was made. At this point we can send messages to
	/// the other node with `Netapp::request`
	pub async fn try_connect(self: Arc<Self>, ip: SocketAddr, id: NodeID) -> Result<(), Error> {
		// Don't connect to ourself, we don't care
		if id == self.id {
			return Ok(());
		}

		// Don't connect if already connected
		if self.client_conns.read().unwrap().contains_key(&id) {
			return Ok(());
		}

		let stream = match self.bind_outgoing_to {
			Some(addr) => {
				let socket = if addr.is_ipv4() {
					TcpSocket::new_v4()?
				} else {
					TcpSocket::new_v6()?
				};
				socket.bind(SocketAddr::new(addr, 0))?;
				socket.connect(ip).await?
			}
			None => TcpStream::connect(ip).await?,
		};
		info!("Connected to {}, negotiating handshake...", ip);
		ClientConn::init(self, stream, id).await?;
		Ok(())
	}

	/// Close the outgoing connection we have to a node specified by its public key,
	/// if such a connection is currently open.
	pub fn disconnect(self: &Arc<Self>, id: &NodeID) {
		let conn = self.client_conns.write().unwrap().remove(id);

		// If id is ourself, we're not supposed to have a connection open
		if *id == self.id {
			// sanity check
			assert!(conn.is_none(), "had a connection to local node");
			return;
		}

		if let Some(c) = conn {
			debug!(
				"Closing connection to {} ({})",
				hex::encode(&c.peer_id[..8]),
				c.remote_addr
			);
			c.close();

			// call on_disconnected_handler immediately, since the connection was removed
			let id = *id;
			let self2 = self.clone();
			tokio::spawn(async move {
				if let Some(h) = self2.on_disconnected_handler.load().as_ref() {
					h(id, false);
				}
			});
		}
	}

	// Called from conn.rs when an incoming connection is successfully established
	// Registers the connection in our list of connections
	// Do not yet call the on_connected handler, because we don't know if the remote
	// has an actual IP address and port we can call them back on.
	// We will know this when they send a Hello message, which is handled below.
	pub(crate) fn connected_as_server(&self, id: NodeID, conn: Arc<ServerConn>) {
		info!(
			"Accepted connection from {} at {}",
			hex::encode(&id[..8]),
			conn.remote_addr
		);

		self.server_conns.write().unwrap().insert(id, conn);
	}

	// Handle hello message from a client. This message is used for them to tell us
	// that they are listening on a certain port number on which we can call them back.
	// At this point we know they are a full network member, and not just a client,
	// and we call the on_connected handler so that the peering strategy knows
	// we have a new potential peer

	// Called from conn.rs when an incoming connection is closed.
	// We deregister the connection from server_conns and call the
	// handler registered by on_disconnected
	pub(crate) fn disconnected_as_server(&self, id: &NodeID, conn: Arc<ServerConn>) {
		info!("Connection from {} closed", hex::encode(&id[..8]));

		let mut conn_list = self.server_conns.write().unwrap();
		if let Some(c) = conn_list.get(id) {
			if Arc::ptr_eq(c, &conn) {
				conn_list.remove(id);
				drop(conn_list);

				if let Some(h) = self.on_disconnected_handler.load().as_ref() {
					h(conn.peer_id, true);
				}
			}
		}
	}

	// Called from conn.rs when an outgoinc connection is successfully established.
	// The connection is registered in self.client_conns, and the
	// on_connected handler is called.
	//
	// Since we are ourself listening, we send them a Hello message so that
	// they know on which port to call us back. (TODO: don't do this if we are
	// just a simple client and not a full p2p node)
	pub(crate) fn connected_as_client(&self, id: NodeID, conn: Arc<ClientConn>) {
		info!("Connection established to {}", hex::encode(&id[..8]));

		{
			let old_c_opt = self.client_conns.write().unwrap().insert(id, conn.clone());
			if let Some(old_c) = old_c_opt {
				tokio::spawn(async move { old_c.close() });
			}
		}

		if let Some(h) = self.on_connected_handler.load().as_ref() {
			h(conn.peer_id, conn.remote_addr, false);
		}

		if let Some(lp) = self.listen_params.load_full() {
			let server_addr = lp.public_addr.map(|x| x.ip());
			let server_port = lp
				.public_addr
				.map(|x| x.port())
				.unwrap_or(lp.listen_addr.port());
			let hello_endpoint = self.hello_endpoint.load_full().unwrap();
			tokio::spawn(async move {
				hello_endpoint
					.call(
						&conn.peer_id,
						HelloMessage {
							server_addr,
							server_port,
						},
						PRIO_NORMAL,
					)
					.await
					.map(|_| ())
					.log_err("Sending hello message");
			});
		}
	}

	// Called from conn.rs when an outgoinc connection is closed.
	// The connection is removed from conn_list, and the on_disconnected handler
	// is called.
	pub(crate) fn disconnected_as_client(&self, id: &NodeID, conn: Arc<ClientConn>) {
		info!("Connection to {} closed", hex::encode(&id[..8]));
		let mut conn_list = self.client_conns.write().unwrap();
		if let Some(c) = conn_list.get(id) {
			if Arc::ptr_eq(c, &conn) {
				conn_list.remove(id);
				drop(conn_list);

				if let Some(h) = self.on_disconnected_handler.load().as_ref() {
					h(conn.peer_id, false);
				}
			}
		}
		// else case: happens if connection was removed in .disconnect()
		// in which case on_disconnected_handler was already called
	}
}

impl EndpointHandler<HelloMessage> for NetApp {
	async fn handle(self: &Arc<Self>, msg: &HelloMessage, from: NodeID) {
		debug!("Hello from {:?}: {:?}", hex::encode(&from[..8]), msg);
		if let Some(h) = self.on_connected_handler.load().as_ref() {
			if let Some(c) = self.server_conns.read().unwrap().get(&from) {
				let remote_ip = msg.server_addr.unwrap_or_else(|| c.remote_addr.ip());
				let remote_addr = SocketAddr::new(remote_ip, msg.server_port);
				h(from, remote_addr, true);
			}
		}
	}
}
