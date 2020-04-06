use std::sync::Arc;
use std::collections::HashMap;
use std::time::Duration;
use std::net::SocketAddr;

use hyper::client::Client;
use tokio::sync::RwLock;

use crate::Config;
use crate::error::Error;
use crate::data::*;
use crate::proto::*;
use crate::rpc::*;

const PING_INTERVAL: Duration = Duration::from_secs(10);
const PING_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_FAILED_PINGS: usize = 3;

pub struct System {
	pub config: Config,
	pub id: UUID,

	pub rpc_client: Client<hyper::client::HttpConnector, hyper::Body>,

	pub members: RwLock<Members>,
}

pub struct Members {
	pub present: Vec<UUID>,
	pub status: HashMap<UUID, NodeStatus>,

	pub config: HashMap<UUID, NodeConfig>,
	pub config_version: u64,
}

pub struct NodeStatus {
	pub addr: SocketAddr,
	remaining_ping_attempts: usize,
}

pub struct NodeConfig {
	pub n_tokens: u32,
}

impl System {
	pub fn new(config: Config, id: UUID) -> Self {
		System{
			config,
			id,
			rpc_client: Client::new(),
			members: RwLock::new(Members{
				present: Vec::new(),
				status: HashMap::new(),
				config: HashMap::new(),
				config_version: 0,
			}),
		}
	}

	pub async fn broadcast(&self) -> Vec<UUID> {
		self.members.read().await.present.clone()
	}
}

pub async fn bootstrap(system: Arc<System>) {
	rpc_call_many_addr(system.clone(),
					   &system.config.bootstrap_peers,
					   &Message::Ping(PingMessage{
						   id: system.id,
						   rpc_port: system.config.rpc_port,
						   present_hash: [0u8; 32],
						   config_version: 0,
					   }),
					   None,
					   PING_TIMEOUT).await;

	unimplemented!() //TODO
}

pub async fn handle_ping(sys: Arc<System>, from: &SocketAddr, ping: &PingMessage) -> Result<Message, Error> {
	unimplemented!() //TODO
}

pub async fn handle_advertise_node(sys: Arc<System>, ping: &AdvertiseNodeMessage) -> Result<Message, Error> {
	unimplemented!() //TODO
}
