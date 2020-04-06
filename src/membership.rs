use std::sync::Arc;
use std::collections::HashMap;
use std::time::Duration;
use std::net::{IpAddr, SocketAddr};

use futures::future::join_all;
use futures::stream::StreamExt;
use hyper::client::Client;
use tokio::sync::RwLock;
use sha2::{Sha256, Digest};

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
	pub status_hash: Hash,

	pub config: HashMap<UUID, NodeConfig>,
	pub config_version: u64,
}

impl Members {
	fn handle_ping(&mut self, ip: IpAddr, info: &PingMessage) {
		match self.present.binary_search(&info.id) {
			Ok(pos) => {}
			Err(pos) => self.present.insert(pos, info.id.clone()),
		}
		self.status.insert(info.id.clone(),
			NodeStatus{
				addr: SocketAddr::new(ip, info.rpc_port),
				remaining_ping_attempts: MAX_FAILED_PINGS,
			});
	}

	fn recalculate_status_hash(&mut self) {
		let mut hasher = Sha256::new();
		for node in self.present.iter() {
			if let Some(status) = self.status.get(node) {
				hasher.input(format!("{} {}\n", hex::encode(node), status.addr));
			}
		}
		self.status_hash.copy_from_slice(&hasher.result()[..]);
	}
}

pub struct NodeStatus {
	pub addr: SocketAddr,
	pub remaining_ping_attempts: usize,
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
				status_hash: [0u8; 32],
				config: HashMap::new(),
				config_version: 0,
			}),
		}
	}

	pub async fn make_ping(&self) -> Message {
		Message::Ping(PingMessage{
			id: self.id,
			rpc_port: self.config.rpc_port,
			present_hash: self.members.read().await.status_hash.clone(),
			config_version: 0,
		})
	}

	pub async fn broadcast(&self) -> Vec<UUID> {
		self.members.read().await.present.clone()
	}

	pub async fn bootstrap(self: Arc<Self>) {
		let ping_msg = self.make_ping().await;
		let ping_resps = join_all(
			self.config.bootstrap_peers.iter().cloned()
			.map(|to| {
				let sys = self.clone();
				let ping_msg_ref = &ping_msg;
				async move {
					(to.clone(), rpc_call_addr(sys, &to, ping_msg_ref, PING_TIMEOUT).await)
				}
			})).await;
		
		let mut members = self.members.write().await;
		for (addr, ping_resp) in ping_resps {
			if let Ok(Message::Ping(info)) = ping_resp {
				members.handle_ping(addr.ip(), &info);

			}
		}
		members.recalculate_status_hash();
		drop(members);

		let resps = rpc_call_many_addr(self.clone(),
									   &self.config.bootstrap_peers,
									   &ping_msg,
									   None,
									   PING_TIMEOUT).await;

		unimplemented!() //TODO
	}

	pub async fn handle_ping(self: Arc<Self>,
							 from: &SocketAddr,
							 ping: &PingMessage)
		-> Result<Message, Error> 
	{
		let mut members = self.members.write().await;
		members.handle_ping(from.ip(), ping);
		members.recalculate_status_hash();
		drop(members);

		Ok(self.make_ping().await)
	}

	pub async fn handle_advertise_node(self: Arc<Self>,
									   ping: &AdvertiseNodeMessage)
		 -> Result<Message, Error>
	{
		unimplemented!() //TODO
	}
}
