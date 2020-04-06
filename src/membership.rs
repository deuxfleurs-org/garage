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
	pub status: HashMap<UUID, NodeStatus>,
	pub status_hash: Hash,

	pub config: NetworkConfig,
}

impl Members {
	fn handle_ping(&mut self, ip: IpAddr, info: &PingMessage) -> bool {
		self.status.insert(info.id.clone(),
			NodeStatus{
				addr: SocketAddr::new(ip, info.rpc_port),
				remaining_ping_attempts: MAX_FAILED_PINGS,
			}).is_none()
	}

	fn handle_advertise_node(&mut self, id: &UUID, addr: &SocketAddr) -> bool {
		if !self.status.contains_key(id) {
			self.status.insert(id.clone(),
				NodeStatus{
					addr: addr.clone(),
					remaining_ping_attempts: MAX_FAILED_PINGS,
				});
			true
		} else {
			false
		}
	}

	fn recalculate_status_hash(&mut self) {
		let mut nodes = self.status.iter().collect::<Vec<_>>();
		nodes.sort_by_key(|(id, _status)| *id);

		let mut hasher = Sha256::new();
		for (id, status) in nodes {
			hasher.input(format!("{} {}\n", hex::encode(id), status.addr));
		}
		self.status_hash.copy_from_slice(&hasher.result()[..]);
	}
}

pub struct NodeStatus {
	pub addr: SocketAddr,
	pub remaining_ping_attempts: usize,
}


impl System {
	pub fn new(config: Config, id: UUID) -> Self {
		System{
			config,
			id,
			rpc_client: Client::new(),
			members: RwLock::new(Members{
				status: HashMap::new(),
				status_hash: [0u8; 32],
				config: NetworkConfig{
					members: HashMap::new(),
					version: 0,
				},
			}),
		}
	}

	pub async fn make_ping(&self) -> Message {
		let members = self.members.read().await;
		Message::Ping(PingMessage{
			id: self.id,
			rpc_port: self.config.rpc_port,
			status_hash: members.status_hash.clone(),
			config_version: members.config.version,
		})
	}

	pub async fn broadcast(self: Arc<Self>, msg: Message, timeout: Duration) {
		let members = self.members.read().await;
		let to = members.status.keys().cloned().collect::<Vec<_>>();
		drop(members);
		rpc_call_many(self.clone(), &to[..], &msg, None, timeout).await;
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

		tokio::spawn(self.ping_loop());
	}

	pub async fn handle_ping(self: Arc<Self>,
							 from: &SocketAddr,
							 ping: &PingMessage)
		-> Result<Message, Error> 
	{
		let mut members = self.members.write().await;
		let is_new = members.handle_ping(from.ip(), ping);
		members.recalculate_status_hash();
		let status_hash = members.status_hash.clone();
		let config_version = members.config.version;
		drop(members);

		if is_new || status_hash != ping.status_hash {
			tokio::spawn(self.clone().pull_status(ping.id.clone()));
		}
		if is_new || config_version < ping.config_version {
			tokio::spawn(self.clone().pull_config(ping.id.clone()));
		}

		Ok(self.make_ping().await)
	}

	pub async fn handle_pull_status(&self) -> Result<Message, Error> {
		let members = self.members.read().await;
		let mut mem = vec![];
		for (node, status) in members.status.iter() {
			mem.push(AdvertisedNode{
				id: node.clone(),
				addr: status.addr.clone(),
			});
		}
		Ok(Message::AdvertiseNodesUp(mem))
	}

	pub async fn handle_pull_config(&self) -> Result<Message, Error> {
		let members = self.members.read().await;
		Ok(Message::AdvertiseConfig(members.config.clone()))
	}

	pub async fn handle_advertise_nodes_up(self: Arc<Self>,
									   adv: &[AdvertisedNode])
		 -> Result<Message, Error>
	{
		let mut propagate = vec![];

		let mut members = self.members.write().await;
		for node in adv.iter() {
			let is_new = members.handle_advertise_node(&node.id, &node.addr);
			if is_new {
				tokio::spawn(self.clone().pull_status(node.id.clone()));
				tokio::spawn(self.clone().pull_config(node.id.clone()));
				propagate.push(node.clone());
			}
		}
		
		if propagate.len() > 0 {
			tokio::spawn(self.clone().broadcast(Message::AdvertiseNodesUp(propagate), PING_TIMEOUT));
		}

		Ok(Message::Ok)
	}

	pub async fn handle_advertise_config(self: Arc<Self>,
										 adv: &NetworkConfig)
		-> Result<Message, Error>
	{
		let mut members = self.members.write().await;
		if adv.version > members.config.version {
			members.config = adv.clone();
			tokio::spawn(self.clone().broadcast(Message::AdvertiseConfig(adv.clone()), PING_TIMEOUT));
		}

		Ok(Message::Ok)
	}

	pub async fn ping_loop(self: Arc<Self>) {
		loop {
			let restart_at = tokio::time::delay_for(PING_INTERVAL);
			
			let members = self.members.read().await;
			let ping_addrs = members.status.iter()
					.map(|(id, status)| (id.clone(), status.addr.clone()))
					.collect::<Vec<_>>();
			drop(members);

			let ping_msg = self.make_ping().await;
			let ping_resps = join_all(
					ping_addrs.iter()
						.map(|(id, addr)| {
							let sys = self.clone();
							let ping_msg_ref = &ping_msg;
							async move {
								(id, addr.clone(), rpc_call_addr(sys, &addr, ping_msg_ref, PING_TIMEOUT).await)
							}
					})).await;

			let mut members = self.members.write().await;
			for (id, addr, ping_resp) in ping_resps {
				if let Ok(Message::Ping(ping)) = ping_resp {
					let is_new = members.handle_ping(addr.ip(), &ping);
					if is_new || members.status_hash != ping.status_hash {
						tokio::spawn(self.clone().pull_status(ping.id.clone()));
					}
					if is_new || members.config.version < ping.config_version {
						tokio::spawn(self.clone().pull_config(ping.id.clone()));
					}
				} else {
					let remaining_attempts = members.status.get(id).map(|x| x.remaining_ping_attempts).unwrap_or(0);
					if remaining_attempts == 0 {
						eprintln!("Removing node {} after too many failed pings", hex::encode(id));
						members.status.remove(id);
					} else {
						if let Some(st) = members.status.get_mut(id) {
							st.remaining_ping_attempts = remaining_attempts - 1;
						}
					}
				}
			}
			drop(members);

			restart_at.await
		}
	}

	pub async fn pull_status(self: Arc<Self>, peer: UUID) {
		let resp = rpc_call(self.clone(),
					 &peer,
					 &Message::PullStatus,
					 PING_TIMEOUT).await;
		if let Ok(Message::AdvertiseNodesUp(nodes)) = resp {
			self.handle_advertise_nodes_up(&nodes).await;
		}
	}

	pub async fn pull_config(self: Arc<Self>, peer: UUID) {
		let resp = rpc_call(self.clone(),
					 &peer,
					 &Message::PullConfig,
					 PING_TIMEOUT).await;
		if let Ok(Message::AdvertiseConfig(config)) = resp {
			self.handle_advertise_config(&config).await;
		}
	}
}
