use std::sync::Arc;
use std::hash::Hash as StdHash;
use std::hash::Hasher;
use std::path::PathBuf;
use std::io::{Read};
use std::collections::HashMap;
use std::time::Duration;
use std::net::{IpAddr, SocketAddr};

use sha2::{Sha256, Digest};
use tokio::prelude::*;
use futures::future::join_all;
use tokio::sync::RwLock;

use crate::server::Config;
use crate::error::Error;
use crate::data::*;
use crate::proto::*;
use crate::rpc_client::*;

const PING_INTERVAL: Duration = Duration::from_secs(10);
const PING_TIMEOUT: Duration = Duration::from_secs(2);
const MAX_FAILED_PINGS: usize = 3;

pub struct System {
	pub config: Config,
	pub id: UUID,

	pub rpc_client: RpcClient,

	pub members: RwLock<Members>,
}

pub struct Members {
	pub status: HashMap<UUID, NodeStatus>,
	pub status_hash: Hash,

	pub config: NetworkConfig,
    pub ring: Vec<RingEntry>,
    pub n_datacenters: usize,
}

pub struct NodeStatus {
	pub addr: SocketAddr,
	pub remaining_ping_attempts: usize,
}

#[derive(Debug)]
pub struct RingEntry {
    pub location: Hash,
    pub node: UUID,
    pub datacenter: u64,
}

impl Members {
	fn handle_ping(&mut self, ip: IpAddr, info: &PingMessage) -> bool {
		let addr = SocketAddr::new(ip, info.rpc_port);
		let old_status = self.status.insert(info.id.clone(),
			NodeStatus{
				addr: addr.clone(),
				remaining_ping_attempts: MAX_FAILED_PINGS,
			});
		match old_status {
			None => {
				eprintln!("Newly pingable node: {}", hex::encode(&info.id));
				true
			}
			Some(x) => x.addr != addr,
		}
	}

	fn recalculate_status_hash(&mut self) {
		let mut nodes = self.status.iter().collect::<Vec<_>>();
		nodes.sort_unstable_by_key(|(id, _status)| *id);

		let mut hasher = Sha256::new();
		eprintln!("Current set of pingable nodes: --");
		for (id, status) in nodes {
			eprintln!("{} {}", hex::encode(&id), status.addr);
			hasher.input(format!("{} {}\n", hex::encode(&id), status.addr));
		}
		eprintln!("END --");
		self.status_hash.as_slice_mut().copy_from_slice(&hasher.result()[..]);
	}

    fn rebuild_ring(&mut self) {
        let mut new_ring = vec![];
        let mut datacenters = vec![];

        for (id, config) in self.config.members.iter() {
            let mut dc_hasher = std::collections::hash_map::DefaultHasher::new();
            config.datacenter.hash(&mut dc_hasher);
            let datacenter = dc_hasher.finish();

            if !datacenters.contains(&datacenter) {
                datacenters.push(datacenter);
            }

            for i in 0..config.n_tokens {
                let location = hash(format!("{} {}", hex::encode(&id), i).as_bytes());

                new_ring.push(RingEntry{
                    location: location.into(),
                    node: id.clone(),
                    datacenter,
                })
            }
        }

        new_ring.sort_unstable_by(|x, y| x.location.cmp(&y.location));
        self.ring = new_ring;
        self.n_datacenters = datacenters.len();

		eprintln!("RING: --");
		for e in self.ring.iter() {
			eprintln!("{:?}", e);
		}
		eprintln!("END --");
    }

    pub fn walk_ring(&self, from: &Hash, n: usize) -> Vec<UUID> {
        if n >= self.config.members.len() {
            return self.config.members.keys().cloned().collect::<Vec<_>>();
        }

        let start = match self.ring.binary_search_by(|x| x.location.cmp(from)) {
            Ok(i) => i,
            Err(i) => if i == 0 {
                self.ring.len() - 1
            } else {
                i - 1
            }
        };

		self.walk_ring_from_pos(start, n)
    }

	fn walk_ring_from_pos(&self, start: usize, n: usize) -> Vec<UUID> {
        let mut ret = vec![];
        let mut datacenters = vec![];

        for delta in 0..self.ring.len() {
            if ret.len() == n {
                break;
            }

            let i = (start + delta) % self.ring.len();

            if datacenters.len() == self.n_datacenters && !ret.contains(&self.ring[i].node) {
                ret.push(self.ring[i].node.clone());
            } else if !datacenters.contains(&self.ring[i].datacenter) {
                ret.push(self.ring[i].node.clone());
                datacenters.push(self.ring[i].datacenter);
            }
        }

        ret
	}
}

fn read_network_config(metadata_dir: &PathBuf) -> Result<NetworkConfig, Error> {
    let mut path = metadata_dir.clone();
    path.push("network_config");

	let mut file = std::fs::OpenOptions::new()
		.read(true)
		.open(path.as_path())?;
	
	let mut net_config_bytes = vec![];
	file.read_to_end(&mut net_config_bytes)?;

    let net_config = rmp_serde::decode::from_read_ref(&net_config_bytes[..])?;

	Ok(net_config)
}

impl System {
	pub fn new(config: Config, id: UUID) -> Self {
        let net_config = match read_network_config(&config.metadata_dir) {
            Ok(x) => x,
            Err(e) => {
				println!("No valid previous network configuration stored ({}), starting fresh.", e);
				NetworkConfig{
					members: HashMap::new(),
					version: 0,
				}
			},
        };
		let mut members = Members{
				status: HashMap::new(),
				status_hash: Hash::default(),
				config: net_config,
                ring: Vec::new(),
                n_datacenters: 0,
        };
		members.recalculate_status_hash();
        members.rebuild_ring();
		System{
			config,
			id,
			rpc_client: RpcClient::new(),
			members: RwLock::new(members),
		}
	}

    async fn save_network_config(self: Arc<Self>) {
        let mut path = self.config.metadata_dir.clone();
        path.push("network_config");

        let members = self.members.read().await;
        let data = rmp_to_vec_all_named(&members.config)
            .expect("Error while encoding network config");
        drop(members);

		let mut f = tokio::fs::File::create(path.as_path()).await
            .expect("Could not create network_config");
		f.write_all(&data[..]).await
            .expect("Could not write network_config");
    }

	pub async fn make_ping(&self) -> Message {
		let members = self.members.read().await;
		Message::Ping(PingMessage{
			id: self.id.clone(),
			rpc_port: self.config.rpc_port,
			status_hash: members.status_hash.clone(),
			config_version: members.config.version,
		})
	}

	pub async fn broadcast(self: Arc<Self>, msg: Message, timeout: Duration) {
		let members = self.members.read().await;
		let to = members.status.keys().filter(|x| **x != self.id).cloned().collect::<Vec<_>>();
		drop(members);
		rpc_call_many(self.clone(), &to[..], &msg, timeout).await;
	}

	pub async fn bootstrap(self: Arc<Self>) {
		let bootstrap_peers = self.config.bootstrap_peers
			.iter()
			.map(|ip| (ip.clone(), None))
			.collect::<Vec<_>>();
		self.clone().ping_nodes(bootstrap_peers).await;

		tokio::spawn(self.ping_loop());
	}

	pub async fn ping_nodes(self: Arc<Self>, peers: Vec<(SocketAddr, Option<UUID>)>) {
		let ping_msg = self.make_ping().await;
		let ping_resps = join_all(
			peers.iter()
			.map(|(addr, id_option)| {
				let sys = self.clone();
				let ping_msg_ref = &ping_msg;
				async move {
					(id_option, addr.clone(), sys.rpc_client.call(&addr, ping_msg_ref, PING_TIMEOUT).await)
				}
			})).await;
		
		let mut members = self.members.write().await;

		let mut has_changes = false;
		let mut to_advertise = vec![];

		for (id_option, addr, ping_resp) in ping_resps {
			if let Ok(Message::Ping(info)) = ping_resp {
				let is_new = members.handle_ping(addr.ip(), &info);
				if is_new {
					has_changes = true;
					to_advertise.push(AdvertisedNode{
						id: info.id.clone(),
						addr: addr.clone(),
					});
				}
				if is_new || members.status_hash != info.status_hash {
					tokio::spawn(self.clone().pull_status(info.id.clone()));
				}
				if is_new || members.config.version < info.config_version {
					tokio::spawn(self.clone().pull_config(info.id.clone()));
				}
			} else if let Some(id) = id_option {
				let remaining_attempts = members.status.get(id).map(|x| x.remaining_ping_attempts).unwrap_or(0);
				if remaining_attempts == 0 {
					eprintln!("Removing node {} after too many failed pings", hex::encode(&id));
					members.status.remove(&id);
					has_changes = true;
				} else {
					if let Some(st) = members.status.get_mut(id) {
						st.remaining_ping_attempts = remaining_attempts - 1;
					}
				}
			}
		}
		if has_changes {
			members.recalculate_status_hash();
		}
		drop(members);

		if to_advertise.len() > 0 {
			self.broadcast(Message::AdvertiseNodesUp(to_advertise), PING_TIMEOUT).await;
		}
	}

	pub async fn handle_ping(self: Arc<Self>,
							 from: &SocketAddr,
							 ping: &PingMessage)
		-> Result<Message, Error> 
	{
		let mut members = self.members.write().await;
		let is_new = members.handle_ping(from.ip(), ping);
		if is_new {
			members.recalculate_status_hash();
		}
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
		let mut to_ping = vec![];

		let mut members = self.members.write().await;
		let mut has_changed = false;

		for node in adv.iter() {
			if node.id == self.id {
				// learn our own ip address
				let self_addr = SocketAddr::new(node.addr.ip(), self.config.rpc_port);
				let old_self = members.status.insert(node.id.clone(),
					NodeStatus{
						addr: self_addr,
						remaining_ping_attempts: MAX_FAILED_PINGS,
					});
				has_changed = match old_self {
					None => true,
					Some(x) => x.addr != self_addr,
				};
			} else if !members.status.contains_key(&node.id) {
				to_ping.push((node.addr.clone(), Some(node.id.clone())));
			}
		}
		if has_changed {
			members.recalculate_status_hash();
		}
		drop(members);

		if to_ping.len() > 0 {
			tokio::spawn(self.clone().ping_nodes(to_ping));
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
            members.rebuild_ring();

			tokio::spawn(self.clone().broadcast(Message::AdvertiseConfig(adv.clone()), PING_TIMEOUT));
            tokio::spawn(self.clone().save_network_config());
		}

		Ok(Message::Ok)
	}

	pub async fn ping_loop(self: Arc<Self>) {
		loop {
			let restart_at = tokio::time::delay_for(PING_INTERVAL);
			
			let members = self.members.read().await;
			let ping_addrs = members.status.iter()
					.filter(|(id, _)| **id != self.id)
					.map(|(id, status)| (status.addr.clone(), Some(id.clone())))
					.collect::<Vec<_>>();
			drop(members);

			self.clone().ping_nodes(ping_addrs).await;

			restart_at.await
		}
	}

	pub fn pull_status(self: Arc<Self>, peer: UUID) -> impl futures::future::Future<Output=()> + Send + 'static {
		async move {
			let resp = rpc_call(self.clone(),
						 &peer,
						 &Message::PullStatus,
						 PING_TIMEOUT).await;
			if let Ok(Message::AdvertiseNodesUp(nodes)) = resp {
				let _: Result<_, _> = self.handle_advertise_nodes_up(&nodes).await;
			}
		}
	}

	pub async fn pull_config(self: Arc<Self>, peer: UUID) {
		let resp = rpc_call(self.clone(),
					 &peer,
					 &Message::PullConfig,
					 PING_TIMEOUT).await;
		if let Ok(Message::AdvertiseConfig(config)) = resp {
			let _: Result<_, _> = self.handle_advertise_config(&config).await;
		}
	}
}
