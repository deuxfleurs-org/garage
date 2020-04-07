use std::time::Duration;
use std::net::SocketAddr;
use serde::{Serialize, Deserialize};

use crate::data::*;

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(2);

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
	Ok,
	Error(String),

	Ping(PingMessage),	
	PullStatus,
	PullConfig,
	AdvertiseNodesUp(Vec<AdvertisedNode>),
	AdvertiseConfig(NetworkConfig),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PingMessage {
	pub id: UUID,
	pub rpc_port: u16,

	pub status_hash: Hash,
	pub config_version: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AdvertisedNode {
	pub id: UUID,
	pub addr: SocketAddr,
}
