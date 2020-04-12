use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Duration;

use crate::data::*;

pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
pub const BLOCK_RW_TIMEOUT: Duration = Duration::from_secs(42);

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
	Ok,
	Error(String),

	Ping(PingMessage),
	PullStatus,
	PullConfig,
	AdvertiseNodesUp(Vec<AdvertisedNode>),
	AdvertiseConfig(NetworkConfig),

	GetBlock(Hash),
	PutBlock(PutBlockMessage),

	TableRPC(String, #[serde(with = "serde_bytes")] Vec<u8>),
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

#[derive(Debug, Serialize, Deserialize)]
pub struct PutBlockMessage {
	pub hash: Hash,

	#[serde(with = "serde_bytes")]
	pub data: Vec<u8>,
}
