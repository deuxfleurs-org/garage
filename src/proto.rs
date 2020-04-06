use std::net::SocketAddr;
use serde::{Serialize, Deserialize};

use crate::data::*;

#[derive(Debug, Serialize, Deserialize)]
pub enum Message {
	Ok,
	Error(String),

	Ping(PingMessage),	
	AdvertiseNode(AdvertiseNodeMessage),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct PingMessage {
	pub id: UUID,
	pub rpc_port: u16,

	pub present_hash: Hash,
	pub config_version: u64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AdvertiseNodeMessage {
	pub id: UUID,
	pub addr: SocketAddr,
}
