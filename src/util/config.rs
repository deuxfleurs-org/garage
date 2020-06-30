use std::io::Read;
use std::net::SocketAddr;
use std::path::PathBuf;

use serde::Deserialize;

use crate::error::Error;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
	pub metadata_dir: PathBuf,
	pub data_dir: PathBuf,

	pub rpc_bind_addr: SocketAddr,

	pub bootstrap_peers: Vec<SocketAddr>,

	#[serde(default = "default_max_concurrent_rpc_requests")]
	pub max_concurrent_rpc_requests: usize,

	#[serde(default = "default_block_size")]
	pub block_size: usize,

	#[serde(default = "default_replication_factor")]
	pub meta_replication_factor: usize,

	#[serde(default = "default_epidemic_fanout")]
	pub meta_epidemic_fanout: usize,

	#[serde(default = "default_replication_factor")]
	pub data_replication_factor: usize,

	pub rpc_tls: Option<TlsConfig>,

	pub s3_api: ApiConfig,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TlsConfig {
	pub ca_cert: String,
	pub node_cert: String,
	pub node_key: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ApiConfig {
	pub api_bind_addr: SocketAddr,
	pub s3_region: String,
}

fn default_max_concurrent_rpc_requests() -> usize {
	12
}
fn default_block_size() -> usize {
	1048576
}
fn default_replication_factor() -> usize {
	3
}
fn default_epidemic_fanout() -> usize {
	3
}

pub fn read_config(config_file: PathBuf) -> Result<Config, Error> {
	let mut file = std::fs::OpenOptions::new()
		.read(true)
		.open(config_file.as_path())?;

	let mut config = String::new();
	file.read_to_string(&mut config)?;

	Ok(toml::from_str(&config)?)
}
