use std::io::Read;
use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{de, Deserialize};

use crate::error::Error;

#[derive(Deserialize, Debug, Clone)]
pub struct Config {
	pub metadata_dir: PathBuf,
	pub data_dir: PathBuf,

	#[serde(deserialize_with = "deserialize_addr")]
	pub rpc_bind_addr: SocketAddr,

	#[serde(deserialize_with = "deserialize_vec_addr")]
	pub bootstrap_peers: Vec<SocketAddr>,
	pub consul_host: Option<String>,
	pub consul_service_name: Option<String>,

	#[serde(default = "default_max_concurrent_rpc_requests")]
	pub max_concurrent_rpc_requests: usize,

	#[serde(default = "default_block_size")]
	pub block_size: usize,

	#[serde(default = "default_control_write_max_faults")]
	pub control_write_max_faults: usize,

	#[serde(default = "default_replication_factor")]
	pub meta_replication_factor: usize,

	#[serde(default = "default_replication_factor")]
	pub data_replication_factor: usize,

	pub rpc_tls: Option<TlsConfig>,

	pub s3_api: ApiConfig,

	pub s3_web: WebConfig,
}

#[derive(Deserialize, Debug, Clone)]
pub struct TlsConfig {
	pub ca_cert: String,
	pub node_cert: String,
	pub node_key: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ApiConfig {
	#[serde(deserialize_with = "deserialize_addr")]
	pub api_bind_addr: SocketAddr,
	pub s3_region: String,
}

#[derive(Deserialize, Debug, Clone)]
pub struct WebConfig {
	#[serde(deserialize_with = "deserialize_addr")]
	pub bind_addr: SocketAddr,
	pub root_domain: String,
	pub index: String,
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
fn default_control_write_max_faults() -> usize {
	1
}

pub fn read_config(config_file: PathBuf) -> Result<Config, Error> {
	let mut file = std::fs::OpenOptions::new()
		.read(true)
		.open(config_file.as_path())?;

	let mut config = String::new();
	file.read_to_string(&mut config)?;

	Ok(toml::from_str(&config)?)
}

fn deserialize_addr<'de, D>(deserializer: D) -> Result<SocketAddr, D::Error>
where
	D: de::Deserializer<'de>,
{
	use std::net::ToSocketAddrs;

	<&str>::deserialize(deserializer)?
		.to_socket_addrs()
		.map_err(|_| de::Error::custom("could not resolve to a socket address"))?
		.next()
		.ok_or(de::Error::custom("could not resolve to a socket address"))
}

fn deserialize_vec_addr<'de, D>(deserializer: D) -> Result<Vec<SocketAddr>, D::Error>
where
	D: de::Deserializer<'de>,
{
	use std::net::ToSocketAddrs;

	let mut res = vec![];
	for s in <Vec<&str>>::deserialize(deserializer)? {
		res.push(
			s.to_socket_addrs()
				.map_err(|_| de::Error::custom("could not resolve to a socket address"))?
				.next()
				.ok_or(de::Error::custom("could not resolve to a socket address"))?,
		);
	}
	Ok(res)
}
