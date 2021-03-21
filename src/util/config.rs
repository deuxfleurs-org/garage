//! Contains type and functions related to Garage configuration file
use std::io::Read;
use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{de, Deserialize};

use crate::error::Error;

/// Represent the whole configuration
#[derive(Deserialize, Debug, Clone)]
pub struct Config {
	/// Path where to store metadata. Should be fast, but low volume
	pub metadata_dir: PathBuf,
	/// Path where to store data. Can be slower, but need higher volume
	pub data_dir: PathBuf,

	/// Address to bind for RPC
	pub rpc_bind_addr: SocketAddr,

	/// Bootstrap peers RPC address
	#[serde(deserialize_with = "deserialize_vec_addr")]
	pub bootstrap_peers: Vec<SocketAddr>,
	/// Consule host to connect to to discover more peers
	pub consul_host: Option<String>,
	/// Consul service name to use
	pub consul_service_name: Option<String>,

	/// Max number of concurrent RPC request
	#[serde(default = "default_max_concurrent_rpc_requests")]
	pub max_concurrent_rpc_requests: usize,

	/// Size of data blocks to save to disk
	#[serde(default = "default_block_size")]
	pub block_size: usize,

	#[serde(default = "default_control_write_max_faults")]
	pub control_write_max_faults: usize,

	/// How many nodes should hold a copy of meta data
	#[serde(default = "default_replication_factor")]
	pub meta_replication_factor: usize,

	/// How many nodes should hold a copy of data
	#[serde(default = "default_replication_factor")]
	pub data_replication_factor: usize,

	/// Configuration for RPC TLS
	pub rpc_tls: Option<TlsConfig>,

	/// Configuration for S3 api
	pub s3_api: ApiConfig,

	/// Configuration for serving files as normal web server
	pub s3_web: WebConfig,
}

/// Configuration for RPC TLS
#[derive(Deserialize, Debug, Clone)]
pub struct TlsConfig {
	/// Path to certificate autority used for all nodes
	pub ca_cert: String,
	/// Path to public certificate for this node
	pub node_cert: String,
	/// Path to private key for this node
	pub node_key: String,
}

/// Configuration for S3 api
#[derive(Deserialize, Debug, Clone)]
pub struct ApiConfig {
	/// Address and port to bind for api serving
	pub api_bind_addr: SocketAddr,
	/// S3 region to use
	pub s3_region: String,
}

/// Configuration for serving files as normal web server
#[derive(Deserialize, Debug, Clone)]
pub struct WebConfig {
	/// Address and port to bind for web serving
	pub bind_addr: SocketAddr,
	/// Suffix to remove from domain name to find bucket
	pub root_domain: String,
	/// Suffix to add when user-agent request path end with "/"
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

/// Read and parse configuration
pub fn read_config(config_file: PathBuf) -> Result<Config, Error> {
	let mut file = std::fs::OpenOptions::new()
		.read(true)
		.open(config_file.as_path())?;

	let mut config = String::new();
	file.read_to_string(&mut config)?;

	Ok(toml::from_str(&config)?)
}

fn deserialize_vec_addr<'de, D>(deserializer: D) -> Result<Vec<SocketAddr>, D::Error>
where
	D: de::Deserializer<'de>,
{
	use std::net::ToSocketAddrs;

	Ok(<Vec<&str>>::deserialize(deserializer)?
		.iter()
		.filter_map(|&name| {
			name.to_socket_addrs()
				.map(|iter| (name, iter))
				.map_err(|_| warn!("Error resolving \"{}\"", name))
				.ok()
		})
		.map(|(name, iter)| {
			let v = iter.collect::<Vec<_>>();
			if v.is_empty() {
				warn!("Error resolving \"{}\"", name)
			}
			v
		})
		.flatten()
		.collect())
}
