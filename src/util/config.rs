//! Contains type and functions related to Garage configuration file
use std::convert::TryFrom;
use std::net::SocketAddr;
use std::path::PathBuf;

use serde::{de, Deserialize};

use crate::error::Error;
use crate::socket_address::UnixOrTCPSocketAddress;

/// Represent the whole configuration
#[derive(Deserialize, Debug, Clone)]
pub struct Config {
	/// Path where to store metadata. Should be fast, but low volume
	pub metadata_dir: PathBuf,
	/// Path where to store data. Can be slower, but need higher volume
	pub data_dir: DataDirEnum,

	/// Whether to fsync after all metadata transactions (disabled by default)
	#[serde(default)]
	pub metadata_fsync: bool,
	/// Whether to fsync after all data block writes (disabled by default)
	#[serde(default)]
	pub data_fsync: bool,

	/// Disable automatic scrubbing of the data directory
	#[serde(default)]
	pub disable_scrub: bool,

	/// Use local timezone
	#[serde(default)]
	pub use_local_tz: bool,

	/// Optional directory where metadata snapshots will be store
	pub metadata_snapshots_dir: Option<PathBuf>,

	/// Automatic snapshot interval for metadata
	#[serde(default)]
	pub metadata_auto_snapshot_interval: Option<String>,

	/// Size of data blocks to save to disk
	#[serde(
		deserialize_with = "deserialize_capacity",
		default = "default_block_size"
	)]
	pub block_size: usize,

	/// Number of replicas. Can be any positive integer, but uneven numbers are more favorable.
	/// - 1 for single-node clusters, or to disable replication
	/// - 3 is the recommended and supported setting.
	#[serde(default)]
	pub replication_factor: Option<usize>,

	/// Consistency mode for all for requests through this node
	/// - Degraded -> Disable read quorum
	/// - Dangerous -> Disable read and write quorum
	#[serde(default = "default_consistency_mode")]
	pub consistency_mode: String,

	/// Legacy option
	pub replication_mode: Option<String>,

	/// Zstd compression level used on data blocks
	#[serde(
		deserialize_with = "deserialize_compression",
		default = "default_compression"
	)]
	pub compression_level: Option<i32>,

	/// Maximum amount of block data to buffer in RAM for sending to
	/// remote nodes when these nodes are on slower links
	#[serde(
		deserialize_with = "deserialize_capacity",
		default = "default_block_ram_buffer_max"
	)]
	pub block_ram_buffer_max: usize,

	/// Skip the permission check of secret files. Useful when
	/// POSIX ACLs (or more complex chmods) are used.
	#[serde(default)]
	pub allow_world_readable_secrets: bool,

	/// RPC secret key: 32 bytes hex encoded
	pub rpc_secret: Option<String>,
	/// Optional file where RPC secret key is read from
	pub rpc_secret_file: Option<PathBuf>,
	/// Address to bind for RPC
	pub rpc_bind_addr: SocketAddr,
	/// Bind outgoing sockets to rpc_bind_addr's IP address as well
	#[serde(default)]
	pub rpc_bind_outgoing: bool,
	/// Public IP address of this node
	pub rpc_public_addr: Option<String>,

	/// In case `rpc_public_addr` was not set, this can filter
	/// the addresses announced to other peers to a specific subnet.
	pub rpc_public_addr_subnet: Option<String>,

	/// Timeout for Netapp's ping messages
	pub rpc_ping_timeout_msec: Option<u64>,
	/// Timeout for Netapp RPC calls
	pub rpc_timeout_msec: Option<u64>,

	// -- Bootstrapping and discovery
	/// Bootstrap peers RPC address
	#[serde(default)]
	pub bootstrap_peers: Vec<String>,

	/// Configuration for automatic node discovery through Consul
	#[serde(default)]
	pub consul_discovery: Option<ConsulDiscoveryConfig>,
	/// Configuration for automatic node discovery through Kubernetes
	#[serde(default)]
	pub kubernetes_discovery: Option<KubernetesDiscoveryConfig>,

	// -- DB
	/// Database engine to use for metadata (options: sqlite, lmdb)
	#[serde(default = "default_db_engine")]
	pub db_engine: String,

	/// LMDB map size
	#[serde(deserialize_with = "deserialize_capacity", default)]
	pub lmdb_map_size: usize,

	// -- APIs
	/// Configuration for S3 api
	pub s3_api: S3ApiConfig,

	/// Configuration for K2V api
	pub k2v_api: Option<K2VApiConfig>,

	/// Configuration for serving files as normal web server
	pub s3_web: Option<WebConfig>,

	/// Configuration for the admin API endpoint
	#[serde(default = "Default::default")]
	pub admin: AdminConfig,

	/// Allow punycode in bucket names
	#[serde(default)]
	pub allow_punycode: bool,
}

/// Value for data_dir: either a single directory or a list of dirs with attributes
#[derive(Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum DataDirEnum {
	Single(PathBuf),
	Multiple(Vec<DataDir>),
}

#[derive(Deserialize, Debug, Clone)]
pub struct DataDir {
	/// Path to the data directory
	pub path: PathBuf,
	/// Capacity of the drive (required if read_only is false)
	#[serde(default)]
	pub capacity: Option<String>,
	/// Whether this is a legacy read-only path (capacity should be None)
	#[serde(default)]
	pub read_only: bool,
}

/// Configuration for S3 api
#[derive(Deserialize, Debug, Clone)]
pub struct S3ApiConfig {
	/// Address and port to bind for api serving
	pub api_bind_addr: Option<UnixOrTCPSocketAddress>,
	/// S3 region to use
	pub s3_region: String,
	/// Suffix to remove from domain name to find bucket. If None,
	/// vhost-style S3 request are disabled
	pub root_domain: Option<String>,
}

/// Configuration for K2V api
#[derive(Deserialize, Debug, Clone)]
pub struct K2VApiConfig {
	/// Address and port to bind for api serving
	pub api_bind_addr: UnixOrTCPSocketAddress,
}

/// Configuration for serving files as normal web server
#[derive(Deserialize, Debug, Clone)]
pub struct WebConfig {
	/// Address and port to bind for web serving
	pub bind_addr: UnixOrTCPSocketAddress,
	/// Suffix to remove from domain name to find bucket
	pub root_domain: String,
	/// Whether to add the requested domain to exported Prometheus metrics
	#[serde(default)]
	pub add_host_to_metrics: bool,
}

/// Configuration for the admin and monitoring HTTP API
#[derive(Deserialize, Debug, Clone, Default)]
pub struct AdminConfig {
	/// Address and port to bind for admin API serving
	pub api_bind_addr: Option<UnixOrTCPSocketAddress>,

	/// Bearer token to use to scrape metrics
	pub metrics_token: Option<String>,
	/// File to read metrics token from
	pub metrics_token_file: Option<PathBuf>,

	/// Bearer token to use to access Admin API endpoints
	pub admin_token: Option<String>,
	/// File to read admin token from
	pub admin_token_file: Option<PathBuf>,

	/// OTLP server to where to export traces
	pub trace_sink: Option<String>,
}

#[derive(Deserialize, Debug, Clone, Default)]
#[serde(rename_all = "lowercase")]
pub enum ConsulDiscoveryAPI {
	#[default]
	Catalog,
	Agent,
}

#[derive(Deserialize, Debug, Clone)]
pub struct ConsulDiscoveryConfig {
	/// The consul api to use when registering: either `catalog` (the default) or `agent`
	#[serde(default)]
	pub api: ConsulDiscoveryAPI,
	/// Consul http or https address to connect to to discover more peers
	pub consul_http_addr: String,
	/// Consul service name to use
	pub service_name: String,
	/// CA TLS certificate to use when connecting to Consul
	pub ca_cert: Option<String>,
	/// Client TLS certificate to use when connecting to Consul
	pub client_cert: Option<String>,
	/// Client TLS key to use when connecting to Consul
	pub client_key: Option<String>,
	/// /// Token to use for connecting to consul
	pub token: Option<String>,
	/// Skip TLS hostname verification
	#[serde(default)]
	pub tls_skip_verify: bool,
	/// Additional tags to add to the service
	#[serde(default)]
	pub tags: Vec<String>,
	/// Additional service metadata to add
	#[serde(default)]
	pub meta: Option<std::collections::HashMap<String, String>>,
}

#[derive(Deserialize, Debug, Clone)]
pub struct KubernetesDiscoveryConfig {
	/// Kubernetes namespace the service discovery resources are be created in
	pub namespace: String,
	/// Service name to filter for in k8s custom resources
	pub service_name: String,
	/// Skip creation of the garagenodes CRD
	#[serde(default)]
	pub skip_crd: bool,
}

/// Read and parse configuration
pub fn read_config(config_file: PathBuf) -> Result<Config, Error> {
	let config = std::fs::read_to_string(config_file)?;

	Ok(toml::from_str(&config)?)
}

fn default_db_engine() -> String {
	"lmdb".into()
}

fn default_block_size() -> usize {
	1048576
}
fn default_block_ram_buffer_max() -> usize {
	256 * 1024 * 1024
}

fn default_consistency_mode() -> String {
	"consistent".into()
}

fn default_compression() -> Option<i32> {
	Some(1)
}

fn deserialize_compression<'de, D>(deserializer: D) -> Result<Option<i32>, D::Error>
where
	D: de::Deserializer<'de>,
{
	struct OptionVisitor;

	impl<'de> serde::de::Visitor<'de> for OptionVisitor {
		type Value = Option<i32>;
		fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
			formatter.write_str("int or 'none'")
		}

		fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
		where
			E: de::Error,
		{
			if value.eq_ignore_ascii_case("none") {
				Ok(None)
			} else {
				Err(E::custom(format!(
					"Invalid compression level: '{}', should be a number, or 'none'",
					value
				)))
			}
		}

		fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
		where
			E: de::Error,
		{
			i32::try_from(v)
				.map(Some)
				.map_err(|_| E::custom("Compression level out of bound".to_owned()))
		}

		fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
		where
			E: de::Error,
		{
			i32::try_from(v)
				.map(Some)
				.map_err(|_| E::custom("Compression level out of bound".to_owned()))
		}
	}

	deserializer.deserialize_any(OptionVisitor)
}

fn deserialize_capacity<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
	D: de::Deserializer<'de>,
{
	struct CapacityVisitor;

	impl<'de> serde::de::Visitor<'de> for CapacityVisitor {
		type Value = usize;
		fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
			formatter.write_str("int or '<capacity>'")
		}

		fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
		where
			E: de::Error,
		{
			value
				.parse::<bytesize::ByteSize>()
				.map(|x| x.as_u64())
				.map_err(|e| E::custom(format!("invalid capacity value: {}", e)))
				.and_then(|v| {
					usize::try_from(v)
						.map_err(|_| E::custom("capacity value out of bound".to_owned()))
				})
		}

		fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E>
		where
			E: de::Error,
		{
			usize::try_from(v).map_err(|_| E::custom("capacity value out of bound".to_owned()))
		}

		fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E>
		where
			E: de::Error,
		{
			usize::try_from(v).map_err(|_| E::custom("capacity value out of bound".to_owned()))
		}
	}

	deserializer.deserialize_any(CapacityVisitor)
}

#[cfg(test)]
mod tests {
	use crate::error::Error;
	use std::fs::File;
	use std::io::Write;

	#[test]
	fn test_rpc_secret() -> Result<(), Error> {
		let path2 = mktemp::Temp::new_file()?;
		let mut file2 = File::create(path2.as_path())?;
		writeln!(
			file2,
			r#"
			metadata_dir = "/tmp/garage/meta"
			data_dir = "/tmp/garage/data"
			replication_factor = 3
			rpc_bind_addr = "[::]:3901"
			rpc_secret = "foo"

			[s3_api]
			s3_region = "garage"
			api_bind_addr = "[::]:3900"
			"#
		)?;

		let config = super::read_config(path2.to_path_buf())?;
		assert_eq!("foo", config.rpc_secret.unwrap());
		drop(path2);
		drop(file2);

		Ok(())
	}
}
