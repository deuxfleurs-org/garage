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

	/// Size of data blocks to save to disk
	#[serde(default = "default_block_size")]
	pub block_size: usize,

	/// Replication mode. Supported values:
	/// - none, 1 -> no replication
	/// - 2 -> 2-way replication
	/// - 3 -> 3-way replication
	// (we can add more aliases for this later)
	pub replication_mode: String,

	/// Zstd compression level used on data blocks
	#[serde(
		deserialize_with = "deserialize_compression",
		default = "default_compression"
	)]
	pub compression_level: Option<i32>,

	/// RPC secret key: 32 bytes hex encoded
	pub rpc_secret: Option<String>,
	/// Optional file where RPC secret key is read from
	pub rpc_secret_file: Option<String>,

	/// Address to bind for RPC
	pub rpc_bind_addr: SocketAddr,
	/// Public IP address of this node
	pub rpc_public_addr: Option<String>,

	/// Timeout for Netapp's ping messagess
	pub rpc_ping_timeout_msec: Option<u64>,
	/// Timeout for Netapp RPC calls
	pub rpc_timeout_msec: Option<u64>,

	// -- Bootstraping and discovery
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
	/// Database engine to use for metadata (options: sled, sqlite, lmdb)
	#[serde(default = "default_db_engine")]
	pub db_engine: String,

	/// Sled cache size, in bytes
	#[serde(default = "default_sled_cache_capacity")]
	pub sled_cache_capacity: u64,
	/// Sled flush interval in milliseconds
	#[serde(default = "default_sled_flush_every_ms")]
	pub sled_flush_every_ms: u64,

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
}

/// Configuration for S3 api
#[derive(Deserialize, Debug, Clone)]
pub struct S3ApiConfig {
	/// Address and port to bind for api serving
	pub api_bind_addr: Option<SocketAddr>,
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
	pub api_bind_addr: SocketAddr,
}

/// Configuration for serving files as normal web server
#[derive(Deserialize, Debug, Clone)]
pub struct WebConfig {
	/// Address and port to bind for web serving
	pub bind_addr: SocketAddr,
	/// Suffix to remove from domain name to find bucket
	pub root_domain: String,
}

/// Configuration for the admin and monitoring HTTP API
#[derive(Deserialize, Debug, Clone, Default)]
pub struct AdminConfig {
	/// Address and port to bind for admin API serving
	pub api_bind_addr: Option<SocketAddr>,

	/// Bearer token to use to scrape metrics
	pub metrics_token: Option<String>,
	/// File to read metrics token from
	pub metrics_token_file: Option<String>,

	/// Bearer token to use to access Admin API endpoints
	pub admin_token: Option<String>,
	/// File to read admin token from
	pub admin_token_file: Option<String>,

	/// OTLP server to where to export traces
	pub trace_sink: Option<String>,
}

#[derive(Deserialize, Debug, Clone)]
pub enum ConsulDiscoveryAPI {
	#[serde(rename_all = "lowercase")]
	Catalog,
	Agent,
}
impl ConsulDiscoveryAPI {
	fn default() -> Self {
		ConsulDiscoveryAPI::Catalog
	}
}

#[derive(Deserialize, Debug, Clone)]
pub struct ConsulDiscoveryConfig {
	/// The consul api to use when registering: either `catalog` (the default) or `agent`
	#[serde(default = "ConsulDiscoveryAPI::default")]
	pub consul_http_api: ConsulDiscoveryAPI,
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
	pub consul_http_token: Option<String>,
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

fn default_db_engine() -> String {
	"sled".into()
}

fn default_sled_cache_capacity() -> u64 {
	128 * 1024 * 1024
}
fn default_sled_flush_every_ms() -> u64 {
	2000
}
fn default_block_size() -> usize {
	1048576
}

/// Read and parse configuration
pub fn read_config(config_file: PathBuf) -> Result<Config, Error> {
	let mut file = std::fs::OpenOptions::new()
		.read(true)
		.open(config_file.as_path())?;

	let mut config = String::new();
	file.read_to_string(&mut config)?;

	let mut parsed_config: Config = toml::from_str(&config)?;

	secret_from_file(
		&mut parsed_config.rpc_secret,
		&parsed_config.rpc_secret_file,
		"rpc_secret",
	)?;
	secret_from_file(
		&mut parsed_config.admin.metrics_token,
		&parsed_config.admin.metrics_token_file,
		"admin.metrics_token",
	)?;
	secret_from_file(
		&mut parsed_config.admin.admin_token,
		&parsed_config.admin.admin_token_file,
		"admin.admin_token",
	)?;

	Ok(parsed_config)
}

fn secret_from_file(
	secret: &mut Option<String>,
	secret_file: &Option<String>,
	name: &'static str,
) -> Result<(), Error> {
	match (&secret, &secret_file) {
		(_, None) => {
			// no-op
		}
		(Some(_), Some(_)) => {
			return Err(format!("only one of `{}` and `{}_file` can be set", name, name).into());
		}
		(None, Some(file_path)) => {
			#[cfg(unix)]
			if std::env::var("GARAGE_ALLOW_WORLD_READABLE_SECRETS").as_deref() != Ok("true") {
				use std::os::unix::fs::MetadataExt;
				let metadata = std::fs::metadata(file_path)?;
				if metadata.mode() & 0o077 != 0 {
					return Err(format!("File {} is world-readable! (mode: 0{:o}, expected 0600)\nRefusing to start until this is fixed, or environment variable GARAGE_ALLOW_WORLD_READABLE_SECRETS is set to true.", file_path, metadata.mode()).into());
				}
			}
			let mut file = std::fs::OpenOptions::new().read(true).open(file_path)?;
			let mut secret_buf = String::new();
			file.read_to_string(&mut secret_buf)?;
			// trim_end: allows for use case such as `echo "$(openssl rand -hex 32)" > somefile`.
			//           also editors sometimes add a trailing newline
			*secret = Some(String::from(secret_buf.trim_end()));
		}
	}
	Ok(())
}

fn default_compression() -> Option<i32> {
	Some(1)
}

fn deserialize_compression<'de, D>(deserializer: D) -> Result<Option<i32>, D::Error>
where
	D: de::Deserializer<'de>,
{
	use std::convert::TryFrom;

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
			replication_mode = "3"
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

	#[test]
	fn test_rpc_secret_file_works() -> Result<(), Error> {
		let path_secret = mktemp::Temp::new_file()?;
		let mut file_secret = File::create(path_secret.as_path())?;
		writeln!(file_secret, "foo")?;
		drop(file_secret);

		let path_config = mktemp::Temp::new_file()?;
		let mut file_config = File::create(path_config.as_path())?;
		let path_secret_path = path_secret.as_path();
		writeln!(
			file_config,
			r#"
			metadata_dir = "/tmp/garage/meta"
			data_dir = "/tmp/garage/data"
			replication_mode = "3"
			rpc_bind_addr = "[::]:3901"
			rpc_secret_file = "{}"

			[s3_api]
			s3_region = "garage"
			api_bind_addr = "[::]:3900"
			"#,
			path_secret_path.display()
		)?;
		let config = super::read_config(path_config.to_path_buf())?;
		assert_eq!("foo", config.rpc_secret.unwrap());

		#[cfg(unix)]
		{
			use std::os::unix::fs::PermissionsExt;
			let metadata = std::fs::metadata(&path_secret_path)?;
			let mut perm = metadata.permissions();
			perm.set_mode(0o660);
			std::fs::set_permissions(&path_secret_path, perm)?;

			std::env::set_var("GARAGE_ALLOW_WORLD_READABLE_SECRETS", "false");
			assert!(super::read_config(path_config.to_path_buf()).is_err());

			std::env::set_var("GARAGE_ALLOW_WORLD_READABLE_SECRETS", "true");
			assert!(super::read_config(path_config.to_path_buf()).is_ok());
		}

		drop(path_config);
		drop(path_secret);
		drop(file_config);
		Ok(())
	}

	#[test]
	fn test_rcp_secret_and_rpc_secret_file_cannot_be_set_both() -> Result<(), Error> {
		let path_config = mktemp::Temp::new_file()?;
		let mut file_config = File::create(path_config.as_path())?;
		writeln!(
			file_config,
			r#"
			metadata_dir = "/tmp/garage/meta"
			data_dir = "/tmp/garage/data"
			replication_mode = "3"
			rpc_bind_addr = "[::]:3901"
			rpc_secret= "dummy"
			rpc_secret_file = "dummy"

			[s3_api]
			s3_region = "garage"
			api_bind_addr = "[::]:3900"
			"#
		)?;
		assert_eq!(
			"only one of `rpc_secret` and `rpc_secret_file` can be set",
			super::read_config(path_config.to_path_buf())
				.unwrap_err()
				.to_string()
		);
		drop(path_config);
		drop(file_config);
		Ok(())
	}
}
