use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::net::{IpAddr, SocketAddr};

use err_derive::Error;
use serde::{Deserialize, Serialize};

use netapp::NodeID;

use garage_util::config::ConsulDiscoveryConfig;

#[derive(Deserialize, Clone, Debug)]
struct ConsulQueryEntry {
	#[serde(rename = "Address")]
	address: String,
	#[serde(rename = "ServicePort")]
	service_port: u16,
	#[serde(rename = "NodeMeta")]
	node_meta: HashMap<String, String>,
}

#[derive(Serialize, Clone, Debug)]
struct ConsulPublishEntry {
	#[serde(rename = "Node")]
	node: String,
	#[serde(rename = "Address")]
	address: IpAddr,
	#[serde(rename = "NodeMeta")]
	node_meta: HashMap<String, String>,
	#[serde(rename = "Service")]
	service: ConsulPublishService,
}

#[derive(Serialize, Clone, Debug)]
struct ConsulPublishService {
	#[serde(rename = "ID")]
	service_id: String,
	#[serde(rename = "Service")]
	service_name: String,
	#[serde(rename = "Tags")]
	tags: Vec<String>,
	#[serde(rename = "Address")]
	address: IpAddr,
	#[serde(rename = "Port")]
	port: u16,
}

// ----

pub struct ConsulDiscovery {
	config: ConsulDiscoveryConfig,
	client: reqwest::Client,
}

impl ConsulDiscovery {
	pub fn new(config: ConsulDiscoveryConfig) -> Result<Self, ConsulError> {
		let client = match (&config.client_cert, &config.client_key) {
			(Some(client_cert), Some(client_key)) => {
				let mut client_cert_buf = vec![];
				File::open(client_cert)?.read_to_end(&mut client_cert_buf)?;

				let mut client_key_buf = vec![];
				File::open(client_key)?.read_to_end(&mut client_key_buf)?;

				let identity = reqwest::Identity::from_pem(
					&[&client_cert_buf[..], &client_key_buf[..]].concat()[..],
				)?;

				if config.tls_skip_verify {
					reqwest::Client::builder()
						.use_rustls_tls()
						.danger_accept_invalid_certs(true)
						.identity(identity)
						.build()?
				} else if let Some(ca_cert) = &config.ca_cert {
					let mut ca_cert_buf = vec![];
					File::open(ca_cert)?.read_to_end(&mut ca_cert_buf)?;

					reqwest::Client::builder()
						.use_rustls_tls()
						.add_root_certificate(reqwest::Certificate::from_pem(&ca_cert_buf[..])?)
						.identity(identity)
						.build()?
				} else {
					reqwest::Client::builder()
						.use_rustls_tls()
						.identity(identity)
						.build()?
				}
			}
			(None, None) => reqwest::Client::new(),
			_ => return Err(ConsulError::InvalidTLSConfig),
		};

		Ok(Self { client, config })
	}

	// ---- READING FROM CONSUL CATALOG ----

	pub async fn get_consul_nodes(&self) -> Result<Vec<(NodeID, SocketAddr)>, ConsulError> {
		let url = format!(
			"{}/v1/catalog/service/{}",
			self.config.consul_http_addr, self.config.service_name
		);

		let http = self.client.get(&url).send().await?;
		let entries: Vec<ConsulQueryEntry> = http.json().await?;

		let mut ret = vec![];
		for ent in entries {
			let ip = ent.address.parse::<IpAddr>().ok();
			let pubkey = ent
				.node_meta
				.get("pubkey")
				.and_then(|k| hex::decode(&k).ok())
				.and_then(|k| NodeID::from_slice(&k[..]));
			if let (Some(ip), Some(pubkey)) = (ip, pubkey) {
				ret.push((pubkey, SocketAddr::new(ip, ent.service_port)));
			} else {
				warn!(
					"Could not process node spec from Consul: {:?} (invalid IP or public key)",
					ent
				);
			}
		}
		debug!("Got nodes from Consul: {:?}", ret);

		Ok(ret)
	}

	// ---- PUBLISHING TO CONSUL CATALOG ----

	pub async fn publish_consul_service(
		&self,
		node_id: NodeID,
		hostname: &str,
		rpc_public_addr: SocketAddr,
	) -> Result<(), ConsulError> {
		let node = format!("garage:{}", hex::encode(&node_id[..8]));

		let advertisement = ConsulPublishEntry {
			node: node.clone(),
			address: rpc_public_addr.ip(),
			node_meta: [
				("pubkey".to_string(), hex::encode(node_id)),
				("hostname".to_string(), hostname.to_string()),
			]
			.iter()
			.cloned()
			.collect(),
			service: ConsulPublishService {
				service_id: node.clone(),
				service_name: self.config.service_name.clone(),
				tags: vec!["advertised-by-garage".into(), hostname.into()],
				address: rpc_public_addr.ip(),
				port: rpc_public_addr.port(),
			},
		};

		let url = format!("{}/v1/catalog/register", self.config.consul_http_addr);

		let http = self.client.put(&url).json(&advertisement).send().await?;
		http.error_for_status()?;

		Ok(())
	}
}

/// Regroup all Consul discovery errors
#[derive(Debug, Error)]
pub enum ConsulError {
	#[error(display = "IO error: {}", _0)]
	Io(#[error(source)] std::io::Error),
	#[error(display = "HTTP error: {}", _0)]
	Reqwest(#[error(source)] reqwest::Error),
	#[error(display = "Invalid Consul TLS configuration")]
	InvalidTLSConfig,
}
