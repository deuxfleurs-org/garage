use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::net::{IpAddr, SocketAddr};

use serde::{Deserialize, Serialize};
use thiserror::Error;

use garage_net::NodeID;

use garage_util::config::ConsulDiscoveryAPI;
use garage_util::config::ConsulDiscoveryConfig;

const META_PREFIX: &str = "fr-deuxfleurs-garagehq";

#[derive(Deserialize, Clone, Debug)]
struct ConsulQueryEntry {
	#[serde(rename = "Address")]
	address: String,
	#[serde(rename = "ServicePort")]
	service_port: u16,
	#[serde(rename = "ServiceMeta")]
	meta: HashMap<String, String>,
}

#[derive(Serialize, Clone, Debug)]
#[serde(untagged)]
enum PublishRequest {
	Catalog(ConsulPublishEntry),
	Service(ConsulPublishService),
}

#[derive(Serialize, Clone, Debug)]
struct ConsulPublishEntry {
	#[serde(rename = "Node")]
	node: String,
	#[serde(rename = "Address")]
	address: IpAddr,
	#[serde(rename = "Service")]
	service: ConsulPublishCatalogService,
}

#[derive(Serialize, Clone, Debug)]
struct ConsulPublishCatalogService {
	#[serde(rename = "ID")]
	service_id: String,
	#[serde(rename = "Service")]
	service_name: String,
	#[serde(rename = "Tags")]
	tags: Vec<String>,
	#[serde(rename = "Meta")]
	meta: HashMap<String, String>,
	#[serde(rename = "Address")]
	address: IpAddr,
	#[serde(rename = "Port")]
	port: u16,
}

#[derive(Serialize, Clone, Debug)]
struct ConsulPublishService {
	#[serde(rename = "ID")]
	service_id: String,
	#[serde(rename = "Name")]
	service_name: String,
	#[serde(rename = "Tags")]
	tags: Vec<String>,
	#[serde(rename = "Address")]
	address: IpAddr,
	#[serde(rename = "Port")]
	port: u16,
	#[serde(rename = "Meta")]
	meta: HashMap<String, String>,
}

// ----
pub struct ConsulDiscovery {
	config: ConsulDiscoveryConfig,
	client: reqwest::Client,
}

impl ConsulDiscovery {
	pub fn new(config: ConsulDiscoveryConfig) -> Result<Self, ConsulError> {
		let mut builder: reqwest::ClientBuilder = reqwest::Client::builder().use_rustls_tls();
		if config.tls_skip_verify {
			builder = builder.danger_accept_invalid_certs(true);
		} else if let Some(ca_cert) = &config.ca_cert {
			let mut ca_cert_buf = vec![];
			File::open(ca_cert)?.read_to_end(&mut ca_cert_buf)?;
			builder =
				builder.add_root_certificate(reqwest::Certificate::from_pem(&ca_cert_buf[..])?);
		}

		match &config.api {
			ConsulDiscoveryAPI::Catalog => match (&config.client_cert, &config.client_key) {
				(Some(client_cert), Some(client_key)) => {
					let mut client_cert_buf = vec![];
					File::open(client_cert)?.read_to_end(&mut client_cert_buf)?;

					let mut client_key_buf = vec![];
					File::open(client_key)?.read_to_end(&mut client_key_buf)?;

					let identity = reqwest::Identity::from_pem(
						&[&client_cert_buf[..], &client_key_buf[..]].concat()[..],
					)?;

					builder = builder.identity(identity);
				}
				(None, None) => {}
				_ => return Err(ConsulError::InvalidTLSConfig),
			},
			ConsulDiscoveryAPI::Agent => {}
		}

		if let Some(token) = &config.token {
			let mut headers = reqwest::header::HeaderMap::new();
			headers.insert(
				"x-consul-token",
				reqwest::header::HeaderValue::from_str(token.extract_secret())?,
			);
			builder = builder.default_headers(headers);
		}

		let client: reqwest::Client = builder.build()?;

		Ok(Self { client, config })
	}

	// ---- READING FROM CONSUL CATALOG ----
	/// Query Consul for Garage nodes registered under the configured service name.
	///
	/// This method supports querying multiple Consul datacenters for WAN or
	/// multi-datacenter deployments. If `config.datacenters` is set and non-empty,
	/// each listed datacenter is queried and the results are aggregated. Otherwise,
	/// only the local datacenter is queried. `config.datacenters` does not need to be set
	/// when all the datacenters are on the same LAN, in this case service discovery works normally
	///
	/// # Returns
	/// A list of `(NodeID, SocketAddr)` pairs corresponding to all valid discovered
	/// nodes across the queried datacenters.
	pub async fn get_consul_nodes(&self) -> Result<Vec<(NodeID, SocketAddr)>, ConsulError> {
		let mut ret = vec![];

		let dcs_to_query: Vec<Option<&str>> = match &self.config.datacenters {
			dcs if !dcs.is_empty() => dcs.iter().map(|dc| Some(dc.as_str())).collect(),
			_ => vec![None],
		};

		for dc in dcs_to_query {
			let url = match dc {
				Some(datacenter) => format!(
					"{}/v1/catalog/service/{}?dc={}",
					self.config.consul_http_addr, self.config.service_name, datacenter
				),
				None => format!(
					"{}/v1/catalog/service/{}",
					self.config.consul_http_addr, self.config.service_name
				),
			};

			let http = self.client.get(&url).send().await?;
			let entries: Vec<ConsulQueryEntry> = http.json().await?;

			for ent in entries {
				let ip = ent.address.parse::<IpAddr>().ok();
				let pubkey = ent
					.meta
					.get(&format!("{}-pubkey", META_PREFIX))
					.and_then(|k| hex::decode(k).ok())
					.and_then(|k| NodeID::from_slice(&k[..]));
				if let (Some(ip), Some(pubkey)) = (ip, pubkey) {
					ret.push((pubkey, SocketAddr::new(ip, ent.service_port)));
				} else {
					warn!(
						"Could not process node spec from Consul: {:?} (invalid IP address or node ID/pubkey)",
						ent
					);
				}
			}
		}

		debug!("Got {} nodes from Consul", ret.len());
		Ok(ret)
	}
	// ---- PUBLISHING TO CONSUL CATALOG ----

	#[cfg(feature = "consul-discovery")]
	pub async fn deregister_consul_service(&self, node_id: NodeID) -> Result<(), ConsulError> {
		let node = format!("garage:{}", hex::encode(&node_id[..8]));
		let url = format!(
			"{}/v1/{}",
			self.config.consul_http_addr,
			(match &self.config.api {
				ConsulDiscoveryAPI::Catalog => format!("catalog/deregister"),
				ConsulDiscoveryAPI::Agent => format!("agent/service/deregister/{}", node),
			})
		);

		let req = self.client.put(&url);

		let http = if matches!(&self.config.api, ConsulDiscoveryAPI::Catalog) {
			let deregister_request = serde_json::json!({
				"Node": node,
				"ServiceID": node,
			});
			let req = req.json(&deregister_request);
			req.send().await?
		} else {
			req.send().await?
		};
		http.error_for_status()?;

		debug!("Deregistered service {} from Consul", node);
		Ok(())
	}

	pub async fn publish_consul_service(
		&self,
		node_id: NodeID,
		hostname: &str,
		rpc_public_addr: SocketAddr,
	) -> Result<(), ConsulError> {
		let node = format!("garage:{}", hex::encode(&node_id[..8]));
		let tags = [
			vec!["advertised-by-garage".into(), hostname.into()],
			self.config.tags.clone(),
		]
		.concat();

		let mut meta = self.config.meta.clone().unwrap_or_default();
		meta.insert(format!("{}-pubkey", META_PREFIX), hex::encode(node_id));
		meta.insert(format!("{}-hostname", META_PREFIX), hostname.to_string());

		let url = format!(
			"{}/v1/{}",
			self.config.consul_http_addr,
			(match &self.config.api {
				ConsulDiscoveryAPI::Catalog => "catalog/register",
				ConsulDiscoveryAPI::Agent => "agent/service/register?replace-existing-checks",
			})
		);

		let req = self.client.put(&url);
		let advertisement: PublishRequest = match &self.config.api {
			ConsulDiscoveryAPI::Catalog => PublishRequest::Catalog(ConsulPublishEntry {
				node: node.clone(),
				address: rpc_public_addr.ip(),
				service: ConsulPublishCatalogService {
					service_id: node.clone(),
					service_name: self.config.service_name.clone(),
					tags,
					meta: meta.clone(),
					address: rpc_public_addr.ip(),
					port: rpc_public_addr.port(),
				},
			}),
			ConsulDiscoveryAPI::Agent => PublishRequest::Service(ConsulPublishService {
				service_id: node.clone(),
				service_name: self.config.service_name.clone(),
				tags,
				meta,
				address: rpc_public_addr.ip(),
				port: rpc_public_addr.port(),
			}),
		};
		let http = req.json(&advertisement).send().await?;
		http.error_for_status()?;

		Ok(())
	}
}

/// Regroup all Consul discovery errors
#[derive(Debug, Error)]
pub enum ConsulError {
	#[error("IO error: {0}")]
	Io(#[from] std::io::Error),
	#[error("HTTP error: {0}")]
	Reqwest(#[from] reqwest::Error),
	#[error("Invalid Consul TLS configuration")]
	InvalidTLSConfig,
	#[error("Token error: {0}")]
	Token(#[from] reqwest::header::InvalidHeaderValue),
}
