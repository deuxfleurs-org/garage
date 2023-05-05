use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::net::{IpAddr, SocketAddr};

use err_derive::Error;
use serde::{Deserialize, Serialize};

use netapp::NodeID;

use garage_util::config::ConsulServiceConfig;

const META_PREFIX: &str = "fr-deuxfleurs-garagehq";

#[derive(Deserialize, Clone, Debug)]
struct ConsulQueryEntry {
	#[serde(rename = "Address")]
	address: String,
	#[serde(rename = "ServicePort")]
	service_port: u16,
	#[serde(rename = "ServiceMeta")]
	service_meta: HashMap<String, String>,
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

pub struct ConsulServiceDiscovery {
	config: ConsulServiceConfig,
	client: reqwest::Client,
}

impl ConsulServiceDiscovery {
	pub fn new(config: ConsulServiceConfig) -> Result<Self, ConsulError> {
		let mut builder: reqwest::ClientBuilder = match &config.ca_cert {
			Some(client_ca) => {
				let mut ca_cert_buf = vec![];
				File::open(client_ca)?.read_to_end(&mut ca_cert_buf)?;

				let req: reqwest::ClientBuilder = reqwest::Client::builder()
					.add_root_certificate(reqwest::Certificate::from_pem(&ca_cert_buf[..])?)
					.use_rustls_tls();

				if config.tls_skip_verify {
					req.danger_accept_invalid_certs(true)
				} else {
					req
				}
			}
			None => reqwest::Client::builder(),
		};

		if let Some(token) = &config.consul_http_token {
			let mut headers = reqwest::header::HeaderMap::new();
			headers.insert("x-consul-token", reqwest::header::HeaderValue::from_str(&token)?);
			builder = builder.default_headers(headers);
		}

		let client = builder.build()?;

		Ok(Self { client, config })
	}

	// ---- READING FROM CONSUL CATALOG ----

	pub async fn get_consul_services(&self) -> Result<Vec<(NodeID, SocketAddr)>, ConsulError> {
		let url = format!(
			"{}/v1/catalog/service/{}",
			self.config.consul_http_addr, self.config.service_name
		);

		let req = self.client.get(&url);
		let http = req.send().await?;
		let entries: Vec<ConsulQueryEntry> = http.json().await?;

		let mut ret = vec![];
		for ent in entries {
			let ip = ent.address.parse::<IpAddr>().ok();
			let pubkey = ent
				.service_meta
				.get(&format!("{}-pubkey", META_PREFIX))
				.and_then(|k| hex::decode(k).ok())
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

		let tags = [
			vec!["advertised-by-garage".into(), hostname.into()],
			self.config.tags.clone(),
		]
		.concat();

		let advertisement: ConsulPublishService = ConsulPublishService {
			service_id: node.clone(),
			service_name: self.config.service_name.clone(),
			tags,
			meta: [
				(format!("{}-pubkey", META_PREFIX), hex::encode(node_id)),
				(format!("{}-hostname", META_PREFIX), hostname.to_string()),
			]
			.iter()
			.cloned()
			.collect(),
			address: rpc_public_addr.ip(),
			port: rpc_public_addr.port(),
		};

		let url = format!(
			"{}/v1/agent/service/register?replace-existing-checks",
			self.config.consul_http_addr
		);

		let req = self.client.put(&url);
		let http = req.json(&advertisement).send().await?;
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
	#[error(display = "Invalid HTTP header error: {}", _0)]
	HeaderValue(#[error(source)] reqwest::header::InvalidHeaderValue),
}
