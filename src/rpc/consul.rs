use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};

use tokio::fs::File;
use tokio::io::AsyncReadExt;

use err_derive::Error;
use serde::{Deserialize, Serialize};

use netapp::NodeID;

use garage_util::config::ConsulDiscoveryConfig;

async fn make_consul_client(
	config: &ConsulDiscoveryConfig,
) -> Result<reqwest::Client, ConsulError> {
	match (&config.client_cert, &config.client_key) {
		(Some(client_cert), Some(client_key)) => {
			let mut client_cert_buf = vec![];
			File::open(client_cert)
				.await?
				.read_to_end(&mut client_cert_buf)
				.await?;

			let mut client_key_buf = vec![];
			File::open(client_key)
				.await?
				.read_to_end(&mut client_key_buf)
				.await?;

			let identity = reqwest::Identity::from_pem(
				&[&client_cert_buf[..], &client_key_buf[..]].concat()[..],
			)?;

			if config.tls_skip_verify {
				Ok(reqwest::Client::builder()
					.use_rustls_tls()
					.danger_accept_invalid_certs(true)
					.identity(identity)
					.build()?)
			} else if let Some(ca_cert) = &config.ca_cert {
				let mut ca_cert_buf = vec![];
				File::open(ca_cert)
					.await?
					.read_to_end(&mut ca_cert_buf)
					.await?;

				Ok(reqwest::Client::builder()
					.use_rustls_tls()
					.add_root_certificate(reqwest::Certificate::from_pem(&ca_cert_buf[..])?)
					.identity(identity)
					.build()?)
			} else {
				Ok(reqwest::Client::builder()
					.use_rustls_tls()
					.identity(identity)
					.build()?)
			}
		}
		(None, None) => Ok(reqwest::Client::new()),
		_ => Err(ConsulError::InvalidTLSConfig),
	}
}

// ---- READING FROM CONSUL CATALOG ----

#[derive(Deserialize, Clone, Debug)]
struct ConsulQueryEntry {
	#[serde(rename = "Address")]
	address: String,
	#[serde(rename = "ServicePort")]
	service_port: u16,
	#[serde(rename = "NodeMeta")]
	node_meta: HashMap<String, String>,
}

pub async fn get_consul_nodes(
	consul_config: &ConsulDiscoveryConfig,
) -> Result<Vec<(NodeID, SocketAddr)>, ConsulError> {
	let url = format!(
		"http://{}/v1/catalog/service/{}",
		consul_config.consul_host, consul_config.service_name
	);

	let client = make_consul_client(consul_config).await?;
	let http = client.get(&url).send().await?;
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

pub async fn publish_consul_service(
	consul_config: &ConsulDiscoveryConfig,
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
			service_name: consul_config.service_name.clone(),
			tags: vec!["advertised-by-garage".into(), hostname.into()],
			address: rpc_public_addr.ip(),
			port: rpc_public_addr.port(),
		},
	};

	let url = format!("http://{}/v1/catalog/register", consul_config.consul_host);

	let client = make_consul_client(consul_config).await?;
	let http = client.put(&url).json(&advertisement).send().await?;
	http.error_for_status()?;

	Ok(())
}

/// Regroup all Garage errors
#[derive(Debug, Error)]
pub enum ConsulError {
	#[error(display = "IO error: {}", _0)]
	Io(#[error(source)] std::io::Error),
	#[error(display = "HTTP error: {}", _0)]
	Reqwest(#[error(source)] reqwest::Error),
	#[error(display = "Invalid Consul TLS configuration")]
	InvalidTLSConfig,
}
