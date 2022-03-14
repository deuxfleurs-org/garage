use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};

use hyper::client::Client;
use hyper::StatusCode;
use hyper::{Body, Method, Request};
use serde::{Deserialize, Serialize};

use netapp::NodeID;

use garage_util::error::Error;

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
	consul_host: &str,
	consul_service_name: &str,
) -> Result<Vec<(NodeID, SocketAddr)>, Error> {
	let url = format!(
		"http://{}/v1/catalog/service/{}",
		consul_host, consul_service_name
	);
	let req = Request::builder()
		.uri(url)
		.method(Method::GET)
		.body(Body::default())?;

	let client = Client::new();

	let resp = client.request(req).await?;
	if resp.status() != StatusCode::OK {
		return Err(Error::Message(format!("HTTP error {}", resp.status())));
	}

	let body = hyper::body::to_bytes(resp.into_body()).await?;
	let entries = serde_json::from_slice::<Vec<ConsulQueryEntry>>(body.as_ref())?;

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
	consul_host: &str,
	consul_service_name: &str,
	node_id: NodeID,
	hostname: &str,
	rpc_public_addr: SocketAddr,
) -> Result<(), Error> {
	let node = format!("garage:{}", hex::encode(&node_id[..8]));

	let advertisment = ConsulPublishEntry {
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
			service_name: consul_service_name.to_string(),
			tags: vec!["advertised-by-garage".into(), hostname.into()],
			address: rpc_public_addr.ip(),
			port: rpc_public_addr.port(),
		},
	};

	let url = format!("http://{}/v1/catalog/register", consul_host);
	let req_body = serde_json::to_string(&advertisment)?;
	debug!("Request body for consul adv: {}", req_body);

	let req = Request::builder()
		.uri(url)
		.method(Method::PUT)
		.body(Body::from(req_body))?;

	let client = Client::new();

	let resp = client.request(req).await?;
	debug!("Response of advertising to Consul: {:?}", resp);
	let resp_code = resp.status();
	let resp_bytes = &hyper::body::to_bytes(resp.into_body()).await?;
	debug!(
		"{}",
		std::str::from_utf8(resp_bytes).unwrap_or("<invalid utf8>")
	);

	if resp_code != StatusCode::OK {
		return Err(Error::Message(format!("HTTP error {}", resp_code)));
	}

	Ok(())
}
