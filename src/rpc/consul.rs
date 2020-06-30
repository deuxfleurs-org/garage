use std::net::{IpAddr, SocketAddr};

use hyper::client::Client;
use hyper::StatusCode;
use hyper::{Body, Method, Request};
use serde::Deserialize;

use garage_util::error::Error;

#[derive(Deserialize, Clone)]
struct ConsulEntry {
	#[serde(alias = "Address")]
	address: String,
	#[serde(alias = "ServicePort")]
	service_port: u16,
}

pub async fn get_consul_nodes(
	consul_host: &str,
	consul_service_name: &str,
) -> Result<Vec<SocketAddr>, Error> {
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
	let entries = serde_json::from_slice::<Vec<ConsulEntry>>(body.as_ref())?;

	let mut ret = vec![];
	for ent in entries {
		let ip = ent
			.address
			.parse::<IpAddr>()
			.map_err(|e| Error::Message(format!("Could not parse IP address: {}", e)))?;
		ret.push(SocketAddr::new(ip, ent.service_port));
	}
	debug!("Got nodes from Consul: {:?}", ret);

	Ok(ret)
}
