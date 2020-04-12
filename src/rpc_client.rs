use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::IntoBuf;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt;
use futures_util::future::FutureExt;
use hyper::client::{Client, HttpConnector};
use hyper::{Body, Method, Request, StatusCode};
use hyper_rustls::HttpsConnector;

use crate::data::*;
use crate::error::Error;
use crate::membership::System;
use crate::proto::Message;
use crate::server::*;
use crate::tls_util;

pub async fn rpc_call_many(
	sys: Arc<System>,
	to: &[UUID],
	msg: &Message,
	timeout: Duration,
) -> Vec<Result<Message, Error>> {
	let mut resp_stream = to
		.iter()
		.map(|to| rpc_call(sys.clone(), to, msg, timeout))
		.collect::<FuturesUnordered<_>>();

	let mut results = vec![];
	while let Some(resp) = resp_stream.next().await {
		results.push(resp);
	}
	results
}

pub async fn rpc_try_call_many(
	sys: Arc<System>,
	to: &[UUID],
	msg: &Message,
	stop_after: usize,
	timeout: Duration,
) -> Result<Vec<Message>, Error> {
	let mut resp_stream = to
		.iter()
		.map(|to| rpc_call(sys.clone(), to, msg, timeout))
		.collect::<FuturesUnordered<_>>();

	let mut results = vec![];
	let mut errors = vec![];

	while let Some(resp) = resp_stream.next().await {
		match resp {
			Ok(msg) => {
				results.push(msg);
				if results.len() >= stop_after {
					break;
				}
			}
			Err(e) => {
				errors.push(e);
			}
		}
	}

	if results.len() >= stop_after {
		Ok(results)
	} else {
		let mut msg = "Too many failures:".to_string();
		for e in errors {
			msg += &format!("\n{}", e);
		}
		Err(Error::Message(msg))
	}
}

pub async fn rpc_call(
	sys: Arc<System>,
	to: &UUID,
	msg: &Message,
	timeout: Duration,
) -> Result<Message, Error> {
	let addr = {
		let status = sys.status.borrow().clone();
		match status.nodes.get(to) {
			Some(status) => status.addr.clone(),
			None => return Err(Error::Message(format!("Peer ID not found"))),
		}
	};
	sys.rpc_client.call(&addr, msg, timeout).await
}

pub enum RpcClient {
	HTTP(Client<HttpConnector, hyper::Body>),
	HTTPS(Client<HttpsConnector<HttpConnector>, hyper::Body>),
}

impl RpcClient {
	pub fn new(tls_config: &Option<TlsConfig>) -> Result<Self, Error> {
		if let Some(cf) = tls_config {
			let ca_certs = tls_util::load_certs(&cf.ca_cert)?;
			let node_certs = tls_util::load_certs(&cf.node_cert)?;
			let node_key = tls_util::load_private_key(&cf.node_key)?;

			let mut config = rustls::ClientConfig::new();

			for crt in ca_certs.iter() {
				config.root_store.add(crt)?;
			}

			config.set_single_client_cert([&ca_certs[..], &node_certs[..]].concat(), node_key)?;

			let mut http_connector = HttpConnector::new();
			http_connector.enforce_http(false);
			let connector =
				HttpsConnector::<HttpConnector>::from((http_connector, Arc::new(config)));

			Ok(RpcClient::HTTPS(Client::builder().build(connector)))
		} else {
			Ok(RpcClient::HTTP(Client::new()))
		}
	}

	pub async fn call(
		&self,
		to_addr: &SocketAddr,
		msg: &Message,
		timeout: Duration,
	) -> Result<Message, Error> {
		let uri = match self {
			RpcClient::HTTP(_) => format!("http://{}/rpc", to_addr),
			RpcClient::HTTPS(_) => format!("https://{}/rpc", to_addr),
		};

		let req = Request::builder()
			.method(Method::POST)
			.uri(uri)
			.body(Body::from(rmp_to_vec_all_named(msg)?))?;

		let resp_fut = match self {
			RpcClient::HTTP(client) => client.request(req).fuse(),
			RpcClient::HTTPS(client) => client.request(req).fuse(),
		};
		let resp = tokio::time::timeout(timeout, resp_fut)
			.await?
			.map_err(|e| {
				eprintln!("RPC client error: {}", e);
				e
			})?;

		if resp.status() == StatusCode::OK {
			let body = hyper::body::to_bytes(resp.into_body()).await?;
			let msg = rmp_serde::decode::from_read::<_, Message>(body.into_buf())?;
			match msg {
				Message::Error(e) => Err(Error::RPCError(e)),
				x => Ok(x),
			}
		} else {
			Err(Error::RPCError(format!("Status code {}", resp.status())))
		}
	}
}
