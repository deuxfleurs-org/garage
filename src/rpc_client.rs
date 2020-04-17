use std::borrow::Borrow;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::IntoBuf;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt;
use futures_util::future::FutureExt;
use hyper::client::{Client, HttpConnector};
use hyper::{Body, Method, Request, StatusCode};

use crate::data::*;
use crate::error::Error;
use crate::membership::System;
use crate::proto::Message;
use crate::server::*;
use crate::tls_util;

pub async fn rpc_call_many(
	sys: Arc<System>,
	to: &[UUID],
	msg: Message,
	timeout: Duration,
) -> Vec<Result<Message, Error>> {
	let msg = Arc::new(msg);
	let mut resp_stream = to
		.iter()
		.map(|to| rpc_call(sys.clone(), to, msg.clone(), timeout))
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
	msg: Message,
	stop_after: usize,
	timeout: Duration,
) -> Result<Vec<Message>, Error> {
	let sys2 = sys.clone();
	let msg = Arc::new(msg);
	let mut resp_stream = to
		.to_vec()
		.into_iter()
		.map(move |to| rpc_call(sys2.clone(), to.clone(), msg.clone(), timeout))
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
		// Continue requests in background
		// TODO: make this optionnal (only usefull for write requests)
		sys.background.spawn(async move {
			resp_stream.collect::<Vec<_>>().await;
			Ok(())
		});

		Ok(results)
	} else {
		let mut msg = "Too many failures:".to_string();
		for e in errors {
			msg += &format!("\n{}", e);
		}
		Err(Error::Message(msg))
	}
}

pub async fn rpc_call<M: Borrow<Message>, N: Borrow<UUID>>(
	sys: Arc<System>,
	to: N,
	msg: M,
	timeout: Duration,
) -> Result<Message, Error> {
	let addr = {
		let status = sys.status.borrow().clone();
		match status.nodes.get(to.borrow()) {
			Some(status) => status.addr.clone(),
			None => {
				return Err(Error::Message(format!(
					"Peer ID not found: {:?}",
					to.borrow()
				)))
			}
		}
	};
	sys.rpc_client.call(&addr, msg, timeout).await
}

pub enum RpcClient {
	HTTP(Client<HttpConnector, hyper::Body>),
	HTTPS(Client<tls_util::HttpsConnectorFixedDnsname<HttpConnector>, hyper::Body>),
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

			config.set_single_client_cert([&node_certs[..], &ca_certs[..]].concat(), node_key)?;

			let connector =
				tls_util::HttpsConnectorFixedDnsname::<HttpConnector>::new(config, "garage");

			Ok(RpcClient::HTTPS(Client::builder().build(connector)))
		} else {
			Ok(RpcClient::HTTP(Client::new()))
		}
	}

	pub async fn call<M: Borrow<Message>>(
		&self,
		to_addr: &SocketAddr,
		msg: M,
		timeout: Duration,
	) -> Result<Message, Error> {
		let uri = match self {
			RpcClient::HTTP(_) => format!("http://{}/rpc", to_addr),
			RpcClient::HTTPS(_) => format!("https://{}/rpc", to_addr),
		};

		let req = Request::builder()
			.method(Method::POST)
			.uri(uri)
			.body(Body::from(rmp_to_vec_all_named(msg.borrow())?))?;

		let resp_fut = match self {
			RpcClient::HTTP(client) => client.request(req).fuse(),
			RpcClient::HTTPS(client) => client.request(req).fuse(),
		};
		let resp = tokio::time::timeout(timeout, resp_fut)
			.await?
			.map_err(|e| {
				eprintln!(
					"RPC HTTP client error when connecting to {}: {}",
					to_addr, e
				);
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
