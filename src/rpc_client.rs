use std::borrow::Borrow;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::IntoBuf;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt;
use futures_util::future::FutureExt;
use hyper::client::{Client, HttpConnector};
use hyper::{Body, Method, Request};
use tokio::sync::{watch, Semaphore};

use crate::background::BackgroundRunner;
use crate::data::*;
use crate::error::Error;
use crate::membership::Status;
use crate::rpc_server::RpcMessage;
use crate::server::TlsConfig;
use crate::tls_util;

pub struct RpcClient<M: RpcMessage> {
	status: watch::Receiver<Arc<Status>>,
	background: Arc<BackgroundRunner>,

	pub rpc_addr_client: RpcAddrClient<M>,
}

impl<M: RpcMessage + 'static> RpcClient<M> {
	pub fn new(
		rac: RpcAddrClient<M>,
		background: Arc<BackgroundRunner>,
		status: watch::Receiver<Arc<Status>>,
	) -> Arc<Self> {
		Arc::new(Self {
			rpc_addr_client: rac,
			background,
			status,
		})
	}

	pub fn by_addr(&self) -> &RpcAddrClient<M> {
		&self.rpc_addr_client
	}

	pub async fn call<MB: Borrow<M>, N: Borrow<UUID>>(
		&self,
		to: N,
		msg: MB,
		timeout: Duration,
	) -> Result<M, Error> {
		let addr = {
			let status = self.status.borrow().clone();
			match status.nodes.get(to.borrow()) {
				Some(status) => status.addr,
				None => {
					return Err(Error::Message(format!(
						"Peer ID not found: {:?}",
						to.borrow()
					)))
				}
			}
		};
		self.rpc_addr_client.call(&addr, msg, timeout).await
	}

	pub async fn call_many(&self, to: &[UUID], msg: M, timeout: Duration) -> Vec<Result<M, Error>> {
		let msg = Arc::new(msg);
		let mut resp_stream = to
			.iter()
			.map(|to| self.call(to, msg.clone(), timeout))
			.collect::<FuturesUnordered<_>>();

		let mut results = vec![];
		while let Some(resp) = resp_stream.next().await {
			results.push(resp);
		}
		results
	}

	pub async fn try_call_many(
		self: &Arc<Self>,
		to: &[UUID],
		msg: M,
		stop_after: usize,
		timeout: Duration,
	) -> Result<Vec<M>, Error> {
		let msg = Arc::new(msg);
		let mut resp_stream = to
			.to_vec()
			.into_iter()
			.map(|to| {
				let self2 = self.clone();
				let msg = msg.clone();
				async move { self2.call(to, msg, timeout).await }
			})
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
			self.clone().background.spawn_cancellable(async move {
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
}

pub struct RpcAddrClient<M: RpcMessage> {
	phantom: PhantomData<M>,

	pub http_client: Arc<RpcHttpClient>,
	pub path: String,
}

impl<M: RpcMessage> RpcAddrClient<M> {
	pub fn new(http_client: Arc<RpcHttpClient>, path: String) -> Self {
		Self {
			phantom: PhantomData::default(),
			http_client: http_client,
			path,
		}
	}

	pub async fn call<MB>(
		&self,
		to_addr: &SocketAddr,
		msg: MB,
		timeout: Duration,
	) -> Result<M, Error>
	where
		MB: Borrow<M>,
	{
		self.http_client
			.call(&self.path, to_addr, msg, timeout)
			.await
	}
}

pub struct RpcHttpClient {
	request_limiter: Semaphore,
	method: ClientMethod,
}

enum ClientMethod {
	HTTP(Client<HttpConnector, hyper::Body>),
	HTTPS(Client<tls_util::HttpsConnectorFixedDnsname<HttpConnector>, hyper::Body>),
}

impl RpcHttpClient {
	pub fn new(
		max_concurrent_requests: usize,
		tls_config: &Option<TlsConfig>,
	) -> Result<Self, Error> {
		let method = if let Some(cf) = tls_config {
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

			ClientMethod::HTTPS(Client::builder().build(connector))
		} else {
			ClientMethod::HTTP(Client::new())
		};
		Ok(RpcHttpClient {
			method,
			request_limiter: Semaphore::new(max_concurrent_requests),
		})
	}

	async fn call<M, MB>(
		&self,
		path: &str,
		to_addr: &SocketAddr,
		msg: MB,
		timeout: Duration,
	) -> Result<M, Error>
	where
		MB: Borrow<M>,
		M: RpcMessage,
	{
		let uri = match self.method {
			ClientMethod::HTTP(_) => format!("http://{}/{}", to_addr, path),
			ClientMethod::HTTPS(_) => format!("https://{}/{}", to_addr, path),
		};

		let req = Request::builder()
			.method(Method::POST)
			.uri(uri)
			.body(Body::from(rmp_to_vec_all_named(msg.borrow())?))?;

		let resp_fut = match &self.method {
			ClientMethod::HTTP(client) => client.request(req).fuse(),
			ClientMethod::HTTPS(client) => client.request(req).fuse(),
		};

		let slot = self.request_limiter.acquire().await;
		let resp = tokio::time::timeout(timeout, resp_fut)
			.await
			.map_err(|e| {
				debug!(
					"RPC timeout to {}: {}",
					to_addr,
					debug_serialize(msg.borrow())
				);
				e
			})?
			.map_err(|e| {
				warn!(
					"RPC HTTP client error when connecting to {}: {}",
					to_addr, e
				);
				e
			})?;
		drop(slot);

		let status = resp.status();
		let body = hyper::body::to_bytes(resp.into_body()).await?;
		match rmp_serde::decode::from_read::<_, Result<M, String>>(body.into_buf()) {
			Err(e) => Err(Error::RPCError(
				format!("Invalid reply (deserialize error: {})", e),
				status,
			)),
			Ok(Err(e)) => Err(Error::RPCError(e, status)),
			Ok(Ok(x)) => Ok(x),
		}
	}
}
