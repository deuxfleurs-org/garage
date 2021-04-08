//! Contain structs related to making RPCs
use std::borrow::Borrow;
use std::marker::PhantomData;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use arc_swap::ArcSwapOption;
use futures::future::Future;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt;
use futures_util::future::FutureExt;
use hyper::client::{Client, HttpConnector};
use hyper::{Body, Method, Request};
use tokio::sync::{watch, Semaphore};

use garage_util::background::BackgroundRunner;
use garage_util::config::TlsConfig;
use garage_util::data::*;
use garage_util::error::{Error, RPCError};

use crate::membership::Status;
use crate::rpc_server::RpcMessage;
use crate::tls_util;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

/// Strategy to apply when making RPC
#[derive(Copy, Clone)]
pub struct RequestStrategy {
	/// Max time to wait for reponse
	pub rs_timeout: Duration,
	/// Min number of response to consider the request successful
	pub rs_quorum: usize,
	/// Should requests be dropped after enough response are received
	pub rs_interrupt_after_quorum: bool,
}

impl RequestStrategy {
	/// Create a RequestStrategy with default timeout and not interrupting when quorum reached
	pub fn with_quorum(quorum: usize) -> Self {
		RequestStrategy {
			rs_timeout: DEFAULT_TIMEOUT,
			rs_quorum: quorum,
			rs_interrupt_after_quorum: false,
		}
	}
	/// Set timeout of the strategy
	pub fn with_timeout(mut self, timeout: Duration) -> Self {
		self.rs_timeout = timeout;
		self
	}
	/// Set if requests can be dropped after quorum has been reached
	/// In general true for read requests, and false for write
	pub fn interrupt_after_quorum(mut self, interrupt: bool) -> Self {
		self.rs_interrupt_after_quorum = interrupt;
		self
	}
}

/// Shortcut for a boxed async function taking a message, and resolving to another message or an
/// error
pub type LocalHandlerFn<M> =
	Box<dyn Fn(Arc<M>) -> Pin<Box<dyn Future<Output = Result<M, Error>> + Send>> + Send + Sync>;

/// Client used to send RPC
pub struct RpcClient<M: RpcMessage> {
	status: watch::Receiver<Arc<Status>>,
	background: Arc<BackgroundRunner>,

	local_handler: ArcSwapOption<(UUID, LocalHandlerFn<M>)>,

	rpc_addr_client: RpcAddrClient<M>,
}

impl<M: RpcMessage + 'static> RpcClient<M> {
	/// Create a new RpcClient from an address, a job runner, and the status of all RPC servers
	pub fn new(
		rac: RpcAddrClient<M>,
		background: Arc<BackgroundRunner>,
		status: watch::Receiver<Arc<Status>>,
	) -> Arc<Self> {
		Arc::new(Self {
			rpc_addr_client: rac,
			background,
			status,
			local_handler: ArcSwapOption::new(None),
		})
	}

	/// Set the local handler, to process RPC to this node without network usage
	pub fn set_local_handler<F, Fut>(&self, my_id: UUID, handler: F)
	where
		F: Fn(Arc<M>) -> Fut + Send + Sync + 'static,
		Fut: Future<Output = Result<M, Error>> + Send + 'static,
	{
		let handler_arc = Arc::new(handler);
		let handler: LocalHandlerFn<M> = Box::new(move |msg| {
			let handler_arc2 = handler_arc.clone();
			Box::pin(async move { handler_arc2(msg).await })
		});
		self.local_handler.swap(Some(Arc::new((my_id, handler))));
	}

	/// Get a RPC client to make calls using node's SocketAddr instead of its ID
	pub fn by_addr(&self) -> &RpcAddrClient<M> {
		&self.rpc_addr_client
	}

	/// Make a RPC call
	pub async fn call(&self, to: UUID, msg: M, timeout: Duration) -> Result<M, Error> {
		self.call_arc(to, Arc::new(msg), timeout).await
	}

	/// Make a RPC call from a message stored in an Arc
	pub async fn call_arc(&self, to: UUID, msg: Arc<M>, timeout: Duration) -> Result<M, Error> {
		if let Some(lh) = self.local_handler.load_full() {
			let (my_id, local_handler) = lh.as_ref();
			if to.borrow() == my_id {
				return local_handler(msg).await;
			}
		}
		let status = self.status.borrow().clone();
		let node_status = match status.nodes.get(&to) {
			Some(node_status) => {
				if node_status.is_up() {
					node_status
				} else {
					return Err(Error::from(RPCError::NodeDown(to)));
				}
			}
			None => {
				return Err(Error::Message(format!(
					"Peer ID not found: {:?}",
					to.borrow()
				)))
			}
		};
		match self
			.rpc_addr_client
			.call(&node_status.addr, msg, timeout)
			.await
		{
			Err(rpc_error) => {
				node_status.num_failures.fetch_add(1, Ordering::SeqCst);
				Err(Error::from(rpc_error))
			}
			Ok(x) => x,
		}
	}

	/// Make a RPC call to multiple servers, returning a Vec containing each result
	pub async fn call_many(&self, to: &[UUID], msg: M, timeout: Duration) -> Vec<Result<M, Error>> {
		let msg = Arc::new(msg);
		let mut resp_stream = to
			.iter()
			.map(|to| self.call_arc(*to, msg.clone(), timeout))
			.collect::<FuturesUnordered<_>>();

		let mut results = vec![];
		while let Some(resp) = resp_stream.next().await {
			results.push(resp);
		}
		results
	}

	/// Make a RPC call to multiple servers, returning either a Vec of responses, or an error if
	/// strategy could not be respected due to too many errors
	pub async fn try_call_many(
		self: &Arc<Self>,
		to: &[UUID],
		msg: M,
		strategy: RequestStrategy,
	) -> Result<Vec<M>, Error> {
		let timeout = strategy.rs_timeout;

		let msg = Arc::new(msg);
		let mut resp_stream = to
			.to_vec()
			.into_iter()
			.map(|to| {
				let self2 = self.clone();
				let msg = msg.clone();
				async move { self2.call_arc(to, msg, timeout).await }
			})
			.collect::<FuturesUnordered<_>>();

		let mut results = vec![];
		let mut errors = vec![];

		while let Some(resp) = resp_stream.next().await {
			match resp {
				Ok(msg) => {
					results.push(msg);
					if results.len() >= strategy.rs_quorum {
						break;
					}
				}
				Err(e) => {
					errors.push(e);
				}
			}
		}

		if results.len() >= strategy.rs_quorum {
			// Continue requests in background.
			// Continue the remaining requests immediately using tokio::spawn
			// but enqueue a task in the background runner
			// to ensure that the process won't exit until the requests are done
			// (if we had just enqueued the resp_stream.collect directly in the background runner,
			// the requests might have been put on hold in the background runner's queue,
			// in which case they might timeout or otherwise fail)
			if !strategy.rs_interrupt_after_quorum {
				let wait_finished_fut = tokio::spawn(async move {
					resp_stream.collect::<Vec<_>>().await;
				});
				self.background.spawn(wait_finished_fut.map(|_| Ok(())));
			}

			Ok(results)
		} else {
			let errors = errors.iter().map(|e| format!("{}", e)).collect::<Vec<_>>();
			Err(Error::from(RPCError::TooManyErrors(errors)))
		}
	}
}

/// Thin wrapper arround an `RpcHttpClient` specifying the path of the request
pub struct RpcAddrClient<M: RpcMessage> {
	phantom: PhantomData<M>,

	http_client: Arc<RpcHttpClient>,
	path: String,
}

impl<M: RpcMessage> RpcAddrClient<M> {
	/// Create an RpcAddrClient from an HTTP client and the endpoint to reach for RPCs
	pub fn new(http_client: Arc<RpcHttpClient>, path: String) -> Self {
		Self {
			phantom: PhantomData::default(),
			http_client: http_client,
			path,
		}
	}

	/// Make a RPC
	pub async fn call<MB>(
		&self,
		to_addr: &SocketAddr,
		msg: MB,
		timeout: Duration,
	) -> Result<Result<M, Error>, RPCError>
	where
		MB: Borrow<M>,
	{
		self.http_client
			.call(&self.path, to_addr, msg, timeout)
			.await
	}
}

/// HTTP client used to make RPCs
pub struct RpcHttpClient {
	request_limiter: Semaphore,
	method: ClientMethod,
}

enum ClientMethod {
	HTTP(Client<HttpConnector, hyper::Body>),
	HTTPS(Client<tls_util::HttpsConnectorFixedDnsname<HttpConnector>, hyper::Body>),
}

impl RpcHttpClient {
	/// Create a new RpcHttpClient
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

	/// Make a RPC
	async fn call<M, MB>(
		&self,
		path: &str,
		to_addr: &SocketAddr,
		msg: MB,
		timeout: Duration,
	) -> Result<Result<M, Error>, RPCError>
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

		trace!("({}) Acquiring request_limiter slot...", path);
		let slot = self.request_limiter.acquire().await;
		trace!("({}) Got slot, doing request to {}...", path, to_addr);
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

		let status = resp.status();
		trace!("({}) Request returned, got status {}", path, status);
		let body = hyper::body::to_bytes(resp.into_body()).await?;
		drop(slot);

		match rmp_serde::decode::from_read::<_, Result<M, String>>(&body[..])? {
			Err(e) => Ok(Err(Error::RemoteError(e, status))),
			Ok(x) => Ok(Ok(x)),
		}
	}
}
