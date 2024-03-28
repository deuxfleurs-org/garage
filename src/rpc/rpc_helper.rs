//! Contain structs related to making RPCs
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt;
use tokio::select;
use tokio::sync::watch;

use opentelemetry::KeyValue;
use opentelemetry::{
	trace::{FutureExt as OtelFutureExt, Span, TraceContextExt, Tracer},
	Context,
};

pub use garage_net::endpoint::{Endpoint, EndpointHandler, StreamingEndpointHandler};
pub use garage_net::message::{
	IntoReq, Message as Rpc, OrderTag, Req, RequestPriority, Resp, PRIO_BACKGROUND, PRIO_HIGH,
	PRIO_NORMAL, PRIO_SECONDARY,
};
use garage_net::peering::PeeringManager;
pub use garage_net::{self, NetApp, NodeID};

use garage_util::data::*;
use garage_util::error::Error;
use garage_util::metrics::RecordDuration;

use crate::metrics::RpcMetrics;
use crate::ring::Ring;

// Default RPC timeout = 5 minutes
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(300);

/// Strategy to apply when making RPC
pub struct RequestStrategy<T> {
	/// Min number of response to consider the request successful
	pub rs_quorum: Option<usize>,
	/// Should requests be dropped after enough response are received
	pub rs_interrupt_after_quorum: bool,
	/// Request priority
	pub rs_priority: RequestPriority,
	/// Custom timeout for this request
	rs_timeout: Timeout,
	/// Data to drop when everything completes
	rs_drop_on_complete: T,
}

#[derive(Copy, Clone)]
enum Timeout {
	None,
	Default,
	Custom(Duration),
}

impl Clone for RequestStrategy<()> {
	fn clone(&self) -> Self {
		RequestStrategy {
			rs_quorum: self.rs_quorum,
			rs_interrupt_after_quorum: self.rs_interrupt_after_quorum,
			rs_priority: self.rs_priority,
			rs_timeout: self.rs_timeout,
			rs_drop_on_complete: (),
		}
	}
}

impl RequestStrategy<()> {
	/// Create a RequestStrategy with default timeout and not interrupting when quorum reached
	pub fn with_priority(prio: RequestPriority) -> Self {
		RequestStrategy {
			rs_quorum: None,
			rs_interrupt_after_quorum: false,
			rs_priority: prio,
			rs_timeout: Timeout::Default,
			rs_drop_on_complete: (),
		}
	}
	/// Add an item to be dropped on completion
	pub fn with_drop_on_completion<T>(self, drop_on_complete: T) -> RequestStrategy<T> {
		RequestStrategy {
			rs_quorum: self.rs_quorum,
			rs_interrupt_after_quorum: self.rs_interrupt_after_quorum,
			rs_priority: self.rs_priority,
			rs_timeout: self.rs_timeout,
			rs_drop_on_complete: drop_on_complete,
		}
	}
}

impl<T> RequestStrategy<T> {
	/// Set quorum to be reached for request
	pub fn with_quorum(mut self, quorum: usize) -> Self {
		self.rs_quorum = Some(quorum);
		self
	}
	/// Set if requests can be dropped after quorum has been reached
	/// In general true for read requests, and false for write
	pub fn interrupt_after_quorum(mut self, interrupt: bool) -> Self {
		self.rs_interrupt_after_quorum = interrupt;
		self
	}
	/// Deactivate timeout for this request
	pub fn without_timeout(mut self) -> Self {
		self.rs_timeout = Timeout::None;
		self
	}
	/// Set custom timeout for this request
	pub fn with_custom_timeout(mut self, timeout: Duration) -> Self {
		self.rs_timeout = Timeout::Custom(timeout);
		self
	}
	/// Extract drop_on_complete item
	fn extract_drop_on_complete(self) -> (RequestStrategy<()>, T) {
		(
			RequestStrategy {
				rs_quorum: self.rs_quorum,
				rs_interrupt_after_quorum: self.rs_interrupt_after_quorum,
				rs_priority: self.rs_priority,
				rs_timeout: self.rs_timeout,
				rs_drop_on_complete: (),
			},
			self.rs_drop_on_complete,
		)
	}
}

#[derive(Clone)]
pub struct RpcHelper(Arc<RpcHelperInner>);

struct RpcHelperInner {
	our_node_id: Uuid,
	peering: Arc<PeeringManager>,
	ring: watch::Receiver<Arc<Ring>>,
	metrics: RpcMetrics,
	rpc_timeout: Duration,
}

impl RpcHelper {
	pub(crate) fn new(
		our_node_id: Uuid,
		peering: Arc<PeeringManager>,
		ring: watch::Receiver<Arc<Ring>>,
		rpc_timeout: Option<Duration>,
	) -> Self {
		let metrics = RpcMetrics::new();

		Self(Arc::new(RpcHelperInner {
			our_node_id,
			peering,
			ring,
			metrics,
			rpc_timeout: rpc_timeout.unwrap_or(DEFAULT_TIMEOUT),
		}))
	}

	pub fn rpc_timeout(&self) -> Duration {
		self.0.rpc_timeout
	}

	pub async fn call<M, N, H, S>(
		&self,
		endpoint: &Endpoint<M, H>,
		to: Uuid,
		msg: N,
		strat: RequestStrategy<()>,
	) -> Result<S, Error>
	where
		M: Rpc<Response = Result<S, Error>>,
		N: IntoReq<M> + Send,
		H: StreamingEndpointHandler<M>,
	{
		let metric_tags = [
			KeyValue::new("rpc_endpoint", endpoint.path().to_string()),
			KeyValue::new("from", format!("{:?}", self.0.our_node_id)),
			KeyValue::new("to", format!("{:?}", to)),
		];

		self.0.metrics.rpc_counter.add(1, &metric_tags);

		let node_id = to.into();
		let rpc_call = endpoint
			.call_streaming(&node_id, msg, strat.rs_priority)
			.record_duration(&self.0.metrics.rpc_duration, &metric_tags);

		let timeout = async {
			match strat.rs_timeout {
				Timeout::None => futures::future::pending().await,
				Timeout::Default => tokio::time::sleep(self.0.rpc_timeout).await,
				Timeout::Custom(t) => tokio::time::sleep(t).await,
			}
		};

		select! {
			res = rpc_call => {
				if res.is_err() {
					self.0.metrics.rpc_netapp_error_counter.add(1, &metric_tags);
				}
				let res = res?.into_msg();

				if res.is_err() {
					self.0.metrics.rpc_garage_error_counter.add(1, &metric_tags);
				}

				Ok(res?)
			}
			() = timeout => {
				self.0.metrics.rpc_timeout_counter.add(1, &metric_tags);
				Err(Error::Timeout)
			}
		}
	}

	pub async fn call_many<M, N, H, S>(
		&self,
		endpoint: &Endpoint<M, H>,
		to: &[Uuid],
		msg: N,
		strat: RequestStrategy<()>,
	) -> Result<Vec<(Uuid, Result<S, Error>)>, Error>
	where
		M: Rpc<Response = Result<S, Error>>,
		N: IntoReq<M>,
		H: StreamingEndpointHandler<M>,
	{
		let msg = msg.into_req().map_err(garage_net::error::Error::from)?;

		let resps = join_all(
			to.iter()
				.map(|to| self.call(endpoint, *to, msg.clone(), strat.clone())),
		)
		.await;
		Ok(to
			.iter()
			.cloned()
			.zip(resps.into_iter())
			.collect::<Vec<_>>())
	}

	pub async fn broadcast<M, N, H, S>(
		&self,
		endpoint: &Endpoint<M, H>,
		msg: N,
		strat: RequestStrategy<()>,
	) -> Result<Vec<(Uuid, Result<S, Error>)>, Error>
	where
		M: Rpc<Response = Result<S, Error>>,
		N: IntoReq<M>,
		H: StreamingEndpointHandler<M>,
	{
		let to = self
			.0
			.peering
			.get_peer_list()
			.iter()
			.map(|p| p.id.into())
			.collect::<Vec<_>>();
		self.call_many(endpoint, &to[..], msg, strat).await
	}

	/// Make a RPC call to multiple servers, returning either a Vec of responses,
	/// or an error if quorum could not be reached due to too many errors
	pub async fn try_call_many<M, N, H, S, T>(
		&self,
		endpoint: &Arc<Endpoint<M, H>>,
		to: &[Uuid],
		msg: N,
		strategy: RequestStrategy<T>,
	) -> Result<Vec<S>, Error>
	where
		M: Rpc<Response = Result<S, Error>> + 'static,
		N: IntoReq<M>,
		H: StreamingEndpointHandler<M> + 'static,
		S: Send + 'static,
		T: Send + 'static,
	{
		let quorum = strategy.rs_quorum.unwrap_or(to.len());

		let tracer = opentelemetry::global::tracer("garage");
		let span_name = if strategy.rs_interrupt_after_quorum {
			format!("RPC {} to {} of {}", endpoint.path(), quorum, to.len())
		} else {
			format!(
				"RPC {} to {} (quorum {})",
				endpoint.path(),
				to.len(),
				quorum
			)
		};
		let mut span = tracer.start(span_name);
		span.set_attribute(KeyValue::new("from", format!("{:?}", self.0.our_node_id)));
		span.set_attribute(KeyValue::new("to", format!("{:?}", to)));
		span.set_attribute(KeyValue::new("quorum", quorum as i64));
		span.set_attribute(KeyValue::new(
			"interrupt_after_quorum",
			strategy.rs_interrupt_after_quorum.to_string(),
		));

		self.try_call_many_internal(endpoint, to, msg, strategy, quorum)
			.with_context(Context::current_with_span(span))
			.await
	}

	async fn try_call_many_internal<M, N, H, S, T>(
		&self,
		endpoint: &Arc<Endpoint<M, H>>,
		to: &[Uuid],
		msg: N,
		strategy: RequestStrategy<T>,
		quorum: usize,
	) -> Result<Vec<S>, Error>
	where
		M: Rpc<Response = Result<S, Error>> + 'static,
		N: IntoReq<M>,
		H: StreamingEndpointHandler<M> + 'static,
		S: Send + 'static,
		T: Send + 'static,
	{
		let msg = msg.into_req().map_err(garage_net::error::Error::from)?;

		let (strategy, drop_on_complete) = strategy.extract_drop_on_complete();

		// Build future for each request
		// They are not started now: they are added below in a FuturesUnordered
		// object that will take care of polling them (see below)
		let requests = to.iter().cloned().map(|to| {
			let self2 = self.clone();
			let msg = msg.clone();
			let endpoint2 = endpoint.clone();
			let strategy = strategy.clone();
			(to, async move {
				self2.call(&endpoint2, to, msg, strategy).await
			})
		});

		// Vectors in which success results and errors will be collected
		let mut successes = vec![];
		let mut errors = vec![];

		if strategy.rs_interrupt_after_quorum {
			// Case 1: once quorum is reached, other requests don't matter.
			// What we do here is only send the required number of requests
			// to reach a quorum, priorizing nodes with the lowest latency.
			// When there are errors, we start new requests to compensate.

			// Reorder requests to priorize closeness / low latency
			let request_order = self.request_order(to);
			let mut ord_requests = vec![(); request_order.len()]
				.into_iter()
				.map(|_| None)
				.collect::<Vec<_>>();
			for (to, fut) in requests {
				let i = request_order.iter().position(|x| *x == to).unwrap();
				ord_requests[i] = Some((to, fut));
			}

			// Make an iterator to take requests in their sorted order
			let mut requests = ord_requests.into_iter().map(Option::unwrap);

			// resp_stream will contain all of the requests that are currently in flight.
			// (for the moment none, they will be added in the loop below)
			let mut resp_stream = FuturesUnordered::new();

			// Do some requests and collect results
			'request_loop: while successes.len() < quorum {
				// If the current set of requests that are running is not enough to possibly
				// reach quorum, start some new requests.
				while successes.len() + resp_stream.len() < quorum {
					if let Some((req_to, fut)) = requests.next() {
						let tracer = opentelemetry::global::tracer("garage");
						let span = tracer.start(format!("RPC to {:?}", req_to));
						resp_stream.push(tokio::spawn(
							fut.with_context(Context::current_with_span(span)),
						));
					} else {
						// If we have no request to add, we know that we won't ever
						// reach quorum: bail out now.
						break 'request_loop;
					}
				}
				assert!(!resp_stream.is_empty()); // because of loop invariants

				// Wait for one request to terminate
				match resp_stream.next().await.unwrap().unwrap() {
					Ok(msg) => {
						successes.push(msg);
					}
					Err(e) => {
						errors.push(e);
					}
				}
			}
		} else {
			// Case 2: all of the requests need to be sent in all cases,
			// and need to terminate. (this is the case for writes that
			// must be spread to n nodes)
			// Just start all the requests in parallel and return as soon
			// as the quorum is reached.
			let mut resp_stream = requests
				.map(|(_, fut)| fut)
				.collect::<FuturesUnordered<_>>();

			while let Some(resp) = resp_stream.next().await {
				match resp {
					Ok(msg) => {
						successes.push(msg);
						if successes.len() >= quorum {
							break;
						}
					}
					Err(e) => {
						errors.push(e);
					}
				}
			}

			if !resp_stream.is_empty() {
				// Continue remaining requests in background.
				// Note: these requests can get interrupted on process shutdown,
				// we must not count on them being executed for certain.
				// For all background things that have to happen with certainty,
				// they have to be put in a proper queue that is persisted to disk.
				tokio::spawn(async move {
					resp_stream.collect::<Vec<Result<_, _>>>().await;
					drop(drop_on_complete);
				});
			}
		}

		if successes.len() >= quorum {
			Ok(successes)
		} else {
			let errors = errors.iter().map(|e| format!("{}", e)).collect::<Vec<_>>();
			Err(Error::Quorum(quorum, successes.len(), to.len(), errors))
		}
	}

	pub fn request_order(&self, nodes: &[Uuid]) -> Vec<Uuid> {
		// Retrieve some status variables that we will use to sort requests
		let peer_list = self.0.peering.get_peer_list();
		let ring: Arc<Ring> = self.0.ring.borrow().clone();
		let our_zone = match ring.layout.node_role(&self.0.our_node_id) {
			Some(pc) => &pc.zone,
			None => "",
		};

		// Augment requests with some information used to sort them.
		// The tuples are as follows:
		//         (is another node?, is another zone?, latency, node ID, request future)
		// We store all of these tuples in a vec that we can sort.
		// By sorting this vec, we priorize ourself, then nodes in the same zone,
		// and within a same zone we priorize nodes with the lowest latency.
		let mut nodes = nodes
			.iter()
			.map(|to| {
				let peer_zone = match ring.layout.node_role(to) {
					Some(pc) => &pc.zone,
					None => "",
				};
				let peer_avg_ping = peer_list
					.iter()
					.find(|x| x.id.as_ref() == to.as_slice())
					.and_then(|pi| pi.avg_ping)
					.unwrap_or_else(|| Duration::from_secs(10));
				(
					*to != self.0.our_node_id,
					peer_zone != our_zone,
					peer_avg_ping,
					*to,
				)
			})
			.collect::<Vec<_>>();

		// Sort requests by (priorize ourself, priorize same zone, priorize low latency)
		nodes.sort_by_key(|(diffnode, diffzone, ping, _to)| (*diffnode, *diffzone, *ping));

		nodes
			.into_iter()
			.map(|(_, _, _, to)| to)
			.collect::<Vec<_>>()
	}
}
