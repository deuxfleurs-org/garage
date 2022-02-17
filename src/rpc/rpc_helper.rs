//! Contain structs related to making RPCs
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use futures::future::join_all;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt;
use futures_util::future::FutureExt;
use tokio::select;
use tokio::sync::{watch, Semaphore};

use opentelemetry::KeyValue;
use opentelemetry::{
	trace::{FutureExt as OtelFutureExt, Span, TraceContextExt, Tracer},
	Context,
};

pub use netapp::endpoint::{Endpoint, EndpointHandler, Message as Rpc};
use netapp::peering::fullmesh::FullMeshPeeringStrategy;
pub use netapp::proto::*;
pub use netapp::{NetApp, NodeID};

use garage_util::background::BackgroundRunner;
use garage_util::data::*;
use garage_util::error::Error;

use crate::metrics::RpcMetrics;
use crate::ring::Ring;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

// Try to never have more than 200MB of outgoing requests
// buffered at the same time. Other requests are queued until
// space is freed.
const REQUEST_BUFFER_SIZE: usize = 200 * 1024 * 1024;

/// Strategy to apply when making RPC
#[derive(Copy, Clone)]
pub struct RequestStrategy {
	/// Max time to wait for reponse
	pub rs_timeout: Duration,
	/// Min number of response to consider the request successful
	pub rs_quorum: Option<usize>,
	/// Should requests be dropped after enough response are received
	pub rs_interrupt_after_quorum: bool,
	/// Request priority
	pub rs_priority: RequestPriority,
}

impl RequestStrategy {
	/// Create a RequestStrategy with default timeout and not interrupting when quorum reached
	pub fn with_priority(prio: RequestPriority) -> Self {
		RequestStrategy {
			rs_timeout: DEFAULT_TIMEOUT,
			rs_quorum: None,
			rs_interrupt_after_quorum: false,
			rs_priority: prio,
		}
	}
	/// Set quorum to be reached for request
	pub fn with_quorum(mut self, quorum: usize) -> Self {
		self.rs_quorum = Some(quorum);
		self
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

#[derive(Clone)]
pub struct RpcHelper(Arc<RpcHelperInner>);

struct RpcHelperInner {
	our_node_id: Uuid,
	fullmesh: Arc<FullMeshPeeringStrategy>,
	background: Arc<BackgroundRunner>,
	ring: watch::Receiver<Arc<Ring>>,
	request_buffer_semaphore: Arc<Semaphore>,
	metrics: RpcMetrics,
}

impl RpcHelper {
	pub(crate) fn new(
		our_node_id: Uuid,
		fullmesh: Arc<FullMeshPeeringStrategy>,
		background: Arc<BackgroundRunner>,
		ring: watch::Receiver<Arc<Ring>>,
	) -> Self {
		let sem = Arc::new(Semaphore::new(REQUEST_BUFFER_SIZE));

		let metrics = RpcMetrics::new(sem.clone());

		Self(Arc::new(RpcHelperInner {
			our_node_id,
			fullmesh,
			background,
			ring,
			request_buffer_semaphore: sem,
			metrics,
		}))
	}

	pub async fn call<M, H, S>(
		&self,
		endpoint: &Endpoint<M, H>,
		to: Uuid,
		msg: M,
		strat: RequestStrategy,
	) -> Result<S, Error>
	where
		M: Rpc<Response = Result<S, Error>>,
		H: EndpointHandler<M>,
	{
		self.call_arc(endpoint, to, Arc::new(msg), strat).await
	}

	pub async fn call_arc<M, H, S>(
		&self,
		endpoint: &Endpoint<M, H>,
		to: Uuid,
		msg: Arc<M>,
		strat: RequestStrategy,
	) -> Result<S, Error>
	where
		M: Rpc<Response = Result<S, Error>>,
		H: EndpointHandler<M>,
	{
		let queueing_start_time = SystemTime::now();
		let metric_tags = [KeyValue::new("endpoint", endpoint.path().to_string())];

		let msg_size = rmp_to_vec_all_named(&msg)?.len() as u32;
		let permit = self
			.0
			.request_buffer_semaphore
			.acquire_many(msg_size)
			.await?;

		self.0.metrics.rpc_queueing_time.record(
			queueing_start_time
				.elapsed()
				.map_or(0.0, |d| d.as_secs_f64()),
			&metric_tags,
		);
		self.0.metrics.rpc_counter.add(1, &metric_tags);
		let rpc_start_time = SystemTime::now();

		let tracer = opentelemetry::global::tracer("garage");
		let mut span = tracer.start(format!("RPC {}", endpoint.path()));
		span.set_attribute(KeyValue::new("to", format!("{:?}", to)));

		let node_id = to.into();
		let rpc_call = endpoint
			.call(&node_id, &msg, strat.rs_priority)
			.with_context(Context::current_with_span(span));

		select! {
			res = rpc_call => {
				drop(permit);

				if res.is_err() {
					self.0.metrics.rpc_netapp_error_counter.add(1, &metric_tags);
				}
				let res = res?;

				self.0.metrics.rpc_duration
			.record(rpc_start_time.elapsed().map_or(0.0, |d| d.as_secs_f64()), &metric_tags);
				if res.is_err() {
					self.0.metrics.rpc_garage_error_counter.add(1, &metric_tags);
				}

				Ok(res?)
			}
			_ = tokio::time::sleep(strat.rs_timeout) => {
				drop(permit);
				self.0.metrics.rpc_timeout_counter.add(1, &metric_tags);
				Err(Error::Timeout)
			}
		}
	}

	pub async fn call_many<M, H, S>(
		&self,
		endpoint: &Endpoint<M, H>,
		to: &[Uuid],
		msg: M,
		strat: RequestStrategy,
	) -> Vec<(Uuid, Result<S, Error>)>
	where
		M: Rpc<Response = Result<S, Error>>,
		H: EndpointHandler<M>,
	{
		let msg = Arc::new(msg);
		let resps = join_all(
			to.iter()
				.map(|to| self.call_arc(endpoint, *to, msg.clone(), strat)),
		)
		.await;
		to.iter()
			.cloned()
			.zip(resps.into_iter())
			.collect::<Vec<_>>()
	}

	pub async fn broadcast<M, H, S>(
		&self,
		endpoint: &Endpoint<M, H>,
		msg: M,
		strat: RequestStrategy,
	) -> Vec<(Uuid, Result<S, Error>)>
	where
		M: Rpc<Response = Result<S, Error>>,
		H: EndpointHandler<M>,
	{
		let to = self
			.0
			.fullmesh
			.get_peer_list()
			.iter()
			.map(|p| p.id.into())
			.collect::<Vec<_>>();
		self.call_many(endpoint, &to[..], msg, strat).await
	}

	/// Make a RPC call to multiple servers, returning either a Vec of responses,
	/// or an error if quorum could not be reached due to too many errors
	pub async fn try_call_many<M, H, S>(
		&self,
		endpoint: &Arc<Endpoint<M, H>>,
		to: &[Uuid],
		msg: M,
		strategy: RequestStrategy,
	) -> Result<Vec<S>, Error>
	where
		M: Rpc<Response = Result<S, Error>> + 'static,
		H: EndpointHandler<M> + 'static,
		S: Send + 'static,
	{
		let quorum = strategy.rs_quorum.unwrap_or(to.len());

		let tracer = opentelemetry::global::tracer("garage");
		let mut span = tracer.start(format!("RPC {} to {:?}", endpoint.path(), to));
		span.set_attribute(KeyValue::new("to", format!("{:?}", to)));
		span.set_attribute(KeyValue::new("quorum", quorum as i64));

		async {
			let msg = Arc::new(msg);

			// Build future for each request
			// They are not started now: they are added below in a FuturesUnordered
			// object that will take care of polling them (see below)
			let requests = to.iter().cloned().map(|to| {
				let self2 = self.clone();
				let msg = msg.clone();
				let endpoint2 = endpoint.clone();
				(to, async move {
					self2.call_arc(&endpoint2, to, msg, strategy).await
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

				// Retrieve some status variables that we will use to sort requests
				let peer_list = self.0.fullmesh.get_peer_list();
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
				let mut requests = requests
					.map(|(to, fut)| {
						let peer_zone = match ring.layout.node_role(&to) {
							Some(pc) => &pc.zone,
							None => "",
						};
						let peer_avg_ping = peer_list
							.iter()
							.find(|x| x.id.as_ref() == to.as_slice())
							.map(|pi| pi.avg_ping)
							.flatten()
							.unwrap_or_else(|| Duration::from_secs(1));
						(
							to != self.0.our_node_id,
							peer_zone != our_zone,
							peer_avg_ping,
							to,
							fut,
						)
					})
					.collect::<Vec<_>>();

				// Sort requests by (priorize ourself, priorize same zone, priorize low latency)
				requests.sort_by_key(|(diffnode, diffzone, ping, _to, _fut)| {
					(*diffnode, *diffzone, *ping)
				});

				// Make an iterator to take requests in their sorted order
				let mut requests = requests.into_iter();

				// resp_stream will contain all of the requests that are currently in flight.
				// (for the moment none, they will be added in the loop below)
				let mut resp_stream = FuturesUnordered::new();

				// Do some requests and collect results
				'request_loop: while successes.len() < quorum {
					// If the current set of requests that are running is not enough to possibly
					// reach quorum, start some new requests.
					while successes.len() + resp_stream.len() < quorum {
						if let Some((_, _, _, req_to, fut)) = requests.next() {
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
					// Continue the remaining requests immediately using tokio::spawn
					// but enqueue a task in the background runner
					// to ensure that the process won't exit until the requests are done
					// (if we had just enqueued the resp_stream.collect directly in the background runner,
					// the requests might have been put on hold in the background runner's queue,
					// in which case they might timeout or otherwise fail)
					let wait_finished_fut = tokio::spawn(async move {
						resp_stream.collect::<Vec<Result<_, _>>>().await;
					});
					self.0.background.spawn(wait_finished_fut.map(|_| Ok(())));
				}
			}

			if successes.len() >= quorum {
				Ok(successes)
			} else {
				let errors = errors.iter().map(|e| format!("{}", e)).collect::<Vec<_>>();
				Err(Error::Quorum(quorum, successes.len(), to.len(), errors))
			}
		}
		.with_context(Context::current_with_span(span))
		.await
	}
}
