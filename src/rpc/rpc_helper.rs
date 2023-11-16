//! Contain structs related to making RPCs
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use futures::future::join_all;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt;
use tokio::select;

use opentelemetry::KeyValue;
use opentelemetry::{
	trace::{FutureExt as OtelFutureExt, Span, TraceContextExt, Tracer},
	Context,
};

pub use netapp::endpoint::{Endpoint, EndpointHandler, StreamingEndpointHandler};
pub use netapp::message::{
	IntoReq, Message as Rpc, OrderTag, Req, RequestPriority, Resp, PRIO_BACKGROUND, PRIO_HIGH,
	PRIO_NORMAL, PRIO_SECONDARY,
};
use netapp::peering::fullmesh::FullMeshPeeringStrategy;
pub use netapp::{self, NetApp, NodeID};

use garage_util::data::*;
use garage_util::error::Error;
use garage_util::metrics::RecordDuration;

use crate::layout::LayoutHelper;
use crate::metrics::RpcMetrics;

// Default RPC timeout = 5 minutes
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(300);

/// Strategy to apply when making RPC
#[derive(Copy, Clone)]
pub struct RequestStrategy {
	/// Min number of response to consider the request successful
	rs_quorum: Option<usize>,
	/// Send all requests at once
	rs_send_all_at_once: Option<bool>,
	/// Request priority
	rs_priority: RequestPriority,
	/// Custom timeout for this request
	rs_timeout: Timeout,
}

#[derive(Copy, Clone)]
enum Timeout {
	None,
	Default,
	Custom(Duration),
}

impl RequestStrategy {
	/// Create a RequestStrategy with default timeout and not interrupting when quorum reached
	pub fn with_priority(prio: RequestPriority) -> Self {
		RequestStrategy {
			rs_quorum: None,
			rs_send_all_at_once: None,
			rs_priority: prio,
			rs_timeout: Timeout::Default,
		}
	}
	/// Set quorum to be reached for request
	pub fn with_quorum(mut self, quorum: usize) -> Self {
		self.rs_quorum = Some(quorum);
		self
	}
	/// Set quorum to be reached for request
	pub fn send_all_at_once(mut self, value: bool) -> Self {
		self.rs_send_all_at_once = Some(value);
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
}

#[derive(Clone)]
pub struct RpcHelper(Arc<RpcHelperInner>);

struct RpcHelperInner {
	our_node_id: Uuid,
	fullmesh: Arc<FullMeshPeeringStrategy>,
	layout: Arc<RwLock<LayoutHelper>>,
	metrics: RpcMetrics,
	rpc_timeout: Duration,
}

impl RpcHelper {
	pub(crate) fn new(
		our_node_id: Uuid,
		fullmesh: Arc<FullMeshPeeringStrategy>,
		layout: Arc<RwLock<LayoutHelper>>,
		rpc_timeout: Option<Duration>,
	) -> Self {
		let metrics = RpcMetrics::new();

		Self(Arc::new(RpcHelperInner {
			our_node_id,
			fullmesh,
			layout,
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
		strat: RequestStrategy,
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
		strat: RequestStrategy,
	) -> Result<Vec<(Uuid, Result<S, Error>)>, Error>
	where
		M: Rpc<Response = Result<S, Error>>,
		N: IntoReq<M>,
		H: StreamingEndpointHandler<M>,
	{
		let msg = msg.into_req().map_err(netapp::error::Error::from)?;

		let resps = join_all(
			to.iter()
				.map(|to| self.call(endpoint, *to, msg.clone(), strat)),
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
		strat: RequestStrategy,
	) -> Result<Vec<(Uuid, Result<S, Error>)>, Error>
	where
		M: Rpc<Response = Result<S, Error>>,
		N: IntoReq<M>,
		H: StreamingEndpointHandler<M>,
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
	pub async fn try_call_many<M, N, H, S>(
		&self,
		endpoint: &Arc<Endpoint<M, H>>,
		to: &[Uuid],
		msg: N,
		strategy: RequestStrategy,
	) -> Result<Vec<S>, Error>
	where
		M: Rpc<Response = Result<S, Error>> + 'static,
		N: IntoReq<M>,
		H: StreamingEndpointHandler<M> + 'static,
		S: Send + 'static,
	{
		let quorum = strategy.rs_quorum.unwrap_or(to.len());

		let tracer = opentelemetry::global::tracer("garage");
		let span_name = format!("Read RPC {} to {} of {}", endpoint.path(), quorum, to.len());

		let mut span = tracer.start(span_name);
		span.set_attribute(KeyValue::new("from", format!("{:?}", self.0.our_node_id)));
		span.set_attribute(KeyValue::new("to", format!("{:?}", to)));
		span.set_attribute(KeyValue::new("quorum", quorum as i64));

		self.try_call_many_inner(endpoint, to, msg, strategy, quorum)
			.with_context(Context::current_with_span(span))
			.await
	}

	async fn try_call_many_inner<M, N, H, S>(
		&self,
		endpoint: &Arc<Endpoint<M, H>>,
		to: &[Uuid],
		msg: N,
		strategy: RequestStrategy,
		quorum: usize,
	) -> Result<Vec<S>, Error>
	where
		M: Rpc<Response = Result<S, Error>> + 'static,
		N: IntoReq<M>,
		H: StreamingEndpointHandler<M> + 'static,
		S: Send + 'static,
	{
		// Once quorum is reached, other requests don't matter.
		// What we do here is only send the required number of requests
		// to reach a quorum, priorizing nodes with the lowest latency.
		// When there are errors, we start new requests to compensate.

		// Reorder requests to priorize closeness / low latency
		let request_order = self.request_order(to);
		let send_all_at_once = strategy.rs_send_all_at_once.unwrap_or(false);

		// Build future for each request
		// They are not started now: they are added below in a FuturesUnordered
		// object that will take care of polling them (see below)
		let msg = msg.into_req().map_err(netapp::error::Error::from)?;
		let mut requests = request_order.into_iter().map(|to| {
			let self2 = self.clone();
			let msg = msg.clone();
			let endpoint2 = endpoint.clone();
			(to, async move {
				self2.call(&endpoint2, to, msg, strategy).await
			})
		});

		// Vectors in which success results and errors will be collected
		let mut successes = vec![];
		let mut errors = vec![];

		// resp_stream will contain all of the requests that are currently in flight.
		// (for the moment none, they will be added in the loop below)
		let mut resp_stream = FuturesUnordered::new();

		// Do some requests and collect results
		while successes.len() < quorum {
			// If the current set of requests that are running is not enough to possibly
			// reach quorum, start some new requests.
			while send_all_at_once || successes.len() + resp_stream.len() < quorum {
				if let Some((req_to, fut)) = requests.next() {
					let tracer = opentelemetry::global::tracer("garage");
					let span = tracer.start(format!("RPC to {:?}", req_to));
					resp_stream.push(fut.with_context(Context::current_with_span(span)));
				} else {
					break;
				}
			}

			if successes.len() + resp_stream.len() < quorum {
				// We know we won't ever reach quorum
				break;
			}

			// Wait for one request to terminate
			match resp_stream.next().await.unwrap() {
				Ok(msg) => {
					successes.push(msg);
				}
				Err(e) => {
					errors.push(e);
				}
			}
		}

		if successes.len() >= quorum {
			Ok(successes)
		} else {
			let errors = errors.iter().map(|e| format!("{}", e)).collect::<Vec<_>>();
			Err(Error::Quorum(
				quorum,
				None,
				successes.len(),
				to.len(),
				errors,
			))
		}
	}

	pub fn request_order(&self, nodes: &[Uuid]) -> Vec<Uuid> {
		// Retrieve some status variables that we will use to sort requests
		let peer_list = self.0.fullmesh.get_peer_list();
		let layout = self.0.layout.read().unwrap();
		let our_zone = match layout.current().node_role(&self.0.our_node_id) {
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
				let peer_zone = match layout.current().node_role(to) {
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

	pub async fn try_write_many_sets<M, N, H, S>(
		&self,
		endpoint: &Arc<Endpoint<M, H>>,
		to_sets: &[Vec<Uuid>],
		msg: N,
		strategy: RequestStrategy,
	) -> Result<Vec<S>, Error>
	where
		M: Rpc<Response = Result<S, Error>> + 'static,
		N: IntoReq<M>,
		H: StreamingEndpointHandler<M> + 'static,
		S: Send + 'static,
	{
		let quorum = strategy
			.rs_quorum
			.expect("internal error: missing quroum in try_write_many_sets");

		let tracer = opentelemetry::global::tracer("garage");
		let span_name = format!(
			"Write RPC {} (quorum {} in {} sets)",
			endpoint.path(),
			quorum,
			to_sets.len()
		);

		let mut span = tracer.start(span_name);
		span.set_attribute(KeyValue::new("from", format!("{:?}", self.0.our_node_id)));
		span.set_attribute(KeyValue::new("to", format!("{:?}", to_sets)));
		span.set_attribute(KeyValue::new("quorum", quorum as i64));

		self.try_write_many_sets_inner(endpoint, to_sets, msg, strategy, quorum)
			.with_context(Context::current_with_span(span))
			.await
	}

	async fn try_write_many_sets_inner<M, N, H, S>(
		&self,
		endpoint: &Arc<Endpoint<M, H>>,
		to_sets: &[Vec<Uuid>],
		msg: N,
		strategy: RequestStrategy,
		quorum: usize,
	) -> Result<Vec<S>, Error>
	where
		M: Rpc<Response = Result<S, Error>> + 'static,
		N: IntoReq<M>,
		H: StreamingEndpointHandler<M> + 'static,
		S: Send + 'static,
	{
		let msg = msg.into_req().map_err(netapp::error::Error::from)?;

		let mut peers = HashMap::<Uuid, Vec<usize>>::new();
		for (i, set) in to_sets.iter().enumerate() {
			for peer in set.iter() {
				peers.entry(*peer).or_default().push(i);
			}
		}

		let requests = peers.iter().map(|(peer, _)| {
			let self2 = self.clone();
			let msg = msg.clone();
			let endpoint2 = endpoint.clone();
			let to = *peer;
			let tracer = opentelemetry::global::tracer("garage");
			let span = tracer.start(format!("RPC to {:?}", to));
			let fut = async move { (to, self2.call(&endpoint2, to, msg, strategy).await) };
			fut.with_context(Context::current_with_span(span))
		});
		let mut resp_stream = requests.collect::<FuturesUnordered<_>>();

		let mut successes = vec![];
		let mut errors = vec![];

		let mut set_counters = vec![(0, 0); to_sets.len()];

		while let Some((node, resp)) = resp_stream.next().await {
			match resp {
				Ok(msg) => {
					for set in peers.get(&node).unwrap().iter() {
						set_counters[*set].0 += 1;
					}
					successes.push(msg);
				}
				Err(e) => {
					for set in peers.get(&node).unwrap().iter() {
						set_counters[*set].1 += 1;
					}
					errors.push(e);
				}
			}

			if set_counters.iter().all(|(ok_cnt, _)| *ok_cnt >= quorum) {
				// Success

				// Continue all other requets in background
				tokio::spawn(async move {
					resp_stream.collect::<Vec<(Uuid, Result<_, _>)>>().await;
				});

				return Ok(successes);
			}

			if set_counters
				.iter()
				.enumerate()
				.any(|(i, (_, err_cnt))| err_cnt + quorum > to_sets[i].len())
			{
				// Too many errors in this set, we know we won't get a quorum
				break;
			}
		}

		// Failure, could not get quorum
		let errors = errors.iter().map(|e| format!("{}", e)).collect::<Vec<_>>();
		Err(Error::Quorum(
			quorum,
			Some(to_sets.len()),
			successes.len(),
			peers.len(),
			errors,
		))
	}
}
