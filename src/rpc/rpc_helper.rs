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

use crate::layout::{LayoutHelper, LayoutVersion};
use crate::metrics::RpcMetrics;

// Default RPC timeout = 5 minutes
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(300);

/// Strategy to apply when making RPC
pub struct RequestStrategy<T> {
	/// Min number of response to consider the request successful
	rs_quorum: Option<usize>,
	/// Send all requests at once
	rs_send_all_at_once: Option<bool>,
	/// Request priority
	rs_priority: RequestPriority,
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
			rs_send_all_at_once: self.rs_send_all_at_once,
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
			rs_send_all_at_once: None,
			rs_priority: prio,
			rs_timeout: Timeout::Default,
			rs_drop_on_complete: (),
		}
	}
	/// Add an item to be dropped on completion
	pub fn with_drop_on_completion<T>(self, drop_on_complete: T) -> RequestStrategy<T> {
		RequestStrategy {
			rs_quorum: self.rs_quorum,
			rs_send_all_at_once: self.rs_send_all_at_once,
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
	/// Extract drop_on_complete item
	fn extract_drop_on_complete(self) -> (RequestStrategy<()>, T) {
		(
			RequestStrategy {
				rs_quorum: self.rs_quorum,
				rs_send_all_at_once: self.rs_send_all_at_once,
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
	layout: Arc<RwLock<LayoutHelper>>,
	metrics: RpcMetrics,
	rpc_timeout: Duration,
}

impl RpcHelper {
	pub(crate) fn new(
		our_node_id: Uuid,
		peering: Arc<PeeringManager>,
		layout: Arc<RwLock<LayoutHelper>>,
		rpc_timeout: Option<Duration>,
	) -> Self {
		let metrics = RpcMetrics::new();

		Self(Arc::new(RpcHelperInner {
			our_node_id,
			peering,
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
		strat: RequestStrategy<()>,
	) -> Result<S, Error>
	where
		M: Rpc<Response = Result<S, Error>>,
		N: IntoReq<M> + Send,
		H: StreamingEndpointHandler<M>,
	{
		let tracer = opentelemetry::global::tracer("garage");
		let span_name = format!("RPC [{}] to {:?}", endpoint.path(), to);
		let mut span = tracer.start(span_name);
		span.set_attribute(KeyValue::new("from", format!("{:?}", self.0.our_node_id)));
		span.set_attribute(KeyValue::new("to", format!("{:?}", to)));

		let metric_tags = [
			KeyValue::new("rpc_endpoint", endpoint.path().to_string()),
			KeyValue::new("from", format!("{:?}", self.0.our_node_id)),
			KeyValue::new("to", format!("{:?}", to)),
		];

		self.0.metrics.rpc_counter.add(1, &metric_tags);

		let node_id = to.into();
		let rpc_call = endpoint
			.call_streaming(&node_id, msg, strat.rs_priority)
			.with_context(Context::current_with_span(span))
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
		let tracer = opentelemetry::global::tracer("garage");
		let span_name = format!("RPC [{}] call_many {} nodes", endpoint.path(), to.len());
		let span = tracer.start(span_name);

		let msg = msg.into_req().map_err(garage_net::error::Error::from)?;

		let resps = join_all(
			to.iter()
				.map(|to| self.call(endpoint, *to, msg.clone(), strat.clone())),
		)
		.with_context(Context::current_with_span(span))
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
	///
	/// If RequestStrategy has send_all_at_once set, then all requests will be
	/// sent at once, and `try_call_many` will return as soon as a quorum of
	/// responses is achieved, dropping and cancelling the remaining requests.
	///
	/// Otherwise, `quorum` requests will be sent at the same time, and if an
	/// error response is received, a new request will be sent to replace it.
	/// The ordering of nodes to which requests are sent is determined by
	/// the `RpcHelper::request_order` function, which takes into account
	/// parameters such as node zones and measured ping values.
	///
	/// In both cases, the basic contract of this function is that even in the
	/// absence of failures, the RPC call might not be driven to completion
	/// on all of the specified nodes. It is therefore unfit for broadcast
	/// write operations where we expect all nodes to successfully store
	/// the written date.
	pub async fn try_call_many<M, N, H, S>(
		&self,
		endpoint: &Arc<Endpoint<M, H>>,
		to: &[Uuid],
		msg: N,
		strategy: RequestStrategy<()>,
	) -> Result<Vec<S>, Error>
	where
		M: Rpc<Response = Result<S, Error>> + 'static,
		N: IntoReq<M>,
		H: StreamingEndpointHandler<M> + 'static,
		S: Send + 'static,
	{
		let quorum = strategy.rs_quorum.unwrap_or(to.len());

		let tracer = opentelemetry::global::tracer("garage");
		let span_name = format!(
			"RPC [{}] try_call_many (quorum {}/{})",
			endpoint.path(),
			quorum,
			to.len()
		);

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
		strategy: RequestStrategy<()>,
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

		// TODO: this could be made more aggressive, e.g. if after 2x the
		// average ping of a given request, the response is not yet received,
		// preemptively send an additional request to any remaining nodes.

		// Reorder requests to priorize closeness / low latency
		let request_order =
			self.request_order(&self.0.layout.read().unwrap().current(), to.iter().copied());
		let send_all_at_once = strategy.rs_send_all_at_once.unwrap_or(false);

		// Build future for each request
		// They are not started now: they are added below in a FuturesUnordered
		// object that will take care of polling them (see below)
		let msg = msg.into_req().map_err(garage_net::error::Error::from)?;
		let mut requests = request_order.into_iter().map(|to| {
			let self2 = self.clone();
			let msg = msg.clone();
			let endpoint2 = endpoint.clone();
			let strategy = strategy.clone();
			async move { self2.call(&endpoint2, to, msg, strategy).await }
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
				if let Some(fut) = requests.next() {
					resp_stream.push(fut)
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

	/// Make a RPC call to multiple servers, returning either a Vec of responses,
	/// or an error if quorum could not be reached due to too many errors
	///
	/// Contrary to try_call_many, this function is especially made for broadcast
	/// write operations. In particular:
	///
	/// - The request are sent to all specified nodes as soon as `try_write_many_sets`
	///   is invoked.
	///
	/// - When `try_write_many_sets` returns, all remaining requests that haven't
	///   completed move to a background task so that they have a chance to
	///   complete successfully if there are no failures.
	///
	/// In addition, the nodes to which requests should be sent are divided in
	/// "quorum sets", and `try_write_many_sets` only returns once a quorum
	/// has been validated in each set. This is used in the case of cluster layout
	/// changes, where data has to be written both in the old layout and in the
	/// new one as long as all nodes have not successfully tranisitionned and
	/// moved all data to the new layout.
	pub async fn try_write_many_sets<M, N, H, S, T>(
		&self,
		endpoint: &Arc<Endpoint<M, H>>,
		to_sets: &[Vec<Uuid>],
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
		let quorum = strategy
			.rs_quorum
			.expect("internal error: missing quorum value in try_write_many_sets");

		let tracer = opentelemetry::global::tracer("garage");
		let span_name = format!(
			"RPC [{}] try_write_many_sets (quorum {} in {} sets)",
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

	async fn try_write_many_sets_inner<M, N, H, S, T>(
		&self,
		endpoint: &Arc<Endpoint<M, H>>,
		to_sets: &[Vec<Uuid>],
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
		// Peers may appear in many quorum sets. Here, build a list of peers,
		// mapping to the index of the quorum sets in which they appear.
		let mut result_tracker = QuorumSetResultTracker::new(to_sets, quorum);

		let (strategy, drop_on_complete) = strategy.extract_drop_on_complete();

		// Send one request to each peer of the quorum sets
		let msg = msg.into_req().map_err(garage_net::error::Error::from)?;
		let requests = result_tracker.nodes.keys().map(|peer| {
			let self2 = self.clone();
			let msg = msg.clone();
			let endpoint2 = endpoint.clone();
			let to = *peer;
			let strategy = strategy.clone();
			async move { (to, self2.call(&endpoint2, to, msg, strategy).await) }
		});
		let mut resp_stream = requests.collect::<FuturesUnordered<_>>();

		// Drive requests to completion
		while let Some((node, resp)) = resp_stream.next().await {
			// Store the response in the correct vector and increment the
			// appropriate counters
			result_tracker.register_result(node, resp);

			// If we have a quorum of ok in all quorum sets, then it's a success!
			if result_tracker.all_quorums_ok() {
				// Continue all other requests in background
				tokio::spawn(async move {
					resp_stream.collect::<Vec<(Uuid, Result<_, _>)>>().await;
					drop(drop_on_complete);
				});

				return Ok(result_tracker.success_values());
			}

			// If there is a quorum set for which too many errors were received,
			// we know it's impossible to get a quorum, so return immediately.
			if result_tracker.too_many_failures() {
				break;
			}
		}

		// At this point, there is no quorum and we know that a quorum
		// will never be achieved. Currently, we drop all remaining requests.
		// Should we still move them to background so that they can continue
		// for non-failed nodes? Not doing so has no impact on correctness,
		// but it means that more cancellation messages will be sent. Idk.
		// (When an in-progress request future is dropped, Netapp automatically
		// sends a cancellation message to the remote node to inform it that
		// the result is no longer needed. In turn, if the remote node receives
		// the cancellation message in time, it interrupts the task of the
		// running request handler.)

		// Failure, could not get quorum
		Err(result_tracker.quorum_error())
	}

	// ---- functions not related to MAKING RPCs, but just determining to what nodes
	//      they should be made and in which order ----

	/// Determine to what nodes, and in what order, requests to read a data block
	/// should be sent. All nodes in the Vec returned by this function are tried
	/// one by one until there is one that returns the block (in block/manager.rs).
	///
	/// We want to have the best chance of finding the block in as few requests
	/// as possible, and we want to avoid nodes that answer slowly.
	///
	/// Note that when there are several active layout versions, the block might
	/// be stored only by nodes of the latest version (in case of a block that was
	/// written after the layout change), or only by nodes of the oldest active
	/// version (for all blocks that were written before). So we have to try nodes
	/// of all layout versions. We also want to try nodes of all layout versions
	/// fast, so as to optimize the chance of finding the block fast.
	///
	/// Therefore, the strategy is the following:
	///
	/// 1. ask first all nodes of all currently active layout versions
	///   -> ask the preferred node in all layout versions (older to newer),
	///      then the second preferred onde in all verions, etc.
	///   -> we start by the oldest active layout version first, because a majority
	///      of blocks will have been saved before the layout change
	/// 2. ask all nodes of historical layout versions, for blocks which have not
	///    yet been transferred to their new storage nodes
	///
	/// The preference order, for each layout version, is given by `request_order`,
	/// based on factors such as nodes being in the same datacenter,
	/// having low ping, etc.
	pub fn block_read_nodes_of(&self, position: &Hash, rpc_helper: &RpcHelper) -> Vec<Uuid> {
		let layout = self.0.layout.read().unwrap();

		// Compute, for each layout version, the set of nodes that might store
		// the block, and put them in their preferred order as of `request_order`.
		let mut vernodes = layout.versions().iter().map(|ver| {
			let nodes = ver.nodes_of(position, ver.replication_factor);
			rpc_helper.request_order(layout.current(), nodes)
		});

		let mut ret = if layout.versions().len() == 1 {
			// If we have only one active layout version, then these are the
			// only nodes we ask in step 1
			vernodes.next().unwrap()
		} else {
			let vernodes = vernodes.collect::<Vec<_>>();

			let mut nodes = Vec::<Uuid>::with_capacity(12);
			for i in 0..layout.current().replication_factor {
				for vn in vernodes.iter() {
					if let Some(n) = vn.get(i) {
						if !nodes.contains(&n) {
							if *n == self.0.our_node_id {
								// it's always fast (almost free) to ask locally,
								// so always put that as first choice
								nodes.insert(0, *n);
							} else {
								nodes.push(*n);
							}
						}
					}
				}
			}

			nodes
		};

		// Second step: add nodes of older layout versions
		let old_ver_iter = layout.inner().old_versions.iter().rev();
		for ver in old_ver_iter {
			let nodes = ver.nodes_of(position, ver.replication_factor);
			for node in rpc_helper.request_order(layout.current(), nodes) {
				if !ret.contains(&node) {
					ret.push(node);
				}
			}
		}

		ret
	}

	fn request_order(
		&self,
		layout: &LayoutVersion,
		nodes: impl Iterator<Item = Uuid>,
	) -> Vec<Uuid> {
		// Retrieve some status variables that we will use to sort requests
		let peer_list = self.0.peering.get_peer_list();
		let our_zone = layout.get_node_zone(&self.0.our_node_id).unwrap_or("");

		// Augment requests with some information used to sort them.
		// The tuples are as follows:
		//         (is another node?, is another zone?, latency, node ID, request future)
		// We store all of these tuples in a vec that we can sort.
		// By sorting this vec, we priorize ourself, then nodes in the same zone,
		// and within a same zone we priorize nodes with the lowest latency.
		let mut nodes = nodes
			.map(|to| {
				let peer_zone = layout.get_node_zone(&to).unwrap_or("");
				let peer_avg_ping = peer_list
					.iter()
					.find(|x| x.id.as_ref() == to.as_slice())
					.and_then(|pi| pi.avg_ping)
					.unwrap_or_else(|| Duration::from_secs(10));
				(
					to != self.0.our_node_id,
					peer_zone != our_zone,
					peer_avg_ping,
					to,
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

// ------- utility for tracking successes/errors among write sets --------

pub struct QuorumSetResultTracker<S, E> {
	/// The set of nodes and the index of the quorum sets they belong to
	pub nodes: HashMap<Uuid, Vec<usize>>,
	/// The quorum value, i.e. number of success responses to await in each set
	pub quorum: usize,

	/// The success responses received
	pub successes: Vec<(Uuid, S)>,
	/// The error responses received
	pub failures: Vec<(Uuid, E)>,

	/// The counters for successes in each set
	pub success_counters: Box<[usize]>,
	/// The counters for failures in each set
	pub failure_counters: Box<[usize]>,
	/// The total number of nodes in each set
	pub set_lens: Box<[usize]>,
}

impl<S, E> QuorumSetResultTracker<S, E>
where
	E: std::fmt::Display,
{
	pub fn new<A>(sets: &[A], quorum: usize) -> Self
	where
		A: AsRef<[Uuid]>,
	{
		let mut nodes = HashMap::<Uuid, Vec<usize>>::new();
		for (i, set) in sets.iter().enumerate() {
			for node in set.as_ref().iter() {
				nodes.entry(*node).or_default().push(i);
			}
		}

		let num_nodes = nodes.len();
		Self {
			nodes,
			quorum,
			successes: Vec::with_capacity(num_nodes),
			failures: vec![],
			success_counters: vec![0; sets.len()].into_boxed_slice(),
			failure_counters: vec![0; sets.len()].into_boxed_slice(),
			set_lens: sets
				.iter()
				.map(|x| x.as_ref().len())
				.collect::<Vec<_>>()
				.into_boxed_slice(),
		}
	}

	pub fn register_result(&mut self, node: Uuid, result: Result<S, E>) {
		match result {
			Ok(s) => {
				self.successes.push((node, s));
				for set in self.nodes.get(&node).unwrap().iter() {
					self.success_counters[*set] += 1;
				}
			}
			Err(e) => {
				self.failures.push((node, e));
				for set in self.nodes.get(&node).unwrap().iter() {
					self.failure_counters[*set] += 1;
				}
			}
		}
	}

	pub fn all_quorums_ok(&self) -> bool {
		self.success_counters
			.iter()
			.all(|ok_cnt| *ok_cnt >= self.quorum)
	}

	pub fn too_many_failures(&self) -> bool {
		self.failure_counters
			.iter()
			.zip(self.set_lens.iter())
			.any(|(err_cnt, set_len)| *err_cnt + self.quorum > *set_len)
	}

	pub fn success_values(self) -> Vec<S> {
		self.successes
			.into_iter()
			.map(|(_, x)| x)
			.collect::<Vec<_>>()
	}

	pub fn quorum_error(self) -> Error {
		let errors = self
			.failures
			.iter()
			.map(|(n, e)| format!("{:?}: {}", n, e))
			.collect::<Vec<_>>();
		Error::Quorum(
			self.quorum,
			Some(self.set_lens.len()),
			self.successes.len(),
			self.nodes.len(),
			errors,
		)
	}
}
