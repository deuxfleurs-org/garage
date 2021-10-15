//! Contain structs related to making RPCs
use std::sync::Arc;
use std::time::Duration;

use futures::future::join_all;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt;
use futures_util::future::FutureExt;
use tokio::select;

pub use netapp::endpoint::{Endpoint, EndpointHandler, Message as Rpc};
use netapp::peering::fullmesh::FullMeshPeeringStrategy;
pub use netapp::proto::*;
pub use netapp::{NetApp, NodeID};

use garage_util::background::BackgroundRunner;
use garage_util::error::Error;
use garage_util::data::Uuid;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);

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
pub struct RpcHelper {
	pub(crate) fullmesh: Arc<FullMeshPeeringStrategy>,
	pub(crate) background: Arc<BackgroundRunner>,
}

impl RpcHelper {
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
		let node_id = to.into();
		select! {
			res = endpoint.call(&node_id, &msg, strat.rs_priority) => Ok(res??),
			_ = tokio::time::sleep(strat.rs_timeout) => Err(Error::Timeout),
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
			.fullmesh
			.get_peer_list()
			.iter()
			.map(|p| p.id.into())
			.collect::<Vec<_>>();
		self.call_many(endpoint, &to[..], msg, strat).await
	}

	/// Make a RPC call to multiple servers, returning either a Vec of responses, or an error if
	/// strategy could not be respected due to too many errors
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
		S: Send,
	{
		let msg = Arc::new(msg);
		let mut resp_stream = to
			.to_vec()
			.into_iter()
			.map(|to| {
				let self2 = self.clone();
				let msg = msg.clone();
				let endpoint2 = endpoint.clone();
				async move { self2.call_arc(&endpoint2, to, msg, strategy).await }
			})
			.collect::<FuturesUnordered<_>>();

		let mut results = vec![];
		let mut errors = vec![];
		let quorum = strategy.rs_quorum.unwrap_or(to.len());

		while let Some(resp) = resp_stream.next().await {
			match resp {
				Ok(msg) => {
					results.push(msg);
					if results.len() >= quorum {
						break;
					}
				}
				Err(e) => {
					errors.push(e);
				}
			}
		}

		if results.len() >= quorum {
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
			Err(Error::TooManyErrors(errors))
		}
	}
}
