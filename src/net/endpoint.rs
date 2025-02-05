use std::future::Future;
use std::marker::PhantomData;
use std::sync::Arc;

use arc_swap::ArcSwapOption;
use futures::future::{BoxFuture, FutureExt};

use crate::error::Error;
use crate::message::*;
use crate::netapp::*;

/// This trait should be implemented by an object of your application
/// that can handle a message of type `M`, if it wishes to handle
/// streams attached to the request and/or to send back streams
/// attached to the response..
///
/// The handler object should be in an Arc, see `Endpoint::set_handler`
pub trait StreamingEndpointHandler<M>: Send + Sync
where
	M: Message,
{
	fn handle(self: &Arc<Self>, m: Req<M>, from: NodeID) -> impl Future<Output = Resp<M>> + Send;
}

/// If one simply wants to use an endpoint in a client fashion,
/// without locally serving requests to that endpoint,
/// use the unit type `()` as the handler type:
/// it will panic if it is ever made to handle request.
impl<M: Message> EndpointHandler<M> for () {
	async fn handle(self: &Arc<()>, _m: &M, _from: NodeID) -> M::Response {
		panic!("This endpoint should not have a local handler.");
	}
}

// ----

/// This trait should be implemented by an object of your application
/// that can handle a message of type `M`, in the cases where it doesn't
/// care about attached stream in the request nor in the response.
pub trait EndpointHandler<M>: Send + Sync
where
	M: Message,
{
	fn handle(self: &Arc<Self>, m: &M, from: NodeID) -> impl Future<Output = M::Response> + Send;
}

impl<T, M> StreamingEndpointHandler<M> for T
where
	T: EndpointHandler<M>,
	M: Message,
{
	async fn handle(self: &Arc<Self>, mut m: Req<M>, from: NodeID) -> Resp<M> {
		// Immediately drop stream to ignore all data that comes in,
		// instead of buffering it indefinitely
		drop(m.take_stream());
		Resp::new(EndpointHandler::handle(self, m.msg(), from).await)
	}
}

// ----

/// This struct represents an endpoint for message of type `M`.
///
/// Creating a new endpoint is done by calling `NetApp::endpoint`.
/// An endpoint is identified primarily by its path, which is specified
/// at creation time.
///
/// An `Endpoint` is used both to send requests to remote nodes,
/// and to specify the handler for such requests on the local node.
/// The type `H` represents the type of the handler object for
/// endpoint messages (see `StreamingEndpointHandler`).
pub struct Endpoint<M, H>
where
	M: Message,
	H: StreamingEndpointHandler<M>,
{
	_phantom: PhantomData<M>,
	netapp: Arc<NetApp>,
	path: String,
	handler: ArcSwapOption<H>,
}

impl<M, H> Endpoint<M, H>
where
	M: Message,
	H: StreamingEndpointHandler<M>,
{
	pub(crate) fn new(netapp: Arc<NetApp>, path: String) -> Self {
		Self {
			_phantom: PhantomData::default(),
			netapp,
			path,
			handler: ArcSwapOption::from(None),
		}
	}

	/// Get the path of this endpoint
	pub fn path(&self) -> &str {
		&self.path
	}

	/// Set the object that is responsible of handling requests to
	/// this endpoint on the local node.
	pub fn set_handler(&self, h: Arc<H>) {
		self.handler.swap(Some(h));
	}

	/// Call this endpoint on a remote node (or on the local node,
	/// for that matter). This function invokes the full version that
	/// allows to attach a stream to the request and to
	/// receive such a stream attached to the response.
	pub async fn call_streaming<T>(
		&self,
		target: &NodeID,
		req: T,
		prio: RequestPriority,
	) -> Result<Resp<M>, Error>
	where
		T: IntoReq<M>,
	{
		if *target == self.netapp.id {
			match self.handler.load_full() {
				None => Err(Error::NoHandler),
				Some(h) => Ok(h.handle(req.into_req_local(), self.netapp.id).await),
			}
		} else {
			let conn = self
				.netapp
				.client_conns
				.read()
				.unwrap()
				.get(target)
				.cloned();
			match conn {
				None => Err(Error::Message(format!(
					"Not connected: {}",
					hex::encode(&target[..8])
				))),
				Some(c) => c.call(req.into_req()?, self.path.as_str(), prio).await,
			}
		}
	}

	/// Call this endpoint on a remote node. This function is the simplified
	/// version that doesn't allow to have streams attached to the request
	/// or the response; see `call_streaming` for the full version.
	pub async fn call(
		&self,
		target: &NodeID,
		req: M,
		prio: RequestPriority,
	) -> Result<<M as Message>::Response, Error> {
		Ok(self.call_streaming(target, req, prio).await?.into_msg())
	}
}

// ---- Internal stuff ----

pub(crate) type DynEndpoint = Box<dyn GenericEndpoint + Send + Sync>;

pub(crate) trait GenericEndpoint {
	fn handle(&self, req_enc: ReqEnc, from: NodeID) -> BoxFuture<Result<RespEnc, Error>>;
	fn drop_handler(&self);
	fn clone_endpoint(&self) -> DynEndpoint;
}

#[derive(Clone)]
pub(crate) struct EndpointArc<M, H>(pub(crate) Arc<Endpoint<M, H>>)
where
	M: Message,
	H: StreamingEndpointHandler<M>;

impl<M, H> GenericEndpoint for EndpointArc<M, H>
where
	M: Message,
	H: StreamingEndpointHandler<M> + 'static,
{
	fn handle(&self, req_enc: ReqEnc, from: NodeID) -> BoxFuture<Result<RespEnc, Error>> {
		async move {
			match self.0.handler.load_full() {
				None => Err(Error::NoHandler),
				Some(h) => {
					let req = Req::from_enc(req_enc)?;
					let res = h.handle(req, from).await;
					Ok(res.into_enc()?)
				}
			}
		}
		.boxed()
	}

	fn drop_handler(&self) {
		self.0.handler.swap(None);
	}

	fn clone_endpoint(&self) -> DynEndpoint {
		Box::new(Self(self.0.clone()))
	}
}
