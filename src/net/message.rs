use std::fmt;
use std::marker::PhantomData;
use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};
use rand::prelude::*;
use serde::{Deserialize, Serialize};

use futures::stream::StreamExt;

use crate::error::*;
use crate::stream::*;
use crate::util::*;

/// Priority of a request (click to read more about priorities).
///
/// This priority value is used to priorize messages
/// in the send queue of the client, and their responses in the send queue of the
/// server. Lower values mean higher priority.
///
/// This mechanism is useful for messages bigger than the maximum chunk size
/// (set at `0x4000` bytes), such as large file transfers.
/// In such case, all of the messages in the send queue with the highest priority
/// will take turns to send individual chunks, in a round-robin fashion.
/// Once all highest priority messages are sent successfully, the messages with
/// the next highest priority will begin being sent in the same way.
///
/// The same priority value is given to a request and to its associated response.
pub type RequestPriority = u8;

// Usage of priority levels in Garage:
//
// PRIO_HIGH
//      for liveness check events such as pings and important
//      reconfiguration events such as layout changes
//
// PRIO_NORMAL
//      for standard interactive requests to exchange metadata
//
// PRIO_NORMAL | PRIO_SECONDARY
//      for standard interactive requests to exchange block data
//
// PRIO_BACKGROUND
//      for background resync requests to exchange metadata
// PRIO_BACKGROUND | PRIO_SECONDARY
//      for background resync requests to exchange block data

/// Priority class: high
pub const PRIO_HIGH: RequestPriority = 0x20;
/// Priority class: normal
pub const PRIO_NORMAL: RequestPriority = 0x40;
/// Priority class: background
pub const PRIO_BACKGROUND: RequestPriority = 0x80;

/// Priority: primary among given class
pub const PRIO_PRIMARY: RequestPriority = 0x00;
/// Priority: secondary among given class (ex: `PRIO_HIGH | PRIO_SECONDARY`)
pub const PRIO_SECONDARY: RequestPriority = 0x01;

// ----

/// An order tag can be added to a message or a response to indicate
/// whether it should be sent after or before other messages with order tags
/// referencing a same stream
#[derive(Clone, Copy, Serialize, Deserialize, Debug)]
pub struct OrderTag(pub(crate) u64, pub(crate) u64);

/// A stream is an opaque identifier that defines a set of messages
/// or responses that are ordered wrt one another using to order tags.
#[derive(Clone, Copy)]
pub struct OrderTagStream(u64);

impl OrderTag {
	/// Create a new stream from which to generate order tags. Example:
	/// ```ignore
	/// let stream = OrderTag.stream();
	/// let tag_1 = stream.order(1);
	/// let tag_2 = stream.order(2);
	/// ```
	pub fn stream() -> OrderTagStream {
		OrderTagStream(thread_rng().gen())
	}
}
impl OrderTagStream {
	/// Create the order tag for message `order` in this stream
	pub fn order(&self, order: u64) -> OrderTag {
		OrderTag(self.0, order)
	}
}

// ----

/// This trait should be implemented by all messages your application
/// wants to handle. It specifies which data type should be sent
/// as a response to this message in the RPC protocol.
pub trait Message: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static {
	/// The type of the response that is sent in response to this message
	type Response: Serialize + for<'de> Deserialize<'de> + Send + Sync + 'static;
}

// ----

/// The Req<M> is a helper object used to create requests and attach them
/// a stream of data. If the stream is a fixed Bytes and not a ByteStream,
/// Req<M> is cheaply cloneable to allow the request to be sent to different
/// peers (Clone will panic if the stream is a ByteStream).
pub struct Req<M: Message> {
	pub(crate) msg: Arc<M>,
	pub(crate) msg_ser: Option<Bytes>,
	pub(crate) stream: AttachedStream,
	pub(crate) order_tag: Option<OrderTag>,
}

impl<M: Message> Req<M> {
	/// Creates a new request from a base message `M`
	pub fn new(v: M) -> Result<Self, Error> {
		Ok(v.into_req()?)
	}

	/// Attach a stream to message in request, where the stream is streamed
	/// from a fixed `Bytes` buffer
	pub fn with_stream_from_buffer(self, b: Bytes) -> Self {
		Self {
			stream: AttachedStream::Fixed(b),
			..self
		}
	}

	/// Attach a stream to message in request, where the stream is
	/// an instance of `ByteStream`. Note than when a `Req<M>` has an attached
	/// stream which is a `ByteStream` instance, it can no longer be cloned
	/// to be sent to different nodes (`.clone()` will panic)
	pub fn with_stream(self, b: ByteStream) -> Self {
		Self {
			stream: AttachedStream::Stream(b),
			..self
		}
	}

	/// Add an order tag to this request to indicate in which order it should
	/// be sent.
	pub fn with_order_tag(self, order_tag: OrderTag) -> Self {
		Self {
			order_tag: Some(order_tag),
			..self
		}
	}

	/// Get a reference to the message `M` contained in this request
	pub fn msg(&self) -> &M {
		&self.msg
	}

	/// Takes out the stream attached to this request, if any
	pub fn take_stream(&mut self) -> Option<ByteStream> {
		std::mem::replace(&mut self.stream, AttachedStream::None).into_stream()
	}

	pub(crate) fn into_enc(
		self,
		prio: RequestPriority,
		path: Bytes,
		telemetry_id: Bytes,
	) -> ReqEnc {
		ReqEnc {
			prio,
			path,
			telemetry_id,
			msg: self.msg_ser.unwrap(),
			stream: self.stream.into_stream(),
			order_tag: self.order_tag,
		}
	}

	pub(crate) fn from_enc(enc: ReqEnc) -> Result<Self, rmp_serde::decode::Error> {
		let msg = rmp_serde::decode::from_slice(&enc.msg)?;
		Ok(Req {
			msg: Arc::new(msg),
			msg_ser: Some(enc.msg),
			stream: enc
				.stream
				.map(AttachedStream::Stream)
				.unwrap_or(AttachedStream::None),
			order_tag: enc.order_tag,
		})
	}
}

/// `IntoReq<M>` represents any object that can be transformed into `Req<M>`
pub trait IntoReq<M: Message> {
	/// Transform the object into a `Req<M>`, serializing the message M
	/// to be sent to remote nodes
	fn into_req(self) -> Result<Req<M>, rmp_serde::encode::Error>;
	/// Transform the object into a `Req<M>`, skipping the serialization
	/// of message M, in the case we are not sending this RPC message to
	/// a remote node
	fn into_req_local(self) -> Req<M>;
}

impl<M: Message> IntoReq<M> for M {
	fn into_req(self) -> Result<Req<M>, rmp_serde::encode::Error> {
		let msg_ser = rmp_to_vec_all_named(&self)?;
		Ok(Req {
			msg: Arc::new(self),
			msg_ser: Some(Bytes::from(msg_ser)),
			stream: AttachedStream::None,
			order_tag: None,
		})
	}
	fn into_req_local(self) -> Req<M> {
		Req {
			msg: Arc::new(self),
			msg_ser: None,
			stream: AttachedStream::None,
			order_tag: None,
		}
	}
}

impl<M: Message> IntoReq<M> for Req<M> {
	fn into_req(self) -> Result<Req<M>, rmp_serde::encode::Error> {
		Ok(self)
	}
	fn into_req_local(self) -> Req<M> {
		self
	}
}

impl<M: Message> Clone for Req<M> {
	fn clone(&self) -> Self {
		let stream = match &self.stream {
			AttachedStream::None => AttachedStream::None,
			AttachedStream::Fixed(b) => AttachedStream::Fixed(b.clone()),
			AttachedStream::Stream(_) => {
				panic!("Cannot clone a Req<_> with a non-buffer attached stream")
			}
		};
		Self {
			msg: self.msg.clone(),
			msg_ser: self.msg_ser.clone(),
			stream,
			order_tag: self.order_tag,
		}
	}
}

impl<M> fmt::Debug for Req<M>
where
	M: Message + fmt::Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
		write!(f, "Req[{:?}", self.msg)?;
		match &self.stream {
			AttachedStream::None => write!(f, "]"),
			AttachedStream::Fixed(b) => write!(f, "; stream=buf:{}]", b.len()),
			AttachedStream::Stream(_) => write!(f, "; stream]"),
		}
	}
}

// ----

/// The Resp<M> represents a full response from a RPC that may have
/// an attached stream.
pub struct Resp<M: Message> {
	pub(crate) _phantom: PhantomData<M>,
	pub(crate) msg: M::Response,
	pub(crate) stream: AttachedStream,
	pub(crate) order_tag: Option<OrderTag>,
}

impl<M: Message> Resp<M> {
	/// Creates a new response from a base response message
	pub fn new(v: M::Response) -> Self {
		Resp {
			_phantom: Default::default(),
			msg: v,
			stream: AttachedStream::None,
			order_tag: None,
		}
	}

	/// Attach a stream to message in response, where the stream is streamed
	/// from a fixed `Bytes` buffer
	pub fn with_stream_from_buffer(self, b: Bytes) -> Self {
		Self {
			stream: AttachedStream::Fixed(b),
			..self
		}
	}

	/// Attach a stream to message in response, where the stream is
	/// an instance of `ByteStream`.
	pub fn with_stream(self, b: ByteStream) -> Self {
		Self {
			stream: AttachedStream::Stream(b),
			..self
		}
	}

	/// Add an order tag to this response to indicate in which order it should
	/// be sent.
	pub fn with_order_tag(self, order_tag: OrderTag) -> Self {
		Self {
			order_tag: Some(order_tag),
			..self
		}
	}

	/// Get a reference to the response message contained in this request
	pub fn msg(&self) -> &M::Response {
		&self.msg
	}

	/// Transforms the `Resp<M>` into the response message it contains,
	/// dropping everything else (including attached data stream)
	pub fn into_msg(self) -> M::Response {
		self.msg
	}

	/// Transforms the `Resp<M>` into, on the one side, the response message
	/// it contains, and on the other side, the associated data stream
	/// if it exists
	pub fn into_parts(self) -> (M::Response, Option<ByteStream>) {
		(self.msg, self.stream.into_stream())
	}

	pub(crate) fn into_enc(self) -> Result<RespEnc, rmp_serde::encode::Error> {
		Ok(RespEnc {
			msg: rmp_to_vec_all_named(&self.msg)?.into(),
			stream: self.stream.into_stream(),
			order_tag: self.order_tag,
		})
	}

	pub(crate) fn from_enc(enc: RespEnc) -> Result<Self, Error> {
		let msg = rmp_serde::decode::from_slice(&enc.msg)?;
		Ok(Self {
			_phantom: Default::default(),
			msg,
			stream: enc
				.stream
				.map(AttachedStream::Stream)
				.unwrap_or(AttachedStream::None),
			order_tag: enc.order_tag,
		})
	}
}

impl<M> fmt::Debug for Resp<M>
where
	M: Message,
	<M as Message>::Response: fmt::Debug,
{
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> Result<(), fmt::Error> {
		write!(f, "Resp[{:?}", self.msg)?;
		match &self.stream {
			AttachedStream::None => write!(f, "]"),
			AttachedStream::Fixed(b) => write!(f, "; stream=buf:{}]", b.len()),
			AttachedStream::Stream(_) => write!(f, "; stream]"),
		}
	}
}

// ----

pub(crate) enum AttachedStream {
	None,
	Fixed(Bytes),
	Stream(ByteStream),
}

impl AttachedStream {
	pub fn into_stream(self) -> Option<ByteStream> {
		match self {
			AttachedStream::None => None,
			AttachedStream::Fixed(b) => Some(Box::pin(futures::stream::once(async move { Ok(b) }))),
			AttachedStream::Stream(s) => Some(s),
		}
	}
}

// ---- ----

/// Encoding for requests into a ByteStream:
/// - priority: u8
/// - path length: u8
/// - path: [u8; path length]
/// - telemetry id length: u8
/// - telemetry id: [u8; telemetry id length]
/// - msg len: u32
/// - msg [u8; ..]
/// - the attached stream as the rest of the encoded stream
pub(crate) struct ReqEnc {
	pub(crate) prio: RequestPriority,
	pub(crate) path: Bytes,
	pub(crate) telemetry_id: Bytes,
	pub(crate) msg: Bytes,
	pub(crate) stream: Option<ByteStream>,
	pub(crate) order_tag: Option<OrderTag>,
}

impl ReqEnc {
	pub(crate) fn encode(self) -> (ByteStream, Option<OrderTag>) {
		let mut buf = BytesMut::with_capacity(
			self.path.len() + self.telemetry_id.len() + self.msg.len() + 16,
		);

		buf.put_u8(self.prio);

		buf.put_u8(self.path.len() as u8);
		buf.put(self.path);

		buf.put_u8(self.telemetry_id.len() as u8);
		buf.put(&self.telemetry_id[..]);

		buf.put_u32(self.msg.len() as u32);

		let header = buf.freeze();

		let res_stream: ByteStream = if let Some(stream) = self.stream {
			Box::pin(futures::stream::iter([Ok(header), Ok(self.msg)]).chain(stream))
		} else {
			Box::pin(futures::stream::iter([Ok(header), Ok(self.msg)]))
		};
		(res_stream, self.order_tag)
	}

	pub(crate) async fn decode(stream: ByteStream) -> Result<Self, Error> {
		Self::decode_aux(stream)
			.await
			.map_err(read_exact_error_to_error)
	}

	async fn decode_aux(stream: ByteStream) -> Result<Self, ReadExactError> {
		let mut reader = ByteStreamReader::new(stream);

		let prio = reader.read_u8().await?;

		let path_len = reader.read_u8().await?;
		let path = reader.read_exact(path_len as usize).await?;

		let telemetry_id_len = reader.read_u8().await?;
		let telemetry_id = reader.read_exact(telemetry_id_len as usize).await?;

		let msg_len = reader.read_u32().await?;
		let msg = reader.read_exact(msg_len as usize).await?;

		Ok(Self {
			prio,
			path,
			telemetry_id,
			msg,
			stream: Some(reader.into_stream()),
			order_tag: None,
		})
	}
}

/// Encoding for responses into a ByteStream:
/// IF SUCCESS:
/// - 0: u8
/// - msg len: u32
/// - msg [u8; ..]
/// - the attached stream as the rest of the encoded stream
/// IF ERROR:
/// - message length + 1: u8
/// - error code: u8
/// - message: [u8; message_length]
pub(crate) struct RespEnc {
	msg: Bytes,
	stream: Option<ByteStream>,
	order_tag: Option<OrderTag>,
}

impl RespEnc {
	pub(crate) fn encode(resp: Result<Self, Error>) -> (ByteStream, Option<OrderTag>) {
		match resp {
			Ok(Self {
				msg,
				stream,
				order_tag,
			}) => {
				let mut buf = BytesMut::with_capacity(4);
				buf.put_u32(msg.len() as u32);
				let header = buf.freeze();

				let res_stream: ByteStream = if let Some(stream) = stream {
					Box::pin(futures::stream::iter([Ok(header), Ok(msg)]).chain(stream))
				} else {
					Box::pin(futures::stream::iter([Ok(header), Ok(msg)]))
				};
				(res_stream, order_tag)
			}
			Err(err) => {
				let err = std::io::Error::new(
					std::io::ErrorKind::Other,
					format!("netapp error: {}", err),
				);
				(
					Box::pin(futures::stream::once(async move { Err(err) })),
					None,
				)
			}
		}
	}

	pub(crate) async fn decode(stream: ByteStream) -> Result<Self, Error> {
		Self::decode_aux(stream)
			.await
			.map_err(read_exact_error_to_error)
	}

	async fn decode_aux(stream: ByteStream) -> Result<Self, ReadExactError> {
		let mut reader = ByteStreamReader::new(stream);

		let msg_len = reader.read_u32().await?;
		let msg = reader.read_exact(msg_len as usize).await?;

		// Check whether the response stream still has data or not.
		// If no more data is coming, this will defuse the request canceller.
		// If we didn't do this, and the client doesn't try to read from the stream,
		// the request canceller doesn't know that we read everything and
		// sends a cancellation message to the server (which they don't care about).
		reader.fill_buffer().await;

		Ok(Self {
			msg,
			stream: Some(reader.into_stream()),
			order_tag: None,
		})
	}
}

fn read_exact_error_to_error(e: ReadExactError) -> Error {
	match e {
		ReadExactError::Stream(err) => Error::Remote(err.kind(), err.to_string()),
		ReadExactError::UnexpectedEos => Error::Framing,
	}
}
