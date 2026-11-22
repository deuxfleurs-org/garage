use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::{pin, Pin};
use std::sync::atomic::{self, AtomicU32};
use std::sync::{Arc, Mutex};
use std::task::Poll;

use arc_swap::ArcSwapOption;
use bytes::Bytes;
use log::{debug, error, trace};

use ed25519_compact::x25519;

use futures::Stream;
use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::{sleep_until, Duration, Instant};

#[cfg(feature = "telemetry")]
use opentelemetry::{
	trace::{FutureExt, Span, SpanKind, TraceContextExt, Tracer},
	Context, KeyValue,
};
#[cfg(feature = "telemetry")]
use opentelemetry_contrib::trace::propagator::binary::*;

use crate::chunk::*;
use crate::error::*;
use crate::key::*;
use crate::message::*;
use crate::netapp::*;
use crate::recv::*;
use crate::send::*;
use crate::stream::*;
use crate::util::*;

pub(crate) struct ClientConn {
	pub(crate) remote_addr: SocketAddr,
	pub(crate) peer_id: NodeID,

	noise: Mutex<snow::TransportState>,
	query_send: ArcSwapOption<mpsc::UnboundedSender<SendItem>>,

	next_query_number: AtomicU32,
	inflight: Mutex<HashMap<RequestID, oneshot::Sender<ByteStream>>>,
}

impl ClientConn {
	pub(crate) async fn init(
		netapp: Arc<NetApp>,
		mut socket: TcpStream,
		peer_id: NodeID,
	) -> Result<(), Error> {
		let deadline = Instant::now() + Duration::from_secs(60);

		let remote_addr = socket.peer_addr()?;

		let local_sk = x25519::SecretKey::from_ed25519(&netapp.privkey)?;
		let remote_pk = x25519::PublicKey::from_ed25519(&peer_id)?;

		// Initiate crypto
		let mut noise = snow::Builder::new(NOISE_PROTOCOL_PATTERN.parse().unwrap())
			.local_private_key(&*local_sk)?
			.remote_public_key(&*remote_pk)?
			.psk(0, &netapp.netid.0)?
			.build_initiator()?;

		let mut buf_plain = new_buffer();
		let mut buf_enc = new_buffer();

		let mut hs_proc = async || {
			// Handshake step 1: client to server
			let len_enc = noise.write_message(&[], &mut buf_enc[..])?;
			write_chunk(pin!(&mut socket), &buf_enc[..len_enc]).await?;

			// Handshake step 2: server to client
			let len_enc = read_chunk(pin!(&mut socket), &mut buf_enc).await?;
			noise.read_message(&buf_enc[..len_enc], &mut buf_plain[..])?;

			// Handshake step 3: client to server
			// Attach version tag and public ed25519 key,
			// the server will check both
			buf_plain[..32].copy_from_slice(&*netapp.id);
			buf_plain[32..48].copy_from_slice(&netapp.version_tag[..]);
			let len_enc = noise.write_message(&buf_plain[..48], &mut buf_enc[..])?;
			write_chunk(pin!(&mut socket), &buf_enc[..len_enc]).await?;

			Ok::<_, Error>(())
		};
		tokio::select! {
			res = hs_proc() => res?,
			_ = sleep_until(deadline) => return Err(Error::HandshakeTimeout),
		};

		// Handshake is done
		let noise = noise.into_transport_mode()?;

		debug!(
			"Handshake complete (client) with {}@{}",
			hex::encode(*peer_id),
			remote_addr
		);

		// Build and launch stuff that manages sending requests client-side
		let (read, write) = socket.into_split();

		let (query_send, query_recv) = mpsc::unbounded_channel();

		let (stop_recv_loop, stop_recv_loop_recv) = watch::channel(false);

		let conn = Arc::new(ClientConn {
			remote_addr,
			peer_id,
			noise: Mutex::new(noise),
			next_query_number: AtomicU32::from(RequestID::default()),
			query_send: ArcSwapOption::new(Some(Arc::new(query_send))),
			inflight: Mutex::new(HashMap::new()),
		});

		netapp.connected_as_client(peer_id, conn.clone());

		let debug_name = format!("CLI {}", hex::encode(&peer_id[..8]));

		tokio::spawn(async move {
			let debug_name_2 = debug_name.clone();
			let send_future = tokio::spawn(conn.clone().send_loop(query_recv, write, debug_name_2));

			let conn2 = conn.clone();
			let recv_future = tokio::spawn(async move {
				select! {
					r = conn2.recv_loop(read, debug_name) => r,
					_ = await_exit(stop_recv_loop_recv) => Ok(())
				}
			});

			send_future.await.log_err("ClientConn send_loop");

			// FIXME: should do here: wait for inflight requests to all have their response
			stop_recv_loop
				.send(true)
				.log_err("ClientConn send true to stop_recv_loop");

			recv_future.await.log_err("ClientConn recv_loop");

			// Make sure we don't wait on any more requests that won't
			// have a response
			conn.inflight.lock().unwrap().clear();

			netapp.disconnected_as_client(&peer_id, conn);
		});

		Ok(())
	}

	pub fn close(&self) {
		self.query_send.store(None);
	}

	pub(crate) async fn call<T>(
		self: Arc<Self>,
		req: Req<T>,
		path: &str,
		prio: RequestPriority,
	) -> Result<Resp<T>, Error>
	where
		T: Message,
	{
		let query_send = self.query_send.load_full().ok_or(Error::ConnectionClosed)?;

		let id = self
			.next_query_number
			.fetch_add(1, atomic::Ordering::Relaxed);

		cfg_if::cfg_if! {
			if #[cfg(feature = "telemetry")] {
				let tracer = opentelemetry::global::tracer("netapp");
				let mut span = tracer.span_builder(format!("RPC >> {}", path))
					.with_kind(SpanKind::Client)
					.start(&tracer);
				let propagator = BinaryPropagator::new();
				let telemetry_id: Bytes = propagator.serialize_into_bytes(span.span_context()).to_vec().into();
			} else {
				let telemetry_id: Bytes = Bytes::new();
			}
		};

		// Encode request
		let req_enc = req.into_enc(prio, path.as_bytes().to_vec().into(), telemetry_id);
		let req_msg_len = req_enc.msg.len();
		let (req_stream, req_order) = req_enc.encode();

		// Send request through
		let (resp_send, resp_recv) = oneshot::channel();
		let old = self.inflight.lock().unwrap().insert(id, resp_send);
		if let Some(old_ch) = old {
			error!(
				"Too many inflight requests! RequestID collision. Interrupting previous request."
			);
			let _ = old_ch.send(Box::pin(futures::stream::once(async move {
				Err(std::io::Error::other(
					"RequestID collision, too many inflight requests",
				))
			})));
		}

		debug!(
			"request: query_send {}, path {}, prio {} (serialized message: {} bytes)",
			id, path, prio, req_msg_len
		);

		#[cfg(feature = "telemetry")]
		span.set_attribute(KeyValue::new("len_query_msg", req_msg_len as i64));

		query_send.send(SendItem::Stream(id, prio, req_order, req_stream))?;

		let canceller = CancelOnDrop::new(id, query_send.as_ref().clone());

		cfg_if::cfg_if! {
			if #[cfg(feature = "telemetry")] {
				let stream = resp_recv
					.with_context(Context::current_with_span(span))
					.await?;
			} else {
				let stream = resp_recv.await?;
			}
		}

		let stream = Box::pin(canceller.for_stream(stream));

		let resp_enc = RespEnc::decode(stream).await?;
		debug!("client: got response to request {} (path {})", id, path);
		Resp::from_enc(resp_enc)
	}
}

impl Crypto for ClientConn {
	fn noise(self: &Arc<Self>) -> std::sync::MutexGuard<'_, snow::TransportState> {
		self.noise.lock().unwrap()
	}
}

impl SendLoop for ClientConn {}

impl RecvLoop for ClientConn {
	fn recv_handler(self: &Arc<Self>, id: RequestID, stream: ByteStream) {
		trace!("ClientConn recv_handler {}", id);

		let mut inflight = self.inflight.lock().unwrap();
		if let Some(ch) = inflight.remove(&id) {
			if ch.send(stream).is_err() {
				debug!("Could not send request response, probably because request was interrupted. Dropping response.");
			}
		} else {
			debug!("Got unexpected response to request {}, dropping it", id);
		}
	}
}

// ----

struct CancelOnDrop {
	id: RequestID,
	query_send: mpsc::UnboundedSender<SendItem>,
}

impl CancelOnDrop {
	fn new(id: RequestID, query_send: mpsc::UnboundedSender<SendItem>) -> Self {
		Self { id, query_send }
	}
	fn for_stream(self, stream: ByteStream) -> CancelOnDropStream {
		CancelOnDropStream {
			cancel: Some(self),
			stream,
		}
	}
}

impl Drop for CancelOnDrop {
	fn drop(&mut self) {
		trace!("cancelling request {}", self.id);
		let _ = self.query_send.send(SendItem::Cancel(self.id));
	}
}

#[pin_project::pin_project]
struct CancelOnDropStream {
	cancel: Option<CancelOnDrop>,
	#[pin]
	stream: ByteStream,
}

impl Stream for CancelOnDropStream {
	type Item = Packet;

	fn poll_next(
		self: Pin<&mut Self>,
		cx: &mut std::task::Context<'_>,
	) -> Poll<Option<Self::Item>> {
		let this = self.project();
		let res = this.stream.poll_next(cx);
		if matches!(res, Poll::Ready(None)) {
			if let Some(c) = this.cancel.take() {
				std::mem::forget(c);
			}
		}
		res
	}

	fn size_hint(&self) -> (usize, Option<usize>) {
		self.stream.size_hint()
	}
}
