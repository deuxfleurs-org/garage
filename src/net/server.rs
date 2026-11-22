use std::collections::HashMap;
use std::net::SocketAddr;
use std::pin::pin;
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwapOption;
use log::*;

use tokio::net::TcpStream;
use tokio::select;
use tokio::sync::{mpsc, watch};
use tokio::time::{sleep_until, Duration, Instant};

use ed25519_compact::x25519;

#[cfg(feature = "telemetry")]
use opentelemetry::{
	trace::{
		FutureExt, Span, SpanContext, SpanId, SpanKind, TraceContextExt, TraceFlags, TraceId,
		TraceState, Tracer,
	},
	Context, KeyValue,
};
#[cfg(feature = "telemetry")]
use opentelemetry_contrib::trace::propagator::binary::*;
#[cfg(feature = "telemetry")]
use rand::{thread_rng, Rng};

use crate::chunk::*;
use crate::error::*;
use crate::key::*;
use crate::message::*;
use crate::netapp::*;
use crate::recv::*;
use crate::send::*;
use crate::stream::*;
use crate::util::*;

// The client and server connection structs (client.rs and server.rs)
// build upon the chunking mechanism which is exclusively contained
// in proto.rs.
// Here, we just care about sending big messages without size limit.
// The format of these messages is described below.
// Chunking happens independently.

// Request message format (client -> server):
// - u8 priority
// - u8 path length
// - [u8; path length] path
// - [u8; *] data

// Response message format (server -> client):
// - u8 response code
// - [u8; *] response

pub(crate) struct ServerConn {
	pub(crate) remote_addr: SocketAddr,
	pub(crate) peer_id: NodeID,

	netapp: Arc<NetApp>,

	// FIXME: split read/write halves to avoid using a mutex
	// (not currently supported by the snow crate)
	noise: Mutex<snow::TransportState>,

	resp_send: ArcSwapOption<mpsc::UnboundedSender<SendItem>>,
	running_handlers: Mutex<HashMap<RequestID, tokio::task::JoinHandle<()>>>,
}

impl ServerConn {
	pub(crate) async fn run(
		netapp: Arc<NetApp>,
		mut socket: TcpStream,
		must_exit: watch::Receiver<bool>,
	) -> Result<(), Error> {
		let deadline = Instant::now() + Duration::from_secs(60);

		let remote_addr = socket.peer_addr()?;

		let local_sk = x25519::SecretKey::from_ed25519(&netapp.privkey)?;

		// Initiate crypto
		let mut noise = snow::Builder::new(NOISE_PROTOCOL_PATTERN.parse().unwrap())
			.local_private_key(&*local_sk)?
			.psk(0, &netapp.netid.0)?
			.build_responder()?;

		let mut buf_plain = new_buffer();
		let mut buf_enc = new_buffer();

		let mut hs_proc = async || {
			// Handshake step 1: client to server
			let len_enc = read_chunk(pin!(&mut socket), &mut buf_enc).await?;
			noise.read_message(&buf_enc[..len_enc], &mut buf_plain[..])?;

			// Handshake step 2: server to client
			let len_enc = noise.write_message(&[], &mut buf_enc[..])?;
			write_chunk(pin!(&mut socket), &buf_enc[..len_enc]).await?;

			// Handshake step 3: client to server
			// During this step, the client attaches a version tag to the message
			// Check that we are running the same version as them.
			let len_enc = read_chunk(pin!(&mut socket), &mut buf_enc).await?;
			let len_plain = noise.read_message(&buf_enc[..len_enc], &mut buf_plain[..])?;

			if len_plain != 48 {
				return Err(Error::Framing);
			}

			let node_id = NodeID::new(checked_length(&buf_plain[..32]).unwrap());
			let node_id_x = x25519::PublicKey::from_ed25519(&node_id)?;
			if *node_id_x != *noise.get_remote_static().unwrap() {
				return Err(Error::Message("key mismatch".to_string()));
			}

			let version_tag = &buf_plain[32..48];
			if version_tag != &netapp.version_tag[..] {
				return Err(Error::VersionMismatch(format!(
					"local: {}, remote: {}",
					hex::encode(netapp.version_tag),
					hex::encode(version_tag)
				)));
			}

			Ok::<_, Error>(node_id)
		};
		let peer_id = tokio::select! {
			res = hs_proc() => res?,
			_ = sleep_until(deadline) => return Err(Error::HandshakeTimeout),
		};

		// Handshake is done
		let noise = noise.into_transport_mode()?;

		debug!(
			"Handshake complete (server) with {}@{}",
			hex::encode(*peer_id),
			remote_addr
		);

		// Build and launch stuff that handles requests server-side
		let (read, write) = socket.into_split();

		let (resp_send, resp_recv) = mpsc::unbounded_channel();

		let conn = Arc::new(ServerConn {
			netapp: netapp.clone(),
			remote_addr,
			noise: Mutex::new(noise),
			peer_id,
			resp_send: ArcSwapOption::new(Some(Arc::new(resp_send))),
			running_handlers: Mutex::new(HashMap::new()),
		});

		netapp.connected_as_server(peer_id, conn.clone());

		let debug_name = format!("SRV {}", hex::encode(&peer_id[..8]));
		let debug_name_2 = debug_name.clone();

		let conn2 = conn.clone();
		let recv_future = tokio::spawn(async move {
			select! {
				r = conn2.recv_loop(read, debug_name_2) => r,
				_ = await_exit(must_exit) => Ok(())
			}
		});
		let send_future = tokio::spawn(conn.clone().send_loop(resp_recv, write, debug_name));

		recv_future.await.log_err("ServerConn recv_loop");
		conn.resp_send.store(None);
		send_future.await.log_err("ServerConn send_loop");

		netapp.disconnected_as_server(&peer_id, conn);

		Ok(())
	}

	async fn recv_handler_aux(self: &Arc<Self>, req_enc: ReqEnc) -> Result<RespEnc, Error> {
		let path = String::from_utf8(req_enc.path.to_vec())?;

		let handler_opt = {
			let endpoints = self.netapp.endpoints.read().unwrap();
			endpoints.get(&path).map(|e| e.clone_endpoint())
		};

		if let Some(handler) = handler_opt {
			cfg_if::cfg_if! {
				if #[cfg(feature = "telemetry")] {
					let tracer = opentelemetry::global::tracer("netapp");

					let mut span = if !req_enc.telemetry_id.is_empty() {
						let propagator = BinaryPropagator::new();
						let context = propagator.deserialize_from_bytes(&req_enc.telemetry_id);
						let context = Context::new().with_remote_span_context(context);
						tracer.span_builder(format!(">> RPC {}", path))
							.with_kind(SpanKind::Server)
							.start_with_context(&tracer, &context)
					} else {
						let mut rng = thread_rng();
						let trace_id = TraceId::from_bytes(rng.gen());
						// Force the trace id of this root span
						let context = Context::new().with_remote_span_context(SpanContext::new(
							trace_id,
							SpanId::INVALID,
							TraceFlags::SAMPLED,
							true,
							TraceState::default(),
						));
						tracer
							.span_builder(format!(">> RPC {}", path))
							.with_kind(SpanKind::Server)
							.start_with_context(&tracer, &context)
					};
					span.set_attribute(KeyValue::new("path", path.to_string()));
					span.set_attribute(KeyValue::new("len_query_msg", req_enc.msg.len() as i64));

					handler.handle(req_enc, self.peer_id)
						.with_context(Context::current_with_span(span))
						.await
				} else {
					handler.handle(req_enc, self.peer_id).await
				}
			}
		} else {
			Err(Error::NoHandler)
		}
	}
}

impl Crypto for ServerConn {
	fn noise(self: &Arc<Self>) -> std::sync::MutexGuard<'_, snow::TransportState> {
		self.noise.lock().unwrap()
	}
}

impl SendLoop for ServerConn {}

impl RecvLoop for ServerConn {
	fn recv_handler(self: &Arc<Self>, id: RequestID, stream: ByteStream) {
		let resp_send = match self.resp_send.load_full() {
			Some(c) => c,
			None => return,
		};

		let mut rh = self.running_handlers.lock().unwrap();

		let self2 = self.clone();
		let jh = tokio::spawn(async move {
			debug!("server: recv_handler got {}", id);

			let (prio, resp_enc_result) = match ReqEnc::decode(stream).await {
				Ok(req_enc) => (req_enc.prio, self2.recv_handler_aux(req_enc).await),
				Err(e) => (PRIO_NORMAL, Err(e)),
			};

			debug!("server: sending response to {}", id);

			let (resp_stream, resp_order) = RespEnc::encode(resp_enc_result);
			resp_send
				.send(SendItem::Stream(id, prio, resp_order, resp_stream))
				.log_err("ServerConn recv_handler send resp bytes");

			self2.running_handlers.lock().unwrap().remove(&id);
		});

		rh.insert(id, jh);
	}

	fn cancel_handler(self: &Arc<Self>, id: RequestID) {
		trace!("received cancel for request {}", id);

		// If the handler is still running, abort it now
		if let Some(jh) = self.running_handlers.lock().unwrap().remove(&id) {
			jh.abort();
		}

		// Inform the response sender that we don't need to send the response
		if let Some(resp_send) = self.resp_send.load_full() {
			let _ = resp_send.send(SendItem::Cancel(id));
		}
	}
}
