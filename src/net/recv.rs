use std::collections::HashMap;
use std::sync::Arc;

use bytes::Bytes;
use log::*;

use futures::AsyncReadExt;
use tokio::sync::mpsc;

use crate::error::*;
use crate::send::*;
use crate::stream::*;

/// Structure to warn when the sender is dropped before end of stream was reached, like when
/// connection to some remote drops while transmitting data
struct Sender {
	inner: Option<mpsc::UnboundedSender<Packet>>,
}

impl Sender {
	fn new(inner: mpsc::UnboundedSender<Packet>) -> Self {
		Sender { inner: Some(inner) }
	}

	fn send(&self, packet: Packet) {
		let _ = self.inner.as_ref().unwrap().send(packet);
	}

	fn end(&mut self) {
		self.inner = None;
	}
}

impl Drop for Sender {
	fn drop(&mut self) {
		if let Some(inner) = self.inner.take() {
			let _ = inner.send(Err(std::io::Error::new(
				std::io::ErrorKind::BrokenPipe,
				"Netapp connection dropped before end of stream",
			)));
		}
	}
}

/// The RecvLoop trait, which is implemented both by the client and the server
/// connection objects (ServerConn and ClientConn) adds a method `.recv_loop()`
/// and a prototype of a handler for received messages `.recv_handler()` that
/// must be filled by implementors. `.recv_loop()` receives messages in a loop
/// according to the protocol defined above: chunks of message in progress of being
/// received are stored in a buffer, and when the last chunk of a message is received,
/// the full message is passed to the receive handler.
pub(crate) trait RecvLoop: Sync + 'static {
	fn recv_handler(self: &Arc<Self>, id: RequestID, stream: ByteStream);
	fn cancel_handler(self: &Arc<Self>, _id: RequestID) {}

	async fn recv_loop<R>(self: Arc<Self>, mut read: R, debug_name: String) -> Result<(), Error>
	where
		R: AsyncReadExt + Unpin + Send + Sync,
	{
		let mut streams: HashMap<RequestID, Sender> = HashMap::new();
		loop {
			trace!(
				"recv_loop({}): in_progress = {:?}",
				debug_name,
				streams.iter().map(|(id, _)| id).collect::<Vec<_>>()
			);

			let mut header_id = [0u8; RequestID::BITS as usize / 8];
			match read.read_exact(&mut header_id[..]).await {
				Ok(_) => (),
				Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
				Err(e) => return Err(e.into()),
			};
			let id = RequestID::from_be_bytes(header_id);

			let mut header_size = [0u8; ChunkLength::BITS as usize / 8];
			read.read_exact(&mut header_size[..]).await?;
			let size = ChunkLength::from_be_bytes(header_size);

			if size == CANCEL_REQUEST {
				if let Some(mut stream) = streams.remove(&id) {
					let _ = stream.send(Err(std::io::Error::new(
						std::io::ErrorKind::Other,
						"netapp: cancel requested",
					)));
					stream.end();
				}
				self.cancel_handler(id);
				continue;
			}

			let has_cont = (size & CHUNK_FLAG_HAS_CONTINUATION) != 0;
			let is_error = (size & CHUNK_FLAG_ERROR) != 0;
			let size = (size & CHUNK_LENGTH_MASK) as usize;
			let mut next_slice = vec![0; size as usize];
			read.read_exact(&mut next_slice[..]).await?;

			let packet = if is_error {
				let kind = u8_to_io_errorkind(next_slice[0]);
				let msg =
					std::str::from_utf8(&next_slice[1..]).unwrap_or("<invalid utf8 error message>");
				debug!(
					"recv_loop({}): got id {}, error {:?}: {}",
					debug_name, id, kind, msg
				);
				Some(Err(std::io::Error::new(kind, msg.to_string())))
			} else {
				trace!(
					"recv_loop({}): got id {}, size {}, has_cont {}",
					debug_name,
					id,
					size,
					has_cont
				);
				if !next_slice.is_empty() {
					Some(Ok(Bytes::from(next_slice)))
				} else {
					None
				}
			};

			let mut sender = if let Some(send) = streams.remove(&(id)) {
				send
			} else {
				let (send, recv) = mpsc::unbounded_channel();
				trace!("recv_loop({}): id {} is new channel", debug_name, id);
				self.recv_handler(
					id,
					Box::pin(tokio_stream::wrappers::UnboundedReceiverStream::new(recv)),
				);
				Sender::new(send)
			};

			if let Some(packet) = packet {
				// If we cannot put packet in channel, it means that the
				// receiving end of the channel is disconnected.
				// We still need to reach eos before dropping this sender
				let _ = sender.send(packet);
			}

			if has_cont {
				assert!(!is_error);
				streams.insert(id, sender);
			} else {
				trace!("recv_loop({}): close channel id {}", debug_name, id);
				sender.end();
			}
		}
		Ok(())
	}
}
