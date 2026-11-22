use std::collections::HashMap;
use std::pin::pin;
use std::sync::Arc;

use bytes::Bytes;
use log::*;

use tokio::io::AsyncRead;
use tokio::sync::mpsc;

use crate::chunk::*;
use crate::error::*;
use crate::stream::*;
use crate::util::checked_length;

pub(crate) trait Crypto {
	fn noise(self: &Arc<Self>) -> std::sync::MutexGuard<'_, snow::TransportState>;
}

/// The `RecvLoop` trait, which is implemented both by the client and the server
/// connection objects (`ServerConn` and `ClientConn`) adds a method `.recv_loop()`
/// and a prototype of a handler for received messages `.recv_handler()` that
/// must be filled by implementors. `.recv_loop()` receives messages in a loop
/// according to the protocol defined above: chunks of message in progress of being
/// received are stored in a buffer, and when the last chunk of a message is received,
/// the full message is passed to the receive handler.
pub(crate) trait RecvLoop: Crypto + Sync + 'static {
	fn recv_handler(self: &Arc<Self>, id: RequestID, stream: ByteStream);
	fn cancel_handler(self: &Arc<Self>, _id: RequestID) {}

	async fn recv_loop<R>(self: Arc<Self>, mut read: R, debug_name: String) -> Result<(), Error>
	where
		R: AsyncRead + Unpin + Send + Sync,
	{
		let mut wire_buf = new_buffer();
		let mut plain_buf = new_buffer();

		let mut streams: HashMap<RequestID, SenderWithGuard> = HashMap::new();
		loop {
			trace!(
				"recv_loop({}): in_progress = {:?}",
				debug_name,
				streams.keys().collect::<Vec<_>>()
			);

			// Read data from wire and decrypt
			let len_wire = match read_chunk(pin!(&mut read), &mut wire_buf).await {
				Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
				x => x?,
			};
			let mut noise = self.noise();
			let len_plain = noise.read_message(&wire_buf[..len_wire], &mut plain_buf[..])?;
			let mut buf = &plain_buf[..len_plain];

			// Process content of decrypted buffer
			let flags: u8 = *buf.split_off_first().ok_or(Error::Framing)?;

			if flags & FLAG_BYE != 0 {
				break;
			}

			if flags & FLAG_REKEY != 0 {
				noise.rekey_incoming();
			}
			drop(noise);

			let id = buf.split_off(..4).ok_or(Error::Framing)?;
			let id = RequestID::from_be_bytes(checked_length(id).unwrap());

			if flags & FLAG_CANCEL != 0 {
				if let Some(mut stream) = streams.remove(&id) {
					stream.send(Err(std::io::Error::other("netapp: cancel requested")));
					stream.end();
				}
				self.cancel_handler(id);
				continue;
			}

			let has_cont = (flags & FLAG_HASMORE) != 0;
			let is_error = (flags & FLAG_ERROR) != 0;

			let packet = if is_error {
				let kind = u8_to_io_errorkind(*buf.split_off_first().ok_or(Error::Framing)?);
				let msg = std::str::from_utf8(buf).unwrap_or("<invalid utf8 error message>");
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
					buf.len(),
					has_cont
				);
				if !buf.is_empty() {
					Some(Ok(Bytes::from(buf.to_vec())))
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
				SenderWithGuard::new(send)
			};

			if let Some(packet) = packet {
				// If we cannot put packet in channel, it means that the
				// receiving end of the channel is disconnected.
				// We still need to reach eos before dropping this sender
				sender.send(packet);
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

/// Structure to warn when the sender is dropped before end of stream was reached, like when
/// connection to some remote drops while transmitting data
struct SenderWithGuard {
	inner: Option<mpsc::UnboundedSender<Packet>>,
}

impl SenderWithGuard {
	fn new(inner: mpsc::UnboundedSender<Packet>) -> Self {
		SenderWithGuard { inner: Some(inner) }
	}

	fn send(&self, packet: Packet) {
		let _ = self.inner.as_ref().unwrap().send(packet);
	}

	fn end(&mut self) {
		self.inner = None;
	}
}

impl Drop for SenderWithGuard {
	fn drop(&mut self) {
		if let Some(inner) = self.inner.take() {
			let _ = inner.send(Err(std::io::Error::new(
				std::io::ErrorKind::BrokenPipe,
				"Netapp connection dropped before end of stream",
			)));
		}
	}
}
