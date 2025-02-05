use std::collections::{HashMap, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes, BytesMut};
use log::*;

use futures::{AsyncWriteExt, Future};
use kuska_handshake::async_std::BoxStreamWrite;
use tokio::sync::mpsc;

use crate::error::*;
use crate::message::*;
use crate::stream::*;

// Messages are sent by chunks
// Chunk format:
// - u32 BE: request id (same for request and response)
// - u16 BE: chunk length + flags:
//		CHUNK_FLAG_HAS_CONTINUATION when this is not the last chunk of the stream
//		CHUNK_FLAG_ERROR if this chunk denotes an error
//		(these two flags are exclusive, an error denotes the end of the stream)
//		**special value** 0xFFFF indicates a CANCEL message
// - [u8; chunk_length], either
//   - if not error: chunk data
//   - if error:
//       - u8: error kind, encoded using error::io_errorkind_to_u8
//       - rest: error message
//   - absent for cancel message

pub(crate) type RequestID = u32;
pub(crate) type ChunkLength = u16;

pub(crate) const MAX_CHUNK_LENGTH: ChunkLength = 0x3FF0;
pub(crate) const CHUNK_FLAG_ERROR: ChunkLength = 0x4000;
pub(crate) const CHUNK_FLAG_HAS_CONTINUATION: ChunkLength = 0x8000;
pub(crate) const CHUNK_LENGTH_MASK: ChunkLength = 0x3FFF;
pub(crate) const CANCEL_REQUEST: ChunkLength = 0xFFFF;

pub(crate) enum SendItem {
	Stream(RequestID, RequestPriority, Option<OrderTag>, ByteStream),
	Cancel(RequestID),
}

// ----

struct SendQueue {
	items: Vec<(u8, SendQueuePriority)>,
}

struct SendQueuePriority {
	items: VecDeque<SendQueueItem>,
	order: HashMap<u64, VecDeque<u64>>,
}

struct SendQueueItem {
	id: RequestID,
	prio: RequestPriority,
	order_tag: Option<OrderTag>,
	data: ByteStreamReader,
	sent: usize,
}

impl SendQueue {
	fn new() -> Self {
		Self {
			items: Vec::with_capacity(64),
		}
	}
	fn push(&mut self, item: SendQueueItem) {
		let prio = item.prio;
		let pos_prio = match self.items.binary_search_by(|(p, _)| p.cmp(&prio)) {
			Ok(i) => i,
			Err(i) => {
				self.items.insert(i, (prio, SendQueuePriority::new()));
				i
			}
		};
		self.items[pos_prio].1.push(item);
	}
	fn remove(&mut self, id: RequestID) {
		for (_, prioq) in self.items.iter_mut() {
			prioq.remove(id);
		}
		self.items.retain(|(_prio, q)| !q.is_empty());
	}
	fn is_empty(&self) -> bool {
		self.items.iter().all(|(_k, v)| v.is_empty())
	}

	// this is like an async fn, but hand implemented
	fn next_ready(&mut self) -> SendQueuePollNextReady<'_> {
		SendQueuePollNextReady { queue: self }
	}
}

impl SendQueuePriority {
	fn new() -> Self {
		Self {
			items: VecDeque::new(),
			order: HashMap::new(),
		}
	}
	fn push(&mut self, item: SendQueueItem) {
		if let Some(OrderTag(stream, order)) = item.order_tag {
			let order_vec = self.order.entry(stream).or_default();
			let i = order_vec.iter().take_while(|o2| **o2 < order).count();
			order_vec.insert(i, order);
		}
		self.items.push_back(item);
	}
	fn remove(&mut self, id: RequestID) {
		if let Some(i) = self.items.iter().position(|x| x.id == id) {
			let item = self.items.remove(i).unwrap();
			if let Some(OrderTag(stream, order)) = item.order_tag {
				let order_vec = self.order.get_mut(&stream).unwrap();
				let j = order_vec.iter().position(|x| *x == order).unwrap();
				order_vec.remove(j).unwrap();
				if order_vec.is_empty() {
					self.order.remove(&stream);
				}
			}
		}
	}
	fn is_empty(&self) -> bool {
		self.items.is_empty()
	}
	fn poll_next_ready(&mut self, ctx: &mut Context<'_>) -> Poll<(RequestID, DataFrame)> {
		// in step 1: poll only streams that have sent 0 bytes, we want to send them in priority
		//            as they most likely represent small requests to be sent first
		// in step 2: poll all streams
		for step in 0..2 {
			for (j, item) in self.items.iter_mut().enumerate() {
				if let Some(OrderTag(stream, order)) = item.order_tag {
					if order > *self.order.get(&stream).unwrap().front().unwrap() {
						continue;
					}
				}

				if step == 0 && item.sent > 0 {
					continue;
				}

				let mut item_reader = item.data.read_exact_or_eos(MAX_CHUNK_LENGTH as usize);
				if let Poll::Ready(bytes_or_err) = Pin::new(&mut item_reader).poll(ctx) {
					let id = item.id;
					let eos = item.data.eos();

					let packet = bytes_or_err.map_err(|e| match e {
						ReadExactError::Stream(err) => err,
						_ => unreachable!(),
					});

					let is_err = packet.is_err();
					let data_frame = DataFrame::from_packet(packet, !eos);
					item.sent += data_frame.data().len();

					if eos || is_err {
						// If item had an order tag, remove it from the corresponding ordering list
						if let Some(OrderTag(stream, order)) = item.order_tag {
							let order_stream = self.order.get_mut(&stream).unwrap();
							assert_eq!(order_stream.pop_front(), Some(order));
							if order_stream.is_empty() {
								self.order.remove(&stream);
							}
						}
						// Remove item from sending queue
						self.items.remove(j);
					} else if step == 0 {
						// Step 0 means that this stream had not sent any bytes yet.
						// Now that it has, and it was not an EOS, we know that it is bigger
						// than one chunk so move it at the end of the queue.
						let item = self.items.remove(j).unwrap();
						self.items.push_back(item);
					}

					return Poll::Ready((id, data_frame));
				}
			}
		}

		Poll::Pending
	}
	fn dump(&self, prio: u8) -> String {
		self.items
			.iter()
			.map(|i| format!("[{} {} {:?} @{}]", prio, i.id, i.order_tag, i.sent))
			.collect::<Vec<_>>()
			.join(" ")
	}
}

struct SendQueuePollNextReady<'a> {
	queue: &'a mut SendQueue,
}

impl<'a> futures::Future for SendQueuePollNextReady<'a> {
	type Output = (RequestID, DataFrame);

	fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
		for (i, (_prio, items_at_prio)) in self.queue.items.iter_mut().enumerate() {
			if let Poll::Ready(res) = items_at_prio.poll_next_ready(ctx) {
				if items_at_prio.is_empty() {
					self.queue.items.remove(i);
				}
				return Poll::Ready(res);
			}
		}
		// If the queue is empty, this futures is eternally pending.
		// This is ok because we use it in a select with another future
		// that can interrupt it.
		Poll::Pending
	}
}

enum DataFrame {
	/// a fixed size buffer containing some data + a boolean indicating whether
	/// there may be more data coming from this stream. Can be used for some
	/// optimization. It's an error to set it to false if there is more data, but it is correct
	/// (albeit sub-optimal) to set it to true if there is nothing coming after
	Data(Bytes, bool),
	/// An error code automatically signals the end of the stream
	Error(Bytes),
}

impl DataFrame {
	fn from_packet(p: Packet, has_cont: bool) -> Self {
		match p {
			Ok(bytes) => {
				assert!(bytes.len() <= MAX_CHUNK_LENGTH as usize);
				Self::Data(bytes, has_cont)
			}
			Err(e) => {
				let mut buf = BytesMut::new();
				buf.put_u8(io_errorkind_to_u8(e.kind()));

				let msg = format!("{}", e).into_bytes();
				if msg.len() > (MAX_CHUNK_LENGTH - 1) as usize {
					buf.put(&msg[..(MAX_CHUNK_LENGTH - 1) as usize]);
				} else {
					buf.put(&msg[..]);
				}

				Self::Error(buf.freeze())
			}
		}
	}

	fn header(&self) -> [u8; 2] {
		let header_u16 = match self {
			DataFrame::Data(data, false) => data.len() as u16,
			DataFrame::Data(data, true) => data.len() as u16 | CHUNK_FLAG_HAS_CONTINUATION,
			DataFrame::Error(msg) => msg.len() as u16 | CHUNK_FLAG_ERROR,
		};
		ChunkLength::to_be_bytes(header_u16)
	}

	fn data(&self) -> &[u8] {
		match self {
			DataFrame::Data(ref data, _) => &data[..],
			DataFrame::Error(ref msg) => &msg[..],
		}
	}
}

/// The SendLoop trait, which is implemented both by the client and the server
/// connection objects (ServerConna and ClientConn) adds a method `.send_loop()`
/// that takes a channel of messages to send and an asynchronous writer,
/// and sends messages from the channel to the async writer, putting them in a queue
/// before being sent and doing the round-robin sending strategy.
///
/// The `.send_loop()` exits when the sending end of the channel is closed,
/// or if there is an error at any time writing to the async writer.
pub(crate) trait SendLoop: Sync {
	async fn send_loop<W>(
		self: Arc<Self>,
		msg_recv: mpsc::UnboundedReceiver<SendItem>,
		mut write: BoxStreamWrite<W>,
		debug_name: String,
	) -> Result<(), Error>
	where
		W: AsyncWriteExt + Unpin + Send + Sync,
	{
		let mut sending = SendQueue::new();
		let mut msg_recv = Some(msg_recv);
		while msg_recv.is_some() || !sending.is_empty() {
			trace!(
				"send_loop({}): queue = {:?}",
				debug_name,
				sending
					.items
					.iter()
					.map(|(prio, i)| i.dump(*prio))
					.collect::<Vec<_>>()
					.join(" ; ")
			);

			let recv_fut = async {
				if let Some(chan) = &mut msg_recv {
					chan.recv().await
				} else {
					futures::future::pending().await
				}
			};
			let send_fut = sending.next_ready();

			// recv_fut is cancellation-safe according to tokio doc,
			// send_fut is cancellation-safe as implemented above?
			tokio::select! {
				biased;	// always read incoming channel first if it has data
				sth = recv_fut => {
					match sth {
						Some(SendItem::Stream(id, prio, order_tag, data)) => {
							trace!("send_loop({}): add stream {} to send", debug_name, id);
							sending.push(SendQueueItem {
								id,
								prio,
								order_tag,
								data: ByteStreamReader::new(data),
								sent: 0,
							})
						}
						Some(SendItem::Cancel(id)) => {
							trace!("send_loop({}): cancelling {}", debug_name, id);
							sending.remove(id);
							let header_id = RequestID::to_be_bytes(id);
							write.write_all(&header_id[..]).await?;
							write.write_all(&ChunkLength::to_be_bytes(CANCEL_REQUEST)).await?;
							write.flush().await?;
						}
						None => {
							msg_recv = None;
						}
					};
				}
				(id, data) = send_fut => {
					trace!(
						"send_loop({}): id {}, send {} bytes, header_size {}",
						debug_name,
						id,
						data.data().len(),
						hex::encode(data.header())
					);

					let header_id = RequestID::to_be_bytes(id);
					write.write_all(&header_id[..]).await?;

					write.write_all(&data.header()).await?;
					write.write_all(data.data()).await?;
					write.flush().await?;
				}
			}
		}

		let _ = write.goodbye().await;
		Ok(())
	}
}
