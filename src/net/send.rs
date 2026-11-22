use std::collections::{HashMap, VecDeque};
use std::pin::{pin, Pin};
use std::sync::Arc;
use std::task::{Context, Poll};

use bytes::{BufMut, Bytes, BytesMut};
use log::*;

use futures::Future;
use tokio::io::AsyncWrite;
use tokio::sync::mpsc;

use crate::chunk::*;
use crate::error::*;
use crate::message::*;
use crate::recv::Crypto;
use crate::stream::*;

const REKEY_EVERY: usize = 16;

pub(crate) enum SendItem {
	Stream(RequestID, RequestPriority, Option<OrderTag>, ByteStream),
	Cancel(RequestID),
}

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
					item.sent += data_frame.bytes.len();

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

struct DataFrame {
	flags: u8,
	bytes: Bytes,
}

impl DataFrame {
	fn from_packet(p: Packet, has_cont: bool) -> Self {
		match p {
			Ok(bytes) => {
				assert!(bytes.len() <= MAX_CHUNK_LENGTH as usize);
				Self {
					bytes,
					flags: if has_cont { FLAG_HASMORE } else { 0 },
				}
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

				Self {
					bytes: buf.freeze(),
					flags: FLAG_ERROR,
				}
			}
		}
	}

	fn serialize(&self, id: RequestID, buf: &mut ChunkBuffer, rekey: bool) -> usize {
		let mut writer = &mut buf[..];

		let flags = writer.split_off_first_mut().unwrap();
		*flags = if rekey {
			self.flags | FLAG_REKEY
		} else {
			self.flags
		};

		let buf_id = writer.split_off_mut(..4).unwrap();
		buf_id.copy_from_slice(&RequestID::to_be_bytes(id));

		writer[..self.bytes.len()].copy_from_slice(&self.bytes);

		self.bytes.len() + 5
	}
}

/// The `SendLoop` trait, which is implemented both by the client and the server
/// connection objects (`ServerConna` and `ClientConn`) adds a method `.send_loop()`
/// that takes a channel of messages to send and an asynchronous writer,
/// and sends messages from the channel to the async writer, putting them in a queue
/// before being sent and doing the round-robin sending strategy.
///
/// The `.send_loop()` exits when the sending end of the channel is closed,
/// or if there is an error at any time writing to the async writer.
pub(crate) trait SendLoop: Sync + Crypto {
	async fn send_loop<W>(
		self: Arc<Self>,
		msg_recv: mpsc::UnboundedReceiver<SendItem>,
		mut write: W,
		debug_name: String,
	) -> Result<(), Error>
	where
		W: AsyncWrite + Unpin + Send + Sync,
	{
		let mut plain_buf = new_buffer();
		let mut wire_buf = new_buffer();

		let mut sending = SendQueue::new();
		let mut msg_recv = Some(msg_recv);

		let mut rekey_counter = REKEY_EVERY;

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
							});
						}
						Some(SendItem::Cancel(id)) => {
							trace!("send_loop({}): cancelling {}", debug_name, id);

							sending.remove(id);

							plain_buf[0] = FLAG_CANCEL;
							plain_buf[1..5].copy_from_slice(&RequestID::to_be_bytes(id));

							let len_wire = {
								let mut noise = self.noise();
								noise.write_message(&plain_buf[..5], &mut wire_buf[..])?
							};

							write_chunk(pin!(&mut write), &wire_buf[..len_wire]).await?;
						}
						None => {
							msg_recv = None;
						}
					}
				}
				(id, data) = send_fut => {
					let rekey = if rekey_counter == 0 {
						rekey_counter = REKEY_EVERY;
						true
					} else {
						rekey_counter -= 1;
						false
					};

					let len_plain = data.serialize(id, &mut plain_buf, rekey);

					trace!(
						"send_loop({}): id {}, {} bytes",
						debug_name,
						id,
						len_plain,
					);

					let len_wire = {
						let mut noise = self.noise();
						let len = noise.write_message(&plain_buf[..len_plain], &mut wire_buf[..])?;
						if rekey {
							noise.rekey_outgoing();
						}
						len
					};

					write_chunk(pin!(&mut write), &wire_buf[..len_wire]).await?;
				}
			}
		}

		// Write Bye message to properly close connection
		let len_wire = {
			let mut noise = self.noise();
			noise.write_message(&[FLAG_BYE], &mut wire_buf[..])?
		};

		let _ = write_chunk(pin!(&mut write), &wire_buf[..len_wire]).await;

		Ok(())
	}
}
